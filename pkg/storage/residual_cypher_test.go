package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func residualExecutor(b *storage.BadgerEngine) *cypher.StorageExecutor {
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(b, "test"), cypher.QueryCachePolicy{MaxEntries: 0})
}

func TestResidualCypherMergeAdmissionCancellation(t *testing.T) {
	b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	require.NoError(t, b.GetSchemaForNamespace("test").AddUniqueConstraint("merge-context", "MergeContext", "id"))
	release, waiting := b.ResidualHoldWriteBarrierForTest()
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := residualExecutor(b).Execute(ctx, "MERGE (:MergeContext {id:'cancelled'})", nil)
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		release()
		cancel()
		<-done
		t.Fatal("MERGE did not reach its actual write admission")
	}
	cancel()
	returned := false
	var queryErr error
	select {
	case queryErr = <-done:
		returned = true
	case <-time.After(2 * time.Second):
	}
	release()
	if !returned {
		queryErr = <-done
	}
	require.True(t, returned, "cancelled MERGE must return before the peer releases admission")
	require.ErrorIs(t, queryErr, context.Canceled)
	result, err := residualExecutor(b).Execute(context.Background(), "MATCH (n:MergeContext) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(0)}}, result.Rows)
}

func TestResidualCypherDurableBody(t *testing.T) {
	b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	writer, reader := residualExecutor(b), residualExecutor(b)
	ctx := context.Background()
	_, err = writer.Execute(ctx, "CREATE (:Owned {id:'durable', v:1})", nil)
	require.NoError(t, err)
	read := func() (*cypher.ExecuteResult, error) {
		return reader.Execute(ctx, "MATCH (n:Owned {id:'durable'}) RETURN n.v", nil)
	}
	_, err = read()
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	t.Cleanup(storage.ResidualPublicationStageForTest(func() { close(entered); <-release }))
	done := make(chan error, 1)
	go func() {
		_, err := writer.Execute(ctx, "MATCH (n:Owned {id:'durable'}) SET n.v=2", nil)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach its durable publication boundary")
	}
	result, readErr := read()
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, readErr)
	require.Equal(t, [][]any{{int64(2)}}, result.Rows)
}

func TestResidualCypherFinalRelationshipImage(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			writer, reader := residualExecutor(b), residualExecutor(b)
			ctx := context.Background()
			run := func(query string) {
				_, err := writer.Execute(ctx, query, nil)
				require.NoError(t, err)
			}
			run("CREATE (:Owned {id:'a'})-[:OWNED {p:0}]->(:Owned {id:'b'})")
			require.NoError(t, b.FlushWriteBehind(nil))
			const read = "MATCH (:Owned {id:'a'})-[r:OWNED]->() RETURN r.p"
			_, err = reader.Execute(ctx, read, nil)
			require.NoError(t, err)
			if !buffered {
				run("BEGIN")
			}
			observed := false
			var observationErr error
			t.Cleanup(storage.ResidualMiddleOperationForTest(func() {
				observed = true
				result, err := reader.Execute(ctx, read, nil)
				if err != nil {
					observationErr = err
					return
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(2) {
					observationErr = fmt.Errorf("intermediate relationship image: %v", result.Rows)
				}
			}))
			run("MATCH (a:Owned {id:'a'})-[r:OWNED]->() SET r.p=1 WITH a,r SET a.middle_marker=true WITH r SET r.p=2")
			if !buffered {
				run("COMMIT")
			}
			require.NoError(t, b.FlushWriteBehind(nil))
			require.True(t, observed)
			require.NoError(t, observationErr)
			result, err := reader.Execute(ctx, read, nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{int64(2)}}, result.Rows)
		})
	}
}

func TestResidualPrefixDropDurableFence(t *testing.T) {
	b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	for _, id := range []storage.NodeID{"victim:a", "victim:b"} {
		_, err = b.CreateNode(&storage.Node{ID: id, Labels: []string{"Owned"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&storage.Edge{ID: "victim:e", StartNode: "victim:a", EndNode: "victim:b", Type: "OWNED"}))
	reader := cypher.NewStorageExecutor(storage.NewNamespacedEngine(b, "victim"), cypher.QueryCachePolicy{MaxEntries: 0})
	const query = "MATCH (n:Owned) RETURN n.v"
	_, err = reader.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	_, err = b.GetStoredNode("victim:a")
	require.NoError(t, err)
	_, err = b.GetStoredEdge("victim:e")
	require.NoError(t, err)
	observed, nodeCached, edgeCached := false, false, false
	var duringReadErr error
	t.Cleanup(b.ResidualPrefixTailForTest("victim:a", "victim:e", func(node, edge bool) {
		observed, nodeCached, edgeCached = true, node, edge
		_, duringReadErr = reader.Execute(context.Background(), query, nil)
	}))
	nodes, edges, err := b.DeleteByPrefix("victim:")
	require.NoError(t, err)
	require.EqualValues(t, 2, nodes)
	require.EqualValues(t, 1, edges)
	require.True(t, observed)
	require.False(t, nodeCached, "node cache must be revoked at the physical prefix action, before cleanup")
	require.False(t, edgeCached, "relationship cache must be revoked at the physical prefix action, before cleanup")
	require.Error(t, duringReadErr, "the release lifecycle owner still excludes reads during pending DROP")
	result, err := reader.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestResidualCypherBeginCarriesCancellationToFlush(t *testing.T) {
	b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	executor := cypher.NewStorageExecutor(b, cypher.QueryCachePolicy{MaxEntries: 0})
	release, waiting := b.ResidualHoldWriteBarrierForTest()
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, joined := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() {
		release()
		cancel()
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("BEGIN worker did not join")
			return
		}
		_, _ = executor.Execute(context.Background(), "ROLLBACK", nil)
	})
	go func() {
		defer close(joined)
		_, err := executor.Execute(ctx, "BEGIN", nil)
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("BEGIN did not reach actual storage admission")
	}
	cancel()
	returned := false
	var beginErr error
	select {
	case beginErr = <-done:
		returned = true
	case <-time.After(2 * time.Second):
	}
	release()
	if !returned {
		select {
		case beginErr = <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("BEGIN did not finish after admission was released")
		}
	}
	require.True(t, returned, "cancelled BEGIN must leave admission before the peer releases it")
	require.ErrorIs(t, beginErr, context.Canceled)
}
