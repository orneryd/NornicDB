package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type projectedPointReadSpy struct {
	*storagefault.QueryNodeReadSpy
	projections [][]string
}

func TestProjectedReadBatchIndexedProjection(t *testing.T) {
	engine := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(engine, "batch")
	seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := t.Context()
	_, err := seed.Execute(ctx, `CREATE CONSTRAINT batch_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE`, nil)
	require.NoError(t, err)
	keys := make([]any, 16)
	for i := range keys {
		keys[i] = fmt.Sprintf("doc-%02d", i)
		_, err = seed.Execute(ctx, `CREATE (:Doc {key: $key, path: $path, status: 'current', body: $body})`, map[string]any{
			"key": keys[i], "path": fmt.Sprintf("/docs/%02d", i), "body": strings.Repeat("large omitted body ", 256),
		})
		require.NoError(t, err)
	}
	spy := &projectedPointReadSpy{QueryNodeReadSpy: storagefault.NewQueryNodeReadSpy(store)}
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
	result, err := exec.Execute(ctx, `UNWIND $keys AS key MATCH (d:Doc {key: key}) RETURN d.key AS key, d.path AS path, d.status AS status`, map[string]any{"keys": keys})
	require.NoError(t, err)
	require.Len(t, result.Rows, 16)
	for i, row := range result.Rows {
		require.Equal(t, []any{keys[i], fmt.Sprintf("/docs/%02d", i), "current"}, row)
	}
	require.Len(t, spy.projections, 16)
	for _, projection := range spy.projections {
		require.ElementsMatch(t, []string{"key", "path", "status"}, projection)
	}
	require.Zero(t, spy.Query.Load())
	require.Zero(t, spy.Full.Load())
}

func (s *projectedPointReadSpy) GetNodeProjected(ctx context.Context, id storage.NodeID, properties []string) (*storage.Node, error) {
	s.projections = append(s.projections, append([]string(nil), properties...))
	return s.Engine.(storage.ProjectedNodeReader).GetNodeProjected(ctx, id, properties)
}

func TestProjectedReadIndexedPointProjection(t *testing.T) {
	engine := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(engine, "point")
	seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	ctx := t.Context()
	_, err := seed.Execute(ctx, `CREATE CONSTRAINT point_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE`, nil)
	require.NoError(t, err)
	body := strings.Repeat("large unrequested document body ", 512)
	_, err = seed.Execute(ctx, `CREATE (:Doc {key: 'one', path: 'guide', status: 'current', extra: 7, body: $body, noise: $noise})`, map[string]any{"body": body, "noise": []byte{1, 2, 3}})
	require.NoError(t, err)
	spy := &projectedPointReadSpy{QueryNodeReadSpy: storagefault.NewQueryNodeReadSpy(store)}
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
	for _, test := range []struct {
		query  string
		want   [][]any
		fields []string
	}{
		{`MATCH (n:Doc {key: $key}) RETURN n.key AS key,n.path AS path,n.status AS status`, [][]any{{"one", "guide", "current"}}, []string{"key", "path", "status"}},
		{`MATCH (n:Doc {key: $key}) RETURN n.path,n.status`, [][]any{{"guide", "current"}}, []string{"key", "path", "status"}},
		{`MATCH (n:Doc) WHERE n.key=$key AND n.extra=7 RETURN n.path`, [][]any{{"guide"}}, []string{"key", "extra", "path"}},
	} {
		spy.projections = nil
		spy.Query.Store(0)
		spy.Full.Store(0)
		result, err := exec.Execute(ctx, test.query, map[string]any{"key": "one"})
		require.NoError(t, err)
		require.Equal(t, test.want, result.Rows)
		require.NotEmpty(t, spy.projections, "indexed property-only reads reach the projection capability")
		require.ElementsMatch(t, test.fields, spy.projections[0])
		require.Zero(t, spy.Query.Load())
		require.Zero(t, spy.Full.Load())
	}
	spy.projections = nil
	result, err := exec.Execute(ctx, `MATCH (n:Doc {key: $key}) RETURN n`, map[string]any{"key": "one"})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Empty(t, spy.projections, "whole entities retain the full read contract")
	_, err = seed.Execute(ctx, `MATCH (n:Doc {key: $key}) SET n.status='updated' RETURN n.path`, map[string]any{"key": "one"})
	require.NoError(t, err)
	rows, err := seed.Execute(ctx, `MATCH (n:Doc {key: $key}) RETURN n.body,n.extra,n.status`, map[string]any{"key": "one"})
	require.NoError(t, err)
	require.Equal(t, [][]any{{body, int64(7), "updated"}}, rows.Rows)
}

// The production database wrapper must expose the same read as a bare engine.
// Assert the real managed capability before observing the query route; an
// outer spy alone would hide a missing capability on the database wrapper.
func TestProjectedReadPointProjectionThroughManagedDatabase(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	require.Implements(t, (*storage.ProjectedNodeReader)(nil), store)
	spy := &projectedPointReadSpy{QueryNodeReadSpy: storagefault.NewQueryNodeReadSpy(store)}
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	_, err = exec.Execute(t.Context(), `CREATE CONSTRAINT managed_doc_key IF NOT EXISTS FOR (d:Doc) REQUIRE d.key IS UNIQUE`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(t.Context(), `CREATE (:Doc {key:'one',path:'guide',status:'current',body:$body})`, map[string]any{"body": strings.Repeat("large omitted body ", 512)})
	require.NoError(t, err)
	exec = NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
	spy.projections = nil
	spy.Query.Store(0)
	spy.Full.Store(0)
	ctx := t.Context()
	run := executeStreamed(ctx, exec, `MATCH (d:Doc {key:'one'}) RETURN d.key AS key,d.path AS path,d.status AS status`, 1)
	rows, _ := run.stream.Next(-1)
	<-run.stream.Done()
	require.NoError(t, run.err)
	if len(rows) == 0 {
		rows = run.result.Rows
	}
	require.Equal(t, [][]any{{"one", "guide", "current"}}, rows)
	require.NotEmpty(t, spy.projections, "the streamed managed read must reach physical projection")
	require.ElementsMatch(t, []string{"key", "path", "status"}, spy.projections[0])
	require.Zero(t, spy.Query.Load())
	require.Zero(t, spy.Full.Load())
}

func TestProjectedReadRepeatedMatchVariable(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		ctx := t.Context()
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "repeated")
		seed := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		for _, setup := range []string{
			"CREATE CONSTRAINT repeated_doc_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE",
			"CREATE (:Doc {key:'one', extra:7}), (:Doc {key:'two', extra:8})",
		} {
			_, err := seed.Execute(ctx, setup, nil)
			require.NoError(t, err)
		}

		spy := &projectedPointReadSpy{QueryNodeReadSpy: storagefault.NewQueryNodeReadSpy(store)}
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
		rows, err := exec.Execute(ctx,
			"UNWIND $keys AS key MATCH (n:Doc {key:key}), (n:Doc {extra:7}) RETURN n.key",
			map[string]any{"keys": []any{"one", "two"}})
		require.NoError(t, err)
		require.Equal(t, [][]any{{"one"}}, rows.Rows)
		require.Len(t, spy.projections, 2)
		for _, fields := range spy.projections {
			require.ElementsMatch(t, []string{"key", "extra"}, fields)
		}
	})
}
