package storage_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type relationshipSeekReleaseContext struct {
	context.Context
	once    sync.Once
	release func()
}

func (ctx *relationshipSeekReleaseContext) Err() error {
	ctx.once.Do(ctx.release)
	return ctx.Context.Err()
}

type relationshipSeekReleaseStore struct {
	storage.Engine
	release func()
}

func (s *relationshipSeekReleaseStore) RelationshipPropertySeeksAvailable() bool {
	return s.Engine.(storage.RelationshipPropertySeeker).RelationshipPropertySeeksAvailable()
}
func (s *relationshipSeekReleaseStore) SeekRelationships(ctx context.Context, typ string, properties []string, values []interface{}) ([]*storage.Edge, bool, error) {
	return s.Engine.(storage.RelationshipPropertySeeker).SeekRelationships(&relationshipSeekReleaseContext{Context: ctx, release: s.release}, typ, properties, values)
}

// The peer atomically moves the one matching value A -> B, then pauses before
// derived-index publication. The query releases that publication at its first
// storage context check. It must see B, rather than old IDs with new bodies.
// Channels and the existing commit-tail hook control the race; no timing bound
// decides correctness, and every goroutine is joined even after an assertion.
func TestRelationshipSeekCypherAtomicPublication(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := storage.NewNamespacedEngine(engine, "test")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := ns.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	for i, value := range []string{"x", "y"} {
		require.NoError(t, ns.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(i)), StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": value, "name": fmt.Sprint(i)}}))
	}
	exec := cypher.NewStorageExecutor(ns)
	_, err = exec.Execute(context.Background(), "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", nil)
	require.NoError(t, err)
	const query = "PROFILE MATCH ()-[r:T]->() WHERE r.p='x' RETURN r.name"
	warm, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"0"}}, warm.Rows)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for i, value := range []string{"y", "x"} {
		edge, err := tx.GetEdge(storage.EdgeID(fmt.Sprintf("test:%d", i)))
		require.NoError(t, err)
		edge.Properties["p"] = value
		require.NoError(t, tx.UpdateEdge(edge))
	}
	physical := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePublication := func() { releaseOnce.Do(func() { close(release) }) }
	restore := storage.RelationshipSeekTestCommitTail(func() { close(physical); <-release })
	defer restore()
	done := make(chan error, 1)
	go func() { done <- tx.Commit() }()
	defer func() { releasePublication(); require.NoError(t, <-done) }()
	select {
	case <-physical:
	case <-time.After(10 * time.Second):
		t.Fatal("peer did not reach controlled publication point")
	}
	exec = cypher.NewStorageExecutor(&relationshipSeekReleaseStore{Engine: ns, release: releasePublication})
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"1"}}, result.Rows, "one atomic committed state always contains exactly one match")
	plan := result.Metadata["plan"].(*cypher.ExecutionPlan)
	var seeks int
	var inspect func(*cypher.PlanOperator)
	inspect = func(op *cypher.PlanOperator) {
		if op.OperatorType == "RelationshipIndexSeek" {
			seeks++
		}
		for _, child := range op.Children {
			inspect(child)
		}
	}
	inspect(plan.Root)
	require.Positive(t, seeks)
}

// The closed sessions remain reachable. Eight distinct physical snapshots
// must not leave graph-sized equality maps in the engine. The retained-heap
// bound is measured after GC; fixture/index population is outside the sample.
func TestRelationshipSeekCypherClosedSnapshotMemory(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := storage.NewNamespacedEngine(engine, "test")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := ns.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	for i := 0; i < 8192; i++ {
		require.NoError(t, ns.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(i)), StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": fmt.Sprint(i)}}))
	}
	exec := cypher.NewStorageExecutor(ns)
	_, err = exec.Execute(context.Background(), "CREATE RANGE INDEX rel_p FOR ()-[r:T]-() ON (r.p)", nil)
	require.NoError(t, err)
	const query = "PROFILE MATCH ()-[r:T]->() WHERE r.p='0' RETURN r.p"
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	warmEntries := storage.RelationshipSeekTestEngineIndexCount(engine)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var sessions []*cypher.StorageExecutor
	for i := 0; i < 8; i++ {
		edge, err := ns.GetEdge("8191")
		require.NoError(t, err)
		edge.Properties["p"] = fmt.Sprintf("tick-%d", i)
		require.NoError(t, ns.UpdateEdge(edge))
		session := cypher.NewStorageExecutor(ns)
		sessions = append(sessions, session)
		_, err = session.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		result, err := session.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"0"}}, result.Rows)
		end := "COMMIT"
		if i%2 != 0 {
			end = "ROLLBACK"
		}
		_, err = session.Execute(context.Background(), end, nil)
		require.NoError(t, err)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(sessions)
	require.LessOrEqual(t, storage.RelationshipSeekTestEngineIndexCount(engine), warmEntries, "closed transactions must not populate the engine-owned cache")
	require.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(4<<20), "closed snapshots retained graph-sized indexes")
}
