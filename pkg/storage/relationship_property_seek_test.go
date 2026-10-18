package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipEqualitySeekRestartAndMaintenance(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	ns := NewNamespacedEngine(engine, "test")
	_, err = ns.CreateNode(&Node{ID: "a"})
	require.NoError(t, err)
	_, err = ns.CreateNode(&Node{ID: "b"})
	require.NoError(t, err)
	require.NoError(t, ns.GetSchema().AddRangeIndexForEntity("rel_key", "KEYED", []string{"key"}, ConstraintEntityRelationship))
	edge := &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "KEYED", Properties: map[string]interface{}{"key": "old"}}
	require.NoError(t, ns.CreateEdge(edge))
	seek := func(value interface{}) []*Edge {
		t.Helper()
		edges, answered, err := ns.SeekRelationships(context.Background(), "KEYED", []string{"key"}, []interface{}{value})
		require.NoError(t, err)
		require.True(t, answered)
		return edges
	}
	require.Len(t, seek("old"), 1)
	edge.Properties["key"] = "new"
	require.NoError(t, ns.UpdateEdge(edge))
	// A delayed older publication cannot restore the old value in the index.
	engine.updateRelationshipUniqueIndexes(&Edge{ID: "test:e", Type: "KEYED", Properties: map[string]interface{}{"key": "old"}})
	require.Empty(t, seek("old"))
	require.Len(t, seek("new"), 1)
	require.NoError(t, engine.Close())
	engine, err = NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns = NewNamespacedEngine(engine, "test")
	require.Len(t, ns.GetSchema().RelationshipEqualityIndexes("KEYED"), 1)
	require.Len(t, seek("new"), 1)
	require.NoError(t, ns.DeleteEdge("e"))
	require.Empty(t, seek("new"))
}

func TestRelationshipEqualitySeekConcurrentWrites(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := NewNamespacedEngine(engine, "test")
	_, err = ns.CreateNode(&Node{ID: "a"})
	require.NoError(t, err)
	_, err = ns.CreateNode(&Node{ID: "b"})
	require.NoError(t, err)
	seek := func() ([]*Edge, bool, error) {
		return ns.SeekRelationships(context.Background(), "KEYED", []string{"key"}, []interface{}{"shared"})
	}
	edges, answered, err := seek()
	require.NoError(t, err)
	require.True(t, answered)
	require.Empty(t, edges)
	var writers sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		writers.Add(1)
		go func(i int) {
			defer writers.Done()
			if err := ns.CreateEdge(&Edge{ID: EdgeID(fmt.Sprint(i)), StartNode: "a", EndNode: "b", Type: "KEYED", Properties: map[string]interface{}{"key": "shared"}}); err != nil {
				errors <- err
				return
			}
			_, _, err := seek()
			errors <- err
		}(i)
	}
	writers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	edges, answered, err = seek()
	require.NoError(t, err)
	require.True(t, answered)
	require.Len(t, edges, 8)
}

func TestRelationshipEqualitySeekSnapshotAndPendingWrites(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := NewNamespacedEngine(engine, "test")
	_, err = ns.CreateNode(&Node{ID: "a"})
	require.NoError(t, err)
	_, err = ns.CreateNode(&Node{ID: "b"})
	require.NoError(t, err)
	edge := &Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "KEYED", Properties: map[string]interface{}{"key": "old"}}
	require.NoError(t, ns.CreateEdge(edge))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()
	edge.Properties["key"] = "peer"
	require.NoError(t, ns.UpdateEdge(edge))
	seek := func(value interface{}) []*Edge {
		t.Helper()
		edges, answered, err := tx.SeekRelationships(context.Background(), "KEYED", []string{"key"}, []interface{}{value})
		require.NoError(t, err)
		require.True(t, answered)
		return edges
	}
	require.Len(t, seek("old"), 1)
	require.Empty(t, seek("peer"))
	pending := &Edge{ID: "test:pending", StartNode: "test:a", EndNode: "test:b", Type: "KEYED", Properties: map[string]interface{}{"key": "mine"}}
	require.NoError(t, tx.CreateEdge(pending))
	require.Len(t, seek("mine"), 1)
	pending.Properties["key"] = "changed"
	require.NoError(t, tx.UpdateEdge(pending))
	require.Empty(t, seek("mine"))
	require.Len(t, seek("changed"), 1)
	require.NoError(t, tx.DeleteEdge(pending.ID))
	require.Empty(t, seek("changed"))
	require.NoError(t, ns.DeleteEdge("e"))
	require.Len(t, seek("old"), 1, "peer delete must not remove a pinned match")
}
