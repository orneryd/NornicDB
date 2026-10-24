package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PrefixStatsAreO1AndCorrect(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "db1:n1"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:n2"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db2:n1"})
	require.NoError(t, err)

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	count, err = engine.NodeCountByPrefix("db2:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	err = engine.DeleteNode("db1:n1")
	require.NoError(t, err)

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "db1:n3"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	count, err = engine.NodeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestBadgerEngine_EdgePrefixStatsAreCorrect_TransactionalAndNonTransactional(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "db1:a"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "db1:b"})
	require.NoError(t, err)

	err = engine.CreateEdge(&Edge{
		ID:        "db1:e1",
		StartNode: "db1:a",
		EndNode:   "db1:b",
		Type:      "R",
	})
	require.NoError(t, err)

	count, err := engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        "db1:e2",
		StartNode: "db1:a",
		EndNode:   "db1:b",
		Type:      "R",
	}))
	require.NoError(t, tx.Commit())

	count, err = engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"db1:e1", "db1:e2"}))

	count, err = engine.EdgeCountByPrefix("db1:")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestBadgerEngine_EdgeCountByPrefix_ClosedEngine(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	_, err = engine.EdgeCountByPrefix("db1:")
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerEngine_NodeCountByPrefix_ClosedEngine(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	_, err = engine.NodeCountByPrefix("db1:")
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerEngine_IterateNodes_EarlyStop(t *testing.T) {
	engine := createTestBadgerEngine(t)

	for i := 0; i < 5; i++ {
		_, err := engine.CreateNode(&Node{
			ID:         NodeID(prefixTestID(fmt.Sprintf("iter-n%d", i))),
			Labels:     []string{"Item"},
			Properties: map[string]interface{}{},
		})
		require.NoError(t, err)
	}

	var count int
	err := engine.IterateNodes(func(n *Node) bool {
		count++
		return count < 3
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestBadgerEngine_IterateNodes_ClosedEngine(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	err = engine.IterateNodes(func(n *Node) bool { return true })
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestBadgerEngine_EdgeCountByPrefix_UncachedScan(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create edges in two different namespaces
	n1 := &Node{ID: "ns1:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	n2 := &Node{ID: "ns1:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}}
	n3 := &Node{ID: "ns2:n3", Labels: []string{"A"}, Properties: map[string]interface{}{}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	require.NoError(t, engine.CreateEdge(&Edge{ID: "ns1:e1", StartNode: "ns1:n1", EndNode: "ns1:n2", Type: "R", Properties: map[string]interface{}{}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "ns2:e2", StartNode: "ns1:n1", EndNode: "ns2:n3", Type: "R", Properties: map[string]interface{}{}}))

	// Clear namespace cache to force a scan
	engine.namespaceCountsMu.Lock()
	delete(engine.namespaceEdgeCounts, "ns1:")
	engine.namespaceCountsMu.Unlock()

	count, err := engine.EdgeCountByPrefix("ns1:")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}
