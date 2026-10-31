//go:build nornic_copycount

// NOR-160 lane (Claude), 31 October 2026: the round-7 review's public
// reproductions (sig/handoff/r73/1688-e6d1-review-r7.md, P1-1, P1-2, P2-1).
// Every copy a read makes is of a value it returns, exactly once.
package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// nor160ReturnedOnce fails for an edge copied but not returned, or copied
// more than once.
func nor160ReturnedOnce(t *testing.T, returned []*Edge, copies map[EdgeID]int) {
	t.Helper()
	got := map[EdgeID]bool{}
	for _, edge := range returned {
		got[edge.ID] = true
	}
	for id, count := range copies {
		require.True(t, got[id], "%s was copied %d time(s) and not returned", id, count)
		require.Equal(t, 1, count, "%s copies", id)
	}
	for id := range got {
		require.Equal(t, 1, copies[id], "%s returned with %d copies", id, copies[id])
	}
}

// P1-1: the transaction's stored relationship reads decide its own writes and
// the direction before any engine relationship is copied.
func TestNOR160R7TransactionIncidentEdgesCopyOnlyWhatTheyReturn(t *testing.T) {
	cases := []struct {
		name  string
		stage func(*testing.T, *BadgerTransaction)
		read  func(*BadgerTransaction) ([]*Edge, error)
	}{
		{"rewritten", func(t *testing.T, tx *BadgerTransaction) {
			updated := nor160EdgeFixtureEdge("test:e1", "test:b")
			updated.Properties["k"] = "tx"
			require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: updated}))
		}, func(tx *BadgerTransaction) ([]*Edge, error) { return tx.IncidentEdges("test:a") }},
		{"deleted", func(t *testing.T, tx *BadgerTransaction) {
			require.NoError(t, tx.DeleteEdge("test:e1"))
		}, func(tx *BadgerTransaction) ([]*Edge, error) { return tx.IncidentEdges("test:a") }},
		{"stored outgoing", func(t *testing.T, tx *BadgerTransaction) {
			require.NoError(t, tx.CreateEdge(&Edge{ID: "test:in", StartNode: "test:b", EndNode: "test:a", Type: "T"}))
		}, func(tx *BadgerTransaction) ([]*Edge, error) { return tx.GetStoredOutgoingEdges("test:a") }},
		{"stored incoming", func(t *testing.T, tx *BadgerTransaction) {
			require.NoError(t, tx.CreateEdge(&Edge{ID: "test:in", StartNode: "test:b", EndNode: "test:a", Type: "T"}))
		}, func(tx *BadgerTransaction) ([]*Edge, error) { return tx.GetStoredIncomingEdges("test:a") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			engine := nor160WriteBehindEdges(t)
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			c.stage(t, tx)
			var returned []*Edge
			_, copies := CountStructuralCopies(func() {
				returned, err = c.read(tx)
			})
			require.NoError(t, err)
			nor160ReturnedOnce(t, returned, copies)
		})
	}
}

// P1-1: a cached self-loop is listed once, and copied once.
func TestNOR160R7TransactionSelfLoopCopiedOnce(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedWrite(t, engine, func(writer *BadgerTransaction) error {
		if _, err := writer.CreateNode(&Node{ID: "test:a", Labels: []string{"Held"}}); err != nil {
			return err
		}
		return writer.CreateEdge(&Edge{ID: "test:loop", StartNode: "test:a", EndNode: "test:a", Type: "T"})
	})
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	_, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	_, err = engine.GetIncomingEdges("test:a")
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	var returned []*Edge
	_, copies := CountStructuralCopies(func() { returned, err = tx.IncidentEdges("test:a") })
	require.NoError(t, err)
	require.Len(t, returned, 1)
	nor160ReturnedOnce(t, returned, copies)
}

// P2-1: a transaction's pending relationships are copied as they are
// delivered: a visitor that stops at the first leaves the rest uncopied.
func TestNOR160R7TransactionEdgeStreamCopiesAtDelivery(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	for _, id := range []EdgeID{"test:p1", "test:p2"} {
		require.NoError(t, tx.CreateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "P"}))
	}
	visited := 0
	_, copies := CountStructuralCopies(func() {
		err = tx.StreamEdgesByType(context.Background(), "P", func(*Edge) error {
			visited++
			return ErrIterationStopped
		})
	})
	require.NoError(t, err)
	require.Equal(t, 1, visited)
	total := 0
	for _, count := range copies {
		total += count
	}
	require.Equal(t, 1, total, "copies: %v", copies)
}

// P1-2: a body over the body cache's byte budget is not copied to be dropped.
func TestNOR160R7BodyCacheAdmissionCopiesOnlyWhatItKeeps(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		InMemory:              true,
		NodeCacheMaxEntries:   100,
		NodeBodyCacheMaxBytes: 900,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	body := func(id NodeID, size int) *Node {
		return &Node{
			runtime:    &nodeRuntime{metadata: NodeRuntimeMetadata{EmbeddingsStoredSeparately: true}},
			ID:         id,
			Labels:     []string{"Document"},
			Properties: map[string]any{"text": strings.Repeat("x", size)},
		}
	}
	nodes, _ := CountStructuralCopies(func() {
		engine.cacheStoreNodeBody("test:big", 1, body("test:big", 2000), engine.db.oracle.published.Load())
		engine.cacheStoreNodeBody("test:small", 1, body("test:small", 100), engine.db.oracle.published.Load())
	})
	require.Zero(t, nodes["test:big"], "a body over budget must not be copied")
	require.Equal(t, 1, nodes["test:small"], "an admitted body is copied once, into the cache")
	engine.nodeBodyCacheMu.RLock()
	_, big := engine.nodeBodyCache["test:big"]
	_, small := engine.nodeBodyCache["test:small"]
	engine.nodeBodyCacheMu.RUnlock()
	require.False(t, big)
	require.True(t, small)
}

// P1-2: a label stream's one-pass tail leaves bodies the body cache holds for
// their visit, so a visitor that stops in the tail leaves none copied.
func TestNOR160R7LabelTailLeavesUnvisitedBodiesUncopied(t *testing.T) {
	// A tail that fits one buffered batch is read in one pass over the node
	// records (a longer one falls back to point reads), and the visitor stops
	// at the tail's first node.
	const count = labelScanPointLookups + 100
	eng, ids := labelScanFixture(t, count)
	// A complete read fills the body cache; the node cache is then emptied so
	// the scan reads bodies.
	require.NoError(t, eng.StreamNodesByLabelProjectedInScope("test:", "Common", nil, func(*Node) error { return nil }))
	eng.nodeCacheMu.Lock()
	clear(eng.nodeCache)
	eng.nodeCacheMu.Unlock()
	visited := 0
	nodes, _ := CountStructuralCopies(func() {
		_ = eng.StreamNodesByLabelProjectedInScope("test:", "Common", nil, func(*Node) error {
			visited++
			if visited == labelScanPointLookups+1 {
				return ErrIterationStopped
			}
			return nil
		})
	})
	require.Equal(t, labelScanPointLookups+1, visited)
	for _, id := range ids[visited:] {
		require.Zero(t, nodes[id], "%s was copied but never visited", id)
	}
}

// A visitor's stop ends a transaction's edge stream: no edge is visited after
// it, from the committed part or the pending tail, and the stream reports
// success, as the engine's edge stream does (NOR-160).
func TestNOR160TransactionEdgeStreamStopEndsTheStream(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:c1", StartNode: "test:a", EndNode: "test:b", Type: "P"}))
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:p1", StartNode: "test:a", EndNode: "test:b", Type: "P"}))
	visited := 0
	require.NoError(t, tx.StreamEdgesByType(context.Background(), "P", func(*Edge) error {
		visited++
		return ErrIterationStopped
	}))
	require.Equal(t, 1, visited)
}
