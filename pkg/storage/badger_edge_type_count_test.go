// SPDX-License-Identifier: MIT
package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Issue #638 regression tests: per-type and positional (label, type) derived
// counters must stay exact across every edge mutation path, including
// transactions, relabels, cascades, prefix deletes, and reopen.

func seedEdgeCountNodes(t *testing.T, engine *BadgerEngine, ids ...string) {
	t.Helper()
	for _, id := range ids {
		_, err := engine.CreateNode(testNode(id))
		require.NoError(t, err)
	}
}

func TestEdgeTypeCounts_CreateDeletePaths(t *testing.T) {
	engine := createTestBadgerEngine(t)
	seedEdgeCountNodes(t, engine, "a", "b", "c")

	require.NoError(t, engine.CreateEdge(testEdge("e1", "a", "b", "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "a", "c", "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e3", "b", "c", "LIKES")))

	count, err := engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	count, err = engine.EdgeCountByType("LIKES")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	// Case-insensitive like GetEdgesByType.
	count, err = engine.EdgeCountByType("knows")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	require.NoError(t, engine.DeleteEdge(EdgeID(prefixTestID("e2"))))

	count, err = engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Bulk create and bulk delete.
	bulk := []*Edge{
		testEdge("b1", "a", "b", "BULK"),
		testEdge("b2", "b", "c", "BULK"),
		testEdge("b3", "c", "a", "BULK"),
	}
	require.NoError(t, engine.BulkCreateEdges(bulk))
	count, err = engine.EdgeCountByType("BULK")
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("b1")), EdgeID(prefixTestID("b2"))}))
	count, err = engine.EdgeCountByType("BULK")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestEdgeTypeCounts_UpdateTypeChange(t *testing.T) {
	engine := createTestBadgerEngine(t)
	seedEdgeCountNodes(t, engine, "a", "b")

	edge := testEdge("e1", "a", "b", "OLD")
	require.NoError(t, engine.CreateEdge(edge))

	updated := testEdge("e1", "a", "b", "NEW")
	require.NoError(t, engine.UpdateEdge(updated))

	count, err := engine.EdgeCountByType("OLD")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	count, err = engine.EdgeCountByType("NEW")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestEdgeTypeCounts_NodeCascadeDelete(t *testing.T) {
	engine := createTestBadgerEngine(t)
	seedEdgeCountNodes(t, engine, "a", "b", "c")

	require.NoError(t, engine.CreateEdge(testEdge("e1", "a", "b", "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "c", "a", "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e3", "b", "a", "LIKES")))

	count, err := engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	// Deleting node a cascades e1 (outgoing), e2 (incoming), e3 (incoming).
	require.NoError(t, engine.DeleteNode(NodeID(prefixTestID("a"))))

	count, err = engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	count, err = engine.EdgeCountByType("LIKES")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

func TestEdgeTypeCounts_PositionalStartAndEndLabels(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("s1")), Labels: []string{"Source"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("s2")), Labels: []string{"Source"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("t1")), Labels: []string{"Target"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("t2")), Labels: []string{"Target"}})
	require.NoError(t, err)

	// s1 -> t1, s1 -> t2, s2 -> t1
	require.NoError(t, engine.CreateEdge(testEdge("e1", "s1", "t1", "FLOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "s1", "t2", "FLOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e3", "s2", "t1", "FLOWS")))

	startCount, err := engine.EdgeCountByStartLabel("Source", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(3), startCount)
	endCount, err := engine.EdgeCountByEndLabel("Target", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(3), endCount)
	// Positional: only 2 FLOWS edges START at s1 specifically are irrelevant;
	// the counters are label-wide, not node-wide.
	endCount, err = engine.EdgeCountByEndLabel("Source", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(0), endCount)
	startCount, err = engine.EdgeCountByStartLabel("Target", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(0), startCount)

	// Deleting e2 moves both tiers.
	require.NoError(t, engine.DeleteEdge(EdgeID(prefixTestID("e2"))))
	startCount, err = engine.EdgeCountByStartLabel("Source", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), startCount)
	endCount, err = engine.EdgeCountByEndLabel("Target", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), endCount)

	// Relabel t1 from Target to Sink: both remaining FLOWS edges end at t1,
	// so the end tier moves entirely to Sink.
	require.NoError(t, engine.UpdateNode(&Node{ID: NodeID(prefixTestID("t1")), Labels: []string{"Sink"}}))
	endCount, err = engine.EdgeCountByEndLabel("Target", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(0), endCount)
	endCount, err = engine.EdgeCountByEndLabel("Sink", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), endCount)

	// Cascade delete of s1 (e1 and e3 are gone? no — e3 is s2->t1, survives).
	// s1 owns e1 only after e2's removal; s2->t1 remains.
	require.NoError(t, engine.DeleteNode(NodeID(prefixTestID("s1"))))
	startCount, err = engine.EdgeCountByStartLabel("Source", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), startCount)
	endCount, err = engine.EdgeCountByEndLabel("Sink", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), endCount)
}

func TestEdgeTypeCounts_TransactionPaths(t *testing.T) {
	engine := createTestBadgerEngine(t)
	seedEdgeCountNodes(t, engine, "a", "b")

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(testEdge("e1", "a", "b", "KNOWS")))
	require.NoError(t, tx.CreateEdge(testEdge("e2", "b", "a", "KNOWS")))
	require.NoError(t, tx.Commit())

	count, err := engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteEdge(EdgeID(prefixTestID("e1"))))
	require.NoError(t, tx.Commit())

	count, err = engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Transactional edge create + positional counters: start node gains label
	// inside the same transaction before the edge is created.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: NodeID(prefixTestID("c")), Labels: []string{"Source"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(testEdge("e3", "c", "a", "FLOWS")))
	require.NoError(t, tx.Commit())

	startCount, err := engine.EdgeCountByStartLabel("Source", "FLOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), startCount)
}

func TestEdgeTypeCounts_ReopenRebuildsDrift(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	seedEdgeCountNodes(t, engine, "a", "b")
	require.NoError(t, engine.CreateEdge(testEdge("e1", "a", "b", "KNOWS")))

	count, err := engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Corrupt the persisted counter and drop the ready marker: reopen must
	// detect the drift and rebuild from the authoritative edge scan.
	err = engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(edgeTypeCountKey("test", "KNOWS"), encodeDerivedCount(42)); err != nil {
			return err
		}
		return txn.Delete(edgeTypeCountReadyKey)
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	defer reopened.Close()

	count, err = reopened.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "reopen must rebuild drifted counters")
}

func TestEdgeTypeCounts_DeleteByPrefixWholeNamespace(t *testing.T) {
	engine := createTestBadgerEngine(t)
	seedEdgeCountNodes(t, engine, "a", "b")
	require.NoError(t, engine.CreateEdge(testEdge("e1", "a", "b", "KNOWS")))
	require.NoError(t, engine.CreateEdge(testEdge("e2", "b", "a", "LIKES")))

	count, err := engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	_, _, err = engine.DeleteByPrefix("test:")
	require.NoError(t, err)

	count, err = engine.EdgeCountByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
	count, err = engine.EdgeCountByType("LIKES")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}

// BenchmarkBadgerEngine_CreateEdge_WithCounters tracks the write-path cost of
// maintaining the per-type and positional counters on every edge create
// (issue #638): endpoint label reads plus tier updates inside the write txn.
func BenchmarkBadgerEngine_CreateEdge_WithCounters(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	for i := 0; i < 16; i++ {
		_, err := engine.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("test:n%d", i)),
			Labels: []string{"Source"},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edge := &Edge{
			ID:        EdgeID(fmt.Sprintf("test:e%d", i)),
			StartNode: NodeID(fmt.Sprintf("test:n%d", i%16)),
			EndNode:   NodeID(fmt.Sprintf("test:n%d", (i+7)%16)),
			Type:      "KNOWS",
		}
		if err := engine.CreateEdge(edge); err != nil {
			b.Fatal(err)
		}
	}
}
