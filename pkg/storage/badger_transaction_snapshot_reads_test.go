package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionEdgeCollectionsRemainGlobalAfterNamespaceWrite(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	for _, namespace := range []string{"alpha", "beta"} {
		for _, name := range []string{"start", "end"} {
			_, err := engine.CreateNode(&Node{ID: NodeID(namespace + ":" + name)})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(namespace + ":edge"), StartNode: NodeID(namespace + ":start"), EndNode: NodeID(namespace + ":end"), Type: "MATCHES"}))
	}
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.CreateNode(&Node{ID: "alpha:pending"})
	require.NoError(t, err)
	require.NoError(t, reader.CreateEdge(&Edge{ID: "alpha:pending-edge", StartNode: "alpha:start", EndNode: "alpha:pending", Type: "OTHER"}))
	all, err := reader.AllEdges()
	require.NoError(t, err)
	ids := make([]EdgeID, 0, len(all))
	for _, edge := range all {
		ids = append(ids, edge.ID)
	}
	require.ElementsMatch(t, []EdgeID{"alpha:edge", "beta:edge", "alpha:pending-edge"}, ids)
	typed, err := reader.GetEdgesByType("MATCHES")
	require.NoError(t, err)
	ids = ids[:0]
	for _, edge := range typed {
		ids = append(ids, edge.ID)
	}
	require.ElementsMatch(t, []EdgeID{"alpha:edge", "beta:edge"}, ids)
	emptyType, err := reader.GetEdgesByType("")
	require.NoError(t, err)
	require.Empty(t, emptyType, "an empty type must not include differently typed pending edges")
}

func TestTransactionLabelLookupDoesNotDecodeUnrelatedNodes(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	targetID := NodeID(prefixTestID("indexed-label-target"))
	unrelatedID := NodeID(prefixTestID("indexed-label-unrelated"))

	_, err := engine.CreateNode(&Node{ID: targetID, Labels: []string{"Tiny"}, Properties: map[string]any{"k": 1}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: unrelatedID, Labels: []string{"Bulk"}, Properties: map[string]any{"payload": "large"}})
	require.NoError(t, err)

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(unrelatedID), []byte{0xff})
	}))

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })

	nodes, err := reader.GetNodesByLabel("Tiny")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, targetID, nodes[0].ID)
}

func TestTransactionEdgeTypeLookupDoesNotDecodeUnrelatedEdges(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	startID := NodeID(prefixTestID("indexed-type-start"))
	endID := NodeID(prefixTestID("indexed-type-end"))
	targetID := EdgeID(prefixTestID("indexed-type-target"))
	unrelatedID := EdgeID(prefixTestID("indexed-type-unrelated"))

	_, err := engine.CreateNode(&Node{ID: startID, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: endID, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: targetID, StartNode: startID, EndNode: endID, Type: "MATCHES"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: unrelatedID, StartNode: startID, EndNode: endID, Type: "NOISE"}))

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(edgeKey(unrelatedID), []byte{0xff})
	}))

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })

	edges, err := reader.GetEdgesByType("MATCHES")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, targetID, edges[0].ID)
}

func TestTransactionIndexedLookupsReturnMatchingCandidateDecodeErrors(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	nodeID := NodeID(prefixTestID("corrupt-indexed-node"))
	endID := NodeID(prefixTestID("corrupt-indexed-end"))
	edgeID := EdgeID(prefixTestID("corrupt-indexed-edge"))
	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Target"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: endID, Labels: []string{"End"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: endID, Type: "TARGET"}))
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey(nodeID), []byte{0xff}); err != nil {
			return err
		}
		return txn.Set(edgeKey(edgeID), []byte{0xff})
	}))
	engine.cacheDeleteNode(nodeID, nodeCacheBodyOnly)
	engine.cacheDeleteEdge(edgeID)

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	_, err = reader.GetNodesByLabel("Target")
	require.Error(t, err)
	_, err = reader.GetEdgesByType("TARGET")
	require.Error(t, err)
}

func TestTransactionIndexedLookupsIgnoreMalformedAndStaleIndexEntries(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	targetNode := NodeID(prefixTestID("indexed-noise-target"))
	mismatchNode := NodeID(prefixTestID("indexed-noise-mismatch"))
	_, err := engine.CreateNode(&Node{ID: targetNode, Labels: []string{"Target"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: mismatchNode, Labels: []string{"Other"}})
	require.NoError(t, err)
	targetEdge := EdgeID(prefixTestID("indexed-noise-edge-target"))
	mismatchEdge := EdgeID(prefixTestID("indexed-noise-edge-mismatch"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: targetEdge, StartNode: targetNode, EndNode: mismatchNode, Type: "MATCHES"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: mismatchEdge, StartNode: targetNode, EndNode: mismatchNode, Type: "NOISE"}))

	mismatchNodeNum, ok := engine.idDict.lookupNodeNumID(mismatchNode)
	require.True(t, ok)
	mismatchEdgeNum, ok := engine.idDict.lookupEdgeNumID(mismatchEdge)
	require.True(t, ok)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		entries := [][]byte{
			append(labelIndexPrefix("Target"), 0x01),
			labelIndexKey("Target", mismatchNodeNum),
			labelIndexKey("Target", ^uint64(0)),
			append(edgeTypeIndexPrefix("MATCHES"), 0x01),
			edgeTypeIndexKey("MATCHES", mismatchEdgeNum),
			edgeTypeIndexKey("MATCHES", ^uint64(0)),
		}
		for _, key := range entries {
			if err := txn.Set(key, nil); err != nil {
				return err
			}
		}
		return nil
	}))
	var allEdges []*Edge
	err = engine.streamEdgesByTypeAtSnapshotWithView(context.Background(), "", "", engine.currentMVCCReadVersion("test"), engine.withView, visibleRead, func(edge *Edge) error {
		allEdges = append(allEdges, edge)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, allEdges, 2)

	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	nodes, err := reader.GetNodesByLabel("Target")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, targetNode, nodes[0].ID)
	edges, err := reader.GetEdgesByType("MATCHES")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, targetEdge, edges[0].ID)
}

// seedSnapshotAdjacencyGraph creates a start node with a single outgoing edge
// to target, plus unrelatedEdges edges wired between throwaway node pairs that
// are NOT adjacent to start. It returns start, target, and the ID of the one
// edge start actually owns. The unrelated edges inflate the total edge count so
// a tx-snapshot adjacency read that scans all edges (O(E)) is distinguishable
// from one that scans only start's adjacency (O(deg(start))).
func seedSnapshotAdjacencyGraph(tb testing.TB, engine *BadgerEngine, unrelatedEdges int) (NodeID, NodeID, EdgeID) {
	tb.Helper()

	start := NodeID(prefixTestID("snap-adj-start"))
	target := NodeID(prefixTestID("snap-adj-target"))
	edgeID := EdgeID(prefixTestID("snap-adj-edge"))

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(tb, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}})
	require.NoError(tb, err)
	require.NoError(tb, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: target, Type: "LINKS"}))

	for i := 0; i < unrelatedEdges; i++ {
		from := NodeID(prefixTestID(fmt.Sprintf("snap-adj-noise-from-%d", i)))
		to := NodeID(prefixTestID(fmt.Sprintf("snap-adj-noise-to-%d", i)))
		_, err := engine.CreateNode(&Node{ID: from, Labels: []string{"Noise"}})
		require.NoError(tb, err)
		_, err = engine.CreateNode(&Node{ID: to, Labels: []string{"Noise"}})
		require.NoError(tb, err)
		require.NoError(tb, engine.CreateEdge(&Edge{
			ID:        EdgeID(prefixTestID(fmt.Sprintf("snap-adj-noise-edge-%d", i))),
			StartNode: from,
			EndNode:   to,
			Type:      "NOISE",
		}))
	}
	return start, target, edgeID
}

// TestTransaction_SnapshotAdjacencyIsNodeLocalAndConsistent proves the Badger
// transaction snapshot adjacency read returns exactly the bound node's edges
// (not the whole graph's) and holds the snapshot across a concurrent commit.
// This exercises getCommittedAdjacentEdgesLocked on the visible-at directional
// adjacency path.
func TestTransaction_SnapshotAdjacencyIsNodeLocalAndConsistent(t *testing.T) {
	engine := createTestBadgerEngine(t)

	start, target, edgeID := seedSnapshotAdjacencyGraph(t, engine, 200)

	txRead, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txRead.Rollback()
	require.False(t, txRead.readTS.IsZero(), "read tx must pin a snapshot version")

	outBefore, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(outBefore),
		"outgoing adjacency must return only start's own edge, not the 200 unrelated edges")

	inBefore, err := txRead.GetIncomingEdges(target)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(inBefore))

	// Commit a second edge from start in a concurrent transaction.
	laterTarget := NodeID(prefixTestID("snap-adj-target-later"))
	laterEdgeID := EdgeID(prefixTestID("snap-adj-edge-later"))
	txInsert, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = txInsert.CreateNode(&Node{ID: laterTarget, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, txInsert.CreateEdge(&Edge{ID: laterEdgeID, StartNode: start, EndNode: laterTarget, Type: "LINKS"}))
	require.NoError(t, txInsert.Commit())

	// The read snapshot must not observe the post-begin edge.
	outAfter, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, sortedEdgeIDStrings(outBefore), sortedEdgeIDStrings(outAfter),
		"snapshot read must be stable across a concurrent commit")

	// A fresh transaction sees both edges.
	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	freshOut, err := txFresh.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID), string(laterEdgeID)}, sortedEdgeIDStrings(freshOut))
}

// TestTransaction_SnapshotAdjacencyRejectsUnknownDirection covers the defensive
// guard in the snapshot branch: an EdgeDirection outside Outgoing/Incoming
// returns ErrInvalidData rather than silently resolving. The callers only pass
// Outgoing/Incoming, so this guard is reachable only from within the package.
func TestTransaction_SnapshotAdjacencyRejectsUnknownDirection(t *testing.T) {
	engine := createTestBadgerEngine(t)
	start, _, _ := seedSnapshotAdjacencyGraph(t, engine, 1)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.False(t, tx.readTS.IsZero(), "read tx must pin a snapshot version")

	tx.mu.Lock()
	_, err = tx.getCommittedAdjacentEdgesLockedWithContext(context.Background(), start, EdgeDirection(99))
	tx.mu.Unlock()
	require.ErrorIs(t, err, ErrInvalidData)
}

// TestTransaction_CommittedAdjacencyNoSnapshotUsesCurrentIndex covers the
// no-snapshot branch (zero read timestamp) of the committed-adjacency and
// committed-nodes helpers, where reads fall back to the current index/head
// rather than the visible-at path.
func TestTransaction_CommittedAdjacencyNoSnapshotUsesCurrentIndex(t *testing.T) {
	engine := createTestBadgerEngine(t)
	start, target, edgeID := seedSnapshotAdjacencyGraph(t, engine, 1)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	// The *Locked helpers require tx.mu held. Capture every result while
	// holding the lock, release it, then assert: a failing assertion calls
	// runtime.Goexit, which would run the deferred Rollback (also locks
	// tx.mu) and deadlock if we asserted under the lock.
	tx.mu.Lock()
	tx.readTS = MVCCVersion{} // force the no-snapshot path deterministically
	zeroed := tx.readTS.IsZero()
	out, outErr := tx.getCommittedAdjacentEdgesLockedWithContext(context.Background(), start, Outgoing)
	in, inErr := tx.getCommittedAdjacentEdgesLockedWithContext(context.Background(), target, Incoming)
	_, badDirErr := tx.getCommittedAdjacentEdgesLockedWithContext(context.Background(), start, EdgeDirection(99))
	nodes, nodesErr := tx.getAllCommittedNodesLocked()
	tx.mu.Unlock()

	require.True(t, zeroed)
	require.NoError(t, outErr)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(out))
	require.NoError(t, inErr)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(in))
	require.ErrorIs(t, badDirErr, ErrInvalidData)
	require.NoError(t, nodesErr)
	require.NotEmpty(t, nodes)
}

// BenchmarkTransaction_SnapshotAdjacency measures tx-snapshot outgoing adjacency
// cost as the total edge count grows while the bound node's degree stays at 1.
// A node-local read stays flat across edge counts; an all-edges scan degrades
// linearly with the total edge count.
func BenchmarkTransaction_SnapshotAdjacency(b *testing.B) {
	for _, unrelatedEdges := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("edges=%d", unrelatedEdges), func(b *testing.B) {
			engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			require.NoError(b, err)
			b.Cleanup(func() { engine.Close() })

			start, _, _ := seedSnapshotAdjacencyGraph(b, engine, unrelatedEdges)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				edges, err := tx.GetOutgoingEdges(start)
				if err != nil {
					b.Fatal(err)
				}
				if len(edges) != 1 {
					b.Fatalf("expected 1 outgoing edge, got %d", len(edges))
				}
				tx.Rollback()
			}
		})
	}
}

// BenchmarkTransaction_LabelLookup compares the former historical full scan
// with the explicit-transaction index path. The target label remains sparse as
// database cardinality grows, matching selective Cypher label predicates.
func BenchmarkTransaction_LabelLookup(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })

	const totalNodes = 5000
	const batchSize = 250
	for offset := 0; offset < totalNodes; offset += batchSize {
		nodes := make([]*Node, 0, batchSize)
		for i := offset; i < offset+batchSize; i++ {
			label := "Bulk"
			if i < 3 {
				label = "Tiny"
			}
			nodes = append(nodes, &Node{
				ID:         NodeID(prefixTestID(fmt.Sprintf("label-bench-%d", i))),
				Labels:     []string{label},
				Properties: map[string]any{"value": i},
			})
		}
		require.NoError(b, engine.BulkCreateNodes(nodes))
	}

	b.Run("historical_full_scan", func(b *testing.B) {
		version := engine.currentMVCCReadVersion("test")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			nodes, err := engine.GetNodesByLabelVisibleAt("Tiny", version)
			if err != nil || len(nodes) != 3 {
				b.Fatalf("lookup returned %d nodes: %v", len(nodes), err)
			}
		}
	})

	b.Run("transaction_index", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := engine.BeginTransaction()
			if err != nil {
				b.Fatal(err)
			}
			nodes, err := tx.GetNodesByLabel("Tiny")
			if err != nil || len(nodes) != 3 {
				b.Fatalf("lookup returned %d nodes: %v", len(nodes), err)
			}
			if err := tx.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
