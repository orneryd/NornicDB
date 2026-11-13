package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The historical read fixtures write the stored state outright so a test owns
// the exact commit versions, floors and tombstones a historical read resolves.
// Nothing here depends on the wall clock, so every case is deterministic.

var historicalBaseTime = time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)

// historicalVersion is the version a fixture commits at: minute offsets from
// historicalBaseTime order the commits, and the sequence breaks ties.
func historicalVersion(minutes int, sequence uint64) MVCCVersion {
	return MVCCVersion{
		CommitTimestamp: historicalBaseTime.Add(time.Duration(minutes) * time.Minute),
		CommitSequence:  sequence,
	}
}

// historicalCutoff is the wall-clock time a caller passes to read at version.
func historicalCutoff(version MVCCVersion) time.Time { return version.CommitTimestamp }

func historicalPutNode(t *testing.T, engine *BadgerEngine, node *Node, version, floor MVCCVersion) {
	t.Helper()
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		data, _, err := engine.encodeNodeInTxn(txn, namespaceForNodeID(node.ID), node)
		if err != nil {
			return err
		}
		if err := txn.Set(nodeKey(node.ID), data); err != nil {
			return err
		}
		return engine.writeNodeMVCCHeadWithFloorInTxn(txn, node.ID, version, false, floor)
	}))
}

func historicalArchiveNode(t *testing.T, engine *BadgerEngine, node *Node, version MVCCVersion) {
	t.Helper()
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		return engine.writeNodeMVCCVersionInTxn(txn, node, version)
	}))
}

func historicalDeleteNode(t *testing.T, engine *BadgerEngine, id NodeID, version, floor MVCCVersion) {
	t.Helper()
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		if err := txn.Delete(nodeKey(id)); err != nil {
			return err
		}
		if err := engine.writeNodeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		return engine.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, true, floor)
	}))
}

func historicalPutEdge(t *testing.T, engine *BadgerEngine, edge *Edge, version, floor MVCCVersion) {
	t.Helper()
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		data, err := engine.encodeEdgeInTxn(txn, namespaceForEdgeID(edge.ID), edge)
		if err != nil {
			return err
		}
		if err := txn.Set(edgeKey(edge.ID), data); err != nil {
			return err
		}
		if err := engine.writeAdjacencyMVCCVersionInTxn(mvccAdjacencyOutgoing, txn, edge.StartNode, edge.ID, version, false); err != nil {
			return err
		}
		if err := engine.writeAdjacencyMVCCVersionInTxn(mvccAdjacencyIncoming, txn, edge.EndNode, edge.ID, version, false); err != nil {
			return err
		}
		return engine.writeEdgeMVCCHeadWithFloorInTxn(txn, edge.ID, version, false, floor)
	}))
}

// historicalMarkPrunedFloor persists the prune marker a real prune pass writes
// when it discards an entity's versions below floor.
func historicalMarkPrunedFloor(t *testing.T, engine *BadgerEngine, id NodeID, floor MVCCVersion) {
	t.Helper()
	require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
		numID, ok := engine.idDict.lookupNodeNumID(id)
		if !ok {
			return ErrNotFound
		}
		logical := append([]byte{prefixMVCCNode}, encodeNumID(numID)...)
		if err := txn.Set(mvccPruneFloorKey(logical), encodeMVCCSortVersion(floor)); err != nil {
			return err
		}
		engine.markMVCCFloorKey(logical)
		return nil
	}))
}

func historicalOpen(t *testing.T, engine Engine, at time.Time) Engine {
	t.Helper()
	historical, err := OpenHistoricalRead(t.Context(), engine, at)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, historical.Close()) })
	return historical
}

// TestHistoricalRead_ChangedLabelsAndProperties proves a historical read answers
// the labels and properties the entity carried at the cutoff, not the current
// ones, and that scans select a historical population rather than a current
// label index.
func TestHistoricalRead_ChangedLabelsAndProperties(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	beforeVersion := historicalVersion(0, 1)
	afterVersion := historicalVersion(10, 2)

	historicalArchiveNode(t, engine, &Node{
		ID:         id,
		Labels:     []string{"Person", "Former"},
		Properties: map[string]any{"name": "before"},
	}, beforeVersion)
	historicallyVectored := &Node{ID: id, Labels: []string{"Person", "Current"}, Properties: map[string]any{"name": "after"}}
	historicallyVectored.NamedEmbeddings = map[string][]float32{"default": {1, 2, 3}}
	historicallyVectored.ChunkEmbeddings = [][]float32{{4, 5, 6}}
	historicalPutNode(t, engine, historicallyVectored, afterVersion, beforeVersion)

	earlier := historicalOpen(t, engine, historicalCutoff(beforeVersion))
	later := historicalOpen(t, engine, historicalCutoff(afterVersion))

	before, err := earlier.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "before", before.Properties["name"])
	require.Equal(t, []string{"Person", "Former"}, before.Labels)
	require.Empty(t, before.NamedEmbeddings, "a historical read has no vectors")
	require.Empty(t, before.ChunkEmbeddings, "a historical read has no vectors")

	after, err := later.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "after", after.Properties["name"])
	require.Equal(t, []string{"Person", "Current"}, after.Labels)
	require.Empty(t, after.NamedEmbeddings, "a historical read must not return current vectors")
	require.Empty(t, after.ChunkEmbeddings, "a historical read must not return current vectors")

	former, err := earlier.GetNodesByLabel(t.Context(), "Former")
	require.NoError(t, err)
	require.Len(t, former, 1)
	require.Equal(t, id, former[0].ID)

	formerLater, err := later.GetNodesByLabel(t.Context(), "Former")
	require.NoError(t, err)
	require.Empty(t, formerLater, "the label was not carried at the later cutoff")

	currentEarlier, err := earlier.GetNodesByLabel(t.Context(), "Current")
	require.NoError(t, err)
	require.Empty(t, currentEarlier, "the label was not carried at the earlier cutoff")

	first, err := earlier.GetFirstNodeByLabel(t.Context(), "Former")
	require.NoError(t, err)
	require.Equal(t, id, first.ID)

	all, err := earlier.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, all, 1)

	count, err := earlier.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	batched, err := earlier.BatchGetNodes(t.Context(), []NodeID{id, "tenant:missing"})
	require.NoError(t, err)
	require.Len(t, batched, 1)
	require.Equal(t, "before", batched[id].Properties["name"])

	streamed := make([]*Node, 0, 1)
	require.NoError(t, earlier.StreamNodesWithOptions(t.Context(), StreamNodesOptions{Label: "Former"}, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Len(t, streamed, 1)
	require.Equal(t, "before", streamed[0].Properties["name"])
}

// TestHistoricalRead_DeletionAndRecreation proves the live-incarnation rules: a
// currently deleted entity is absent at every time, and an ID recreated after a
// retained tombstone never answers with the state of the incarnation it had
// before that tombstone.
func TestHistoricalRead_DeletionAndRecreation(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	creation := historicalVersion(0, 1)
	deletion := historicalVersion(10, 2)
	recreation := historicalVersion(20, 3)

	// The first incarnation is retained, deleted, and then recreated under the
	// same ID; the write path carries the creation floor across both.
	historicalArchiveNode(t, engine, &Node{ID: id, Labels: []string{"First"}, Properties: map[string]any{"name": "first"}}, creation)
	historicalDeleteNode(t, engine, id, deletion, creation)
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"Second"}, Properties: map[string]any{"name": "second"}}, recreation, creation)

	t.Run("state of the first incarnation before its deletion", func(t *testing.T) {
		historical := historicalOpen(t, engine, historicalCutoff(creation))
		_, err := historical.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound,
			"a retained tombstone after the cutoff means the ID was recreated: the earlier incarnation must not answer")
		nodes, err := historical.GetNodesByLabel(t.Context(), "First")
		require.NoError(t, err)
		require.Empty(t, nodes)
		count, err := historical.NodeCount(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 0, count)
	})

	t.Run("state of the current incarnation", func(t *testing.T) {
		historical := historicalOpen(t, engine, historicalCutoff(recreation))
		node, err := historical.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, "second", node.Properties["name"])
		require.Equal(t, []string{"Second"}, node.Labels)
	})

	t.Run("currently deleted entity is absent even before its deletion", func(t *testing.T) {
		historicalDeleteNode(t, engine, id, historicalVersion(30, 4), creation)
		historical := historicalOpen(t, engine, historicalCutoff(recreation))
		_, err := historical.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound)
		nodes, err := historical.GetNodesByLabel(t.Context(), "Second")
		require.NoError(t, err)
		require.Empty(t, nodes)
	})

	t.Run("a live head without its primary body is absent", func(t *testing.T) {
		orphanID := NodeID("tenant:orphan")
		historicalArchiveNode(t, engine, &Node{ID: orphanID, Labels: []string{"Orphan"}}, creation)
		require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
			return engine.writeNodeMVCCHeadWithFloorInTxn(txn, orphanID, creation, false, creation)
		}))
		historical := historicalOpen(t, engine, historicalCutoff(creation))
		_, err := historical.GetNode(t.Context(), orphanID)
		require.ErrorIs(t, err, ErrNotFound, "a head without a body is not a live incarnation")
	})

	t.Run("deleted entity that was never recreated is absent", func(t *testing.T) {
		otherID := NodeID("tenant:gone")
		historicalPutNode(t, engine, &Node{ID: otherID, Labels: []string{"Gone"}, Properties: map[string]any{"name": "gone"}}, creation, creation)
		historicalDeleteNode(t, engine, otherID, deletion, creation)
		historical := historicalOpen(t, engine, historicalCutoff(creation))
		_, err := historical.GetNode(t.Context(), otherID)
		require.ErrorIs(t, err, ErrNotFound,
			"the current head tombstone makes the entity absent even for a time before its deletion")
	})
}

// TestHistoricalRead_RetainedHistoryRules proves the three below/at-floor
// answers a historical read reports instead of a silent absence.
func TestHistoricalRead_RetainedHistoryRules(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	creation := historicalVersion(0, 1)
	later := historicalVersion(10, 2)

	t.Run("before the creation floor is a legitimate absence", func(t *testing.T) {
		historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}}, later, creation)
		historical := historicalOpen(t, engine, historicalCutoff(creation).Add(-time.Minute))
		_, err := historical.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("below a pruned floor reports the discarded history", func(t *testing.T) {
		historicalMarkPrunedFloor(t, engine, id, later)
		historical := historicalOpen(t, engine, historicalCutoff(creation))
		_, err := historical.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrHistoryNotRetained)
		nodes, err := historical.GetNodesByLabel(t.Context(), "L")
		require.ErrorIs(t, err, ErrHistoryNotRetained)
		require.Empty(t, nodes)
	})
}

// TestHistoricalRead_MissingHistoricalRecord proves a missing retained record at
// or after the creation floor is an explicit unavailable answer, never a false
// absence.
func TestHistoricalRead_MissingHistoricalRecord(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	creation := historicalVersion(0, 1)
	later := historicalVersion(10, 2)

	// The entity was created at creation and updated at later, but the superseded
	// body was never retained: a cutoff between them cannot be answered.
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"name": "current"}}, later, creation)

	historical := historicalOpen(t, engine, historicalCutoff(creation))
	_, err := historical.GetNode(t.Context(), id)
	require.ErrorIs(t, err, ErrHistoryUnavailable)
	nodes, err := historical.GetNodesByLabel(t.Context(), "L")
	require.ErrorIs(t, err, ErrHistoryUnavailable)
	require.Empty(t, nodes)

	// The head state remains readable at its own version.
	current := historicalOpen(t, engine, historicalCutoff(later))
	node, err := current.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "current", node.Properties["name"])
}

// TestHistoricalRead_NamespaceScope proves a historical read opened from a
// database-scoped engine keeps that namespace: it reports the namespace and
// lists only that database's entities.
func TestHistoricalRead_NamespaceScope(t *testing.T) {
	engine := newTestEngine(t)
	version := historicalVersion(0, 1)
	tenantID := NodeID("tenant:node")
	otherID := NodeID("other:node")
	historicalPutNode(t, engine, &Node{ID: tenantID, Labels: []string{"Doc"}, Properties: map[string]any{"db": "tenant"}}, version, version)
	historicalPutNode(t, engine, &Node{ID: otherID, Labels: []string{"Doc"}, Properties: map[string]any{"db": "other"}}, version, version)

	historical := historicalOpen(t, NewNamespacedEngine(engine, "tenant"), historicalCutoff(version))
	scoped, ok := historical.(interface{ Namespace() string })
	require.True(t, ok, "a historical read preserves the namespace of the engine it was opened from")
	require.Equal(t, "tenant", scoped.Namespace())

	node, err := historical.GetNode(t.Context(), tenantID)
	require.NoError(t, err)
	require.Equal(t, "tenant", node.Properties["db"])

	_, err = historical.GetNode(t.Context(), otherID)
	require.ErrorIs(t, err, ErrNotFound, "another database's entity is outside this read")

	nodes, err := historical.GetNodesByLabel(t.Context(), "Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, tenantID, nodes[0].ID)

	all, err := historical.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, tenantID, all[0].ID)

	count, err := historical.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	unscoped := historicalOpen(t, engine, historicalCutoff(version))
	allUnscoped, err := unscoped.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, allUnscoped, 2)
}

// TestHistoricalRead_EndpointEligibility proves a relationship is only listed
// while both of its endpoints are nodes the same historical read lists.
func TestHistoricalRead_EndpointEligibility(t *testing.T) {
	engine := newTestEngine(t)
	startID := NodeID("tenant:start")
	endID := NodeID("tenant:end")
	edgeID := EdgeID("tenant:edge")
	version := historicalVersion(0, 1)
	later := historicalVersion(10, 2)

	historicalPutNode(t, engine, &Node{ID: startID, Labels: []string{"Node"}}, version, version)
	historicalPutNode(t, engine, &Node{ID: endID, Labels: []string{"Node"}}, later, later)
	historicalPutEdge(t, engine, &Edge{ID: edgeID, StartNode: startID, EndNode: endID, Type: "REL"}, version, version)

	// The end node did not exist at the cutoff, so the relationship is not part
	// of the graph the cutoff describes.
	early := historicalOpen(t, engine, historicalCutoff(version))
	_, err := early.GetEdge(t.Context(), edgeID)
	require.ErrorIs(t, err, ErrNotFound)
	edges, err := early.GetEdgesByType(t.Context(), "REL")
	require.NoError(t, err)
	require.Empty(t, edges)
	adjacent, err := early.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: startID})
	require.NoError(t, err)
	require.Empty(t, adjacent)
	edgeCount, err := early.EdgeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 0, edgeCount)

	// Both endpoints exist at the later cutoff.
	late := historicalOpen(t, engine, historicalCutoff(later))
	edge, err := late.GetEdge(t.Context(), edgeID)
	require.NoError(t, err)
	require.Equal(t, startID, edge.StartNode)
	require.Equal(t, endID, edge.EndNode)

	between, err := late.GetEdgeBetween(t.Context(), startID, endID, "REL")
	require.NoError(t, err)
	require.Equal(t, edgeID, between.ID)

	outgoing, err := late.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: startID})
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	incoming, err := late.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: endID})
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	both, err := late.ReadAdjacentEdges(t.Context(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: startID})
	require.NoError(t, err)
	require.Len(t, both, 1)

	outDegree, err := late.GetOutDegree(t.Context(), startID)
	require.NoError(t, err)
	require.Equal(t, 1, outDegree)
	inDegree, err := late.GetInDegree(t.Context(), endID)
	require.NoError(t, err)
	require.Equal(t, 1, inDegree)

	edgeCount, err = late.EdgeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, edgeCount)
	typedCount, err := late.EdgeCountByType(t.Context(), "REL")
	require.NoError(t, err)
	require.EqualValues(t, 1, typedCount)
	startLabelCount, err := late.EdgeCountByStartLabel(t.Context(), "Node", "REL")
	require.NoError(t, err)
	require.EqualValues(t, 1, startLabelCount)
	endLabelCount, err := late.EdgeCountByEndLabel(t.Context(), "Node", "REL")
	require.NoError(t, err)
	require.EqualValues(t, 1, endLabelCount)
	missingLabelCount, err := late.EdgeCountByStartLabel(t.Context(), "Absent", "REL")
	require.NoError(t, err)
	require.EqualValues(t, 0, missingLabelCount)

	// A currently deleted endpoint removes the relationship from every read.
	historicalDeleteNode(t, engine, endID, historicalVersion(20, 3), version)
	afterDelete := historicalOpen(t, engine, historicalCutoff(later))
	_, err = afterDelete.GetEdge(t.Context(), edgeID)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestHistoricalRead_Cancellation proves every read path admits the caller's
// context and reports its cancellation.
func TestHistoricalRead_Cancellation(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	edgeID := EdgeID("tenant:edge")
	version := historicalVersion(0, 1)
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}}, version, version)
	historicalPutNode(t, engine, &Node{ID: "tenant:other", Labels: []string{"L"}}, version, version)
	historicalPutEdge(t, engine, &Edge{ID: edgeID, StartNode: id, EndNode: "tenant:other", Type: "REL"}, version, version)

	historical := historicalOpen(t, engine, historicalCutoff(version))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := historical.GetNode(canceled, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetEdge(canceled, edgeID)
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetNodesByLabel(canceled, "L")
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetFirstNodeByLabel(canceled, "L")
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetEdgesByType(canceled, "REL")
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetEdgeBetween(canceled, id, "tenant:other", "REL")
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.AllNodes(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.AllEdges(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.BatchGetNodes(canceled, []NodeID{id})
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.NodeCount(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.EdgeCountByType(canceled, "REL")
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.ReadAdjacentEdges(canceled, AdjacentEdgeRead{Direction: AdjacentBoth, Node: id})
	require.ErrorIs(t, err, context.Canceled)
	_, err = historical.GetOutDegree(canceled, id)
	require.ErrorIs(t, err, context.Canceled)
	err = historical.StreamNodesWithOptions(canceled, StreamNodesOptions{}, func(*Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)

	_, err = OpenHistoricalRead(canceled, engine, historicalCutoff(version))
	require.ErrorIs(t, err, context.Canceled)
}

// TestHistoricalRead_ReadOnlyAndCloseLifetime proves a historical read rejects
// every mutation and that Close releases only what it holds: the store it was
// opened from keeps answering.
func TestHistoricalRead_ReadOnlyAndCloseLifetime(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:node")
	version := historicalVersion(0, 1)
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}}, version, version)

	historical, err := OpenHistoricalRead(t.Context(), engine, historicalCutoff(version))
	require.NoError(t, err)

	require.NotSame(t, engine.GetSchema(), historical.GetSchema(),
		"a historical read must not expose the live schema's indexes")

	node := &Node{ID: "tenant:new"}
	_, err = historical.CreateNode(t.Context(), node)
	require.ErrorIs(t, err, ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.UpdateNode(t.Context(), NodeUpdateRequest{After: node}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.DeleteNode(t.Context(), id), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.CreateEdge(t.Context(), &Edge{ID: "tenant:e", StartNode: id, EndNode: id, Type: "REL"}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.UpdateEdge(t.Context(), EdgeUpdateRequest{After: &Edge{ID: "tenant:e"}}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.DeleteEdge(t.Context(), "tenant:e"), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.BulkCreateNodes(t.Context(), []*Node{node}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.BulkCreateEdges(t.Context(), []*Edge{{ID: "tenant:e"}}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.BulkDeleteNodes(t.Context(), []NodeID{id}), ErrHistoricalReadOnly)
	require.ErrorIs(t, historical.BulkDeleteEdges(t.Context(), []EdgeID{"tenant:e"}), ErrHistoricalReadOnly)
	_, _, err = historical.DeleteByPrefix("tenant:")
	require.ErrorIs(t, err, ErrHistoricalReadOnly)

	require.EqualValues(t, 1, engine.activeMVCCSnapshotReaders.Load(),
		"one historical reader registration for the view's whole lifetime")

	require.NoError(t, historical.Close())
	require.EqualValues(t, 0, engine.activeMVCCSnapshotReaders.Load(),
		"Close releases the reader registration it holds")
	require.NoError(t, historical.Close(), "Close is a no-op once the view is released")

	_, err = historical.GetNode(t.Context(), id)
	require.ErrorIs(t, err, ErrStorageClosed)

	current, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err, "releasing a historical read must leave the store it reads untouched")
	require.Equal(t, id, current.ID)
}

// historicalNonNativeStore is an Engine that keeps the store it answers through
// private: it promotes the Engine interface only, so no wrapper chain reaches a
// native MVCC store.
type historicalNonNativeStore struct{ Engine }

// TestOpenHistoricalRead_TargetResolution proves the native store and namespace
// resolve through the wrapper chain, and that an engine with no reachable
// native MVCC store cannot be pinned.
func TestOpenHistoricalRead_TargetResolution(t *testing.T) {
	version := historicalVersion(0, 1)
	_, err := OpenHistoricalRead(t.Context(), nil, time.Now().UTC())
	require.Error(t, err)

	// An in-memory engine is a wrapper over the native store, so it opens.
	memory := newTestMemoryEngine(t)
	historicalPutNode(t, memory.BadgerEngine, &Node{ID: "tenant:node", Labels: []string{"L"}}, version, version)
	historical, err := OpenHistoricalRead(t.Context(), memory, historicalCutoff(version))
	require.NoError(t, err)
	node, err := historical.GetNode(t.Context(), "tenant:node")
	require.NoError(t, err)
	require.Equal(t, []string{"L"}, node.Labels)
	require.NoError(t, historical.Close())

	// An engine that keeps its native store private cannot be pinned.
	_, err = OpenHistoricalRead(t.Context(), historicalNonNativeStore{Engine: memory}, historicalCutoff(version))
	require.Error(t, err)
}

func TestHistoricalReadStreamAllowsNestedReads(t *testing.T) {
	engine := newTestEngine(t)
	version := historicalVersion(0, 1)
	id := NodeID("tenant:node")
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}}, version, version)
	historical := historicalOpen(t, engine, historicalCutoff(version))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	visited := 0
	err := historical.StreamNodesWithOptions(ctx, StreamNodesOptions{}, func(node *Node) error { _, err := historical.GetNode(ctx, node.ID); visited++; return err })
	require.NoError(t, err)
	require.Equal(t, 1, visited)
}

func TestHistoricalReadPruneMarkerUsesPinnedAdmission(t *testing.T) {
	engine := newTestEngine(t)
	created := historicalVersion(10, 1)
	id := NodeID("tenant:late")
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}}, created, created)
	historical := historicalOpen(t, engine, historicalCutoff(historicalVersion(0, 0)))
	// A later marker cannot change a creation-floor absence in the pinned view.
	historicalMarkPrunedFloor(t, engine, id, created)
	_, err := historical.GetNode(t.Context(), id)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestHistoricalReadReturnedPropertiesRemainOwned(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("tenant:owned")
	version := historicalVersion(0, 1)
	historicalPutNode(t, engine, &Node{ID: id, Labels: []string{"L"}, Properties: map[string]interface{}{"nested": map[string]interface{}{"value": "original"}}}, version, version)
	historical := historicalOpen(t, engine, historicalCutoff(version))
	first, err := historical.GetNode(t.Context(), id)
	require.NoError(t, err)
	first.Properties["nested"].(map[string]interface{})["value"] = "changed"
	first.Labels[0] = "Changed"
	again, err := historical.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "original", again.Properties["nested"].(map[string]interface{})["value"])
	require.Equal(t, []string{"L"}, again.Labels)
	current, err := engine.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, "original", current.Properties["nested"].(map[string]interface{})["value"])
}
