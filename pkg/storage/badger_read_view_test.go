package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func snapshotOverlayFixture(t *testing.T) *BadgerEngine {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	for _, id := range []NodeID{"test:a", "test:b", "test:gone"} {
		bufferedCreate(t, b, &Node{ID: id, Labels: []string{"Old"}, Properties: map[string]any{"v": int64(1)}})
	}
	require.NoError(t, b.FlushWriteBehind(nil))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"Live"}, Properties: map[string]any{"v": int64(2)}}, Policy: ClientNodeEdits}))
	_, err = tx.CreateNode(&Node{ID: "test:new", Labels: []string{"Live"}, Properties: map[string]any{"v": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteGraphNode("test:gone", CascadeEdges))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"v": int64(2)}}))
	require.NoError(t, tx.Commit())
	return b
}

// Every scoped selector is retained across the exact old-snapshot/replay/retire
// schedule. This also checks pure deletes and relabels, rather than only creates.
func TestSnapshotOverlayCaptureClasses(t *testing.T) {
	for _, class := range []string{"point", "batch-projection", "label", "all-nodes", "type", "all-edges", "outgoing", "incoming", "between", "node-edge-snapshot"} {
		t.Run(class, func(t *testing.T) {
			b := snapshotOverlayFixture(t)
			var nodes []*Node
			var edges []*Edge
			var point bufferedEntityState
			var deleted bool
			var touchedNodes map[NodeID]bool
			var touchedEdges map[EdgeID]bool
			view, err := b.captureReadView(context.Background(), func(w *WriteBehindBuffer) (err error) {
				ctx := context.Background()
				switch class {
				case "point", "batch-projection":
					point = w.entityStateLocked(bufferedNodeEntity, "test:a")
					deleted = w.entityStateLocked(bufferedNodeEntity, "test:gone").deleted
				case "label":
					nodes, touchedNodes, err = w.labelOverlayLocked(ctx, "Live")
				case "all-nodes":
					nodes, touchedNodes, err = w.allNodesOverlayLocked(ctx)
				case "type":
					edges, touchedEdges, err = w.typeOverlayLocked(ctx, "REL")
				case "all-edges":
					edges, touchedEdges, err = w.allEdgesOverlayLocked(ctx)
				case "outgoing":
					edges, _, _, err = w.edgeAdjacencyOverlayLocked(ctx, "test:a", true)
				case "incoming":
					edges, _, _, err = w.edgeAdjacencyOverlayLocked(ctx, "test:b", false)
				case "between":
					edges, _, _, _, err = w.edgesBetweenOverlayLocked(ctx, "test:a", "test:b")
				case "node-edge-snapshot":
					if nodes, touchedNodes, err = w.allNodesOverlayLocked(ctx); err != nil {
						return err
					}
					edges, touchedEdges, err = w.allEdgesOverlayLocked(ctx)
				}
				return err
			})
			require.NoError(t, err)
			defer view.Close()
			// Replay and remove the captured generation before consuming either side.
			require.NoError(t, b.FlushWriteBehind(nil))
			require.Empty(t, b.writeBehind.draining)
			_, err = view.txn.Get(edgeKey("test:r"))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			item, err := view.txn.Get(nodeKey("test:a"))
			require.NoError(t, err)
			require.NoError(t, item.Value(func(value []byte) error {
				n, err := b.decodeNode("test", value)
				require.NoError(t, err)
				require.Equal(t, int64(1), n.Properties["v"])
				return nil
			}))
			if point.found {
				require.True(t, deleted)
				require.Equal(t, int64(2), point.node.Properties["v"])
			}
			if nodes != nil {
				require.Len(t, nodes, 2)
				require.True(t, touchedNodes["test:gone"])
				require.True(t, touchedNodes["test:a"])
			}
			if edges != nil {
				require.Len(t, edges, 1)
				require.Equal(t, EdgeID("test:r"), edges[0].ID)
			}
			if touchedEdges != nil {
				require.True(t, touchedEdges["test:r"])
			}
		})
	}
}

func TestSnapshotOverlayPublicReaderClasses(t *testing.T) {
	b := snapshotOverlayFixture(t)
	ctx := context.Background()
	check := func(t *testing.T) {
		for _, read := range []func(NodeID) (*Node, error){b.GetNode, b.GetNodeWithoutEmbeddings, func(id NodeID) (*Node, error) { return b.GetNodeProjected(id, []string{"v"}) }} {
			n, err := read("test:a")
			require.NoError(t, err)
			require.Equal(t, int64(2), n.Properties["v"])
			_, err = read("test:gone")
			require.ErrorIs(t, err, ErrNotFound)
		}
		for _, read := range []func([]NodeID) (map[NodeID]*Node, error){b.BatchGetNodes, b.BatchGetNodesWithoutEmbeddings} {
			n, err := read([]NodeID{"test:a", "test:new", "test:gone"})
			require.NoError(t, err)
			require.Len(t, n, 2)
			require.Equal(t, int64(2), n["test:a"].Properties["v"])
		}
		labels, err := b.HasLabelBatch([]NodeID{"test:a", "test:new", "test:gone"}, "Live")
		require.NoError(t, err)
		require.Len(t, labels, 2)
		nodes, err := b.GetNodesByLabelInScope("test", "Live")
		require.NoError(t, err)
		require.Len(t, nodes, 2)
		nodes, err = b.GetNodesByLabel("Old")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		nodes, err = b.AllNodes()
		require.NoError(t, err)
		require.Len(t, nodes, 3)
		for _, read := range []func() ([]*Edge, error){func() ([]*Edge, error) { return b.GetOutgoingEdges("test:a") }, func() ([]*Edge, error) { return b.GetIncomingEdges("test:b") }, func() ([]*Edge, error) { return b.GetEdgesBetween("test:a", "test:b") }, func() ([]*Edge, error) { return b.GetEdgesByType("REL") }, b.AllEdges} {
			edges, err := read()
			require.NoError(t, err)
			require.Len(t, edges, 1)
			require.Equal(t, EdgeID("test:r"), edges[0].ID)
		}
		e, err := b.GetEdge("test:r")
		require.NoError(t, err)
		require.Equal(t, int64(2), e.Properties["v"])
		require.NotNil(t, b.GetEdgeBetween("test:a", "test:b", "REL"))
		edges, err := b.MatchEdgesBetween("test:a", "test:b", "REL", []string{"v"}, func(e *Edge) bool { return e.Properties["v"] == int64(2) })
		require.NoError(t, err)
		require.Len(t, edges, 1)
		out, in, err := b.GetAdjacentEdges("test:a")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Empty(t, in)
		h, answered, err := b.OutgoingEdgeHeaders("test:a")
		require.NoError(t, err)
		require.True(t, answered)
		require.Len(t, h, 1)
		h, answered, err = b.IncomingEdgeHeaders("test:b")
		require.NoError(t, err)
		require.True(t, answered)
		require.Len(t, h, 1)
		n := 0
		require.NoError(t, b.IterateNodes(func(*Node) bool { n++; return true }))
		require.Equal(t, 3, n)
		for _, stream := range []func(func(*Node) error) error{func(fn func(*Node) error) error { return b.StreamNodes(ctx, fn) }, func(fn func(*Node) error) error {
			return b.StreamNodesByPrefixProjected(ctx, "test:", []string{"v"}, fn)
		}} {
			n = 0
			require.NoError(t, stream(func(*Node) error { n++; return nil }))
			require.Equal(t, 3, n)
		}
		n = 0
		require.NoError(t, b.StreamNodesByLabelProjectedInScope("test", "Live", []string{"v"}, func(*Node) error { n++; return nil }))
		require.Equal(t, 2, n)
		for _, stream := range []func(func(*Edge) error) error{func(fn func(*Edge) error) error { return b.StreamEdges(ctx, fn) }, func(fn func(*Edge) error) error { return b.StreamEdgesByTypeInScope(ctx, "test", "REL", fn) }} {
			n = 0
			require.NoError(t, stream(func(*Edge) error { n++; return nil }))
			require.Equal(t, 1, n)
		}
		count, err := b.NodeCount()
		require.NoError(t, err)
		require.Equal(t, int64(3), count)
		count, err = b.NodeCountByLabelInNamespace("test", "Live")
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
		count, err = b.EdgeCountByTypeInNamespace("test", "REL")
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
		count, err = b.EdgeCountByPrefix("test:")
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	}
	t.Run("buffered", check)
	require.NoError(t, b.FlushWriteBehind(nil))
	t.Run("retired", check)
}

func TestSnapshotOverlayStreamRetainsViewAcrossReplay(t *testing.T) {
	for _, kind := range []string{"nodes", "edges", "label", "type"} {
		t.Run(kind, func(t *testing.T) {
			b := snapshotOverlayFixture(t)
			// A committed row guarantees a callback before pending rows are emitted.
			if kind == "edges" || kind == "type" {
				require.NoError(t, b.CreateEdge(&Edge{ID: "test:committed", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
			}
			if kind == "label" {
				_, err := b.CreateNode(&Node{ID: "test:marker", Labels: []string{"Live"}})
				require.NoError(t, err)
			}
			drained := false
			visit := func() {
				if !drained {
					drained = true
					require.NoError(t, b.FlushWriteBehind(nil))
					bufferedCreate(t, b, &Node{ID: "test:later", Labels: []string{"Live"}})
				}
			}
			nodes := map[NodeID]bool{}
			edges := map[EdgeID]bool{}
			nodeFn := func(n *Node) error { visit(); nodes[n.ID] = true; return nil }
			edgeFn := func(e *Edge) error { visit(); edges[e.ID] = true; return nil }
			var err error
			switch kind {
			case "nodes":
				err = b.StreamNodes(context.Background(), nodeFn)
			case "label":
				err = b.StreamNodesByLabelProjected("Live", nil, nodeFn)
			case "edges":
				err = b.StreamEdges(context.Background(), edgeFn)
			case "type":
				err = b.StreamEdgesByType(context.Background(), "REL", edgeFn)
			}
			require.NoError(t, err)
			require.True(t, drained)
			require.False(t, nodes["test:later"])
			if len(nodes) > 0 {
				require.True(t, nodes["test:a"])
				require.True(t, nodes["test:new"])
				require.False(t, nodes["test:gone"])
			} else {
				require.True(t, edges["test:r"])
				require.True(t, edges["test:committed"])
			}
		})
	}
}

func TestSnapshotOverlayTransactionOwnWrites(t *testing.T) {
	b := snapshotOverlayFixture(t)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, b.FlushWriteBehind(nil))
	n, err := tx.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), n.Properties["v"])
	n.Properties["v"] = int64(3)
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: n, Policy: ClientNodeEdits}))
	n, err = tx.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(3), n.Properties["v"])
	require.NoError(t, tx.DeleteEdge("test:r"))
	edges, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
	require.NoError(t, err)
	require.Empty(t, edges)
	_, err = tx.GetEdge("test:r")
	require.ErrorIs(t, err, ErrNotFound)
}

// Replay progress and retirement are deliberately separate. Entity/index reads
// must still see the generation while counters have already published it.
func TestSnapshotOverlaySchemaAndCounterRetirement(t *testing.T) {
	b := snapshotOverlayFixture(t)
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddPropertyIndex("live_v", "Live", []string{"v"}))
	check := func() {
		require.ElementsMatch(t, []NodeID{"test:a", "test:new"}, schema.PropertyIndexLookup("Live", "v", int64(2)))
		require.Empty(t, schema.PropertyIndexLookup("Live", "v", int64(1)))
		n, err := b.NodeCount()
		require.NoError(t, err)
		require.Equal(t, int64(3), n)
		n, err = b.EdgeCount()
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		n, err = b.NodeCountByLabelInNamespace("test", "Live")
		require.NoError(t, err)
		require.Equal(t, int64(2), n)
		n, err = b.EdgeCountByTypeInNamespace("test", "REL")
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
	}
	check()
	b.writeBehind.rotate()
	b.writeBehind.mu.RLock()
	generation := b.writeBehind.draining[0]
	b.writeBehind.mu.RUnlock()
	require.NoError(t, b.replayWriteBehind(generation))
	require.True(t, generation.IsApplied())
	require.True(t, generation.countsApplied.Load())
	check()
	b.writeBehind.removeDrained(generation)
	check()
	require.Nil(t, b.writeBehind.pendingIndex, "retirement must not re-add its own applied nodes")
}

func TestSnapshotOverlayLateSchemaTracking(t *testing.T) {
	b := snapshotOverlayFixture(t)
	b.writeBehind.rotate()
	b.writeBehind.mu.RLock()
	generation := b.writeBehind.draining[0]
	b.writeBehind.mu.RUnlock()
	require.NoError(t, b.replayWriteBehind(generation))
	// A newly tracked property must use the same retained entity population.
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddPropertyIndex("late_v", "Live", []string{"v"}))
	require.ElementsMatch(t, []NodeID{"test:a", "test:new"}, schema.PropertyIndexLookup("Live", "v", int64(2)))
	b.writeBehind.removeDrained(generation)
}

func TestSnapshotOverlayTransactionLabelCache(t *testing.T) {
	for _, stopEarly := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "prefix"}[stopEarly], func(t *testing.T) {
			b := snapshotOverlayFixture(t)
			require.NoError(t, b.FlushWriteBehind(nil))
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			err = tx.StreamNodesByLabelProjected("Live", []string{"v"}, func(*Node) error {
				if stopEarly {
					return ErrIterationStopped
				}
				return nil
			})
			if stopEarly {
				require.ErrorIs(t, err, ErrIterationStopped)
			} else {
				require.NoError(t, err)
			}
			writer, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, writer.SetNamespace("test"))
			require.NoError(t, writer.SetImplicit(true))
			require.NoError(t, writer.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"Other"}, Properties: map[string]any{"v": int64(3)}}, Policy: ClientNodeEdits}))
			require.NoError(t, writer.DeleteGraphNode("test:new", CascadeEdges))
			_, err = writer.CreateNode(&Node{ID: "test:second", Labels: []string{"Live"}})
			require.NoError(t, err)
			require.NoError(t, writer.Commit())
			check := func() {
				var ids []NodeID
				require.NoError(t, tx.StreamNodesByLabelProjected("Live", []string{"v"}, func(n *Node) error {
					ids = append(ids, n.ID)
					return nil
				}))
				require.Equal(t, []NodeID{"test:second"}, ids)
			}
			check()
			require.NoError(t, b.FlushWriteBehind(nil))
			check()
		})
	}
}
