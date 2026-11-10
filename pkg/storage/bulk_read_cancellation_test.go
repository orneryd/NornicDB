package storage

// Deterministic caller-cancellation coverage for the storage bulk-read
// routes. Each route is exercised three ways on one fixture: a live caller
// context that must return the exact population, a caller context that is
// canceled or expired before the call, and a caller that cancels from inside
// its own visit callback while the loop is still running. A canceled route
// retires with the caller's own context error and no partial result, and it
// leaves no partial cache behind for the next healthy read.
//
// The mid-loop cases visit more records than bulkReadCancellationInterval so
// the bounded in-loop check is guaranteed to run after the caller cancels;
// nothing here depends on wall-clock timing or on a ratio between runs.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// cancellationRouteRecords is the fixture size for a route whose loop only
// observes cancellation on its bounded in-loop check: more than one full
// interval after the record the caller cancels on.
const cancellationRouteRecords = 2 * bulkReadCancellationInterval

// seedCancellationNodes creates count visible Doc nodes with a projected
// property and returns their IDs in creation order.
func seedCancellationNodes(t *testing.T, b *BadgerEngine, count int, buffered ...bool) []NodeID {
	t.Helper()
	ids := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(fmt.Sprintf("test:n-%04d", i))
		node := &Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]any{"name": string(id)},
		}
		if len(buffered) > 0 && buffered[0] {
			bufferedCreate(t, b, node)
		} else {
			_, err := b.CreateNode(t.Context(), node)
			require.NoError(t, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// seedCancellationHub creates one hub with the outgoing relationships this
// fixture expects and one incoming relationship.
func seedCancellationHub(t *testing.T, b *BadgerEngine) (outgoing, incoming []EdgeID) {
	t.Helper()
	for _, id := range []NodeID{"test:hub", "test:a", "test:b", "test:c"} {
		_, err := b.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	for index, end := range []NodeID{"test:a", "test:b", "test:c"} {
		id := EdgeID(fmt.Sprintf("test:out-%d", index))
		require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: id, StartNode: "test:hub", EndNode: end, Type: "R"}))
		outgoing = append(outgoing, id)
	}
	require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: "test:in-0", StartNode: "test:a", EndNode: "test:hub", Type: "R"}))
	return outgoing, []EdgeID{"test:in-0"}
}

// warmCancellationAdjacency reads both directions once so the per-node
// adjacency cache and the edge body cache answer the next read from memory.
func warmCancellationAdjacency(t *testing.T, b *BadgerEngine) {
	t.Helper()
	ctx := context.Background()
	_, err := b.GetOutgoingEdges(ctx, "test:hub")
	require.NoError(t, err)
	_, err = b.GetIncomingEdges(ctx, "test:hub")
	require.NoError(t, err)
}

// TestBulkReadCancellationHotAdjacencyCacheRetires proves the native
// adjacency cache fast path honors the caller's cancellation instead of
// answering from memory after the caller is gone.
func TestBulkReadCancellationHotAdjacencyCacheRetires(t *testing.T) {
	b := createTestBadgerEngine(t)
	outgoing, incoming := seedCancellationHub(t, b)
	warmCancellationAdjacency(t, b)
	ctx := context.Background()

	t.Run("healthy-control-cached-route", func(t *testing.T) {
		out, err := b.GetOutgoingEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
		in, err := b.GetIncomingEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, incoming, testEdgeIDs(in))
		out, in, err = b.GetAdjacentEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
		require.ElementsMatch(t, incoming, testEdgeIDs(in))
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		out, err := b.GetOutgoingEdges(canceled, "test:hub")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, out)
		in, err := b.GetIncomingEdges(canceled, "test:hub")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, in)
		out, in, err = b.GetAdjacentEdges(canceled, "test:hub")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, out)
		require.Nil(t, in)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		out, err := b.GetOutgoingEdges(expired, "test:hub")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, out)
	})

	t.Run("cache-not-poisoned-by-canceled-read", func(t *testing.T) {
		out, err := b.GetOutgoingEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
	})
}

// TestBulkReadCancellationLabelStreamRetires covers the label route on both
// the committed scan and the write-behind overlay capture it merges.
func TestBulkReadCancellationLabelStreamRetires(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-behind=%t", buffered), func(t *testing.T) {
			var b *BadgerEngine
			if buffered {
				b = newWriteBehindTestEngine(t, time.Hour)
			} else {
				b = createTestBadgerEngine(t)
			}
			expected := seedCancellationNodes(t, b, cancellationRouteRecords, buffered)
			if buffered {
				require.Positive(t, b.writeBehind.PendingOps(), "the fixture must keep the overlay unflushed")
			}
			ctx := context.Background()
			scan := func(ctx context.Context, properties []string, visit func(*Node) error) error {
				return b.StreamNodesByLabelProjectedInScope(ctx, "test:", "Doc", properties, visit)
			}

			t.Run("healthy-control", func(t *testing.T) {
				var seen []NodeID
				require.NoError(t, scan(ctx, []string{"name"}, func(node *Node) error {
					require.Equal(t, []string{"name"}, propertyNames(node))
					seen = append(seen, node.ID)
					return nil
				}))
				require.ElementsMatch(t, expected, seen)
			})

			t.Run("canceled-caller", func(t *testing.T) {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				seen := 0
				err := scan(canceled, nil, func(*Node) error {
					seen++
					return nil
				})
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, seen)
			})

			t.Run("expired-deadline", func(t *testing.T) {
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				seen := 0
				err := scan(expired, nil, func(*Node) error {
					seen++
					return nil
				})
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Zero(t, seen)
			})

			t.Run("caller-cancels-mid-stream", func(t *testing.T) {
				canceled, cancel := context.WithCancel(ctx)
				defer cancel()
				seen := 0
				err := scan(canceled, nil, func(*Node) error {
					seen++
					cancel()
					return nil
				})
				require.ErrorIs(t, err, context.Canceled)
				require.Positive(t, seen)
				require.Less(t, seen, len(expected))
			})

			t.Run("no-partial-overlay-published", func(t *testing.T) {
				var seen []NodeID
				require.NoError(t, scan(ctx, nil, func(node *Node) error {
					seen = append(seen, node.ID)
					return nil
				}))
				require.ElementsMatch(t, expected, seen)
			})
		})
	}
}

// TestBulkReadCancellationTransactionPendingOverlayRetires covers the
// transaction route whose committed rows are merged with the transaction's
// pending node and edge overlays.
func TestBulkReadCancellationTransactionPendingOverlayRetires(t *testing.T) {
	b := createTestBadgerEngine(t)
	committed := seedCancellationNodes(t, b, 4)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	pendingNodes := make([]NodeID, 0, cancellationRouteRecords)
	for i := 0; i < cancellationRouteRecords; i++ {
		id := NodeID(fmt.Sprintf("test:pending-n-%04d", i))
		_, err := tx.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
		pendingNodes = append(pendingNodes, id)
	}
	for _, end := range committed {
		edgeID := EdgeID(fmt.Sprintf("test:pending-e-%s", end))
		require.NoError(t, tx.CreateEdge(t.Context(), &Edge{ID: edgeID, StartNode: "test:pending-n-0000", EndNode: end, Type: "R"}))
	}
	expectedNodes := append(append([]NodeID{}, committed...), pendingNodes...)
	ctx := context.Background()

	t.Run("healthy-control", func(t *testing.T) {
		nodes, err := tx.AllNodes(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, expectedNodes, testNodeIDs(nodes))
		edges, err := tx.AllEdges(ctx)
		require.NoError(t, err)
		require.Len(t, edges, len(committed))
		labeled, err := tx.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		require.ElementsMatch(t, expectedNodes, testNodeIDs(labeled))
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		nodes, err := tx.AllNodes(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, nodes)
		edges, err := tx.AllEdges(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, edges)
		labeled, err := tx.GetNodesByLabel(canceled, "Doc")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, labeled)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		nodes, err := tx.AllNodes(expired)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, nodes)
	})

	t.Run("overlay-not-poisoned-by-canceled-read", func(t *testing.T) {
		nodes, err := tx.AllNodes(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, expectedNodes, testNodeIDs(nodes))
	})
}

// TestBulkReadCancellationSnapshotScopedAndProjectedRoutesRetire covers the
// pinned snapshot stream, the namespace-scoped copy and the projected
// wrappers that copy a scanned population.
func TestBulkReadCancellationSnapshotScopedAndProjectedRoutesRetire(t *testing.T) {
	b := createTestBadgerEngine(t)
	expected := seedCancellationNodes(t, b, cancellationRouteRecords)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	ctx := context.Background()

	t.Run("snapshot-healthy-control", func(t *testing.T) {
		var seen []NodeID
		require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Doc", nil, func(node *Node) error {
			seen = append(seen, node.ID)
			return nil
		}))
		require.ElementsMatch(t, expected, seen)
	})

	t.Run("snapshot-canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		seen := 0
		err := tx.StreamNodesByLabelProjected(canceled, "Doc", nil, func(*Node) error {
			seen++
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, seen)
	})

	t.Run("snapshot-caller-cancels-mid-stream", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		defer cancel()
		seen := 0
		err := tx.StreamNodesByLabelProjected(canceled, "Doc", nil, func(*Node) error {
			seen++
			cancel()
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Positive(t, seen)
		require.Less(t, seen, len(expected))
	})

	t.Run("snapshot-stream-not-poisoned-by-canceled-read", func(t *testing.T) {
		var seen []NodeID
		require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Doc", nil, func(node *Node) error {
			seen = append(seen, node.ID)
			return nil
		}))
		require.ElementsMatch(t, expected, seen)
	})

	t.Run("scoped-route", func(t *testing.T) {
		namespaced := NewNamespacedEngine(b, "test")
		scopedExpected := make([]NodeID, 0, len(expected))
		for _, id := range expected {
			scopedExpected = append(scopedExpected, NodeID(id[len("test:"):]))
		}
		nodes, err := namespaced.AllNodes(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, scopedExpected, testNodeIDs(nodes))

		canceled, cancel := context.WithCancel(ctx)
		cancel()
		nodes, err = namespaced.AllNodes(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, nodes)
		seen := 0
		err = namespaced.StreamNodesByLabelProjected(canceled, "Doc", []string{"name"}, func(*Node) error {
			seen++
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, seen)
	})

	t.Run("projected-route", func(t *testing.T) {
		var seen []NodeID
		require.NoError(t, b.StreamNodesByLabelProjectedInScope(ctx, "test:", "Doc", []string{"name"}, func(node *Node) error {
			require.Equal(t, []string{"name"}, propertyNames(node))
			seen = append(seen, node.ID)
			return nil
		}))
		require.ElementsMatch(t, expected, seen)

		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		seen = nil
		err := b.StreamNodesByLabelProjectedInScope(expired, "test:", "Doc", []string{"name"}, func(node *Node) error {
			seen = append(seen, node.ID)
			return nil
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, seen)
	})
}

// propertyNames returns the projected property names of one node.
func propertyNames(node *Node) []string {
	names := make([]string, 0, len(node.Properties))
	for name := range node.Properties {
		names = append(names, name)
	}
	return names
}

// TestBulkReadCancellationHotAdjacencyKernelRetires drives the adjacency
// cache fast-path kernels directly: the public dispatcher checks the context
// before it takes any lock, so these are the calls that must observe a caller
// canceled after that check and still retire with its own error.
func TestBulkReadCancellationHotAdjacencyKernelRetires(t *testing.T) {
	b := createTestBadgerEngine(t)
	outgoing, incoming := seedCancellationHub(t, b)
	warmCancellationAdjacency(t, b)
	ctx := context.Background()

	t.Run("healthy-control-cached-adjacency", func(t *testing.T) {
		out, in, complete, err := b.cachedAdjacency(ctx, nil, "test:hub", true, true, visibleRead)
		require.NoError(t, err)
		require.True(t, complete, "the warmed caches must answer without a view")
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
		require.ElementsMatch(t, incoming, testEdgeIDs(in))
		reader, err := b.captureAdjacencyReader(ctx, "test:hub", true, true, visibleRead)
		require.NoError(t, err)
		require.Nil(t, reader.view, "a complete cached result needs no physical view")
		out, err = reader.read(ctx, true)
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		out, in, complete, err := b.cachedAdjacency(canceled, nil, "test:hub", true, true, visibleRead)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, complete)
		require.Nil(t, out)
		require.Nil(t, in)
		reader, err := b.captureAdjacencyReader(canceled, "test:hub", true, true, visibleRead)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, reader.view)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		_, _, _, err := b.cachedAdjacency(expired, nil, "test:hub", true, true, visibleRead)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		_, err = b.captureAdjacencyReader(expired, "test:hub", true, true, visibleRead)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("materialize-adjacency-bodies", func(t *testing.T) {
		b.invalidateCachesAfterRestore()
		bodies, err := b.materializeAdjEdges(ctx, outgoing, visibleRead)
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(bodies))
		b.invalidateCachesAfterRestore()
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		bodies, err = b.materializeAdjEdges(canceled, outgoing, visibleRead)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, bodies)
	})

	t.Run("cache-not-poisoned", func(t *testing.T) {
		// The materialize subtest dropped the body caches, so this healthy read
		// re-warms them through the dispatcher: a canceled read must never have
		// left a partial adjacency behind for it to serve.
		out, err := b.GetOutgoingEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, outgoing, testEdgeIDs(out))
		in, err := b.GetIncomingEdges(ctx, "test:hub")
		require.NoError(t, err)
		require.ElementsMatch(t, incoming, testEdgeIDs(in))
		cachedOut, cachedIn, complete, err := b.cachedAdjacency(ctx, nil, "test:hub", true, true, visibleRead)
		require.NoError(t, err)
		require.True(t, complete)
		require.ElementsMatch(t, outgoing, testEdgeIDs(cachedOut))
		require.ElementsMatch(t, incoming, testEdgeIDs(cachedIn))
	})
}

// TestBulkReadCancellationOverlayCaptureKernelRetires drives the write-behind
// overlay owner directly: a canceled caller retires the overlay copy instead
// of publishing a population nobody consumes.
func TestBulkReadCancellationOverlayCaptureKernelRetires(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	expected := seedCancellationNodes(t, b, cancellationRouteRecords, true)
	require.Positive(t, b.writeBehind.PendingOps())
	ctx := context.Background()
	capture := func(ctx context.Context) (bufferedEntityResult, error) {
		b.writeBehind.mu.RLock()
		defer b.writeBehind.mu.RUnlock()
		return b.readBufferedEntities(ctx, b.writeBehind, visibleRead, bufferedEntityRead{kind: bufferedLabel, label: "Doc", prefix: "test:"})
	}

	t.Run("healthy-control", func(t *testing.T) {
		result, err := capture(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, testNodeIDs(result.nodes))
		require.Len(t, result.nodesTouched, len(expected))
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		result, err := capture(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, result.nodes)
		require.Empty(t, result.nodesTouched)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		result, err := capture(expired)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Empty(t, result.nodes)
	})

	t.Run("overlay-not-poisoned", func(t *testing.T) {
		result, err := capture(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, testNodeIDs(result.nodes))
	})
}

// TestBulkReadCancellationTransactionMergeKernelRetires drives the pending
// overlay merge directly, where the transaction's committed rows and its own
// staged writes become one population.
func TestBulkReadCancellationTransactionMergeKernelRetires(t *testing.T) {
	tx := &BadgerTransaction{
		engine:       newTestMemoryEngine(t).BadgerEngine,
		Status:       TxStatusActive,
		pendingNodes: make(map[NodeID]*Node, 4),
		pendingEdges: make(map[EdgeID]*Edge, 1),
	}
	committed := make([]*Node, 0, cancellationRouteRecords)
	for i := 0; i < cancellationRouteRecords; i++ {
		committed = append(committed, &Node{ID: NodeID(fmt.Sprintf("test:c-%04d", i))})
	}
	for i := 0; i < 4; i++ {
		id := NodeID(fmt.Sprintf("test:p-%04d", i))
		tx.pendingNodes[id] = &Node{ID: id, Properties: map[string]any{"staged": true}}
	}
	tx.pendingEdges[EdgeID("test:e-pending")] = &Edge{ID: "test:e-pending", StartNode: "test:c-0000", EndNode: "test:p-0000", Type: "R"}
	ctx := context.Background()
	includeAll := func(*Node) bool { return true }
	includeAllEdges := func(*Edge) bool { return true }

	t.Run("healthy-control", func(t *testing.T) {
		merged, err := tx.mergePendingNodesLocked(ctx, committed, includeAll)
		require.NoError(t, err)
		require.Len(t, merged, len(committed)+len(tx.pendingNodes))
		edges, err := tx.mergePendingEdgesLocked(ctx, nil, includeAllEdges)
		require.NoError(t, err)
		require.Equal(t, []EdgeID{"test:e-pending"}, testEdgeIDs(edges))
		merged[0].Properties = map[string]any{"caller": true}
		require.Nil(t, committed[0].Properties, "the merge copies before it escapes")
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		merged, err := tx.mergePendingNodesLocked(canceled, committed, includeAll)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, merged)
		edges, err := tx.mergePendingEdgesLocked(canceled, nil, includeAllEdges)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, edges)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		merged, err := tx.mergePendingNodesLocked(expired, committed, includeAll)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, merged)
	})
}

// TestBulkReadCancellationMVCCNodeWalkKernelRetires drives the MVCC head walk
// that materializes a pinned snapshot's node population.
func TestBulkReadCancellationMVCCNodeWalkKernelRetires(t *testing.T) {
	b := createTestBadgerEngine(t)
	expected := seedCancellationNodes(t, b, cancellationRouteRecords)
	ctx := context.Background()
	version := maxMVCCVersion()
	walk := func(ctx context.Context) ([]NodeID, error) {
		var seen []NodeID
		err := b.withView(func(txn *badger.Txn) error {
			return b.iterateNodesVisibleAtInScopeInTxn(ctx, txn, "test:", version, func(node *Node) error {
				seen = append(seen, node.ID)
				return nil
			})
		})
		return seen, err
	}

	t.Run("healthy-control", func(t *testing.T) {
		seen, err := walk(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, seen)
	})

	t.Run("canceled-caller", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		seen, err := walk(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, seen)
	})

	t.Run("expired-deadline", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		defer cancel()
		seen, err := walk(expired)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, seen)
	})
}
