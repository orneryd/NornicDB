package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

// Author: #1667 implementation lane (Codex). These controls qualify the two
// extracted preparation mechanisms. The consuming ReadStored public contract
// remains in #1644 and requires composition qualification there.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type foundationPreparationContext struct {
	context.Context
	cancel           context.CancelFunc
	checks, cancelAt int
}

func (c *foundationPreparationContext) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

func foundationCancelDuringPreparation(t *testing.T, at int) *foundationPreparationContext {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &foundationPreparationContext{Context: ctx, cancel: cancel, cancelAt: at}
}

func TestCapturedPreparationAdmissionContext(t *testing.T) {
	b := newReadSnapshotTestEngine(t, false)
	b.prefixDrops.mu.Lock()
	if b.prefixDrops.pending == nil {
		b.prefixDrops.pending = make(map[string]*namespaceDropIntent)
	}
	b.prefixDrops.pending["other"] = &namespaceDropIntent{namespace: "other"}
	b.prefixDrops.pendingCount.Store(1)
	b.prefixDrops.mu.Unlock()
	defer func() {
		b.prefixDrops.mu.Lock()
		delete(b.prefixDrops.pending, "other")
		b.prefixDrops.pendingCount.Store(0)
		b.prefixDrops.mu.Unlock()
	}()
	ids := make([]NodeID, 128)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("test:n%d", i))
	}
	ctx := foundationCancelDuringPreparation(t, 3)
	captured := false
	view, err := b.captureReadView(ctx, func(*WriteBehindBuffer) error { captured = true; return nil }, readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: ids})
	if view != nil {
		view.Close()
	}
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, captured, "cancelled admission must not open the capture")
	require.Nil(t, view)
	require.Less(t, ctx.checks, len(ids))
	require.ErrorIs(t, b.pendingReadConflict(context.Background(), readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: []NodeID{"other:n"}}), ErrConflict)
	view, err = b.captureReadView(context.Background(), nil, readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: ids})
	require.NoError(t, err)
	require.NotNil(t, view)
	view.Close()
}

func TestCapturedPreparationAdjacencyContext(t *testing.T) {
	for _, outgoing := range []bool{false, true} {
		for _, tombstones := range []bool{false, true} {
			t.Run(fmt.Sprintf("outgoing=%t/tombstones=%t", outgoing, tombstones), func(t *testing.T) {
				active, draining := newCommitBuffer(), newCommitBuffer()
				for i := 0; i < 128; i++ {
					id := EdgeID(fmt.Sprintf("test:e%d", i))
					start, end := NodeID("test:root"), NodeID("test:other")
					if !outgoing {
						start, end = end, start
					}
					edge := &Edge{ID: id, StartNode: start, EndNode: end, Type: "R"}
					draining.addEdge(edge)
					if tombstones {
						active.deletedEdgeIDs[id] = struct{}{}
					} else {
						active.addEdge(edge)
					}
				}
				w := &WriteBehindBuffer{active: active, draining: []*CommitBuffer{draining}}
				b := &BadgerEngine{}
				request := bufferedEntityRead{kind: bufferedAdjacency, source: "test:root", outgoing: outgoing}
				w.mu.RLock()
				defer w.mu.RUnlock()
				ctx := foundationCancelDuringPreparation(t, 11)
				result, err := b.readBufferedEntities(ctx, w, storedRead, request)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, bufferedEntityResult{}, result, "preparation failure must discard partial candidates and masks")
				require.Less(t, ctx.checks, 128)
				current, err := b.readBufferedEntities(context.Background(), w, storedRead, request)
				require.NoError(t, err)
				require.Len(t, current.suppressed, 128)
				if tombstones {
					require.Empty(t, current.edges)
				} else {
					require.Len(t, current.edges, 128)
				}
			})
		}
	}
	var w *WriteBehindBuffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err := w.edgeAdjacencyOverlayLocked(ctx, "test:root", true)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCapturedPreparationDeletionEnumerationStops(t *testing.T) {
	for _, kind := range []bufferedEntityKind{bufferedNodeEntity, bufferedEdgeEntity} {
		for _, generic := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%d/generic=%t", kind, generic), func(t *testing.T) {
				b := newCommitBuffer()
				for i := 0; i < 128; i++ {
					id := fmt.Sprintf("test:deleted%d", i)
					if generic {
						b.deletes[id] = kind
					} else if kind == bufferedNodeEntity {
						b.deletedNodeIDs[NodeID(id)] = struct{}{}
					} else {
						b.deletedEdgeIDs[EdgeID(id)] = struct{}{}
					}
				}
				calls := 0
				b.deletedEntityIDs(kind, func(string) bool { calls++; return false })
				require.Equal(t, 1, calls, "cancellation stop must stop the deletion producer itself")
			})
		}
	}
}
