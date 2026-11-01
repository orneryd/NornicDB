package storage

// Reproduction 1 on the current release. Author: storage-format-owners lane
// (Claude), #1693, 1 November 2026. Expanded to the batch consumers by the
// storage-format-owners lane (Codex), 1 November 2026.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cancelAtCheck cancels itself at its nth Err call: during a read's admission.
type cancelAtCheck struct {
	context.Context
	cancel           context.CancelFunc
	checks, cancelAt int
}

func (c *cancelAtCheck) Err() error {
	c.checks++
	if c.checks == c.cancelAt {
		c.cancel()
	}
	return c.Context.Err()
}

func TestReadAdmissionObservesCancellation(t *testing.T) {
	b := newTestEngine(t)
	ids := seedCancellationNodes(t, b, 2*bulkReadCancellationInterval)
	for _, read := range []struct {
		name string
		load func(context.Context, []NodeID) (map[NodeID]*Node, error)
	}{
		{"latest-visible", b.BatchGetNodesLatestVisible},
		{"full", b.BatchGetNodes},
		{"without-embeddings", b.BatchGetNodesWithoutEmbeddings},
	} {
		t.Run(read.name, func(t *testing.T) {
			nodes, err := read.load(t.Context(), ids)
			require.NoError(t, err)
			require.Len(t, nodes, len(ids), "healthy control materializes the batch")
			t.Run("cancelled before the read", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				nodes, err := read.load(ctx, ids)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, nodes)
			})
			t.Run("deadline expired before the read", func(t *testing.T) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				nodes, err := read.load(ctx, ids)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, nodes)
			})
			t.Run("cancelled during admission of an unrelated drop", func(t *testing.T) {
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
				inner, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				ctx := &cancelAtCheck{Context: inner, cancel: cancel, cancelAt: 3}
				nodes, err := read.load(ctx, ids)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, nodes)
				require.Less(t, ctx.checks, len(ids), "the read stops soon after the cancellation")
			})
		})
	}
}
