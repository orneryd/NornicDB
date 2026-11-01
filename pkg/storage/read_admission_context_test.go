package storage

// Reproduction 1 on the current release. Author: storage-format-owners lane
// (Claude), #1693, 1 November 2026.

import (
	"context"
	"fmt"
	"testing"

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
	ids := make([]NodeID, 128)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("test:n%d", i))
	}
	t.Run("cancelled before the read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		nodes, err := b.BatchGetNodesLatestVisible(ctx, ids)
		require.ErrorIs(t, err, context.Canceled)
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
		nodes, err := b.BatchGetNodesLatestVisible(ctx, ids)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, nodes)
		require.Less(t, ctx.checks, len(ids), "the read stops soon after the cancellation")
	})
}
