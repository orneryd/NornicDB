package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

type storedCancelBoundary struct{ cancel context.CancelFunc }

func (b storedCancelBoundary) WithStoredEntityRead(_ StoredEntityReadKind, _ string, read func() error) error {
	b.cancel()
	return read()
}

func TestStoredReadCancellationBeforePayload(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, id := range []NodeID{"d7:a", "d7:b"} {
		storedFactNode(t, b, id, "L")
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "d7:e", StartNode: "d7:a", EndNode: "d7:b", Type: "R", Properties: map[string]any{"key": "payload"}}))
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		for _, edge := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			b.updateStoredEntityOwners(func(registry storedEntityObserverRegistry) {
				registry["d7"] = storedEntityObserverEntry{boundary: storedCancelBoundary{cancel}}
			})
			var work StoredReadWork
			r := StoredReadRequest{Mode: mode, NodeProperties: []string{"key"}, EdgeProperties: []string{"key"}, Work: &work}
			if edge {
				r.Edges = []EdgeID{"d7:e"}
			} else {
				r.Nodes = []NodeID{"d7:a"}
			}
			result, err := ReadStored(ctx, b, r)
			cancel()
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			require.Zero(t, work.FullMaterializations)
			require.Zero(t, work.PropertyValues)
			require.Zero(t, work.VectorValues)
		}
	}
}

// Err triggers a real cancel after a fixed amount of actual decoder checking,
// avoiding timer/CPU-speed assumptions. Its Done channel and final Err are the
// ordinary cancel context's, and it stays canceled once the budget is consumed.
type storedDecodeCancelContext struct {
	context.Context
	cancel         context.CancelFunc
	checks, budget int
}

func (c *storedDecodeCancelContext) Err() error {
	c.checks++
	if c.checks > c.budget {
		c.cancel()
	}
	return c.Context.Err()
}
func TestStoredReadCancellationDuringPayload(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i)
	}
	_, err := b.CreateNode(&Node{ID: "d7:n", Labels: []string{"L"}, Properties: map[string]any{"nested": values}})
	require.NoError(t, err)
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		parent, cancel := context.WithCancel(context.Background())
		ctx := &storedDecodeCancelContext{Context: parent, cancel: cancel, budget: 128}
		result, err := ReadStored(ctx, b, StoredReadRequest{Mode: mode, Nodes: []NodeID{"d7:n"}, NodeProperties: []string{"nested"}})
		cancel()
		if mode == StoredMetadataOnly {
			require.NoError(t, err)
			require.NotNil(t, result)
			require.LessOrEqual(t, ctx.checks, ctx.budget)
		} else {
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, result)
			require.Greater(t, ctx.checks, ctx.budget)
		}
	}
}
