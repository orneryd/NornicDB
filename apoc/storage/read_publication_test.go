package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type cancelOnReadCopy struct {
	enabled *atomic.Bool
	cancel  context.CancelFunc
}

func (value cancelOnReadCopy) CallerCopy() any {
	if value.enabled.Load() {
		value.cancel()
	}
	return value
}

func TestOwnedPointReadDoesNotPublishAfterCallerStopsDuringCopy(t *testing.T) {
	for _, kind := range []string{"node", "relationship"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var enabled atomic.Bool
			store := NewInMemoryStorage()
			properties := map[string]any{"probe": cancelOnReadCopy{&enabled, cancel}}
			node, err := store.CreateNode(t.Context(), nil, properties)
			require.NoError(t, err)
			end, err := store.CreateNode(t.Context(), nil, nil)
			require.NoError(t, err)
			rel, err := store.CreateRelationship(t.Context(), node.ID, end.ID, "LINK", properties)
			require.NoError(t, err)
			enabled.Store(true)
			if kind == "node" {
				result, err := store.GetNode(ctx, node.ID)
				require.ErrorIs(t, ctx.Err(), context.Canceled, "the public read must really encounter the copy-time cancellation")
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			} else {
				result, err := store.GetRelationship(ctx, rel.ID)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			}
			enabled.Store(false)
			if kind == "node" {
				retry, err := store.GetNode(t.Context(), node.ID)
				require.NoError(t, err)
				require.Equal(t, node.ID, retry.ID)
			} else {
				retry, err := store.GetRelationship(t.Context(), rel.ID)
				require.NoError(t, err)
				require.Equal(t, rel.ID, retry.ID)
			}
		})
	}
}
