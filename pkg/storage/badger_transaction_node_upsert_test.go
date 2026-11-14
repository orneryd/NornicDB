package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeUpsertResolvesStoredPredecessorBeforeAccounting(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "suppressed_update"}[exists], func(t *testing.T) {
			engine := newTestEngine(t)
			created := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
			if exists {
				_, err := engine.CreateNode(t.Context(), &Node{ID: "test:catalog", CreatedAt: created, Properties: map[string]interface{}{"value": "old"}})
				require.NoError(t, err)
				stored, err := engine.GetStoredNode(t.Context(), "test:catalog")
				require.NoError(t, err)
				require.True(t, stored.CreatedAt.Equal(created))
				created = stored.CreatedAt
				require.NoError(t, engine.SetManualSuppression(t.Context(), StoredEntityNodeKind, "test:catalog", true))
			}
			tx, err := engine.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			calls := 0
			err = tx.UpsertGraphNode(t.Context(), &Node{ID: "test:catalog", Properties: map[string]interface{}{"value": "new"}}, func(op Operation, apply func() error) error {
				calls++
				if exists {
					require.Equal(t, OpUpdateNode, op.Type)
					require.Equal(t, "old", op.OldNode.Properties["value"])
					require.Equal(t, created, op.Node.CreatedAt)
				} else {
					require.Equal(t, OpCreateNode, op.Type)
					require.Nil(t, op.OldNode)
				}
				return apply()
			})
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.NoError(t, tx.Commit())
			node, err := engine.GetStoredNode(t.Context(), "test:catalog")
			require.NoError(t, err)
			require.Equal(t, "new", node.Properties["value"])
			require.False(t, node.CreatedAt.IsZero())
			if exists {
				require.Equal(t, created, node.CreatedAt)
				_, err = engine.GetNode(t.Context(), "test:catalog")
				require.ErrorIs(t, err, ErrNotFound)
			}
		})
	}
}

func TestNodeUpsertRefusalAndCancellationStageNothing(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	refusal := errors.New("accounting refused")
	require.ErrorIs(t, tx.UpsertGraphNode(t.Context(), &Node{ID: "test:refused"}, func(Operation, func() error) error { return refusal }), refusal)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, tx.UpsertGraphNode(ctx, &Node{ID: "test:canceled"}, nil), context.Canceled)
	require.ErrorIs(t, tx.UpsertGraphNode(t.Context(), nil, nil), ErrInvalidData)
	require.NoError(t, tx.Commit())
	for _, id := range []NodeID{"test:refused", "test:canceled"} {
		_, err := engine.GetStoredNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound)
	}
}
