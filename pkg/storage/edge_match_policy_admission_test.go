package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEdgeMatchReadPoliciesRejectInvalidRequestsAtEveryNativeBoundary(t *testing.T) {
	engine := newTestMemoryEngine(t)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	readers := map[string]EdgesBetweenMatcher{
		"engine":          engine,
		"transaction":     tx,
		"namespace":       NewNamespacedEngine(engine, "test"),
		"empty composite": NewCompositeEngine(nil, nil, nil),
	}
	for name, reader := range readers {
		t.Run(name, func(t *testing.T) {
			called := false
			match := func(*Edge) bool { called = true; return true }
			_, err := reader.MatchEdgesBetween(t.Context(), "a", "b", "LINK", nil, match, EdgeMatchReadPolicy(255))
			require.ErrorIs(t, err, ErrInvalidData)
			require.False(t, called)
			_, err = reader.MatchEdgesBetween(t.Context(), "a", "b", "LINK", nil, nil, SnapshotEdgeMatch)
			require.ErrorIs(t, err, ErrInvalidData)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = reader.MatchEdgesBetween(ctx, "a", "b", "LINK", nil, match, SnapshotEdgeMatch)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, called)
		})
	}
}
