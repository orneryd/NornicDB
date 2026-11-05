package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeleteGraphNodeInvalidModeDoesNotStage(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "test:mode"})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.ErrorIs(t, tx.DeleteGraphNode(t.Context(), "test:mode", NodeDeletionMode(255)), ErrInvalidData)
	require.Zero(t, tx.PendingChanges(false).Operations)
	require.Empty(t, tx.Namespace(), "invalid policy does not pin a transaction namespace")
	_, err = tx.GetNode(t.Context(), "test:mode")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = engine.GetNode(t.Context(), "test:mode")
	require.NoError(t, err)
}
