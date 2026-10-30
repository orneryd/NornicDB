package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDeleteGraphNodeInvalidModeDoesNotStage(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:mode"})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.ErrorIs(t, tx.DeleteGraphNode("test:mode", NodeDeletionMode(255)), ErrInvalidData)
	require.Zero(t, tx.PendingChanges(false).Operations)
	require.Empty(t, tx.Namespace(), "invalid policy does not pin a transaction namespace")
	_, err = tx.GetNode("test:mode")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = engine.GetNode("test:mode")
	require.NoError(t, err)
}
