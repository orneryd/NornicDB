package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCPruningRejectsClosedEngineWithoutAccessingRetiredState(t *testing.T) {
	engine := newTestEngine(t)
	require.NoError(t, engine.Close())
	require.NotPanics(t, func() {
		deleted, err := engine.PruneMVCCVersions(t.Context(), MVCCPruneOptions{})
		require.ErrorIs(t, err, ErrStorageClosed)
		require.Zero(t, deleted)
	})
}
