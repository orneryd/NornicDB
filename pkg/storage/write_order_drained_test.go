package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The drained-write owner admits nothing once the engine is closed.
func TestDrainedWriteRejectedAfterClose(t *testing.T) {
	e := newOwnerTestEngine(t, true)
	require.NoError(t, e.Close())
	_, err := e.RepairOrphanedEmbeddingKeys()
	require.ErrorIs(t, err, ErrStorageClosed)
}
