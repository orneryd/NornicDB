package localization

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageWALLogEventsPreserveStableIdentityAndMachineFields(t *testing.T) {
	incomplete := StorageWALIncompleteWriteEvent()
	require.Equal(t, EventStorageWALIncompleteWrite, incomplete.ID)
	require.Equal(t, "wal recovery: detected incomplete write at end", incomplete.Message.Fallback)
	require.Equal(t, "crash_recovery", incomplete.Attrs[0].Value.String())

	skipped := StorageWALCorruptedEmbeddingsSkippedEvent(3, "legacy")
	require.Equal(t, EventStorageWALCorruptedEmbeddingsSkipped, skipped.ID)
	require.Equal(t, "wal recovery: skipped corrupted embedding entries", skipped.Message.Fallback)
	require.Equal(t, int64(3), skipped.Attrs[0].Value.Int64())
	require.Equal(t, "legacy", skipped.Attrs[1].Value.String())
}
