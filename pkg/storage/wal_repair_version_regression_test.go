package storage

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALUnknownVersionDoesNotTruncateOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := walActivePath(dir)
	future, _ := buildAtomicRecord([]byte(`{"future":"valid record"}`), walFormatVersionV3+1)
	require.NoError(t, os.WriteFile(path, future, 0600))
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
	if wal != nil {
		require.NoError(t, wal.Close())
	}
	assert.ErrorContains(t, err, "unsupported WAL version")
	after, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	assert.Equal(t, future, after, "a future format is not a corrupt tail to truncate")
}

func TestWALUnknownVersionDoesNotTruncateDuringRecovery(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "materialized", true: "streaming"}[streaming], func(t *testing.T) {
			dir := t.TempDir()
			path := walActivePath(dir)
			payload, err := json.Marshal(WALEntry{Sequence: 1, Operation: OpCheckpoint, Data: json.RawMessage(`{}`), Checksum: crc32Checksum([]byte(`{}`))})
			require.NoError(t, err)
			good, _ := buildAtomicRecord(payload, 2)
			future, _ := buildAtomicRecord([]byte(`{"future":"record"}`), walFormatVersionV3+1)
			original := append(good, future...)
			require.NoError(t, os.WriteFile(path, original, 0600))
			if streaming {
				target := NewMemoryEngine()
				t.Cleanup(func() { _ = target.Close() })
				_, _, err = RecoverIntoEngine(target, dir, "")
			} else {
				var target *MemoryEngine
				target, _, err = RecoverFromWALWithResult(dir, "")
				if target != nil {
					_ = target.Close()
				}
			}
			assert.ErrorContains(t, err, "unsupported WAL version")
			after, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			assert.Equal(t, original, after)
		})
	}
}
