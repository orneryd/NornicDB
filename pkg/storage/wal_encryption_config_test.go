package storage

import (
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestEncryptedWALSnapshotKeepsStartupKeyAfterCallerMutation(t *testing.T) {
	dir := t.TempDir()
	original := encryptedWALTestKey(21)
	supplied := append([]byte(nil), original...)
	cfg := encryptedWALTestConfig(filepath.Join(dir, "wal"), supplied)
	wal, err := NewWAL(cfg.Dir, cfg)
	require.NoError(t, err)
	base := fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed)
	owner := NewWALEngine(base, wal)
	t.Cleanup(func() { _ = owner.Close() })

	// Callers can reuse their configuration after NewWAL. A later mutation of
	// its key slice must not make recovery snapshots unreadable with the key
	// that sealed the journal at startup.
	supplied[0] ^= 0xff
	require.NoError(t, owner.CreateRecoveryBaseline(t.Context(), filepath.Join(dir, "snapshots")))
	files, err := filepath.Glob(filepath.Join(dir, "snapshots", "snapshot-*.json"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	_, err = LoadSnapshotWithKey(t.Context(), files[0], original)
	require.NoError(t, err)
}
