package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectedNativeBackupRestoresOnlyToProjectedStore(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	open := func(dir string, projected bool) *BadgerEngine {
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, EncryptionKey: key, Projected: projected})
		require.NoError(t, err)
		return engine
	}
	source := open(t.TempDir(), true)
	defer source.Close()
	large := make([]byte, 2<<20)
	for i := range large {
		large[i] = byte(i*131 + i/257)
	}
	id, err := source.CreateNode(t.Context(), &Node{ID: "ns:backup", Properties: map[string]any{"small": int64(3), "large": large}})
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "projected.native")
	require.NoError(t, source.Backup(backup))

	projected := open(t.TempDir(), true)
	defer projected.Close()
	require.NoError(t, projected.Restore(backup))
	selected, err := projected.GetNodeProjected(t.Context(), id, []string{"small"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"small": int64(3)}, selected.Properties)
	full, err := projected.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, large, full.Properties["large"])

	defaultStore := open(t.TempDir(), false)
	defer defaultStore.Close()
	_, err = defaultStore.CreateNode(t.Context(), &Node{ID: "ns:unchanged"})
	require.NoError(t, err)
	require.ErrorIs(t, defaultStore.Restore(backup), ErrInvalidData)
	_, err = defaultStore.GetNode(t.Context(), "ns:unchanged")
	require.NoError(t, err, "mode mismatch must fail before replacing the destination")
}
