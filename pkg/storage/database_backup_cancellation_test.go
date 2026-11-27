package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type cancelBackupWriter struct{ cancel context.CancelFunc }

func (w cancelBackupWriter) Write(data []byte) (int, error) {
	w.cancel()
	return len(data), nil
}

func TestNativeBackupToReportsCancellationDuringWrite(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(t.Context(), &Node{ID: "backup:node"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.ErrorIs(t, engine.BackupTo(ctx, cancelBackupWriter{cancel: cancel}), context.Canceled)
	require.NoError(t, engine.BackupTo(t.Context(), io.Discard))
}

type cancelingBackupStore struct {
	Engine
	cancel context.CancelFunc
}

func (s cancelingBackupStore) BackupTo(_ context.Context, dst io.Writer) error {
	_, err := io.WriteString(dst, "unfinished backup")
	s.cancel()
	return err
}

func TestDatabaseBackupCancellationPreservesPublishedFile(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		name := "shared"
		if mixed {
			name = "mixed"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "existing.backup")
			require.NoError(t, os.WriteFile(path, []byte("previous backup"), 0o600))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			store := cancelingBackupStore{cancel: cancel}
			var err error
			if mixed {
				err = WriteDatabaseBackupArchive(ctx, path, map[string]Engine{"": store, uuid.NewString(): store})
			} else {
				err = WriteNativeDatabaseBackup(ctx, path, store)
			}
			require.ErrorIs(t, err, context.Canceled)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "previous backup", string(data))
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "temporary backup removed")
		})
	}
}
