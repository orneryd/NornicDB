package multidb

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelAfterBackupHeader struct {
	source io.Reader
	cancel context.CancelFunc
}

func (r cancelAfterBackupHeader) Read(data []byte) (int, error) {
	n, err := r.source.Read(data[:min(len(data), 8)])
	r.cancel()
	return n, err
}

func TestOfflineRestoreCancellationStopsNativeLoad(t *testing.T) {
	manager, backing := newCatalogPublicationProbe(t)
	t.Cleanup(func() { _ = manager.Close() })
	var backup bytes.Buffer
	require.NoError(t, backing.native.BackupTo(t.Context(), &backup))
	require.Greater(t, backup.Len(), 8)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	directory := t.TempDir()
	inspect := func(*storage.BadgerEngine) error { return nil }
	reader := cancelAfterBackupHeader{source: bytes.NewReader(backup.Bytes()), cancel: cancel}
	require.ErrorIs(t, restoreNativeStore(ctx, directory, nil, reader, inspect), context.Canceled)
	require.NoError(t, restoreNativeStore(t.Context(), directory, nil, bytes.NewReader(backup.Bytes()), inspect), "cancel releases the temporary store handle")
}
