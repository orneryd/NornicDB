package storage

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEI4R11EpochStartupRefusalKeepsPendingKey(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true})
	require.NoError(t, err)
	require.NoError(t, engine.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
	require.NoError(t, engine.Close())
	frame := walPropertyEpochFrame{Format: 1, Epoch: &WALPropertyEpoch{state: &walPropertyEpochState{
		Version: 1, ID: "epoch", DictionaryID: "dictionary", Bindings: []byte{1},
	}}, Through: "0", Last: "1"}
	idSize := int(walMaxEntrySize)-1024
	var raw []byte
	for i := 0; i < 8; i++ {
		frame.Epoch.state.ID = strings.Repeat("x", idSize)
		data, err := json.Marshal(frame)
		require.NoError(t, err)
		raw = b6HistoricalFrame(t, 1, OpCheckpoint, data)
		size := int(binary.LittleEndian.Uint32(raw[5:]))
		if size <= int(walMaxEntrySize) && size >= int(walMaxEntrySize)-8 { break }
		idSize += int(walMaxEntrySize)-1-size
	}
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.MkdirAll(walDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), raw, 0600))
	before := walDirDigest(t, walDir)
	for attempt := 1; attempt <= 2; attempt++ {
		require.NoError(t, ValidateWALFormat(walDir, WALValidateV4Upgrade), "the preflight used by nornicdb.Open")
		engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, AllowStorageUpgrade: true, CatalogUpgradeV4: graphOnlyCatalogUpgradeV4()})
		if engine != nil { _ = engine.Close() }
		require.ErrorContains(t, err, "property dictionary epoch exceeds maximum frame size")
		_, backupErr := os.Stat(walDir + v4WALBackupSuffix)
		require.ErrorIs(t, backupErr, os.ErrNotExist, "failed conversion must not publish a backup")
		require.Equal(t, before, walDirDigest(t, walDir), "failed conversion must preserve WAL files")
		read, readErr := badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil))
		require.NoError(t, readErr)
		pendingErr := read.View(func(tx *badger.Txn) error { _, err := tx.Get(v4WALRecoveryKey); return err })
		require.NoError(t, read.Close())
		t.Logf("attempt=%d open=%v pending_key=%v", attempt, err, pendingErr)
		require.NoError(t, pendingErr, "the refused startup already persisted the pending key")
	}
}
