package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCurrentWALRejectsOtherFormatsWithoutChangingFiles(t *testing.T) {
	for version := byte(1); version <= 4; version++ {
		t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
			dir := t.TempDir()
			entry := WALEntry{Sequence: 1, Timestamp: time.Now().UTC(), Operation: OpTxBegin, Data: []byte(`{"tx_id":"old"}`)}
			entry.Checksum = crc32Checksum(entry.Data)
			var payload []byte
			var err error
			if version < 3 {
				payload, err = json.Marshal(entry)
			} else if version == 3 {
				payload, err = processWALEntryPayload(walPayloadEncode, &bytes.Buffer{}, &entry, nil, 0)
			} else {
				payload, err = encodeWALPropertyEpochFrame(walPropertyEpochFrame{Format: 1, Through: "ffffffffffffffff", Last: "ffffffffffffffff"}, 1, entry.Timestamp)
			}
			require.NoError(t, err)
			raw, _ := buildAtomicRecord(payload, version)
			path := walActivePath(dir)
			require.NoError(t, os.WriteFile(path, raw, 0600))
			wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
			if wal != nil {
				t.Cleanup(func() { _ = wal.Close() })
			}
			require.ErrorContains(t, err, "unsupported WAL")
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, raw, after)
		})
	}
	t.Run("headerless-json", func(t *testing.T) {
		dir := t.TempDir()
		path := walActivePath(dir)
		raw := []byte("{\"seq\":1,\"op\":\"checkpoint\"}\n")
		require.NoError(t, os.WriteFile(path, raw, 0600))
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
		if wal != nil {
			t.Cleanup(func() { _ = wal.Close() })
		}
		require.ErrorContains(t, err, "unsupported WAL")
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, raw, after)
	})

	t.Run("old-segment-does-not-repair-active-tail", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0700))
		old, _ := buildAtomicRecord([]byte(`{}`), 2)
		segment := filepath.Join(walSegmentsDir(dir), "seg-00000000000000000001-00000000000000000001.wal")
		require.NoError(t, os.WriteFile(segment, old, 0600))
		active := walActivePath(dir)
		torn := []byte("WA")
		require.NoError(t, os.WriteFile(active, torn, 0600))
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "none"})
		if wal != nil {
			t.Cleanup(func() { _ = wal.Close() })
		}
		require.ErrorContains(t, err, "unsupported WAL")
		after, err := os.ReadFile(active)
		require.NoError(t, err)
		require.Equal(t, torn, after)
		after, err = os.ReadFile(segment)
		require.NoError(t, err)
		require.Equal(t, old, after)
	})
}

func TestRecoveryRejectsUnsupportedWALBeforeRestoringSnapshot(t *testing.T) {
	dir := t.TempDir()
	old, _ := buildAtomicRecord([]byte(`{}`), 2)
	path := walActivePath(dir)
	require.NoError(t, os.WriteFile(path, old, 0600))
	snapshot := filepath.Join(t.TempDir(), "snapshot.nds")
	require.NoError(t, SaveSnapshot(&Snapshot{Nodes: []*Node{{ID: "test:from-snapshot"}}}, snapshot))
	destination := newTestMemoryEngine(t)
	_, _, err := RecoverIntoEngine(destination, dir, snapshot)
	require.ErrorContains(t, err, "unsupported WAL")
	count, err := destination.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count, "incompatible WAL must fail before restoring any snapshot")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, old, after)
}
