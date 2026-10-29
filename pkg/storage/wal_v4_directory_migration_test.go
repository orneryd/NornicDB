package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func v4MigrationDirectoryFixture(t *testing.T) (string, string, string, []byte, []byte) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0700))
	makeEntry := func(seq uint64) []byte {
		data := []byte(fmt.Sprintf(`{"tx_id":"tx-%d"}`, seq))
		return priorWALRecord(t, 3, WALEntry{Sequence: seq, Timestamp: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Operation: OpTxBegin, Data: data, Checksum: crc32Checksum(data)})
	}
	oldSegment, oldActive := makeEntry(1), makeEntry(2)
	name := fmt.Sprintf("seg-%020d-%020d.wal", 1, 1)
	segment := filepath.Join(walSegmentsDir(dir), name)
	active := walActivePath(dir)
	require.NoError(t, os.WriteFile(segment, oldSegment, 0600))
	require.NoError(t, os.WriteFile(active, oldActive, 0600))
	{
		_, manifestErr := accessWALManifest(dir, walPublishManifest, &WALManifest{Segments: []WALSegment{{FirstSeq: 1, LastSeq: 1, Path: name}}})
		require.NoError(t, manifestErr)
	}
	return dir, segment, active, oldSegment, oldActive
}

func TestV4UpgradeDirectoryResumesAfterInterruptedPublication(t *testing.T) {
	dir, segment, active, oldSegment, oldActive := v4MigrationDirectoryFixture(t)
	injected := errors.New("interrupt after first replacement")
	changed, err := UpgradeWALV4(dir, WALV4UpgradeOptions{afterPublish: func(string) error { return injected }})
	require.ErrorIs(t, err, injected)
	require.False(t, changed)
	backup := dir + v4WALBackupSuffix
	backupSegment, err := os.ReadFile(filepath.Join(backup, "segments", filepath.Base(segment)))
	require.NoError(t, err)
	require.Equal(t, oldSegment, backupSegment)
	backupActive, err := os.ReadFile(filepath.Join(backup, "wal.log"))
	require.NoError(t, err)
	require.Equal(t, oldActive, backupActive)
	currentSegment, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.Equal(t, walFormatVersionCurrent, currentSegment[4])
	currentActive, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, oldActive, currentActive)
	changed, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.True(t, changed)
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Sequence)
	require.Equal(t, uint64(2), entries[1].Sequence)
	changed, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.False(t, changed, "an already converted WAL is a no-op")
}

func TestV4UpgradeDirectoryPreservesSegmentWithTornActiveMagic(t *testing.T) {
	dir, _, active, _, _ := v4MigrationDirectoryFixture(t)
	require.NoError(t, os.WriteFile(active, []byte("WAL"), 0600))
	changed, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.True(t, changed)
	original, err := os.ReadFile(filepath.Join(dir+v4WALBackupSuffix, "wal.log"))
	require.NoError(t, err)
	require.Equal(t, []byte("WAL"), original)
	converted, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Empty(t, converted)
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(1), entries[0].Sequence)
}

func TestV4UpgradeDirectoryPreservesPreviousMigrationBackup(t *testing.T) {
	dir, _, _, _, _ := v4MigrationDirectoryFixture(t)
	oldBackup := dir + ".before-v4"
	require.NoError(t, os.Mkdir(oldBackup, 0700))
	marker := filepath.Join(oldBackup, "previous-backup")
	require.NoError(t, os.WriteFile(marker, []byte("keep"), 0600))
	changed, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.True(t, changed)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = os.Stat(dir + v4WALBackupSuffix)
	require.NoError(t, err)
}

func TestV4UpgradeDirectoryRejectsChangedSourceAfterInterruption(t *testing.T) {
	dir, _, active, _, oldActive := v4MigrationDirectoryFixture(t)
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{afterPublish: func(string) error { return errors.New("stop") }})
	require.Error(t, err)
	modified := append(bytes.Clone(oldActive), []byte("unexpected")...)
	require.NoError(t, os.WriteFile(active, modified, 0600))
	_, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.Error(t, err)
	after, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, modified, after)
}

func TestV4UpgradeDirectoryValidatesEveryFileBeforeReplacement(t *testing.T) {
	dir, segment, active, oldSegment, oldActive := v4MigrationDirectoryFixture(t)
	oldActive[10] ^= 1
	require.NoError(t, os.WriteFile(active, oldActive, 0600))
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.Error(t, err)
	after, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.Equal(t, oldSegment, after)
	after, err = os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, oldActive, after)
}

func TestV4UpgradeDirectoryTransformFailureLeavesSourcesUntouched(t *testing.T) {
	dir, segment, active, oldSegment, oldActive := v4MigrationDirectoryFixture(t)
	want := errors.New("cannot convert payload")
	visits := 0
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{Transform: func(version uint8, operation OperationType, payload []byte) ([]byte, error) {
		require.Equal(t, uint8(3), version)
		require.Equal(t, OpTxBegin, operation)
		visits++
		if visits == 2 {
			return nil, want
		}
		return payload, nil
	}})
	require.ErrorIs(t, err, want)
	require.Equal(t, 2, visits)
	segmentAfter, err := os.ReadFile(segment)
	require.NoError(t, err)
	activeAfter, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, oldSegment, segmentAfter)
	require.Equal(t, oldActive, activeAfter)
}

func TestV4UpgradeDirectoryRejectsNewSegmentAfterInterruption(t *testing.T) {
	dir, _, _, _, _ := v4MigrationDirectoryFixture(t)
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{afterPublish: func(string) error { return errors.New("stop") }})
	require.Error(t, err)
	data := []byte(`{"tx_id":"new"}`)
	entry := WALEntry{Sequence: 3, Timestamp: time.Now().UTC(), Operation: OpTxBegin, Data: data, Checksum: crc32Checksum(data)}
	name := fmt.Sprintf("seg-%020d-%020d.wal", 3, 3)
	path := filepath.Join(walSegmentsDir(dir), name)
	raw := priorWALRecord(t, 3, entry)
	require.NoError(t, os.WriteFile(path, raw, 0600))
	_, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.Error(t, err, "an externally added segment is absent from the original backup")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}
