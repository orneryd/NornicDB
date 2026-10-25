package storage

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestWALPropertyEpochUnavailableLogRejectsEveryAppend(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, _, commit := bindingCostFixture(t, 1)
	for _, shape := range []string{"missing", "directory", "malformed legacy"} {
		t.Run(shape, func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			path := walActivePath(wal.Config().Dir)
			switch shape {
			case "missing":
				require.NoError(t, os.Remove(path))
			case "directory":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Mkdir(path, 0700))
			case "malformed legacy":
				require.NoError(t, os.WriteFile(path, []byte("not JSON"), 0600))
			}
			// All producer routes must stop before writing through an unknown
			// physical format; none may acknowledge a record after a bad prefix.
			require.Error(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "io:a"}}))
			require.Error(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
			batch := wal.NewBatch()
			require.NoError(t, batch.append(OpCreateNode, WALNodeData{Node: &Node{ID: "io:b"}}))
			require.Error(t, batch.Commit())
			require.Error(t, wal.invalidateCleanPropertyEpoch())
		})
	}
}

func TestWALPropertyEpochEmptyLegacyBoundary(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, _, commit := bindingCostFixture(t, 1)
	for _, readOnly := range []bool{false, true} {
		dir := t.TempDir()
		path := walActivePath(dir)
		require.NoError(t, os.WriteFile(path, []byte("     \n"), 0600))
		wal, err := NewWAL(dir, DefaultWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wal.Close() })
		if readOnly {
			require.NoError(t, wal.file.Close())
			wal.file, err = os.Open(path)
			require.NoError(t, err)
			wal.writer = bufio.NewWriter(wal.file)
			require.Error(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
			continue
		}
		require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
		entries, err := ReadWALEntriesFromDir(dir)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.NotNil(t, entries[0].PropertyEpoch)
	}
}

func TestWALPropertyEpochSealingFailureKeepsActiveRefusal(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, _, _ := bindingCostFixture(t, 1)
	for _, shape := range []string{"bad manifest", "invalid segment", "missing segment", "sync failure"} {
		t.Run(shape, func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			wal.bindPropertyDictionary(source.propKeyDict)
			path := walActivePath(wal.Config().Dir)
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			switch shape {
			case "bad manifest":
				require.NoError(t, os.WriteFile(walManifestPath(wal.Config().Dir), []byte("not JSON"), 0600))
			case "invalid segment":
				require.NoError(t, writeWALManifest(wal.Config().Dir, &WALManifest{Segments: []WALSegment{{Path: "invalid.wal"}}}))
			case "missing segment":
				require.NoError(t, writeWALManifest(wal.Config().Dir, &WALManifest{Segments: []WALSegment{{Path: "seg-00000000000000000001-00000000000000000001.wal", FirstSeq: 1, LastSeq: 1}}}))
			case "sync failure":
				require.NoError(t, wal.file.Close())
			}
			wal.mu.Lock()
			err = wal.sealPropertyEpochsLocked()
			wal.mu.Unlock()
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "an incomplete conversion must leave the active older-reader guard intact")
		})
	}
}

func TestWALPropertyEpochFailedSyncCannotPublishCleanProof(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	dest := reviewBindingEngine(t, dir)
	require.NoError(t, dest.beginSemanticPropertyRestore())
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	dest.SetBufferedCommitWAL(wal)
	require.NoError(t, wal.file.Close())
	require.Error(t, dest.Close())
	_ = wal.Close()
	dest = reviewBindingEngine(t, dir)
	require.False(t, dest.walPropertySource.clean)
	require.True(t, dest.walPropertySource.foreign)
}

func TestWALPropertyEpochCorruptNativeIdentityRejectsOpenAndRestore(t *testing.T) {
	dir := t.TempDir()
	source := reviewBindingEngine(t, dir)
	require.NoError(t, source.db.Update(func(tx *badger.Txn) error { return tx.Set(walPropertyIdentityKey, []byte("not an identity")) }))
	backup := filepath.Join(t.TempDir(), "invalid-identity.backup")
	require.NoError(t, source.Backup(backup))
	require.NoError(t, source.Close())
	opened, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.ErrorContains(t, err, "property dictionary identity")
	require.Nil(t, opened)
	dest := reviewBindingEngine(t, t.TempDir())
	require.ErrorContains(t, dest.Restore(backup), "property dictionary identity")
}
