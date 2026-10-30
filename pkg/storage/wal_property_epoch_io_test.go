package storage

import (
	"bufio"
	"bytes"
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
	old := epochValidationRecord(t, []byte(`{"tx_id":"old"}`), walFormatVersionV3)
	for _, shape := range []string{"missing", "directory", "malformed legacy", "old framing"} {
		t.Run(shape, func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			path := walActivePath(wal.Config().Dir)
			var want []byte
			switch shape {
			case "missing":
				require.NoError(t, os.Remove(path))
			case "directory":
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Mkdir(path, 0700))
			case "malformed legacy":
				want = []byte("not JSON")
				require.NoError(t, os.WriteFile(path, want, 0600))
			case "old framing":
				// Two old records: the first one must not be mistaken for a
				// torn tail that repair may truncate.
				want = append(bytes.Clone(old), epochValidationRecord(t, []byte(`{"tx_id":"old-2"}`), walFormatVersionV3)...)
				require.NoError(t, os.WriteFile(path, want, 0600))
			}
			// All producer routes must stop before writing through an unknown
			// physical format; none may acknowledge a record after a bad prefix.
			require.Error(t, wal.Append(OpCreateNode, WALNodeData{Node: &Node{ID: "io:a"}}))
			require.Error(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
			batch := wal.NewBatch()
			require.NoError(t, batch.append(OpCreateNode, WALNodeData{Node: &Node{ID: "io:b"}}))
			require.Error(t, batch.Commit())
			require.Error(t, wal.invalidateCleanPropertyEpoch())
			if want != nil {
				// Nothing was written through the refused framing.
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, want, after)
			}
		})
	}
}

// An existing headerless historical log cannot be opened for appending, and no
// append path may write through the unknown framing.
func TestWALPropertyEpochHeaderlessLegacyBoundary(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, _, commit := bindingCostFixture(t, 1)
	for _, content := range [][]byte{[]byte("     \n"), []byte(`{"seq":1,"op":"checkpoint"}` + "\n")} {
		t.Run("open", func(t *testing.T) {
			dir := t.TempDir()
			path := walActivePath(dir)
			require.NoError(t, os.WriteFile(path, content, 0600))
			w, err := NewWAL(dir, DefaultWALConfig())
			require.ErrorIs(t, err, ErrWALUnsupportedVersion, "a headerless log cannot be opened for appending")
			require.Nil(t, w)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, after, "a headerless log is never rewritten")
		})
		t.Run("append", func(t *testing.T) {
			// Even an already opened log must refuse the framing when the file
			// underneath it is replaced by headerless bytes.
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			path := walActivePath(wal.Config().Dir)
			require.NoError(t, os.WriteFile(path, content, 0600))
			require.ErrorIs(t, wal.AppendBufferedCommit(commit, source.propKeyDict), ErrWALUnsupportedVersion)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, after)
		})
		t.Run("read only", func(t *testing.T) {
			wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			path := walActivePath(wal.Config().Dir)
			require.NoError(t, os.WriteFile(path, content, 0600))
			require.NoError(t, wal.file.Close())
			wal.file, err = os.Open(path)
			require.NoError(t, err)
			wal.writer = bufio.NewWriter(wal.file)
			require.ErrorIs(t, wal.AppendBufferedCommit(commit, source.propKeyDict), ErrWALUnsupportedVersion)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, after)
		})
	}
}

// An empty active file is a current-format log with nothing in it, so the first
// buffered commit writes both its authority control and its body.
func TestWALPropertyEpochEmptyLogAppend(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	source, _, commit := bindingCostFixture(t, 1)
	dir := t.TempDir()
	path := walActivePath(dir)
	require.NoError(t, os.WriteFile(path, nil, 0600))
	wal, err := NewWAL(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	require.NoError(t, wal.AppendBufferedCommit(commit, source.propKeyDict))
	entries, err := ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[0].PropertyEpoch)
	_, versions, controls := walEpochRawBodies(t, path)
	require.Equal(t, 1, controls)
	for _, version := range versions {
		require.Equal(t, walFormatVersionCurrent, version)
	}
}

func TestWALPropertyEpochSealingFailureKeepsControls(t *testing.T) {
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
			// The attachment published a current-framing control; a failed
			// seal must leave it exactly as it was.
			require.NotEmpty(t, before)
			wal.mu.Lock()
			err = wal.sealPropertyEpochsLocked()
			wal.mu.Unlock()
			require.Error(t, err)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, before, after, "an incomplete seal must leave the active authority control intact")
			_, versions, controls := walEpochRawBodies(t, path)
			require.NotZero(t, controls)
			for _, version := range versions {
				require.Equal(t, walFormatVersionCurrent, version)
			}
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

// A future framing version is refused at open exactly like an older one, so a
// binary can never silently write through a format it does not understand. The
// future record follows one current record, so it cannot be taken for a torn
// tail that repair may truncate.
func TestWALPropertyEpochFutureFramingRefusedWithoutMutation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	var current bytes.Buffer
	payload, err := processWALEntryPayload(walPayloadEncode, &current, &WALEntry{Sequence: 1, Operation: OpCreateNode, Data: []byte(`{}`), Checksum: crc32Checksum([]byte(`{}`))}, nil, 0)
	require.NoError(t, err)
	lead, _ := buildAtomicRecord(payload, walFormatVersionCurrent)
	future, _ := buildAtomicRecord([]byte(`{"future":"record"}`), walFormatVersionCurrent+1)
	raw := append(lead, future...)
	dir := t.TempDir()
	path := walActivePath(dir)
	require.NoError(t, os.WriteFile(path, raw, 0600))
	w, err := NewWAL(dir, DefaultWALConfig())
	require.ErrorIs(t, err, ErrWALUnsupportedVersion)
	require.Nil(t, w)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}
