package search

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// A rename can succeed even when the following directory sync fails. A retry
// must finish that sync before it reports the already-converted image safe.
func TestRelationshipSnapshotConversionRetriesDirectorySync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bm25.v2"+relationshipVectorsFileSuffix)
	want := relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: map[string]*relationshipEntry{"edge": {Source: 17, Vectors: [][]float32{{1, 0, -1}}}}}
	require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, want), 0o600))
	failure := errors.New("injected directory-sync failure")
	calls := 0
	syncDirectory := func(dir string) error {
		calls++
		require.Equal(t, filepath.Dir(path), dir)
		require.NoError(t, storage.SyncDir(dir))
		if calls == 1 {
			return failure
		}
		return nil
	}
	err := UpgradeRelationshipSnapshot(path, true, syncDirectory)
	require.ErrorIs(t, err, failure)
	afterFirst, err := os.ReadFile(path)
	require.NoError(t, err)
	var first relationshipVectorsFile
	require.NoError(t, msgpack.Unmarshal(afterFirst, &first))
	require.Equal(t, want, first)
	require.NoError(t, UpgradeRelationshipSnapshot(path, true, syncDirectory))
	require.Equal(t, 2, calls, "already-converted retry must sync the directory")
	afterRetry, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, afterFirst, afterRetry)
}

// Only an initially absent snapshot is optional. If a file has been opened,
// a later ENOENT (for example during directory sync) is an upgrade failure.
func TestRelationshipSnapshotUpgradeMissingVersusSyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bm25.v2"+relationshipVectorsFileSuffix)
	require.NoError(t, UpgradeRelationshipSnapshot(path, true, nil))
	want := relationshipVectorsFile{Format: relationshipVectorsFormat}
	require.NoError(t, os.WriteFile(path, encodeGobSnapshot(t, want), 0o600))
	err := UpgradeRelationshipSnapshot(path, true, func(string) error { return os.ErrNotExist })
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, UpgradeRelationshipSnapshot(path, true, storage.SyncDir))
}
