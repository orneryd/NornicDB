package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestV4UpgradeConversionFailurePublishesNoBackup: validation converts every
// record before the upgrade touches the journal, but a conversion can still
// fail later (an I/O failure, or a payload conversion only the write pass
// reaches). The upgrade publishes the durable pre-upgrade backup only after
// every file has converted, so such a failure leaves the journal exactly as it
// was: no backup, no staging directories, no replaced file. The next attempt
// converts the same journal and reads every record back.
func TestV4UpgradeConversionFailurePublishesNoBackup(t *testing.T) {
	dir, segment, active, oldSegment, oldActive := v4MigrationDirectoryFixture(t)
	parent := filepath.Dir(dir)
	injected := errors.New("cannot convert payload")
	visits := 0
	// The first two conversions are validation's preview of the segment and the
	// active file; the third is the conversion write pass.
	_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{Transform: func(_ uint8, _ OperationType, payload []byte) ([]byte, error) {
		visits++
		if visits == 3 {
			return nil, injected
		}
		return payload, nil
	}})
	require.ErrorIs(t, err, injected)
	require.Equal(t, 3, visits, "the refusal happens in the conversion pass, after validation")

	segmentAfter, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.Equal(t, oldSegment, segmentAfter, "a failed conversion keeps the sealed segment")
	activeAfter, err := os.ReadFile(active)
	require.NoError(t, err)
	require.Equal(t, oldActive, activeAfter, "a failed conversion keeps the active journal")
	_, err = os.Lstat(dir + v4WALBackupSuffix)
	require.True(t, os.IsNotExist(err), "a failed conversion must not publish the pre-upgrade backup")
	for _, pattern := range []string{".wal-v4-backup-*", ".wal-v4-upgrade-*"} {
		staged, err := filepath.Glob(filepath.Join(parent, pattern))
		require.NoError(t, err)
		require.Empty(t, staged, "a failed conversion must not leave %s behind", pattern)
	}

	changed, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.True(t, changed, "the untouched journal converts on the next attempt")
	backup, err := os.ReadFile(filepath.Join(dir+v4WALBackupSuffix, "wal.log"))
	require.NoError(t, err)
	require.Equal(t, oldActive, backup, "the published backup holds the pre-upgrade journal")
	entries, err := ReadWALEntriesFromDir(t.Context(), dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, uint64(1), entries[0].Sequence)
	require.Equal(t, uint64(2), entries[1].Sequence)
	changed, err = UpgradeWALV4(dir, WALV4UpgradeOptions{})
	require.NoError(t, err)
	require.False(t, changed, "an already converted journal is a no-op")
}

func TestV4UpgradeBackupCopyFailureRemovesStaging(t *testing.T) {
	dir, segment, active, oldSegment, oldActive := v4MigrationDirectoryFixture(t)
	marker := filepath.Join(dir, v4WALBackupComplete)
	require.NoError(t, os.WriteFile(marker, []byte("operator-owned reserved name"), 0600))
	var originalError string
	for attempt := 0; attempt < 2; attempt++ {
		_, err := UpgradeWALV4(dir, WALV4UpgradeOptions{})
		require.Error(t, err)
		if attempt == 0 {
			originalError = err.Error()
		} else {
			require.Equal(t, originalError, err.Error())
		}
		// A failed attempt preserves every source byte, including the file that
		// caused refusal. Repeating it must not accumulate owned scratch directories.
		got, readErr := os.ReadFile(segment)
		require.NoError(t, readErr)
		require.Equal(t, oldSegment, got)
		got, readErr = os.ReadFile(active)
		require.NoError(t, readErr)
		require.Equal(t, oldActive, got)
		got, readErr = os.ReadFile(marker)
		require.NoError(t, readErr)
		require.Equal(t, "operator-owned reserved name", string(got))
		_, statErr := os.Lstat(dir + v4WALBackupSuffix)
		require.True(t, os.IsNotExist(statErr))
		for _, pattern := range []string{".wal-v4-backup-*", ".wal-v4-upgrade-*"} {
			stages, globErr := filepath.Glob(filepath.Join(filepath.Dir(dir), pattern))
			require.NoError(t, globErr)
			require.Empty(t, stages, "attempt %d must clean only its owned %s staging", attempt, pattern)
		}
	}
}
