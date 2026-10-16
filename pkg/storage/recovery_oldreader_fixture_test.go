package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRecoveryGenerateOldReaderFixtures writes only disposable compatibility
// fixtures when explicitly requested; normal test runs have no external output.
func TestRecoveryGenerateOldReaderFixtures(t *testing.T) {
	dir := os.Getenv("NORNICDB_RECOVERY_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set fixture directory to run the isolated old-reader comparison")
	}
	require.NoError(t, os.MkdirAll(dir, 0700))
	n, _ := recoveryFieldFixture()
	wal, err := NewWAL(filepath.Join(dir, "wal"), &WALConfig{SyncMode: "immediate"})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpCreateNode, WALNodeData{Node: n}, "test"))
	require.NoError(t, wal.Close())
	require.NoError(t, SaveSnapshot(&Snapshot{Sequence: 1, Nodes: []*Node{n}}, filepath.Join(dir, "snapshot.nds")))
}
