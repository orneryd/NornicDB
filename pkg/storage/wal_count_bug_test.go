package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// walCountCycle is the exact node population both WAL count tests establish:
// ten nodes created, all ten deleted, then five nodes written into the empty
// store.
func walCountCycle(recreateIsUpdate bool) nodeCountCycle {
	return nodeCountCycle{
		createIDs: []string{
			"node-0", "node-1", "node-2", "node-3", "node-4",
			"node-5", "node-6", "node-7", "node-8", "node-9",
		},
		recreateIDs:      []string{"new-node-0", "new-node-1", "new-node-2", "new-node-3", "new-node-4"},
		recreateIsUpdate: recreateIsUpdate,
		wantCounts:       [3]int64{10, 0, 5},
	}
}

// newWALCountEngine builds the real Badger+WAL stack both count tests use and
// returns the WAL wrapper whose NodeCount is asserted.
func newWALCountEngine(t *testing.T) *WALEngine {
	t.Helper()
	tmpDir := t.TempDir()

	badger, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(tmpDir))
	require.NoError(t, err)

	wal, err := NewWAL(filepath.Join(tmpDir, "wal"), nil)
	require.NoError(t, err)
	runtimeFixtureOwner.Own(t, wal)

	return NewWALEngine(badger, wal)
}

// TestWALEngine_CountAfterDeleteRecreate tests the full WAL+Badger stack to
// verify node counts are correct after a delete and recreate cycle where the
// recreate goes through UpdateNode (the AsyncEngine flush path).
func TestWALEngine_CountAfterDeleteRecreate(t *testing.T) {
	walCountCycle(true).run(t, newWALCountEngine(t))
}

// TestAsyncEngine_CountAfterDeleteRecreate tests the full WAL+Badger stack
// with the recreate going through CreateNode (simulating what Cypher MERGE
// does). The AsyncEngine itself is not part of this stack.
func TestAsyncEngine_CountAfterDeleteRecreate(t *testing.T) {
	walCountCycle(false).run(t, newWALCountEngine(t))
}
