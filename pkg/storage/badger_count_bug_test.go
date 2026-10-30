package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/stretchr/testify/require"
)

// newBadgerCountEngine builds the real on-disk BadgerEngine the count tests
// use; no AsyncEngine or WALEngine is wrapped around it.
func newBadgerCountEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

// TestBadgerEngine_UpdateNode_WasInsertAfterDelete tests that UpdateNode
// correctly detects new nodes as inserts after all nodes have been deleted.
// This reproduces the bug where nodeCount stays at 0/1 after delete+recreate.
// The recreate writes brand-new IDs that never existed before.
func TestBadgerEngine_UpdateNode_WasInsertAfterDelete(t *testing.T) {
	nodeCountCycle{
		createIDs: []string{
			"node-0", "node-1", "node-2", "node-3", "node-4",
			"node-5", "node-6", "node-7", "node-8", "node-9",
		},
		recreateIDs:      []string{"new-uuid-0", "new-uuid-1", "new-uuid-2", "new-uuid-3", "new-uuid-4"},
		recreateIsUpdate: true,
		nameProperties:   true,
		wantCounts:       [3]int64{10, 0, 5},
	}.run(t, newBadgerCountEngine(t))
}

// TestBadgerEngine_UpdateNode_WasInsertWithSameIDs tests what happens when
// UpdateNode is called with IDs that previously existed but were deleted.
func TestBadgerEngine_UpdateNode_WasInsertWithSameIDs(t *testing.T) {
	nodeCountCycle{
		createIDs:        []string{"reuse-id-0", "reuse-id-1", "reuse-id-2", "reuse-id-3", "reuse-id-4"},
		recreateIDs:      []string{"reuse-id-0", "reuse-id-1", "reuse-id-2", "reuse-id-3", "reuse-id-4"},
		recreateIsUpdate: true,
		recreateLabel:    "Test",
		wantCounts:       [3]int64{5, 0, 5},
	}.run(t, newBadgerCountEngine(t))
}
