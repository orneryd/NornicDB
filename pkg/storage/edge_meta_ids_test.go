package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Provenance records appended back to back without an edge ID each get
// their own.
func TestEdgeMetaAppendGivesEachRecordItsOwnEdgeID(t *testing.T) {
	store := NewEdgeMetaStore()
	ctx := context.Background()
	for i := 0; i < 64; i++ {
		require.NoError(t, store.Append(ctx, EdgeMeta{Src: "a", Dst: "b", Label: "relates_to", SignalType: "similarity"}))
	}
	history, err := store.GetHistory(ctx, "a", "b", "relates_to")
	require.NoError(t, err)
	require.Len(t, history, 64)
	seen := map[string]bool{}
	for _, meta := range history {
		require.NotEmpty(t, meta.EdgeID)
		require.False(t, seen[meta.EdgeID], "duplicate edge ID %s", meta.EdgeID)
		seen[meta.EdgeID] = true
	}
}
