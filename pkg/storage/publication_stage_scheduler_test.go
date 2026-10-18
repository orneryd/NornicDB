package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Registration follows completed physical writes, not timestamp assignment.
// Helping a later writer must therefore find older overlapping publications
// even when they registered last, without running unrelated publications.
func TestPublicationStageOutOfOrderRegistration(t *testing.T) {
	var stage publicationStage
	var ran []uint64
	for _, ts := range []uint64{4, 3, 2, 1} {
		keys := []any{NodeID("db:a"), NodeID("db:a")}
		if ts == 2 {
			keys = []any{NodeID("db:a"), NodeID("db:b")}
		} else if ts == 3 {
			keys = []any{NodeID("db:b")}
		} else if ts == 4 {
			keys = []any{NodeID("db:unrelated")}
		}
		stage.register(ts, &commitPublication{keys: keys, run: func() { ran = append(ran, ts) }})
	}
	stage.apply(3)
	require.Equal(t, []uint64{1, 2, 3}, ran)
	require.Equal(t, 1, stage.pendingCount())
	stage.apply(4)
	require.Equal(t, []uint64{1, 2, 3, 4}, ran)
	require.Zero(t, stage.pendingCount())
	require.Empty(t, stage.byKey)
	stage.apply(3) // already helped: no second execution
	require.Len(t, ran, 4)
}
