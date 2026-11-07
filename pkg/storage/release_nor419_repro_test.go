package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReleaseNOR419SingleEntryLabelCounts(t *testing.T) {
	source := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: t.TempDir(), wal: true})
	_, err := source.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Old"}})
	require.NoError(t, err)
	tx, err := source.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	node.Labels = []string{"New"}
	require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())
	entries, err := ReadWALEntriesFromDir(source.bufferedCommitWAL.Config().Dir)
	require.NoError(t, err)
	target := createTestBadgerEngine(t)
	_, err = target.CreateNode(t.Context(), &Node{ID: "test:a", Labels: []string{"Old"}})
	require.NoError(t, err)
	found := false
	for _, entry := range entries {
		if entry.Operation == OpBufferedCommit {
			found = true
			require.NoError(t, ReplayWALEntry(target, entry))
		}
	}
	require.True(t, found)
	got, err := target.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	require.Equal(t, []string{"New"}, got.Labels)
	old, err := target.NodeCountByLabelInNamespace(t.Context(), "test", "Old")
	require.NoError(t, err)
	fresh, err := target.NodeCountByLabelInNamespace(t.Context(), "test", "New")
	require.NoError(t, err)
	require.Equal(t, int64(0), old)
	require.Equal(t, int64(1), fresh)
}
