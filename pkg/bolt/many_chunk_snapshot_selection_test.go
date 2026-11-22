package bolt

import (
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newestPublishedSnapshot matches the .json population used by recovery and
// pruning. A new compaction can create its .tmp file after journal truncation.
func newestPublishedSnapshot(t *testing.T, root string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "snapshots", "snapshot-*.json"))
	require.NoError(t, err)
	if len(files) == 0 {
		return ""
	}
	return files[len(files)-1]
}

func TestManyChunkSnapshotSelectionSkipsInterruptedWrite(t *testing.T) {
	ctx := t.Context()
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	const chunks = 3
	vectors := journalVectors(chunks, 0)
	_, err = engine.CreateNode(ctx, &storage.Node{
		ID:              "test:many",
		Labels:          []string{"Many"},
		Properties:      map[string]any{"id": "many", "flag": true},
		ChunkEmbeddings: vectors,
	})
	require.NoError(t, err)

	root := t.TempDir()
	snapshots := filepath.Join(root, "snapshots")
	require.NoError(t, os.MkdirAll(snapshots, 0o755))
	published := filepath.Join(snapshots, "snapshot-20260101-000000.000000000.json")
	require.NoError(t, storage.SaveStreamingSnapshot(ctx, engine, published, storage.SnapshotOptions{}))
	interrupted := filepath.Join(snapshots, "snapshot-20260101-000000.000000001.json.tmp")
	file, err := os.Create(interrupted)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	// The broader snapshot-* selection this test used before takes the
	// unfinished file, and the reader reports the release run's message.
	all, err := filepath.Glob(filepath.Join(snapshots, "snapshot-*"))
	require.NoError(t, err)
	require.Equal(t, []string{published, interrupted}, all, "the interrupted write sorts after the published snapshot")
	opened, err := os.Open(interrupted)
	require.NoError(t, err)
	_, err = storage.ReadSnapshot(ctx, opened, &manyChunkSnapshotProof{t: t, want: vectors})
	require.NoError(t, opened.Close())
	require.ErrorContains(t, err, "invalid streaming snapshot header", "an unfinished snapshot is not a readable snapshot")

	selected := newestPublishedSnapshot(t, root)
	require.Equal(t, published, selected)
	requireManyChunkSnapshot(t, ctx, selected, chunks)
}
