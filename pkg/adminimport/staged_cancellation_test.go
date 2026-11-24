package adminimport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStagedImportCancellationRemovesUnpublishedStore(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(t.TempDir(), "nodes.csv")
	require.NoError(t, os.WriteFile(input, []byte(":ID\na\nb\n"), 0o600))
	opts := Options{DatabaseName: "prepared", NodeSources: []string{"Item=" + input}, ChunkSize: 1}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	id, _, err := StageFull(canceled, root, opts)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, id)
	require.NoDirExists(t, storage.PreparedDatabasesDirectory(root))

	var existed bool
	ctx := fixtureowner.NewCheckpointContext(2, func() {
		entries, _ := os.ReadDir(storage.PreparedDatabasesDirectory(root))
		existed = len(entries) == 1
	})
	t.Cleanup(ctx.Cancel)
	id, _, err = StageFull(ctx, root, opts)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, existed, "cancellation interrupted a private store that had been allocated")
	require.Empty(t, id)
	entries, err := os.ReadDir(storage.PreparedDatabasesDirectory(root))
	require.NoError(t, err)
	require.Empty(t, entries)

	id, _, err = StageFull(t.Context(), root, opts)
	require.NoError(t, err)
	_, graph, err := storage.ReadPreparedDatabase(root, id, opts.DatabaseName)
	require.NoError(t, err)
	require.EqualValues(t, 2, graph.Nodes)
}
