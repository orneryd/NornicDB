package bolt

import (
	"context"
	"path/filepath"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-412: a rebuild of an empty store from a snapshot plus the journal, the
// recovery used when the store itself is damaged, through a single-node
// server's own paths: the embedding worker embeds a node, a snapshot is
// taken, a statement over Bolt changes one property and keeps the vectors,
// and the store is rebuilt. The statement's journal record leaves the vectors
// out and says which ones it kept. The rebuild restores them when the
// snapshot holds exactly those, and presents no others as them: after
// "regenerate embeddings" the snapshot holds the older set, which the journal
// cannot replace because the worker's writes are not in it.

// kvRebuild rebuilds an empty store from a snapshot of lab's store taken by
// snapshot and the journal lab's server wrote, and returns the node.
func kvRebuild(t *testing.T, root, snapshotPath string, id storage.NodeID) *storage.Node {
	t.Helper()
	rebuilt, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	result, _, err := storage.RecoverIntoEngine(context.Background(), rebuilt, filepath.Join(root, "wal"), snapshotPath)
	require.NoError(t, err)
	require.Zero(t, result.Failed, "%v", result.Errors)
	got, err := rebuilt.GetNode(context.Background(), id)
	require.NoError(t, err)
	return got
}

func (l *kvLab) snapshot() string {
	l.t.Helper()
	walEngine := l.db.GetBaseStorageForManager().(*storage.WALEngine)
	snapshot, err := walEngine.GetWAL().CreateSnapshot(walEngine.GetInnerEngine())
	require.NoError(l.t, err)
	path := filepath.Join(l.root, "rebuild-snapshot.json")
	require.NoError(l.t, storage.SaveSnapshot(snapshot, path))
	return path
}

func TestKeptVectorsRebuildFromSnapshot(t *testing.T) {
	const update = "MATCH (s:Large {id:'large'}) SET s.hidden = true"
	for _, explicit := range kvForms() {
		t.Run(kvForm(explicit)+"/embeddings unchanged since the snapshot", func(t *testing.T) {
			provider := &kvEmbedder{chunks: 4}
			root := t.TempDir()
			lab := kvOpen(t, root, kvWorkerConfig())
			lab.mustRun("CREATE (:Large {id:'large', text:'unchanged text'})", nil)
			embedded := lab.awaitEmbedding(provider, "")
			snapshotPath := lab.snapshot()
			_, err := lab.run(update, nil, explicit)
			require.NoError(t, err)
			live := lab.nodesByLabel("Large")[0]
			require.Equal(t, embedded, kvVectorPrint(live.ChunkEmbeddings))
			id := storage.NodeID("nornic:" + string(live.ID))
			lab.close()

			got := kvRebuild(t, root, snapshotPath, id)
			require.Equal(t, true, got.Properties["hidden"], "the statement is applied")
			require.Equal(t, embedded, kvVectorPrint(got.ChunkEmbeddings), "the vectors the statement kept are restored")
			require.Equal(t, true, got.EmbedMeta["has_embedding"])
			require.False(t, storage.NodeNeedsEmbedding(got))
		})
		t.Run(kvForm(explicit)+"/embeddings regenerated after the snapshot", func(t *testing.T) {
			provider := &kvEmbedder{chunks: 4}
			root := t.TempDir()
			lab := kvOpen(t, root, kvWorkerConfig())
			lab.mustRun("CREATE (:Large {id:'large', text:'unchanged text'})", nil)
			first := lab.awaitEmbedding(provider, "")
			snapshotPath := lab.snapshot()
			_, err := lab.db.ClearAllEmbeddings()
			require.NoError(t, err)
			second := lab.awaitEmbedding(provider, first)
			require.NotEqual(t, first, second)
			_, err = lab.run(update, nil, explicit)
			require.NoError(t, err)
			live := lab.nodesByLabel("Large")[0]
			require.Equal(t, second, kvVectorPrint(live.ChunkEmbeddings), "the live server holds the regenerated vectors")
			id := storage.NodeID("nornic:" + string(live.ID))
			lab.close()

			got := kvRebuild(t, root, snapshotPath, id)
			require.Equal(t, true, got.Properties["hidden"], "the statement is applied")
			require.NotEqual(t, first, kvVectorPrint(got.ChunkEmbeddings), "the snapshot's older vectors are not presented as the ones the statement kept")
			require.Empty(t, got.ChunkEmbeddings)
			require.Nil(t, got.EmbedMeta)
			require.True(t, storage.NodeNeedsEmbedding(got), "the node is left to be embedded again")
		})
	}
}
