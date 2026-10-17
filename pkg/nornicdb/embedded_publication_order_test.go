package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// embeddedPublicationFixture persists a node and its embedding the way the
// embed worker does, and returns the qualified copy its callback would
// publish, captured before any later mutation.
func embeddedPublicationFixture(t *testing.T) (*DB, *search.Service, *storage.Node, []float32) {
	t.Helper()
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dbName := db.defaultDatabaseName()
	svc, err := db.EnsureSearchIndexesBuilt(ctx, dbName, nil)
	require.NoError(t, err)
	require.True(t, svc.IsReady())

	_, err = db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"SearchSource"}, Properties: map[string]interface{}{"content": "bravo filler"}})
	require.NoError(t, err)
	vector := unitVector(config.Memory.EmbeddingDimensions)
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = [][]float32{vector}
	sidecar, ok := db.storage.(storage.EmbeddingSidecarUpdater)
	require.True(t, ok)
	require.NoError(t, sidecar.UpdateNodeEmbeddingSidecar(embedded))

	qualified := storage.CopyNode(embedded)
	qualified.ID = storage.NodeID(dbName + ":source")
	return db, svc, qualified, vector
}

func textHitIDs(t *testing.T, svc *search.Service, term string) []storage.NodeID {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.Limit, opts.MinRRFScore = 10, 0
	response, err := svc.Search(context.Background(), term, nil, opts)
	require.NoError(t, err)
	ids := make([]storage.NodeID, 0, len(response.Results))
	for _, r := range response.Results {
		ids = append(ids, r.NodeID)
	}
	return ids
}

// #1360 review: a delete applied between the embed callback's pending check
// and its publication stays applied when the callback resumes.
func TestEmbeddedPublicationAfterDeleteDoesNotResurrect(t *testing.T) {
	db, svc, callbackCopy, vector := embeddedPublicationFixture(t)

	// The deletion commits and a running flush applies it; nothing is pending.
	require.NoError(t, db.storage.DeleteNode("source"))
	require.NoError(t, svc.RemoveNode("source"))

	// The paused callback resumes with its older copy.
	db.publishEmbeddedNode(callbackCopy)

	require.Empty(t, textHitIDs(t, svc, "bravo"), "the deleted node's keywords came back")
	require.Empty(t, vectorOnlySearch(t, svc, vector).Results, "the deleted node's vector came back")
	require.Zero(t, svc.EmbeddingCount())
}

// #1360 review: a newer update applied before an older embed callback resumes
// is not overwritten by the callback's copy.
func TestEmbeddedPublicationDoesNotOverwriteNewerUpdate(t *testing.T) {
	db, svc, callbackCopy, oldVector := embeddedPublicationFixture(t)

	newVector := make([]float32, len(oldVector))
	newVector[1] = 1
	newer, err := db.storage.GetNode("source")
	require.NoError(t, err)
	newer = storage.CopyNode(newer)
	newer.Properties["content"] = "charlie filler"
	require.NoError(t, db.storage.UpdateNode(newer))
	newer.ChunkEmbeddings = [][]float32{newVector}
	require.NoError(t, db.storage.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(newer))
	current, err := db.storage.GetNode("source")
	require.NoError(t, err)
	require.Len(t, current.ChunkEmbeddings, 1)
	require.Equal(t, newVector, current.ChunkEmbeddings[0], "storage holds the newer vector")
	require.NoError(t, svc.PublishStoredNode("source"))

	// The older callback resumes after the newer state was published.
	db.publishEmbeddedNode(callbackCopy)

	require.Equal(t, []storage.NodeID{"source"}, textHitIDs(t, svc, "charlie"))
	require.Empty(t, textHitIDs(t, svc, "bravo"), "the older text overwrote the update")
	// The indexed vector is the newer one: it scores 1 against the newer
	// vector and 0 against the older one, which are orthogonal.
	for _, probe := range []struct {
		vector []float32
		want   float64
	}{{oldVector, 0}, {newVector, 1}} {
		candidates, err := svc.VectorSearchCandidates(context.Background(), probe.vector, search.DefaultSearchOptions())
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		require.InDelta(t, probe.want, candidates[0].Score, 1e-6, "the older vector overwrote the update")
	}
	hits := vectorOnlySearch(t, svc, newVector).Results
	require.Len(t, hits, 1)
	require.Equal(t, storage.NodeID("source"), hits[0].NodeID)
	require.Equal(t, 1, svc.EmbeddingCount())
}
