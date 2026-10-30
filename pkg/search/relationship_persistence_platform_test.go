package search

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A bare relative index path must save before publishing, close successfully
// and be reused by another worker without buying the same vectors again.
func TestRelationshipEmbeddingPersistsWithRelativeIndexPath(t *testing.T) {
	t.Chdir(t.TempDir())
	engine := relationshipTestEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath("bm25")
	var calls atomic.Int64
	svc.EnableRelationshipSearch(func(context.Context, *storage.Edge) (RelationshipEmbedding, error) {
		if calls.Add(1) == 2 {
			// Finish the real worker's initial batch, then join its save.
			svc.lifecycleCancel()
		}
		return RelationshipEmbedding{Vectors: [][]float32{{1, 0, 0}}}, nil
	})
	svc.StartRelationshipIndex()
	<-svc.relationshipSearch().worker.Done()
	require.NoError(t, svc.Close())
	require.Equal(t, 2, svc.RelationshipEmbeddingCount())
	saved, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Len(t, saved, 2)
	_, err = os.Stat("bm25" + relationshipVectorsFileSuffix)
	require.NoError(t, err)

	reused := &countingEmbed{}
	restarted := startRelationshipService(t, engine, reused.embed, func(s *Service) {
		s.SetPersistenceEnabled(true)
		s.SetFulltextIndexPath("bm25")
	})
	<-restarted.relationshipSearch().loaded.Done()
	require.Equal(t, 2, restarted.RelationshipEmbeddingCount())
	require.Zero(t, reused.calls.Load())
	require.NoError(t, restarted.Close())
}

// Use the configured platform helper at the post-rename boundary. Its failure
// must prevent publication, and retry must reuse the generated vectors. This
// same test uses storage's Windows policy when built and run on Windows.
func TestRelationshipEmbeddingDirectorySyncUsesPlatformHelper(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	path := filepath.Join(t.TempDir(), "bm25")
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath(path)
	embed := &countingEmbed{}
	svc.EnableRelationshipSearch(embed.embed)
	rs := svc.relationshipSearch()
	platformSync := rs.syncDirectory
	var syncCalls int
	syncFailure := errors.New("injected directory-sync failure")
	rs.syncDirectory = func(dir string) error {
		syncCalls++
		require.Equal(t, filepath.Dir(path), dir)
		_, err := os.Stat(path + relationshipVectorsFileSuffix)
		require.NoError(t, err, "the file rename precedes directory durability")
		require.Zero(t, svc.RelationshipEmbeddingCount(), "publication follows directory durability")
		require.NoError(t, platformSync(dir))
		if syncCalls == 1 {
			return syncFailure
		}
		return nil
	}
	edge, err := engine.GetEdge("r1")
	require.NoError(t, err)
	svc.IndexRelationship(edge)
	changed, failed := svc.embedPendingRelationships(context.Background(), rs)
	require.Equal(t, 1, syncCalls, "save must use the configured platform helper")
	require.False(t, changed)
	require.True(t, failed)
	require.ErrorIs(t, rs.saveErr, syncFailure)
	require.Zero(t, svc.RelationshipEmbeddingCount())
	changed, failed = svc.embedPendingRelationships(context.Background(), rs)
	require.Equal(t, 2, syncCalls)
	require.True(t, changed)
	require.False(t, failed)
	require.EqualValues(t, 1, embed.calls.Load(), "sync retry must not repeat the provider request")
	require.Equal(t, 1, svc.RelationshipEmbeddingCount())
	saved, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Len(t, saved, 1)
}
