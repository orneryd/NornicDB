package search

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause the actual worker immediately before the file write. Counts and
// vector search must not expose embeddings while their restart image is absent.
func TestRelationshipEmbeddingPersistsBeforePublication(t *testing.T) {
	for _, chunks := range []int{1, 2} {
		t.Run(map[int]string{1: "whole", 2: "chunked"}[chunks], func(t *testing.T) {
			engine := relationshipTestEngine(t)
			path := filepath.Join(t.TempDir(), "bm25")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			embed := func(context.Context, *storage.Edge) (RelationshipEmbedding, error) {
				result := RelationshipEmbedding{}
				for i := 0; i < chunks; i++ {
					result.Vectors = append(result.Vectors, []float32{1, 0, 0})
					result.Spans = append(result.Spans, embeddingutil.ChunkSpan{Chunk: i, Property: "reason", Start: i, End: i + 1})
				}
				return result, nil
			}
			svc := startRelationshipService(t, engine, embed, func(s *Service) {
				s.SetPersistenceEnabled(true)
				s.SetFulltextIndexPath(path)
				s.EnableRelationshipSearch(embed)
				s.relationshipSearch().beforeSave = func() {
					close(entered)
					<-release
				}
			})
			<-entered
			require.Zero(t, svc.RelationshipEmbeddingCount(), "unsaved vectors must not be published")
			response, err := svc.SearchRelationships(context.Background(), "", []float32{1, 0, 0}, nil)
			require.NoError(t, err)
			require.Empty(t, response.Results, "search must not expose unsaved vectors")
			unblock()
			waitRelationshipVectors(t, svc, 2)
			saved := svc.loadRelationshipVectors()
			require.Len(t, saved, 2)
			require.Len(t, saved["r1"].Vectors, chunks)
			require.Len(t, saved["r1"].Spans, chunks)
			require.NoError(t, svc.Close())

			var calls atomic.Int64
			restarted := startRelationshipService(t, engine, func(ctx context.Context, edge *storage.Edge) (RelationshipEmbedding, error) {
				calls.Add(1)
				return embed(ctx, edge)
			}, func(s *Service) {
				s.SetPersistenceEnabled(true)
				s.SetFulltextIndexPath(path)
			})
			waitRelationshipVectors(t, restarted, 2)
			require.Zero(t, calls.Load(), "restart must reuse the saved embeddings")
		})
	}
}
