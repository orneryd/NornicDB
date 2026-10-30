package search

import (
	"context"
	"fmt"
	"os"
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
			saved, err := svc.loadRelationshipVectors()
			require.NoError(t, err)
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

func TestRelationshipEmbeddingCloseJoinsSave(t *testing.T) {
	engine := relationshipTestEngine(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, func(s *Service) {
		s.SetPersistenceEnabled(true)
		s.SetFulltextIndexPath(filepath.Join(t.TempDir(), "bm25"))
		s.EnableRelationshipSearch(embed.embed)
		s.relationshipSearch().beforeSave = func() { close(entered); <-release }
	})
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()
	<-svc.lifecycleCtx.Done()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while the save was blocked: %v", err)
	default:
	}
	unblock()
	require.NoError(t, <-closed)
	select {
	case <-svc.relationshipSearch().worker.Done():
	default:
		t.Fatal("Close did not join the worker")
	}
	saved, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Len(t, saved, 2)
	require.NoError(t, svc.Close())
	svc.StartRelationshipIndex()
}

func TestRelationshipEmbeddingSaveRetryKeepsGeneratedVectors(t *testing.T) {
	engine := relationshipTestEngine(t)
	dir := filepath.Join(t.TempDir(), "not-created")
	embed := &countingEmbed{}
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.EnableRelationshipSearch(embed.embed)
	edge, err := engine.GetEdge("r1")
	require.NoError(t, err)
	svc.IndexRelationship(edge)
	changed, failed := svc.embedPendingRelationships(context.Background(), svc.relationshipSearch())
	require.False(t, changed)
	require.True(t, failed)
	require.Zero(t, svc.RelationshipEmbeddingCount())
	require.EqualValues(t, 1, embed.calls.Load())
	require.NoError(t, os.Mkdir(dir, 0o700))
	changed, failed = svc.embedPendingRelationships(context.Background(), svc.relationshipSearch())
	require.True(t, changed)
	require.False(t, failed)
	require.EqualValues(t, 1, embed.calls.Load(), "a save retry must not call the provider again")
	require.Equal(t, 1, svc.RelationshipEmbeddingCount())
	saved, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Len(t, saved, 1)
}

func TestRelationshipEmbeddingSaveDoesNotPublishSupersededVectors(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(map[bool]string{false: "update", true: "delete"}[remove], func(t *testing.T) {
			engine := relationshipTestEngine(t)
			embed := &countingEmbed{}
			svc := NewService(engine, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetPersistenceEnabled(true)
			svc.SetFulltextIndexPath(filepath.Join(t.TempDir(), "bm25"))
			svc.EnableRelationshipSearch(embed.embed)
			edge, err := engine.GetEdge("r1")
			require.NoError(t, err)
			svc.IndexRelationship(edge)
			svc.relationshipSearch().beforeSave = func() {
				if remove {
					svc.RemoveRelationship(edge.ID)
				} else {
					edge.Properties["reason"] = "new text during the save"
					svc.IndexRelationship(edge)
				}
			}
			changed, failed := svc.embedPendingRelationships(context.Background(), svc.relationshipSearch())
			require.False(t, changed)
			require.False(t, failed)
			require.Zero(t, svc.RelationshipEmbeddingCount())
		})
	}
}

func TestRelationshipEmbeddingSavesWhileWritesContinue(t *testing.T) {
	engine := relationshipTestEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetPersistenceEnabled(true)
	svc.SetFulltextIndexPath(filepath.Join(t.TempDir(), "bm25"))
	calls := 0
	svc.EnableRelationshipSearch(func(context.Context, *storage.Edge) (RelationshipEmbedding, error) {
		calls++
		if calls <= 4 {
			svc.IndexRelationship(&storage.Edge{ID: storage.EdgeID(fmt.Sprint(calls)), Type: "CITES", Properties: map[string]any{"reason": "next document"}})
		}
		return RelationshipEmbedding{Vectors: [][]float32{{1, 0, 0}}}, nil
	})
	edge, err := engine.GetEdge("r1")
	require.NoError(t, err)
	svc.IndexRelationship(edge)
	changed, failed := svc.embedPendingRelationships(context.Background(), svc.relationshipSearch())
	require.True(t, changed)
	require.False(t, failed)
	require.Equal(t, 1, calls, "new arrivals must not postpone this batch's save")
	require.Equal(t, 1, svc.RelationshipEmbeddingCount())
	saved, err := svc.loadRelationshipVectors()
	require.NoError(t, err)
	require.Len(t, saved, 1)
}
