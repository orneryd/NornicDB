package search

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// staleEdgeEngine serves an edge's older version, as a read can while the
// write that changed it isn't visible yet.
type staleEdgeEngine struct {
	storage.Engine
	mu    sync.Mutex
	stale map[storage.EdgeID]*storage.Edge
}

func (e *staleEdgeEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	e.mu.Lock()
	old := e.stale[id]
	e.mu.Unlock()
	if old != nil {
		return storage.CopyEdge(old), nil
	}
	return e.Engine.GetEdge(id)
}

// A relationship edited after it was indexed is embedded from the version
// the index took, even when storage still serves the older one: its vectors
// were refused for the older version's text and never retried (I65).
func TestRelationshipEmbedsTheIndexedVersionOverAStaleRead(t *testing.T) {
	engine := &staleEdgeEngine{Engine: relationshipTestEngine(t), stale: map[storage.EdgeID]*storage.Edge{}}
	var mu sync.Mutex
	var embedded []string
	embed := func(_ context.Context, edge *storage.Edge) ([][]float32, error) {
		text, _ := edge.Properties["search_text"].(string)
		mu.Lock()
		embedded = append(embedded, text)
		mu.Unlock()
		if text == "" {
			return nil, nil
		}
		return [][]float32{{1, 0, 0}}, nil
	}
	svc := startRelationshipService(t, engine, embed, nil)
	<-svc.relationshipSearch().ready

	original, err := engine.Engine.GetEdge("r1")
	require.NoError(t, err)
	engine.mu.Lock()
	engine.stale["r1"] = storage.CopyEdge(original)
	engine.mu.Unlock()
	edited := storage.CopyEdge(original)
	edited.Properties["search_text"] = "zephyrine bridge explanation"
	require.NoError(t, engine.Engine.UpdateEdge(edited))
	svc.IndexRelationship(edited)

	require.Eventually(t, func() bool {
		rs := svc.relationshipSearch()
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		entry := rs.entries["r1"]
		return entry != nil && len(entry.Vectors) == 1
	}, 5*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, embedded, "zephyrine bridge explanation")
}

// An embedding overtaken by a later write is refused, and the later version
// is embedded; a pending relationship that can no longer be read and has no
// copy is skipped.
func TestRelationshipEmbeddingSupersededByALaterWrite(t *testing.T) {
	engine := relationshipTestEngine(t)
	first := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	embed := func(_ context.Context, edge *storage.Edge) ([][]float32, error) {
		if edge.Properties["search_text"] == "first" {
			once.Do(func() { close(first) })
			<-release
		}
		return [][]float32{{1, 0, 0}}, nil
	}
	svc := startRelationshipService(t, engine, embed, nil)
	rs := svc.relationshipSearch()
	<-rs.ready

	edge, err := engine.GetEdge("r1")
	require.NoError(t, err)
	edge.Properties["search_text"] = "first"
	svc.IndexRelationship(storage.CopyEdge(edge))
	<-first
	edge.Properties["search_text"] = "second"
	svc.IndexRelationship(storage.CopyEdge(edge))
	close(release)
	require.Eventually(t, func() bool {
		rs.mu.RLock()
		defer rs.mu.RUnlock()
		entry := rs.entries["r1"]
		return entry != nil && entry.Source == relationshipSource(edge) && len(entry.Vectors) == 1
	}, 5*time.Second, 10*time.Millisecond)

	// Pending without a copy, and gone from storage: skipped.
	rs.mu.Lock()
	rs.entries["ghost"] = &relationshipEntry{}
	rs.pending["ghost"] = nil
	rs.mu.Unlock()
	changed, failed := svc.embedPendingRelationships(context.Background(), rs)
	require.False(t, changed)
	require.False(t, failed)
}
