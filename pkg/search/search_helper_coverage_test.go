package search

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServiceWarmAndIntrospectionHelpers(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { engine.Close() })

	svc := NewService(engine, 4, nil)
	require.Equal(t, "unknown", svc.CurrentStrategy())
	require.Equal(t, 0, svc.FulltextDocCount())

	svc.setFulltext(nil)
	require.Equal(t, 0, svc.FulltextDocCount())

	svc.MarkReadyDisabled()
	require.True(t, svc.IsReady())
	require.Equal(t, 0, svc.FulltextDocCount())
}

func TestPropertyVectorHelpers(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { engine.Close() })

	svc := NewService(engine, 4, nil)
	svc.buildInProgress.Store(true)
	svc.nodePropVector = map[string]map[string]string{
		"node-1": {"title": nodePropertyVectorKey("node-1", "title"), "body": nodePropertyVectorKey("node-1", "body")},
		"node-2": {"title": nodePropertyVectorKey("node-2", "title")},
	}

	require.NoError(t, svc.vectorIndex.Add(nodePropertyVectorKey("node-1", "title"), []float32{1, 0, 0, 0}))
	require.NoError(t, svc.vectorIndex.Add(nodePropertyVectorKey("node-1", "body"), []float32{0, 1, 0, 0}))
	require.NoError(t, svc.vectorIndex.Add(nodePropertyVectorKey("node-2", "title"), []float32{0, 0, 1, 0}))

	require.Equal(t, 2, svc.CountPropertyVectorEntries("title"))
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))
	require.Equal(t, 0, svc.CountPropertyVectorEntries("missing"))
	require.Equal(t, 0, (*Service)(nil).CountPropertyVectorEntries("title"))

	svc.RemovePropertyVectorIndex("title")
	require.Equal(t, 0, svc.CountPropertyVectorEntries("title"))
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))
	require.Contains(t, svc.nodePropVector["node-1"], "body")
	_, exists := svc.nodePropVector["node-2"]
	require.False(t, exists)

	svc.RemovePropertyVectorIndex("")
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))

	(*Service)(nil).RemovePropertyVectorIndex("body")

	liveSvc := NewService(engine, 4, nil)
	liveSvc.resultCache.Put("cached", &SearchResponse{Query: "cached"})
	liveSvc.nodePropVector = map[string]map[string]string{
		"node-1": {"body": nodePropertyVectorKey("node-1", "body")},
		"node-2": {"title": nodePropertyVectorKey("node-2", "title")},
	}
	require.NoError(t, liveSvc.vectorIndex.Add(nodePropertyVectorKey("node-1", "body"), []float32{0, 1, 0, 0}))
	require.NoError(t, liveSvc.vectorIndex.Add(nodePropertyVectorKey("node-2", "title"), []float32{0, 0, 1, 0}))

	liveSvc.RemovePropertyVectorIndex("body")
	require.Nil(t, liveSvc.resultCache.Get("cached"))
	require.Equal(t, 0, liveSvc.CountPropertyVectorEntries("body"))
	require.Equal(t, 1, liveSvc.CountPropertyVectorEntries("title"))
	if liveSvc.persistTimer != nil {
		liveSvc.persistTimer.Stop()
	}
}

func TestScoreVectorIDDotBranches(t *testing.T) {
	svc := &Service{}
	score, ok := svc.scoreVectorIDDot([]float32{1, 0}, "missing")
	require.False(t, ok)
	require.Equal(t, 0.0, score)

	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { engine.Close() })
	svc = NewService(engine, 2, nil)
	score, ok = svc.scoreVectorIDDot([]float32{1, 0}, "missing")
	require.False(t, ok)
	require.Equal(t, 0.0, score)

	require.NoError(t, svc.vectorIndex.Add("v1", []float32{0.5, 0.5}))
	score, ok = svc.scoreVectorIDDot([]float32{1, 0}, "v1")
	require.True(t, ok)
	require.InDelta(t, 0.70710677, score, 1e-6)
}

func TestIVFPQBackgroundPersistenceNoopBranches(t *testing.T) {
	svc := &Service{}
	require.Empty(t, svc.ivfpqPersistenceBasePath("", ""))
	require.Equal(t, "hnsw-path", svc.ivfpqPersistenceBasePath("vector-path", "hnsw-path"))
	require.Equal(t, "vector-path", svc.ivfpqPersistenceBasePath("vector-path", ""))

	svc.persistIVFPQBackground("", "")
	svc.persistIVFPQBackground(filepath.Join(t.TempDir(), "vectors"), "")
	svc.ivfpqIndex = &IVFPQIndex{}
	svc.persistIVFPQBackground(filepath.Join(t.TempDir(), "vectors"), "")
}

func TestFilterByPropertiesDirect(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng, DefaultVectorDimensions, nil)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{"Doc"}, map[string]any{"color": "blue", "tag": "keep"})
	makeNode(t, eng, "nornic:n2", []string{"Doc"}, map[string]any{"color": "red", "tag": "skip"})

	results := buildFilterResults("nornic:n1", "nornic:n2", "nornic:missing")
	seenOrphans := map[string]bool{}

	filtered := svc.filterByProperties(ctx, results, &SearchOptions{Filters: map[string][]string{"color": {"blue"}}}, seenOrphans)
	require.Len(t, filtered, 1)
	require.Equal(t, "nornic:n1", filtered[0].ID)
	require.True(t, seenOrphans["nornic:missing"])

	all := svc.filterByProperties(ctx, results[:2], &SearchOptions{}, seenOrphans)
	require.Equal(t, results[:2], all)
}
