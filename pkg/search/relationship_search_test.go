package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// wordVector embeds text as a bag of three words, enough to rank by meaning
// in tests: "bridge" and "span" mean the same.
func wordVector(text string) []float32 {
	vector := make([]float32, 3)
	for _, word := range strings.Fields(strings.ToLower(text)) {
		switch word {
		case "bridge", "span":
			vector[0]++
		case "river":
			vector[1]++
		default:
			vector[2] += 0.1
		}
	}
	return vector
}

type countingEmbed struct {
	calls atomic.Int64
	fail  atomic.Bool
}

func (c *countingEmbed) embed(_ context.Context, edge *storage.Edge) (RelationshipEmbedding, error) {
	c.calls.Add(1)
	if c.fail.Load() {
		return RelationshipEmbedding{}, errors.New("no embedder yet")
	}
	reason, _ := edge.Properties["reason"].(string)
	if reason == "" {
		return RelationshipEmbedding{}, nil
	}
	return RelationshipEmbedding{Vectors: [][]float32{wordVector(reason)}}, nil
}

func relationshipTestEngine(t *testing.T) storage.Engine {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"name": "doc " + string(id)}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"reason": "zephyrine bridge over the river"}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r2", StartNode: "b", EndNode: "c", Type: "CONTRADICTS", Properties: map[string]any{"reason": "quarrel about dates"}}))
	return engine
}

func startRelationshipService(t *testing.T, engine storage.Engine, embed RelationshipEmbedFunc, configure func(*Service)) *Service {
	t.Helper()
	svc := fixtureowner.Own(t, NewService(engine, 3, nil))
	if configure != nil {
		configure(svc)
	}
	svc.EnableRelationshipSearch(embed)
	svc.StartRelationshipIndex()
	return svc
}

// waitRelationshipVectors waits for the expected vector population and any
// additional publication boundary required by the caller.
func waitRelationshipVectors(t *testing.T, svc *Service, want int, published ...func(*Service) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		if svc.RelationshipEmbeddingCount() != want {
			return false
		}
		for _, boundary := range published {
			if !boundary(svc) {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

// relationshipSnapshotPublished checks the complete durable image, independently
// of vector visibility. Both boundaries must hold before read/reopen assertions.
func relationshipSnapshotPublished(svc *Service) bool {
	saved, err := svc.loadRelationshipVectors()
	if err != nil {
		return false
	}
	rs := svc.relationshipSearch()
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	count := 0
	for id, entry := range rs.entries {
		if len(entry.Vectors) == 0 {
			continue
		}
		count++
		stored := saved[id]
		if stored == nil || stored.Source != entry.Source || !reflect.DeepEqual(stored.Vectors, entry.Vectors) || !reflect.DeepEqual(stored.Spans, entry.Spans) {
			return false
		}
	}
	return len(saved) == count
}

// Relationship search finds a relationship by a word in its properties, and
// by meaning with an embedding, naming its type and both endpoints; Types
// filters by relationship type; node search never returns relationships
// (#1065).
func TestRelationshipSearch(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, nil)
	require.True(t, svc.RelationshipSearchEnabled())
	waitRelationshipVectors(t, svc, 2)

	opts := DefaultSearchOptions()
	opts.Relationships = true
	response, err := svc.Search(ctx, "zephyrine", nil, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	hit := response.Results[0]
	require.Equal(t, storage.EdgeID("r1"), hit.Relationship.ID)
	require.Equal(t, "r1", hit.ID)
	require.Equal(t, "CITES", hit.Relationship.Type)
	require.Equal(t, storage.NodeID("a"), hit.Relationship.StartNode)
	require.Equal(t, storage.NodeID("b"), hit.Relationship.EndNode)
	require.Equal(t, "zephyrine bridge over the river", hit.Properties["reason"])
	require.Equal(t, 1, hit.BM25Rank)
	require.Empty(t, hit.NodeID)
	require.Equal(t, "relationship_bm25", response.SearchMethod)

	// By meaning: "span" shares no word with r1, only its vector.
	response, err = svc.SearchRelationships(ctx, "span", wordVector("span"), opts)
	require.NoError(t, err)
	require.NotEmpty(t, response.Results)
	require.Equal(t, storage.EdgeID("r1"), response.Results[0].Relationship.ID)
	require.Equal(t, 1, response.Results[0].VectorRank)
	require.Equal(t, "relationship_vector", response.SearchMethod)
	response, err = svc.SearchRelationships(ctx, "quarrel", wordVector("quarrel"), opts)
	require.NoError(t, err)
	require.Equal(t, "relationship_rrf_hybrid", response.SearchMethod)
	require.Equal(t, storage.EdgeID("r2"), response.Results[0].Relationship.ID)

	// The type is searchable, and filters.
	response, err = svc.SearchRelationships(ctx, "CONTRADICTS", nil, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	typed := *opts
	typed.Types = []string{"CITES"}
	response, err = svc.SearchRelationships(ctx, "quarrel zephyrine", nil, &typed)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "CITES", response.Results[0].Relationship.Type)

	// Node search is unchanged: no relationship among its results.
	require.NoError(t, svc.BuildIndexes(ctx))
	nodes, err := svc.Search(ctx, "zephyrine", nil, DefaultSearchOptions())
	require.NoError(t, err)
	require.Empty(t, nodes.Results)
}

// Writes reach relationship search through IndexEdge and RemoveEdge; a
// relationship whose text changed is embedded again; a deleted one isn't
// returned even before it is removed (#1065).
func TestRelationshipSearchFollowsWrites(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, nil)
	waitRelationshipVectors(t, svc, 2)
	opts := &SearchOptions{Limit: 10, Relationships: true}

	edge := &storage.Edge{ID: "r3", StartNode: "a", EndNode: "c", Type: "CITES", Properties: map[string]any{"reason": "obsidian river"}}
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, svc.IndexEdge(edge))
	waitRelationshipVectors(t, svc, 3)
	response, err := svc.SearchRelationships(ctx, "obsidian", nil, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)

	// Unchanged text: not embedded again. Changed: embedded again.
	calls := embed.calls.Load()
	require.NoError(t, svc.IndexEdge(edge))
	edge.Properties = map[string]any{"reason": "basalt span"}
	require.NoError(t, engine.UpdateEdge(storage.EdgeUpdateRequest{After: edge}))
	require.NoError(t, svc.IndexEdge(edge))
	require.Eventually(t, func() bool { return embed.calls.Load() == calls+1 }, 5*time.Second, 10*time.Millisecond)
	waitRelationshipVectors(t, svc, 3)
	response, err = svc.SearchRelationships(ctx, "obsidian", nil, opts)
	require.NoError(t, err)
	require.Empty(t, response.Results)
	response, err = svc.SearchRelationships(ctx, "basalt", nil, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)

	// Deleted in storage: not returned. Removed: gone from the index.
	require.NoError(t, engine.DeleteEdge("r3"))
	response, err = svc.SearchRelationships(ctx, "basalt", nil, opts)
	require.NoError(t, err)
	require.Empty(t, response.Results)
	require.NoError(t, svc.RemoveEdge("r3"))
	waitRelationshipVectors(t, svc, 2)

	// A relationship with no text leaves the keyword index.
	empty := &storage.Edge{ID: "r1", StartNode: "a", EndNode: "b", Type: "", Properties: map[string]any{}}
	svc.IndexRelationship(empty)
	response, err = svc.SearchRelationships(ctx, "zephyrine", nil, opts)
	require.NoError(t, err)
	require.Empty(t, response.Results)
}

// With search indexes persisted, the vectors are saved, and a new service
// keeps those of unchanged relationships instead of embedding them again. An
// embed failure is retried (#1065).
func TestRelationshipSearchPersistsVectors(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	path := filepath.Join(t.TempDir(), "bm25")
	persisted := func(svc *Service) {
		svc.SetPersistenceEnabled(true)
		svc.SetFulltextIndexPath(path)
	}
	first := &countingEmbed{}
	svc := startRelationshipService(t, engine, first.embed, persisted)
	waitRelationshipVectors(t, svc, 2, relationshipSnapshotPublished)
	require.NoError(t, svc.Close())

	edge, err := engine.GetEdge("r2")
	require.NoError(t, err)
	edge.Properties["reason"] = "quarrel about river dates"
	require.NoError(t, engine.UpdateEdge(storage.EdgeUpdateRequest{After: edge}))
	second := &countingEmbed{}
	svc = startRelationshipService(t, engine, second.embed, persisted)
	waitRelationshipVectors(t, svc, 2, relationshipSnapshotPublished)
	require.Equal(t, int64(1), second.calls.Load(), "only the changed relationship is embedded")
	response, err := svc.SearchRelationships(ctx, "span", wordVector("span"), &SearchOptions{Limit: 1, Relationships: true})
	require.NoError(t, err)
	require.Equal(t, storage.EdgeID("r1"), response.Results[0].Relationship.ID)
	require.NoError(t, svc.Close())

	// No embedder yet: nothing embedded, and a search still works by keyword.
	failing := &countingEmbed{}
	failing.fail.Store(true)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r4", StartNode: "c", EndNode: "a", Type: "CITES", Properties: map[string]any{"reason": "granite"}}))
	svc = startRelationshipService(t, engine, failing.embed, persisted)
	require.Eventually(t, func() bool { return failing.calls.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)
	response, err = svc.SearchRelationships(ctx, "granite", nil, &SearchOptions{Relationships: true})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, 2, svc.RelationshipEmbeddingCount())
}

// Off by default: a relationship search fails with a localized error, and
// the relationship hooks are no-ops (#1065).
func TestRelationshipSearchDisabled(t *testing.T) {
	svc := NewService(relationshipTestEngine(t), 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.False(t, svc.RelationshipSearchEnabled())
	require.Zero(t, svc.RelationshipEmbeddingCount())
	svc.StartRelationshipIndex()
	svc.IndexRelationship(&storage.Edge{ID: "r1"})
	svc.RemoveRelationship("r1")
	_, err := svc.Search(context.Background(), "zephyrine", nil, &SearchOptions{Relationships: true})
	require.ErrorIs(t, err, ErrRelationshipSearchDisabled)
	require.ErrorContains(t, err, "NORNICDB_SEARCH_RELATIONSHIPS_ENABLED")
	var nilService *Service
	require.False(t, nilService.RelationshipSearchEnabled())
	nilService.EnableRelationshipSearch(nil)

	// Keyword-only, without an embed function: no vectors, keyword hits.
	keyword := startRelationshipService(t, relationshipTestEngine(t), nil, nil)
	response, err := keyword.SearchRelationships(context.Background(), "zephyrine", wordVector("bridge"), nil)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Zero(t, keyword.RelationshipEmbeddingCount())
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	pending := NewService(relationshipTestEngine(t), 3, nil)
	t.Cleanup(func() { _ = pending.Close() })
	pending.EnableRelationshipSearch(nil)
	_, err = pending.SearchRelationships(cancelled, "zephyrine", nil, nil)
	require.ErrorIs(t, err, context.Canceled, "a search waits for the index build")
}

type failingEdgeStreamEngine struct{ storage.Engine }

func (failingEdgeStreamEngine) AllEdges() ([]*storage.Edge, error) {
	return nil, errors.New("storage unavailable")
}

// Results carry a relationship's title, description and preview, keep to
// the limit and break ties by ID; vectors of another dimension and zero
// vectors are skipped; a failed build, a deleted relationship, an unreadable
// or unwritable vector file and a failed embedding are survived (#1065).
func TestRelationshipSearchEdges(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r5", StartNode: "a", EndNode: "c", Type: "CITES", Properties: map[string]any{"title": "Twin", "description": "first twin", "content": "twin content"}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r6", StartNode: "c", EndNode: "a", Type: "CITES", Properties: map[string]any{"title": "Twin", "description": "first twin", "text": "twin content"}}))
	svc := startRelationshipService(t, engine, nil, nil)
	response, err := svc.SearchRelationships(ctx, "twin", nil, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, "r5", response.Results[0].ID, "equal scores order by ID")
	require.Equal(t, "Twin", response.Results[0].Title)
	require.Equal(t, "first twin", response.Results[0].Description)
	require.Equal(t, "twin content", response.Results[0].ContentPreview)
	response, err = svc.SearchRelationships(ctx, "twin", nil, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Equal(t, "twin content", response.Results[1].ContentPreview)

	// Another dimension, and a zero vector: no vector hit.
	rs := svc.relationshipSearch()
	rs.mu.Lock()
	rs.entries["r5"].Vectors = [][]float32{{1, 0}}
	rs.entries["r6"].Vectors = [][]float32{normalizeVectorCopy([]float32{0, 0, 0})}
	rs.mu.Unlock()
	response, err = svc.SearchRelationships(ctx, "nothing", []float32{0, 0, 1}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	for _, result := range response.Results {
		require.NotEqual(t, "r5", result.ID)
	}

	// A build whose storage scan fails still opens, empty.
	failing := NewService(failingEdgeStreamEngine{Engine: engine}, 3, nil)
	t.Cleanup(func() { _ = failing.Close() })
	failing.EnableRelationshipSearch(nil)
	failing.StartRelationshipIndex()
	response, err = failing.SearchRelationships(ctx, "twin", nil, nil)
	require.NoError(t, err)
	require.Empty(t, response.Results)
	var nilService *Service
	require.NoError(t, nilService.IndexEdge(&storage.Edge{ID: "x"}))

	// An unreadable vector file is reported and ignored; an unwritable one is logged.
	dir := t.TempDir()
	path := filepath.Join(dir, "bm25")
	require.NoError(t, os.WriteFile(path+relationshipVectorsFileSuffix, []byte("not gob"), 0o600))
	persisted := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = persisted.Close() })
	persisted.SetPersistenceEnabled(true)
	persisted.SetFulltextIndexPath(path)
	unreadable, err := persisted.loadRelationshipVectors()
	require.Error(t, err, "an unreadable vector file is reported")
	require.Nil(t, unreadable)
	require.NoError(t, os.Remove(path+relationshipVectorsFileSuffix))
	require.NoError(t, os.MkdirAll(filepath.Join(path+relationshipVectorsFileSuffix, "occupied"), 0o700))
	persisted.EnableRelationshipSearch(nil)
	prs := persisted.relationshipSearch()
	prs.entries["r1"] = &relationshipEntry{Source: 1, Vectors: [][]float32{{1, 0, 0}}}
	persisted.saveRelationshipVectors(prs)
	_, err = os.Stat(path + relationshipVectorsFileSuffix + ".tmp")
	require.True(t, os.IsNotExist(err), "the temporary file is removed")
	missingDir := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = missingDir.Close() })
	missingDir.SetPersistenceEnabled(true)
	missingDir.SetFulltextIndexPath(filepath.Join(dir, "missing", "bm25"))
	missingDir.saveRelationshipVectors(prs)

	// A relationship deleted before it is embedded is skipped; a failed
	// embedding is retried after the delay.
	previous := relationshipEmbedRetryDelay
	relationshipEmbedRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { relationshipEmbedRetryDelay = previous })
	flaky := &countingEmbed{}
	flaky.fail.Store(true)
	retrying := startRelationshipService(t, relationshipTestEngine(t), flaky.embed, nil)
	require.Eventually(t, func() bool { return flaky.calls.Load() >= 2 }, 5*time.Second, 10*time.Millisecond, "retried")
	flaky.fail.Store(false)
	waitRelationshipVectors(t, retrying, 2)
	retrying.relationshipSearch().mu.Lock()
	retrying.relationshipSearch().pending["gone"] = nil
	retrying.relationshipSearch().mu.Unlock()
	changed, failed := retrying.embedPendingRelationships(ctx, retrying.relationshipSearch())
	require.False(t, changed)
	require.False(t, failed)
}

// Relationships equally similar to the query are ordered by ID among the
// vector hits, and so are relationships equal in the fused ranking (here one
// first by vector only, the other first by keyword only); a nil service has
// no relationship search.
func TestRelationshipSearchTiesOrderByID(t *testing.T) {
	ctx := context.Background()
	same := func(context.Context, *storage.Edge) (RelationshipEmbedding, error) {
		return RelationshipEmbedding{Vectors: [][]float32{{1, 0, 0}}}, nil
	}
	svc := startRelationshipService(t, relationshipTestEngine(t), same, nil)
	waitRelationshipVectors(t, svc, 2)
	response, err := svc.SearchRelationships(ctx, "unmatched", []float32{1, 0, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 2)
	require.Equal(t, []int{1, 2}, []int{response.Results[0].VectorRank, response.Results[1].VectorRank})
	require.Equal(t, "r1", response.Results[0].ID)

	onlyR1 := func(_ context.Context, edge *storage.Edge) (RelationshipEmbedding, error) {
		if edge.ID == "r1" {
			return RelationshipEmbedding{Vectors: [][]float32{{1, 0, 0}}}, nil
		}
		return RelationshipEmbedding{}, nil
	}
	svc = startRelationshipService(t, relationshipTestEngine(t), onlyR1, nil)
	waitRelationshipVectors(t, svc, 1)
	response, err = svc.SearchRelationships(ctx, "quarrel", []float32{1, 0, 0}, &SearchOptions{Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.Results, 2)
	require.Equal(t, response.Results[0].Score, response.Results[1].Score)
	require.Equal(t, []string{"r1", "r2"}, []string{response.Results[0].ID, response.Results[1].ID})

	var nilService *Service
	require.Nil(t, nilService.relationshipSearch())
}

// A relationship reported before it is readable (a buffered commit can
// report a write before reads see it) is embedded from the write's copy.
func TestRelationshipSearchEmbedsReportedCopy(t *testing.T) {
	engine := relationshipTestEngine(t)
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, nil)
	waitRelationshipVectors(t, svc, 2)
	svc.IndexRelationship(&storage.Edge{ID: "unread", StartNode: "a", EndNode: "c", Type: "CITES", Properties: map[string]any{"reason": "a bridge not yet stored"}})
	waitRelationshipVectors(t, svc, 3)
	svc.WakeRelationshipEmbedding() // no pending work: a no-op
	NewService(engine, 3, nil).WakeRelationshipEmbedding()
}

// A relationship hit carries matches as a node hit does: the query terms in
// its properties, with their offsets, and the best chunks of a vector hit;
// SkipMatches leaves them out.
func TestRelationshipSearchMatches(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, nil)
	waitRelationshipVectors(t, svc, 2)

	opts := DefaultSearchOptions()
	opts.Relationships = true
	response, err := svc.SearchRelationships(ctx, "river", nil, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 1)
	require.Equal(t, []SearchMatch{{Kind: SearchMatchTerm, Chunk: -1, Term: "river", Property: "reason", Start: 26, End: 31}}, response.Results[0].Matches)

	response, err = svc.SearchRelationships(ctx, "", wordVector("span"), opts)
	require.NoError(t, err)
	require.Equal(t, storage.EdgeID("r1"), response.Results[0].Relationship.ID)
	matches := response.Results[0].Matches
	require.Len(t, matches, 1)
	require.Equal(t, SearchMatchChunk, matches[0].Kind)
	require.Equal(t, 0, matches[0].Chunk)
	require.Greater(t, matches[0].Score, 0.5)
	require.Empty(t, matches[0].Property)

	skipped := *opts
	skipped.SkipMatches = true
	response, err = svc.SearchRelationships(ctx, "river", wordVector("river"), &skipped)
	require.NoError(t, err)
	require.Empty(t, response.Results[0].Matches)
}
