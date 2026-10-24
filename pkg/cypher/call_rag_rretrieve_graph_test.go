package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// bridgeFirstReranker ranks a candidate mentioning a bridge first.
type bridgeFirstReranker struct {
	calls     *int
	available bool
}

func (bridgeFirstReranker) Name() string                       { return "bridge-first" }
func (bridgeFirstReranker) Enabled() bool                      { return true }
func (r bridgeFirstReranker) IsAvailable(context.Context) bool { return r.available }
func (r bridgeFirstReranker) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	*r.calls++
	ranked := make([]search.RerankResult, 0, len(candidates))
	for index, candidate := range candidates {
		score := 0.1
		if strings.Contains(candidate.Content, "bridge") {
			score = 0.9
		}
		ranked = append(ranked, search.RerankResult{ID: candidate.ID, Content: candidate.Content, OriginalRank: index + 1, FinalScore: score})
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].FinalScore > ranked[j].FinalScore })
	return ranked, nil
}

// db.rretrieveGraph reranks the fused pool of nodes and relationships with
// the configured reranker, in its rows and its page; db.retrieveGraph
// doesn't; a failing reranker keeps the fused order and reports rerank_failed;
// an unavailable one isn't called.
func TestCallDbRRetrieveGraphReranksFusedPool(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)
	_, err := exec.Execute(ctx, `CREATE (:Doc {name: 'a', content: 'zephyrine zephyrine harbour notes'})-[:CITES {reason: 'zephyrine bridge'}]->(:Doc {name: 'b', content: 'unrelated'}),
		(:Note {name: 'n', content: 'zephyrine zephyrine'})`, nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))
	require.Eventually(t, func() bool {
		result, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine'})", nil)
		return err == nil && len(result.Rows) == 3
	}, 5*time.Second, 20*time.Millisecond)

	calls := 0
	svc.SetReranker(bridgeFirstReranker{calls: &calls, available: true})
	const rows = "YIELD kind, search_method, fallback_reason RETURN kind, search_method, fallback_reason"
	plain, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine', embedding: [1.0, 0.0, 0.0]}) "+rows, nil)
	require.NoError(t, err)
	require.Equal(t, 0, calls, "db.retrieveGraph doesn't follow the configured reranker")
	require.NotEqual(t, "relationship", plain.Rows[0][0], "the relationship isn't first by fused rank")

	reranked, err := exec.Execute(ctx, "CALL db.rretrieveGraph({query: 'zephyrine', embedding: [1.0, 0.0, 0.0]}) "+rows, nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Len(t, reranked.Rows, 3)
	require.Equal(t, "relationship", reranked.Rows[0][0])
	require.True(t, strings.HasSuffix(reranked.Rows[0][1].(string), "+rerank"), reranked.Rows[0][1])
	require.Equal(t, "", reranked.Rows[0][2])

	// The page form, yielded after another clause.
	page, err := exec.Execute(ctx, "WITH 'zephyrine' AS q CALL db.rretrieveGraph({query: q, embedding: [1.0, 0.0, 0.0], n: 5}) YIELD page RETURN page.results[0].kind AS first, page.search_method AS method", nil)
	require.NoError(t, err)
	require.Equal(t, "relationship", page.Rows[0][0])
	require.True(t, strings.HasSuffix(page.Rows[0][1].(string), "+rerank"), page.Rows[0][1])

	// A failing reranker: the fused order, and rerank_failed.
	svc.SetReranker(failingQueryChunkReranker{})
	// (Each statement's text differs: the executor caches a read's result.)
	failed, err := exec.Execute(ctx, "CALL db.rretrieveGraph({query: 'zephyrine', embedding: [1.0, 0.0, 0.0], limit: 9}) "+rows, nil)
	require.NoError(t, err)
	require.Equal(t, plain.Rows[0][0], failed.Rows[0][0])
	require.Equal(t, string(search.SearchFallbackRerankFailed), failed.Rows[0][2])

	// An unavailable reranker isn't called.
	calls = 0
	svc.SetReranker(bridgeFirstReranker{calls: &calls, available: false})
	_, err = exec.Execute(ctx, "CALL db.rretrieveGraph({query: 'zephyrine', embedding: [1.0, 0.0, 0.0], limit: 8})", nil)
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}

// oneChunkMatchingEmbedder embeds "bravo" towards the test's node and every
// other chunk away from it, so only the first chunk of a query matches.
type oneChunkMatchingEmbedder struct{ chunks []string }

func (e oneChunkMatchingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if text == "bravo" {
		return []float32{1, 0}, nil
	}
	return []float32{-1, 0}, nil
}

func (e oneChunkMatchingEmbedder) ChunkText(string, int, int) ([]string, error) { return e.chunks, nil }

// A query split into chunks of which only one matches: the chunks that match
// nothing don't report rerank_no_candidates, and one reranked hit reports
// rerank_scores_flat, as db.rretrieve does.
func TestCallDbRRetrieveGraphReranksChunkedQueryHits(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(oneChunkMatchingEmbedder{chunks: []string{"bravo", "zzzz", "yyyy"}})
	_, err := store.CreateNode(&storage.Node{ID: "src", Labels: []string{"Source"}, Properties: map[string]interface{}{"text": "bravo notes"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	svc := search.NewServiceWithDimensions(store, 2)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)
	calls := 0
	svc.SetReranker(bridgeFirstReranker{calls: &calls, available: true})

	for _, procedure := range []string{"db.rretrieveGraph", "db.rretrieve"} {
		result, err := exec.Execute(ctx, "CALL "+procedure+"({query: 'bravo zzzz yyyy', limit: 5}) YIELD fallback_reason RETURN fallback_reason", nil)
		require.NoError(t, err, procedure)
		require.Len(t, result.Rows, 1, procedure)
		require.Equal(t, string(search.SearchFallbackRerankScoresFlat), result.Rows[0][0], procedure)
	}
	require.Greater(t, calls, 0)
}
