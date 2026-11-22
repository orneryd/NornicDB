package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// reverseWindowReranker records the actual provider boundary and reverses each
// submitted window. Scores are distinct within a window, not globally ordered.
type reverseWindowReranker struct {
	mu    sync.Mutex
	calls [][]string
}

func (*reverseWindowReranker) Name() string                     { return "reverse-window" }
func (*reverseWindowReranker) Enabled() bool                    { return true }
func (*reverseWindowReranker) IsAvailable(context.Context) bool { return true }
func (r *reverseWindowReranker) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	ids := make([]string, len(candidates))
	results := make([]search.RerankResult, len(candidates))
	for i, candidate := range candidates {
		ids[i] = candidate.ID
		at := len(candidates) - 1 - i
		results[at] = search.RerankResult{ID: candidate.ID, OriginalRank: i + 1, NewRank: at + 1, FinalScore: 1 - float64(at)/float64(len(candidates)+1)}
	}
	r.mu.Lock()
	r.calls = append(r.calls, ids)
	r.mu.Unlock()
	return results, nil
}

// Exercise the public configured-rerank adapter: the search.Service tests use
// service.Search directly, so cannot catch this adapter re-enabling Stage 2
// after the continuation producer disabled it for individual chunks.
func TestRetrieveContinuationOwnsRerankWindows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, procedure string
			chunks          []string
			supplied        bool
			ceiling         int
		}{
			{name: "embedded", procedure: "db.rretrieve", chunks: []string{"alpha"}},
			{name: "multiple chunks", procedure: "db.rretrieve", chunks: []string{"alpha", "alpha beta"}},
			{name: "supplied embedding", procedure: "db.rretrieve", supplied: true},
			{name: "plain retrieval", procedure: "db.retrieve", chunks: []string{"alpha"}},
			{name: "caller ceiling", procedure: "db.rretrieve", chunks: []string{"alpha"}, ceiling: 3},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ranked_windows")
				for i := 0; i < 12; i++ {
					_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("note-%02d", i)), Labels: []string{"Note"}, Properties: map[string]any{"content": "alpha beta synthetic note"}, ChunkEmbeddings: [][]float32{{1, 0}}})
					require.NoError(t, err)
				}
				service := search.NewService(store, 2, nil)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				require.NoError(t, service.BuildIndexes(ctx))
				service.SetSearchResultCachePolicy(0, 0)
				provider := &reverseWindowReranker{}
				service.SetReranker(provider)
				exec := NewStorageExecutor(store)
				exec.SetSearchService(service)
				exec.SetEmbedder(rerankTruthfulnessEmbedder{chunks: tc.chunks})
				request := map[string]any{"query": "alpha", "limit": int64(3), "n": int64(2), "mode": "ranked", "rerankTopK": int64(4), "minRRFScore": float64(0)}
				if tc.supplied {
					request["embedding"] = []any{float64(1), float64(0)}
				}
				if tc.ceiling > 0 {
					request["maxResults"] = int64(tc.ceiling)
				}
				want := 12
				if tc.ceiling > 0 {
					want = tc.ceiling
				}
				var got []string
				seen := map[string]bool{}
				var terminal map[string]any
				for pageIndex := 0; pageIndex < 12; pageIndex++ {
					result, err := exec.Execute(ctx, "CALL "+tc.procedure+"($request) YIELD page RETURN page", map[string]any{"request": request})
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					page := result.Rows[0][0].(map[string]any)
					require.Equal(t, int64(len(got)), page["position"])
					require.Equal(t, false, page["fallback_triggered"])
					if page["total"] != nil {
						require.Equal(t, int64(want), page["total"], "known total describes the admitted stream, not this page")
					}
					for _, value := range page["results"].([]any) {
						node := value.(map[string]any)["node"].(*storage.Node)
						id := string(node.ID)
						require.False(t, seen[id], "duplicate delivered ID %s", id)
						seen[id] = true
						got = append(got, id)
					}
					if !page["has_more"].(bool) {
						terminal = page
						break
					}
					request = map[string]any{"qid": page["qid"], "n": int64(2)}
				}
				require.NotNil(t, terminal, "stream must terminate")
				require.Len(t, got, want, "continuation must deliver the eligible population up to the caller ceiling")
				require.Equal(t, int64(want), terminal["total"])
				require.Equal(t, int64(want), terminal["ranked_count"])
				require.Equal(t, int64(want), terminal["discovered"])
				if tc.ceiling == 0 {
					require.Equal(t, true, terminal["collection_exhausted"])
					require.Equal(t, true, terminal["ranked_pool_exhausted"])
					require.Equal(t, search.SearchContinuationCollectionComplete, terminal["completion"])
				} else {
					require.Equal(t, false, terminal["collection_exhausted"])
					require.Equal(t, false, terminal["ranked_pool_exhausted"])
					require.Equal(t, search.SearchContinuationMaxResultsComplete, terminal["completion"])
				}
				provider.mu.Lock()
				defer provider.mu.Unlock()
				if tc.procedure == "db.retrieve" {
					require.Empty(t, provider.calls)
					return
				}
				var expected []string
				submitted := map[string]bool{}
				for _, call := range provider.calls {
					require.LessOrEqual(t, len(call), 4, "TopK bounds provider windows")
					for i := len(call) - 1; i >= 0; i-- {
						require.False(t, submitted[call[i]], "provider candidate must be submitted once")
						submitted[call[i]] = true
						expected = append(expected, call[i])
					}
				}
				require.Len(t, expected, want)
				require.Equal(t, expected, got, "retain each admitted window's provider order across pages")
			})
		}
	})
}
