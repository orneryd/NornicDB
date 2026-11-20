package voyage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRerankerMergedOrderMatchesOneProviderRequest is the NOR401 audit of the
// global batched ranking at the reported 250-document, 8192-character window:
// rerank-2.5's documented 600,000-token budget splits it into consecutive
// provider requests, and the merged answer must reproduce the order, scores and
// candidate identities one provider request over the same population would have
// returned.
//
// The reference is the same adapter pointed at a model Voyage doesn't document,
// which keeps the API's 1000-document cap and no invented token budget, so it
// sends the same 250 documents in one request. Both stubs score documents with
// the same deterministic function, so the two answers are comparable.
func TestRerankerMergedOrderMatchesOneProviderRequest(t *testing.T) {
	const (
		queryBytes = 512
		document   = 8192
		count      = 250
	)
	query := strings.Repeat("q", queryBytes)
	candidates := rerankBatchCandidates(count, document)

	// One document costs the query plus that document, so the published
	// budget admits this many documents per request.
	perRequest := 600_000 / (len(query) + document)
	wantCounts := make([]int, 0, count/perRequest+1)
	for remaining := count; remaining > 0; {
		size := min(remaining, perRequest)
		wantCounts = append(wantCounts, size)
		remaining -= size
	}
	require.Greater(t, len(wantCounts), 1, "the reported window has to exceed one request")

	// The comparison is only well defined when every document has its own
	// provider score: equal scores keep the merged window's order, which is not
	// comparable to a single provider order.
	scores := make(map[float64]string, count)
	for _, candidate := range candidates {
		score := rerankStubScore(candidate.Content)
		if previous, ok := scores[score]; ok {
			t.Fatalf("deterministic stub score collision between %s and %s", previous, candidate.ID)
		}
		scores[score] = candidate.ID
	}

	batched := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
	batchedResults, err := rerankNewReranker(t, batched, "rerank-2.5", false).Rerank(context.Background(), query, candidates)
	require.NoError(t, err)
	require.Equal(t, wantCounts, batched.documentCounts(), "the published budget splits the window")
	for _, request := range batched.recorded() {
		require.Equal(t, query, request.query)
		require.True(t, request.truncation)
		require.LessOrEqual(t, len(request.documents), 1000)
		require.LessOrEqual(t, request.cost, 600_000)
		require.LessOrEqual(t, len(request.documents)*(len(query)+document), 600_000, "the query is counted for every document")
	}

	single := newRerankStub(t, rerankStubLimits{})
	singleResults, err := rerankNewReranker(t, single, "rerank-custom-9", false).Rerank(context.Background(), query, candidates)
	require.NoError(t, err)
	require.Equal(t, []int{count}, single.documentCounts(), "the reference population is one provider request")

	require.Len(t, batchedResults, count)
	require.Len(t, singleResults, count)
	for position := range singleResults {
		require.Equal(t, singleResults[position].ID, batchedResults[position].ID, "merged position %d", position)
		require.Equal(t, singleResults[position].Content, batchedResults[position].Content, "merged position %d", position)
		require.Equal(t, singleResults[position].OriginalRank, batchedResults[position].OriginalRank, "merged position %d", position)
		require.Equal(t, singleResults[position].NewRank, batchedResults[position].NewRank, "merged position %d", position)
		require.Equal(t, singleResults[position].BiScore, batchedResults[position].BiScore, "merged position %d", position)
		require.Equal(t, singleResults[position].CrossScore, batchedResults[position].CrossScore, "merged position %d", position)
		require.Equal(t, singleResults[position].FinalScore, batchedResults[position].FinalScore, "merged position %d", position)
		require.False(t, batchedResults[position].Filtered, "merged position %d", position)
	}
	require.NotEqual(t, candidates[0].ID, batchedResults[0].ID, "the merged answer is a provider ranking, not the submission order")
}

// TestRerankResponseCarriesProviderUsage records the NOR401 usage census at the
// mechanical provider boundary: the client normalizes the provider's
// usage.total_tokens for every rerank response, exactly as it does for
// embeddings. Reranker.Rerank does not aggregate or report those totals, so the
// audit's usage-census parity claim has no owner above this client contract; the
// witness here is the response field the census would read.
func TestRerankResponseCarriesProviderUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data":  []map[string]any{{"index": 0, "relevance_score": 0.9}},
			"model": "rerank-2.5",
			"usage": map[string]any{"total_tokens": 1234},
		}))
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(Config{APIKey: "test-key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	response, err := client.Rerank(context.Background(), "query", []string{"alpha"}, RerankOptions{
		Model: "rerank-2.5", TopK: 1, Truncation: true,
	})
	require.NoError(t, err)
	require.Equal(t, Usage{TotalTokens: 1234}, response.Usage)
	require.Equal(t, "rerank-2.5", response.Model)
}
