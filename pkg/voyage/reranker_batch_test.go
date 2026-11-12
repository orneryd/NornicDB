package voyage

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// rerankStubLimits is the published Voyage rerank request contract a stub
// enforces: documents per request, the aggregate request budget, and the
// query-plus-document limit those totals are counted under. enforced is false for
// a model name Voyage doesn't document, which has no published budget.
type rerankStubLimits struct {
	documentsPerRequest int
	requestBudget       int
	pairLimit           int
	enforced            bool
}

type rerankStubRequest struct {
	query      string
	documents  []string
	topK       int
	truncation bool
	cost       int
}

// rerankStub is a Voyage rerank endpoint that enforces the documented limits,
// so a request the real API would refuse fails here too.
type rerankStub struct {
	server   *httptest.Server
	limits   rerankStubLimits
	mu       sync.Mutex
	requests []rerankStubRequest
	failFrom int
	status   int
	body     string
}

func newRerankStub(t *testing.T, limits rerankStubLimits) *rerankStub {
	t.Helper()
	stub := &rerankStub{limits: limits}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

func (s *rerankStub) failFromRequest(ordinal, status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failFrom, s.status, s.body = ordinal, status, body
}

func (s *rerankStub) recorded() []rerankStubRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rerankStubRequest(nil), s.requests...)
}

func (s *rerankStub) documentCounts() []int {
	counts := make([]int, 0)
	for _, request := range s.recorded() {
		counts = append(counts, len(request.documents))
	}
	return counts
}

func (s *rerankStub) handle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query      string   `json:"query"`
		Documents  []string `json:"documents"`
		Model      string   `json:"model"`
		TopK       int      `json:"top_k"`
		Truncation bool     `json:"truncation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record := rerankStubRequest{query: body.Query, documents: body.Documents, topK: body.TopK, truncation: body.Truncation}
	oversized := false
	for _, document := range body.Documents {
		cost := len(body.Query) + len(document)
		if s.limits.pairLimit > 0 && cost > s.limits.pairLimit {
			oversized = true
			if body.Truncation {
				// The provider shortens an over-long pair to the model limit
				// before counting it.
				cost = s.limits.pairLimit
			}
		}
		record.cost += cost
	}
	s.mu.Lock()
	s.requests = append(s.requests, record)
	ordinal := len(s.requests)
	failFrom, status, failure := s.failFrom, s.status, s.body
	s.mu.Unlock()

	if oversized && !body.Truncation {
		// truncation: false makes the provider refuse the request instead of
		// shortening the text.
		http.Error(w, fmt.Sprintf(`{"detail":"the query plus a document has %d tokens, the max allowed is %d"}`, record.cost, s.limits.pairLimit), http.StatusBadRequest)
		return
	}

	if failFrom > 0 && ordinal >= failFrom {
		if status == 0 {
			status = http.StatusInternalServerError
		}
		http.Error(w, failure, status)
		return
	}
	if s.limits.enforced {
		if s.limits.documentsPerRequest > 0 && len(body.Documents) > s.limits.documentsPerRequest {
			http.Error(w, fmt.Sprintf(`{"detail":"%d documents requested, the max allowed is %d"}`, len(body.Documents), s.limits.documentsPerRequest), http.StatusBadRequest)
			return
		}
		if s.limits.requestBudget > 0 && record.cost > s.limits.requestBudget {
			http.Error(w, fmt.Sprintf(`{"detail":"the request has %d tokens, the max allowed is %d"}`, record.cost, s.limits.requestBudget), http.StatusBadRequest)
			return
		}
	}

	ranked := make([]int, len(body.Documents))
	for index := range ranked {
		ranked[index] = index
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		return rerankStubScore(body.Documents[ranked[i]]) > rerankStubScore(body.Documents[ranked[j]])
	})
	if body.TopK > 0 && body.TopK < len(ranked) {
		ranked = ranked[:body.TopK]
	}
	data := make([]map[string]any, 0, len(ranked))
	for _, index := range ranked {
		data = append(data, map[string]any{"index": index, "relevance_score": rerankStubScore(body.Documents[index])})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "model": body.Model})
}

// rerankStubScore is a deterministic provider score that does not follow the
// submission order, so a merged order has to come from the scores.
func rerankStubScore(document string) float64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(document))
	return float64(hash.Sum64()%1_000_003) / 1_000_003
}

// rerankBatchCandidates builds count distinct candidates of exactly size bytes
// each, in submission order.
func rerankBatchCandidates(count, size int) []Candidate {
	candidates := make([]Candidate, count)
	for index := range candidates {
		suffix := fmt.Sprintf("%04d", index)
		padding := size - len(suffix)
		if padding < 0 {
			padding = 0
		}
		candidates[index] = Candidate{
			ID:      "doc-" + suffix,
			Content: suffix + strings.Repeat("7", padding),
			Score:   1 - float64(index)/float64(count),
		}
	}
	return candidates
}

func rerankNewReranker(t *testing.T, stub *rerankStub, model string, truncationDisabled bool) *Reranker {
	t.Helper()
	reranker, err := NewReranker(&RerankerConfig{
		Enabled: true, APIURL: stub.server.URL, APIKey: "test-key", Model: model,
		Timeout: time.Second, DisableTruncation: truncationDisabled,
	})
	require.NoError(t, err)
	return reranker
}

// TestRerankerSplitsProviderRequestsWithinBudget reproduces the reported
// failure: 250 candidates at 2,047 bytes with a 512-byte query cost
// 512*250 + 2,047*250 = 639,750 provider tokens, which is above rerank-2.5's
// 600,000-token request budget. The window has to be split into requests that
// each fit, and the merged ranking has to cover every candidate once.
func TestRerankerSplitsProviderRequestsWithinBudget(t *testing.T) {
	const (
		queryBytes = 512
		document   = 2047
		count      = 250
	)
	query := strings.Repeat("q", queryBytes)
	candidates := rerankBatchCandidates(count, document)
	stub := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
	reranker := rerankNewReranker(t, stub, "rerank-2.5", false)

	results, err := reranker.Rerank(context.Background(), query, candidates)
	require.NoError(t, err)

	// 600,000 / (512 + 2,047) = 234.4, so the documented bounds are 234 then 16.
	requests := stub.recorded()
	require.Equal(t, []int{234, 16}, stub.documentCounts())

	sent := make([]string, 0, count)
	for _, request := range requests {
		require.Equal(t, query, request.query, "the query is sent unchanged with every request")
		require.True(t, request.truncation)
		require.LessOrEqual(t, len(request.documents), 1000)
		require.LessOrEqual(t, request.cost, 600_000, "each request stays inside the model budget")
		sent = append(sent, request.documents...)
	}
	require.Len(t, sent, count)
	for index, document := range sent {
		require.Equal(t, candidates[index].Content, document, "every candidate is sent exactly once, in order, whole")
	}

	require.Len(t, results, count)
	order := make([]int, count)
	for index := range order {
		order[index] = index
	}
	sort.SliceStable(order, func(i, j int) bool {
		return rerankStubScore(candidates[order[i]].Content) > rerankStubScore(candidates[order[j]].Content)
	})
	seen := make(map[string]bool, count)
	for position, result := range results {
		candidate := candidates[order[position]]
		require.Equal(t, candidate.ID, result.ID)
		require.Equal(t, candidate.Content, result.Content)
		require.Equal(t, order[position]+1, result.OriginalRank, "the local provider index is remapped to the candidate's position")
		require.Equal(t, position+1, result.NewRank)
		require.Equal(t, candidate.Score, result.BiScore)
		require.Equal(t, rerankStubScore(candidate.Content), result.CrossScore)
		require.Equal(t, rerankStubScore(candidate.Content), result.FinalScore)
		require.False(t, result.Filtered)
		require.False(t, seen[result.ID], "each candidate appears once in the merged ranking")
		seen[result.ID] = true
	}
}

// TestRerankerRequestBoundsFollowModelCapabilities proves the split follows the
// model's published documents-per-request, request budget and pair limit instead
// of one global constant.
func TestRerankerRequestBoundsFollowModelCapabilities(t *testing.T) {
	const (
		queryBytes = 512
		document   = 20_000
		count      = 250
	)
	query := strings.Repeat("q", queryBytes)
	candidates := rerankBatchCandidates(count, document)
	for _, testCase := range []struct {
		model  string
		limits rerankStubLimits
		want   []int
	}{
		// 600,000 / (512 + 20,000) = 29.2: the 32K pair limit does not bind.
		{"rerank-3", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true}, []int{29, 29, 29, 29, 29, 29, 29, 29, 18}},
		{"rerank-3-lite", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true}, []int{29, 29, 29, 29, 29, 29, 29, 29, 18}},
		{"rerank-2.5", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true}, []int{29, 29, 29, 29, 29, 29, 29, 29, 18}},
		{"rerank-2.5-lite", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true}, []int{29, 29, 29, 29, 29, 29, 29, 29, 18}},
		// The 16K pair limit binds: 600,000 / 16,000 = 37.5.
		{"rerank-2", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 16_000, enforced: true}, []int{37, 37, 37, 37, 37, 37, 28}},
		// 600,000 / 8,000 = 75.
		{"rerank-2-lite", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 8000, enforced: true}, []int{75, 75, 75, 25}},
		// rerank-1 has the lower 300,000 budget and an 8K pair limit.
		{"rerank-1", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 300_000, pairLimit: 8000, enforced: true}, []int{37, 37, 37, 37, 37, 37, 28}},
		// 300,000 / 4,096 = 73.2.
		{"rerank-lite-1", rerankStubLimits{documentsPerRequest: 1000, requestBudget: 300_000, pairLimit: 4000, enforced: true}, []int{75, 75, 75, 25}},
	} {
		t.Run(testCase.model, func(t *testing.T) {
			stub := newRerankStub(t, testCase.limits)
			reranker := rerankNewReranker(t, stub, testCase.model, false)
			results, err := reranker.Rerank(context.Background(), query, candidates)
			require.NoError(t, err)
			require.Equal(t, testCase.want, stub.documentCounts())
			require.Len(t, results, count)
			sent := make([]string, 0, count)
			for _, request := range stub.recorded() {
				require.LessOrEqual(t, request.cost, testCase.limits.requestBudget)
				sent = append(sent, request.documents...)
			}
			for index, document := range sent {
				require.Equal(t, candidates[index].Content, document)
			}
		})
	}
}

// TestRerankerBatchFailureIsAllOrNothing proves a failed request is reported as
// a failure of the rerank, not as a partial ranking of the requests that
// succeeded.
func TestRerankerBatchFailureIsAllOrNothing(t *testing.T) {
	const (
		queryBytes = 512
		document   = 2047
		count      = 250
	)
	query := strings.Repeat("q", queryBytes)
	candidates := rerankBatchCandidates(count, document)
	stub := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
	// A non-retryable provider failure: the client retries only transient ones,
	// so the recorded requests show what the adapter itself sent.
	stub.failFromRequest(2, http.StatusBadRequest, `{"detail":"upstream rejected the request"}`)
	reranker := rerankNewReranker(t, stub, "rerank-2.5", false)

	results, err := reranker.Rerank(context.Background(), query, candidates)
	require.Error(t, err)
	require.Nil(t, results, "no partial ranking is returned when a request fails")
	require.Contains(t, err.Error(), "rerank documents 234..249", "the failure names the failed request")
	require.Contains(t, err.Error(), "upstream rejected the request")
	require.Equal(t, []int{234, 16}, stub.documentCounts(), "the failing request is not retried by the adapter")
}

// TestRerankerKeepsTruncationChoiceFaithful proves the adapter never shortens
// client text: with truncation enabled an over-limit pair is still sent whole
// for the provider to shorten, and with truncation disabled the provider's own
// refusal reaches the caller.
func TestRerankerKeepsTruncationChoiceFaithful(t *testing.T) {
	query := strings.Repeat("q", 512)
	candidates := rerankBatchCandidates(1, 40_000)

	t.Run("truncation enabled sends the pair whole", func(t *testing.T) {
		stub := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
		reranker := rerankNewReranker(t, stub, "rerank-2.5", false)
		results, err := reranker.Rerank(context.Background(), query, candidates)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, candidates[0].ID, results[0].ID)
		requests := stub.recorded()
		require.Len(t, requests, 1)
		require.Equal(t, candidates[0].Content, requests[0].documents[0])
		require.True(t, requests[0].truncation)
	})

	t.Run("truncation disabled surfaces the refusal", func(t *testing.T) {
		stub := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
		reranker := rerankNewReranker(t, stub, "rerank-2.5", true)
		results, err := reranker.Rerank(context.Background(), query, candidates)
		require.Error(t, err)
		require.Nil(t, results)
		require.Contains(t, err.Error(), "the max allowed is 32000")
		requests := stub.recorded()
		require.Len(t, requests, 1)
		require.Equal(t, candidates[0].Content, requests[0].documents[0], "the client does not shorten the document")
		require.False(t, requests[0].truncation)
	})
}

// TestRerankerUndocumentedModelKeepsAPIRequestCap proves a model name Voyage
// doesn't document gets no invented token budget while retaining the known
// provider API's document-count cap.
func TestRerankerUndocumentedModelKeepsAPIRequestCap(t *testing.T) {
	candidates := rerankBatchCandidates(1_200, 4096)
	stub := newRerankStub(t, rerankStubLimits{})
	reranker := rerankNewReranker(t, stub, "rerank-custom-9", false)

	results, err := reranker.Rerank(context.Background(), "query", candidates)
	require.NoError(t, err)
	require.Equal(t, []int{1000, 200}, stub.documentCounts())
	require.Len(t, results, 1_200)

	expected := make([]int, len(candidates))
	for index := range expected {
		expected[index] = index
	}
	sort.SliceStable(expected, func(i, j int) bool {
		return rerankStubScore(candidates[expected[i]].Content) > rerankStubScore(candidates[expected[j]].Content)
	})
	for position, result := range results {
		// Merged requests retain global candidate identities and score order.
		require.Equal(t, candidates[expected[position]].ID, result.ID)
		require.Equal(t, expected[position]+1, result.OriginalRank)
		require.Equal(t, position+1, result.NewRank)
	}
}

// Byte-based bounds remain conservative on token-dense Unicode without
// cutting document or query strings at a multi-byte character boundary.
func TestRerankerBudgetPreservesDenseText(t *testing.T) {
	for _, unit := range []string{"число ", "表格123", "12345 "} {
		t.Run(unit, func(t *testing.T) {
			query := strings.Repeat(unit, 60)
			candidates := rerankBatchCandidates(250, 4)
			for i := range candidates {
				candidates[i].Content = fmt.Sprintf("%04d", i) + strings.Repeat(unit, 700)
			}
			stub := newRerankStub(t, rerankStubLimits{documentsPerRequest: 1000, requestBudget: 600_000, pairLimit: 32_000, enforced: true})
			reranker := rerankNewReranker(t, stub, "rerank-3", true)
			results, err := reranker.Rerank(context.Background(), query, candidates)
			require.NoError(t, err)
			require.Len(t, results, len(candidates))
			require.Greater(t, len(stub.recorded()), 1)
			sent := 0
			for _, request := range stub.recorded() {
				require.Equal(t, query, request.query)
				require.True(t, utf8.ValidString(request.query))
				require.LessOrEqual(t, request.cost, 600_000)
				for _, doc := range request.documents {
					require.True(t, utf8.ValidString(doc))
					require.Equal(t, candidates[sent].Content, doc)
					sent++
				}
			}
			require.Equal(t, len(candidates), sent)
		})
	}
}
