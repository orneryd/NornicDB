package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/voyage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rerankBudgetStub is a Voyage rerank endpoint that refuses a request outside
// rerank-2.5's published bounds (1000 documents, 600,000 tokens counted as
// query*numDocs + sumDocTokens, 32,000 per query-document pair) and records what
// the adapter actually sent, so a failing provider batch is observed where the
// real API would refuse it.
type rerankBudgetStub struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []rerankBudgetRequest
	failAt   int
}

// rerankBudgetRequest is one recorded provider request.
type rerankBudgetRequest struct {
	query     string
	documents int
	cost      int
}

func newRerankBudgetStub(t *testing.T) *rerankBudgetStub {
	t.Helper()
	stub := &rerankBudgetStub{}
	stub.server = httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(stub.server.Close)
	return stub
}

// failRequest makes the ordinal-th request (1-based) fail with a non-retryable
// provider refusal, as the provider does when it rejects a request.
func (s *rerankBudgetStub) failRequest(ordinal int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAt = ordinal
}

func (s *rerankBudgetStub) recorded() []rerankBudgetRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rerankBudgetRequest(nil), s.requests...)
}

func (s *rerankBudgetStub) documentCounts() []int {
	counts := make([]int, 0)
	for _, request := range s.recorded() {
		counts = append(counts, request.documents)
	}
	return counts
}

func (s *rerankBudgetStub) handle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query     string   `json:"query"`
		Documents []string `json:"documents"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record := rerankBudgetRequest{query: body.Query, documents: len(body.Documents)}
	for _, document := range body.Documents {
		record.cost += len(body.Query) + len(document)
	}
	s.mu.Lock()
	s.requests = append(s.requests, record)
	ordinal, failAt := len(s.requests), s.failAt
	s.mu.Unlock()

	if record.documents > 1000 || record.cost > 600_000 {
		http.Error(w, `{"detail":"request exceeds the published model bounds"}`, http.StatusBadRequest)
		return
	}
	if ordinal == failAt {
		http.Error(w, `{"detail":"upstream rejected the request"}`, http.StatusBadRequest)
		return
	}
	data := make([]map[string]any, record.documents)
	for index := range body.Documents {
		data[index] = map[string]any{"index": index, "relevance_score": 1 - float64(index)*0.001}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "model": "rerank-2.5"})
}

// rerankAuditCandidates builds count distinct candidates of exactly document
// bytes each, in submission order.
func rerankAuditCandidates(count, document int) []RerankCandidate {
	candidates := make([]RerankCandidate, count)
	for index := range candidates {
		prefix := fmt.Sprintf("%04d", index)
		padding := document - len(prefix)
		if padding < 0 {
			padding = 0
		}
		candidates[index] = RerankCandidate{
			ID:      "doc-" + prefix,
			Content: prefix + strings.Repeat("7", padding),
			Score:   1 - float64(index)/float64(count),
		}
	}
	return candidates
}

func newRerankAuditVoyage(t *testing.T, stub *rerankBudgetStub) *voyage.Reranker {
	t.Helper()
	reranker, err := voyage.NewReranker(&voyage.RerankerConfig{
		Enabled: true, APIURL: stub.server.URL, APIKey: "test-key",
		Model: "rerank-2.5", Timeout: time.Second,
	})
	require.NoError(t, err)
	return reranker
}

func newRerankAuditService(t *testing.T, reranker Reranker) (*Service, *bytes.Buffer) {
	t.Helper()
	service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetReranker(reranker)
	var logs bytes.Buffer
	service.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	return service, &logs
}

// TestRerankBatchedProviderFailureSurfacesRerankFailed is the NOR401 audit of a
// batch failure through the real Voyage adapter and the search owner. A
// 250-document window at the audited 512-byte query and 2047-byte document
// shape costs 639,750 tokens, so rerank-2.5's documented 600,000-token budget
// splits it into two provider requests. When the second request is refused, the
// window must be reported as rerank_failed in its original order with the
// provider's cause persisted, never as a successful rerank of the batch that
// already answered. The same window with both batches answered must rank every
// candidate exactly once.
func TestRerankBatchedProviderFailureSurfacesRerankFailed(t *testing.T) {
	const (
		queryBytes = 512
		document   = 2047
		count      = 250
	)
	query := strings.Repeat("q", queryBytes)
	candidates := rerankAuditCandidates(count, document)

	t.Run("successful batches rank the whole window", func(t *testing.T) {
		stub := newRerankBudgetStub(t)
		reranker := newRerankAuditVoyage(t, stub)
		service, _ := newRerankAuditService(t, reranker)

		outcome := service.runRerank(t.Context(), reranker, query, candidates, rerankRunOptions{providerLimit: count})
		require.Equal(t, rerankMethodApplied, outcome.Method)
		require.False(t, outcome.FallbackTriggered)
		require.Empty(t, outcome.FallbackReason)
		require.Nil(t, outcome.failure)
		require.Equal(t, []int{234, 16}, stub.documentCounts())
		for _, request := range stub.recorded() {
			require.Equal(t, query, request.query, "the query is sent unchanged with every request")
			require.LessOrEqual(t, request.documents, 1000)
			require.LessOrEqual(t, request.cost, 600_000, "each request stays inside the published budget")
		}

		require.Len(t, outcome.Results, count)
		seen := make(map[string]bool, count)
		for position, result := range outcome.Results {
			require.Equal(t, position+1, result.NewRank)
			require.False(t, seen[result.ID], "candidate %s ranked twice", result.ID)
			seen[result.ID] = true
			require.NotEmpty(t, result.Content)
		}
		for _, candidate := range candidates {
			require.True(t, seen[candidate.ID], "candidate %s missing from the merged ranking", candidate.ID)
		}
	})

	t.Run("failed batch is rerank_failed not a false success", func(t *testing.T) {
		stub := newRerankBudgetStub(t)
		stub.failRequest(2)
		reranker := newRerankAuditVoyage(t, stub)
		service, logs := newRerankAuditService(t, reranker)

		outcome := service.runRerank(t.Context(), reranker, query, candidates, rerankRunOptions{providerLimit: count})
		require.Equal(t, rerankMethodOriginal, outcome.Method)
		require.True(t, outcome.FallbackTriggered)
		require.Equal(t, SearchFallbackRerankFailed, outcome.FallbackReason)
		require.Equal(t, "rerank_failed", string(outcome.FallbackReason))
		require.NotNil(t, outcome.failure)
		require.Contains(t, outcome.failure.Error(), "rerank failed (voyage)")

		require.Len(t, outcome.Results, count, "the bounded window is kept whole on the reported fallback")
		for position, result := range outcome.Results {
			require.Equal(t, candidates[position].ID, result.ID, "fallback keeps the original order")
			require.Equal(t, position+1, result.NewRank)
		}
		require.Equal(t, []int{234, 16}, stub.documentCounts(), "the failed batch is reported, not retried by the adapter")

		// The provider's cause and the failed request range are persisted for
		// the operator; the caller sees the canonical rerank failure.
		require.Contains(t, logs.String(), "Reranking failed (voyage)")
		require.Contains(t, logs.String(), "rerank documents 234..249")
		require.Contains(t, logs.String(), "upstream rejected the request")
	})

	t.Run("explicit candidates receive the canonical error", func(t *testing.T) {
		stub := newRerankBudgetStub(t)
		stub.failRequest(2)
		reranker := newRerankAuditVoyage(t, stub)
		service, _ := newRerankAuditService(t, reranker)

		options := DefaultSearchOptions()
		options.RerankTopK = count
		results, err := service.RerankCandidates(t.Context(), query, candidates, options)
		require.Error(t, err)
		require.Nil(t, results, "a failed provider batch returns no partial ranking")
		require.Contains(t, err.Error(), "rerank failed (voyage)")
	})
}

// TestNOR401RerankProviderTextCutsPreserveUTF8 asserts the approved provider
// input contract. It intentionally fails on the shipped byte-index cuts;
// corrected code must preserve UTF-8 and the configured character allowance.
// The actual exported rerankers call their existing scorer/generator DI seams.
func TestNOR401RerankProviderTextCutsPreserveUTF8(t *testing.T) {
	for _, tc := range []struct {
		name, query, document string
		max                   int
	}{
		{"ascii", "query123", "123456789", 5},
		{"cyrillic", "запрос", "число", 5},
		{"cjk", "查询问题测试", "汉字数字测试", 5},
		{"mixed", "A汉字B5Z", "A汉字B5Z", 5},
		{"one-character", "éé", "éé", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantDoc := string([]rune(tc.document)[:tc.max])
			wantQuery := string([]rune(tc.query)[:tc.max])
			t.Run("local", func(t *testing.T) {
				scorer := &captureScorer{score: 0.5}
				reranker := NewLocalReranker(scorer, &LocalRerankerConfig{
					Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max,
				}, testRerankAdmission())
				results, err := reranker.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.document, Score: 0.5}})
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Len(t, scorer.docs, 1)
				assert.True(t, utf8.ValidString(scorer.docs[0]), "model input must be valid UTF-8")
				assert.Equal(t, wantDoc, scorer.docs[0], "document allowance counts characters")
				require.Equal(t, tc.query, scorer.queries[0], "local query has no configured truncation")
			})
			t.Run("llm", func(t *testing.T) {
				var prompt string
				reranker := NewLLMReranker(&LLMRerankerConfig{
					Enabled: true, Timeout: time.Second, MaxCandidates: 4, MaxDocChars: tc.max, MaxQueryChars: tc.max,
				}, func(_ context.Context, actual string) (string, error) {
					prompt = actual
					return `{"ranked":[{"index":0,"score":0.9}]}`, nil
				})
				results, err := reranker.Rerank(t.Context(), tc.query, []RerankCandidate{{ID: "doc", Content: tc.document, Score: 0.5}})
				require.NoError(t, err)
				require.Len(t, results, 1)
				assert.True(t, utf8.ValidString(prompt), "model prompt must be valid UTF-8")
				assert.Contains(t, prompt, "QUERY:\n"+wantQuery+"\n\n", "query allowance counts characters")
				assert.Contains(t, prompt, wantDoc+"\n---\n", "document allowance counts characters")
			})
		})
	}
}

// TestRerankProviderTextBudgetCensus records the current provider text budgets
// the NOR401 audit names: search preparation at 2048 characters per candidate,
// the local GGUF reranker at 512 and the LLM reranker at 700 per document plus
// 800 per query. They are three separate owners with three separate numbers.
func TestRerankProviderTextBudgetCensus(t *testing.T) {
	require.Equal(t, 2048, defaultRerankMaxDocumentChars, "search candidate preparation")
	require.Equal(t, 2048, effectiveRerankMaxChars(&SearchOptions{}))

	local := DefaultLocalRerankerConfig()
	require.Equal(t, 512, local.MaxDocChars, "local GGUF reranker document budget")
	require.Equal(t, 50, local.MaxCandidates)

	llm := DefaultLLMRerankerConfig()
	require.Equal(t, 700, llm.MaxDocChars, "LLM reranker document budget")
	require.Equal(t, 800, llm.MaxQueryChars, "LLM reranker query budget")
	require.Equal(t, 25, llm.MaxCandidates)
}
