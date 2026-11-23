package voyage

// Usage parity for the provider rerank path.
//
// The Voyage reranker splits a candidate window into the provider requests the
// model's published budget allows. Each response the provider actually
// completes carries its own token accounting, and it is reported exactly once
// per completed request through the same debug record the embedding providers
// emit (`rerank provider usage`, with provider, model, inputs and
// total_tokens). The record is the billable provider total: it is not a locally
// computed estimate, not one record for the whole window, and not multiplied by
// the documents a request carried.
//
// A request the provider never completed - a wire failure, a canceled call, a
// request blocked before it was sent - reports nothing, and a completed
// response stays reportable even when its payload is rejected as unusable,
// because those tokens were billed.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	rerankUsageQueryBytes     = 512
	rerankUsageDocumentBytes  = 2047
	rerankUsageCandidateCount = 250
)

type rerankUsageRequest struct {
	query     string
	documents []string
}

// rerankUsageMock answers like the Voyage rerank endpoint, records the requests
// it received and reports the accounting the real API returns. Request
// boundaries and reported usage are what these tests observe; the model's
// published budget decides the split, and reranker_batch_test.go owns the
// budget-enforcing stub for the limits themselves.
type rerankUsageMock struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []rerankUsageRequest
	// usage reports the provider's total_tokens for request number ordinal
	// (1-based), which carried documents documents. A nil value omits the usage
	// member, as a response body without accounting does.
	usage func(ordinal, documents int) int
	// answer, when set, is returned verbatim as the provider response body.
	answer string
	// failFrom is the 1-based request ordinal from which the provider fails.
	failFrom int
	status   int
	failure  string
}

func newRerankUsageMock(t *testing.T) *rerankUsageMock {
	t.Helper()
	mock := &rerankUsageMock{}
	mock.server = httptest.NewServer(http.HandlerFunc(mock.handle))
	t.Cleanup(mock.server.Close)
	return mock
}

func (m *rerankUsageMock) failFromRequest(ordinal, status int, failure string) {
	m.failFrom, m.status, m.failure = ordinal, status, failure
}

func (m *rerankUsageMock) recorded() []rerankUsageRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]rerankUsageRequest(nil), m.requests...)
}

func (m *rerankUsageMock) documentCounts() []int {
	counts := make([]int, 0)
	for _, request := range m.recorded() {
		counts = append(counts, len(request.documents))
	}
	return counts
}

func (m *rerankUsageMock) handle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query     string   `json:"query"`
		Documents []string `json:"documents"`
		Model     string   `json:"model"`
		TopK      int      `json:"top_k"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.requests = append(m.requests, rerankUsageRequest{
		query:     body.Query,
		documents: append([]string(nil), body.Documents...),
	})
	ordinal := len(m.requests)
	usage, answer, failFrom, status, failure := m.usage, m.answer, m.failFrom, m.status, m.failure
	m.mu.Unlock()

	if failFrom > 0 && ordinal >= failFrom {
		if status == 0 {
			status = http.StatusBadRequest
		}
		http.Error(w, failure, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if answer != "" {
		_, _ = w.Write([]byte(answer))
		return
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
	response := map[string]any{"data": data, "model": body.Model}
	if usage != nil {
		response["usage"] = map[string]any{"total_tokens": usage(ordinal, len(body.Documents))}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func rerankUsageReranker(t *testing.T, mock *rerankUsageMock) *Reranker {
	t.Helper()
	reranker, err := NewReranker(&RerankerConfig{
		Enabled: true, APIURL: mock.server.URL, APIKey: "test-key", Model: "rerank-2.5",
		Timeout: time.Second,
	})
	require.NoError(t, err)
	return reranker
}

// rerankUsageRecord is one decoded provider-usage debug record.
type rerankUsageRecord struct {
	Provider    string
	Model       string
	Inputs      int
	TotalTokens int
}

func captureRerankUsageLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

// rerankUsageRecords decodes the usage records the adapter emitted, so a test
// counts real records instead of matching a substring of the captured output.
func rerankUsageRecords(t *testing.T, logs *bytes.Buffer) []rerankUsageRecord {
	t.Helper()
	records := make([]rerankUsageRecord, 0)
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var decoded struct {
			Message     string `json:"msg"`
			Provider    string `json:"provider"`
			Model       string `json:"model"`
			Inputs      int    `json:"inputs"`
			TotalTokens int    `json:"total_tokens"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &decoded), "captured slog record: %s", line)
		if decoded.Message != "rerank provider usage" {
			continue
		}
		records = append(records, rerankUsageRecord{
			Provider:    decoded.Provider,
			Model:       decoded.Model,
			Inputs:      decoded.Inputs,
			TotalTokens: decoded.TotalTokens,
		})
	}
	return records
}

// TestRerankerReportsUsageForEveryProviderRequest proves the provider's own
// token total is reported once for each request a window was split into: not
// once for the whole window, and not multiplied by the documents of a request.
func requireRerankWindowUsage(t *testing.T, logs *bytes.Buffer, wantRequests, wantTokens int) {
	t.Helper()
	var records int
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record struct {
			Message  string `json:"msg"`
			Requests int    `json:"completed_requests"`
			Tokens   int    `json:"total_tokens"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.Message != "rerank window usage" {
			continue
		}
		records++
		require.Equal(t, wantRequests, record.Requests)
		require.Equal(t, wantTokens, record.Tokens)
	}
	if wantRequests == 0 {
		require.Zero(t, records)
	} else {
		require.Equal(t, 1, records)
	}
}

func TestRerankerReportsUsageForEveryProviderRequest(t *testing.T) {
	mock := newRerankUsageMock(t)
	// Provider totals that no local byte or document count can reproduce: only
	// the response body carries them.
	mock.usage = func(ordinal, _ int) int { return 100_003 * ordinal }
	reranker := rerankUsageReranker(t, mock)
	logs := captureRerankUsageLogs(t)

	results, err := reranker.Rerank(
		context.Background(),
		strings.Repeat("q", rerankUsageQueryBytes),
		rerankBatchCandidates(rerankUsageCandidateCount, rerankUsageDocumentBytes),
	)
	require.NoError(t, err)

	requests := mock.recorded()
	require.Equal(t, []int{234, 16}, mock.documentCounts(), "the window is split into two requests inside the budget")

	require.Len(t, results, rerankUsageCandidateCount)
	seen := make(map[string]bool, len(results))
	for index, result := range results {
		require.Equal(t, index+1, result.NewRank, "the merged ranking is renumbered")
		require.False(t, seen[result.ID], "each candidate appears once in the merged ranking")
		seen[result.ID] = true
		if index+1 < len(results) {
			require.GreaterOrEqual(t, result.CrossScore, results[index+1].CrossScore, "the merged ranking is ordered by provider score")
		}
	}

	records := rerankUsageRecords(t, logs)
	require.Len(t, records, len(requests), "one usage record for each completed provider request")
	for index, record := range records {
		require.Equal(t, "voyage", record.Provider)
		require.Equal(t, "rerank-2.5", record.Model)
		require.Equal(t, len(requests[index].documents), record.Inputs, "inputs are that request's documents")
		require.Equal(t, 100_003*(index+1), record.TotalTokens, "the provider's reported total is recorded, not a local estimate")
	}
	requireRerankWindowUsage(t, logs, 2, 300_009)
}

// TestRerankerUsageRecordsOnlyCompletedProviderRequests proves a failed request
// keeps the whole-window failure and reports no usage of its own, while every
// request the provider did complete still reports the tokens it billed.
func TestRerankerUsageRecordsOnlyCompletedProviderRequests(t *testing.T) {
	mock := newRerankUsageMock(t)
	mock.usage = func(ordinal, _ int) int { return 100_003 * ordinal }
	mock.failFromRequest(2, http.StatusBadRequest, `{"detail":"upstream rejected the request"}`)
	reranker := rerankUsageReranker(t, mock)
	logs := captureRerankUsageLogs(t)

	results, err := reranker.Rerank(
		context.Background(),
		strings.Repeat("q", rerankUsageQueryBytes),
		rerankBatchCandidates(rerankUsageCandidateCount, rerankUsageDocumentBytes),
	)
	require.Error(t, err)
	require.Nil(t, results, "no partial ranking is returned when a request fails")
	require.Contains(t, err.Error(), "rerank documents 234..249", "the failure names the failed request")
	require.Contains(t, err.Error(), "upstream rejected the request")
	require.Equal(t, []int{234, 16}, mock.documentCounts(), "the failing request is not retried by the adapter")

	records := rerankUsageRecords(t, logs)
	require.Len(t, records, 1, "only the request the provider completed reports usage")
	require.Equal(t, 234, records[0].Inputs)
	require.Equal(t, 100_003, records[0].TotalTokens)
	requireRerankWindowUsage(t, logs, 1, 100_003)
}

// TestRerankerReportsUsageOfResponseRejectedAsUnusable proves the response the
// provider completed is billable even when the wire owner rejects its payload:
// the usage is reported and the call still fails as a whole.
func TestRerankerReportsUsageOfResponseRejectedAsUnusable(t *testing.T) {
	mock := newRerankUsageMock(t)
	// An index the single-document request cannot have produced, so the shared
	// wire owner rejects the answer.
	mock.answer = `{"data":[{"index":7,"relevance_score":0.9}],"model":"rerank-2.5","usage":{"total_tokens":4242}}`
	reranker := rerankUsageReranker(t, mock)
	logs := captureRerankUsageLogs(t)

	results, err := reranker.Rerank(context.Background(), "query", []Candidate{{ID: "a", Content: "alpha", Score: 0.4}})
	require.ErrorContains(t, err, "invalid results")
	require.Nil(t, results)
	require.Equal(t, []int{1}, mock.documentCounts())

	records := rerankUsageRecords(t, logs)
	require.Len(t, records, 1, "a completed provider response is billable even when its payload is unusable")
	require.Equal(t, 1, records[0].Inputs)
	require.Equal(t, 4242, records[0].TotalTokens)
	requireRerankWindowUsage(t, logs, 1, 4242)
}

// TestRerankerReportsNoUsageWithoutACompletedRequest proves a canceled call
// sends no provider request and invents no usage for it.
func TestRerankerReportsNoUsageWithoutACompletedRequest(t *testing.T) {
	mock := newRerankUsageMock(t)
	mock.usage = func(int, int) int { return 4242 }
	reranker := rerankUsageReranker(t, mock)
	logs := captureRerankUsageLogs(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := reranker.Rerank(ctx, "query", rerankBatchCandidates(2, 16))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, results)
	require.Empty(t, mock.recorded(), "a canceled call sends no provider request")
	require.Empty(t, rerankUsageRecords(t, logs))
	requireRerankWindowUsage(t, logs, 0, 0)
}
