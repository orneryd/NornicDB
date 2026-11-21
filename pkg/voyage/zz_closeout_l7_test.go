package voyage

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-016: TestRerankerUndocumentedModelKeepsAPIRequestCap builds its reranker
// with a one-second client timeout (rerankNewReranker, Timeout: time.Second) and
// sends 1,000 documents of 4,096 bytes in its first request. Under -race the
// request fails with "Client.Timeout exceeded while awaiting headers". This test
// sends the same candidates with a 60 s timeout and logs how long each request
// takes, run with and without -race.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestL7016RerankRequestDuration(t *testing.T) {
	candidates := rerankBatchCandidates(1_200, 4096)
	stub := newRerankStub(t, rerankStubLimits{})
	var mu sync.Mutex
	var handler []time.Duration
	timed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		stub.handle(w, r)
		mu.Lock()
		handler = append(handler, time.Since(start))
		mu.Unlock()
	}))
	t.Cleanup(timed.Close)
	reranker, err := NewReranker(&RerankerConfig{
		Enabled: true, APIURL: timed.URL, APIKey: "test-key", Model: "rerank-custom-9",
		Timeout: time.Minute,
	})
	require.NoError(t, err)
	start := time.Now()
	results, err := reranker.Rerank(context.Background(), "query", candidates)
	total := time.Since(start)
	mu.Lock()
	t.Logf("L7_016 total=%s server_handler_per_request=%v documents_per_request=%v", total, handler, stub.documentCounts())
	mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, []int{1000, 200}, stub.documentCounts())
	require.Len(t, results, 1_200)
}
