package cypher

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-search-continuation, member E18).
// Test-side only. Same fixture as TestV152PortChunkedRerankProviderLimit; the
// reranker is wrapped so every provider call logs the query length it was
// given and the search-package frames that issued it. Expected decisive value:
// the over-limit call comes from Service.applyContinuationRerank with the
// whole query (rerank_continuation.go:102), because chunkSearchOptions
// (chunked_search.go:143-151) turns the per-chunk rerank off for continuation.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type rcaStackReranker struct {
	inner search.Reranker
	mu    *sync.Mutex
	log   *[]string
}

func (r rcaStackReranker) Name() string                       { return r.inner.Name() }
func (r rcaStackReranker) Enabled() bool                      { return r.inner.Enabled() }
func (r rcaStackReranker) IsAvailable(ctx context.Context) bool { return r.inner.IsAvailable(ctx) }
func (r rcaStackReranker) Rerank(ctx context.Context, query string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	pcs := make([]uintptr, 40)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, "/pkg/search.") {
			names = append(names, f.Function[strings.LastIndex(f.Function, ".")+1:])
		}
		if !more {
			break
		}
	}
	r.mu.Lock()
	*r.log = append(*r.log, fmt.Sprintf("query_bytes=%d candidates=%d via=%s", len(query), len(candidates), strings.Join(names, "<")))
	r.mu.Unlock()
	return r.inner.Rerank(ctx, query, candidates)
}

func TestRCAContinuationRerankSendsWholeQuery(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_QUERY_MAX_CHUNKS", "32")
	t.Setenv("NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW", "truncate")
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(t.Context(), &storage.Node{
		ID: "doc0", Labels: []string{"Document"},
		Properties:      map[string]any{"content": "alpha beta gamma retained document 0"},
		ChunkEmbeddings: [][]float32{{1, 0}},
	})
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(t.Context()))
	service.SetSearchResultCachePolicy(0, 0)
	var mu sync.Mutex
	var log []string
	// A local always-accepting provider: the decisive value is the query the
	// service hands the provider, not the provider's answer.
	service.SetReranker(rcaStackReranker{inner: rcaAcceptAllReranker{}, mu: &mu, log: &log})
	exec := NewStorageExecutor(store)
	exec.SetSearchService(service)
	exec.SetEmbedder(v152PortByteEmbedder{})
	query := "alpha " + strings.Repeat("\U0001e900", 8000)
	for _, shape := range []struct{ name, statement string }{
		{"one-shot", "CALL db.rretrieve({query:$query, limit:3, rerankTopK:3, minRRFScore:0}) YIELD fallback_triggered RETURN fallback_triggered"},
		{"continuation", "CALL db.rretrieve({query:$query, limit:3, rerankTopK:3, minRRFScore:0, n:3}) YIELD page RETURN page"},
	} {
		mu.Lock()
		log = nil
		mu.Unlock()
		_, err := exec.Execute(t.Context(), shape.statement, map[string]any{"query": query})
		mu.Lock()
		for _, line := range log {
			t.Logf("RCA_RERANK route=%s query_bytes_total=%d %s", shape.name, len(query), line)
		}
		mu.Unlock()
		t.Logf("RCA_RERANK route=%s err=%v", shape.name, err)
	}
	_ = time.Second
}

type rcaAcceptAllReranker struct{}

func (rcaAcceptAllReranker) Name() string                     { return "rca-accept" }
func (rcaAcceptAllReranker) Enabled() bool                    { return true }
func (rcaAcceptAllReranker) IsAvailable(context.Context) bool { return true }
func (rcaAcceptAllReranker) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	out := make([]search.RerankResult, 0, len(candidates))
	for i, c := range candidates {
		out = append(out, search.RerankResult{ID: c.ID, OriginalRank: i + 1, FinalScore: 1 - float64(i)*0.01})
	}
	return out, nil
}
