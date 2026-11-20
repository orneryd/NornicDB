package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
)

type v152PortByteEmbedder struct{}

func (v152PortByteEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func (v152PortByteEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, func(value string) (int, error) {
		return len(value), nil
	})
}

// Section 18. Rebuild the full-versus-chunked request-size oracle and both
// report populations using a local HTTP reranker with a 1,024-byte query limit.
func TestV152PortChunkedRerankProviderLimit(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_QUERY_MAX_CHUNKS", "32")
	t.Setenv("NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW", "truncate")

	for _, test := range []struct {
		documents int
		prefix    string
	}{{1, "alpha "}, {3, "alpha beta gamma "}} {
		t.Run(fmt.Sprint(test.documents), func(t *testing.T) {
			var callsMu sync.Mutex
			var calls []int
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					w.WriteHeader(http.StatusOK)
					return
				}
				var request struct {
					Query     string   `json:"query"`
					Documents []string `json:"documents"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				callsMu.Lock()
				calls = append(calls, len(request.Query))
				callsMu.Unlock()
				if len(request.Query) > 1024 {
					http.Error(w, "synthetic query exceeds 1024 bytes", http.StatusRequestEntityTooLarge)
					return
				}
				results := make([]map[string]any, 0, len(request.Documents))
				for i := range request.Documents {
					results = append(results, map[string]any{"index": i, "relevance_score": 0.9 - 0.2*float64(i)})
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]any{"results": results}); err != nil {
					t.Errorf("encode rerank reply: %v", err)
				}
			}))
			defer peer.Close()

			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			for id := range test.documents {
				_, err := store.CreateNode(t.Context(), &storage.Node{
					ID: storage.NodeID(fmt.Sprintf("doc%d", id)), Labels: []string{"Document"},
					Properties:      map[string]any{"content": fmt.Sprintf("alpha beta gamma retained document %d", id)},
					ChunkEmbeddings: [][]float32{{1, 0}},
				})
				require.NoError(t, err)
			}
			service := search.NewService(store, 2, nil)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			require.NoError(t, service.BuildIndexes(t.Context()))
			service.SetSearchResultCachePolicy(0, 0)
			service.SetReranker(search.NewCrossEncoder(&search.CrossEncoderConfig{
				Enabled: true, APIURL: peer.URL, TopK: 3, Timeout: time.Second,
			}))
			exec := NewStorageExecutor(store)
			exec.SetSearchService(service)
			exec.SetEmbedder(v152PortByteEmbedder{})
			query := test.prefix + strings.Repeat("\U0001e900", 8000)
			for _, shape := range []struct{ name, statement string }{
				{"one-shot", "CALL db.rretrieve({query:$query, limit:3, rerankTopK:3, minRRFScore:0}) YIELD fallback_triggered, fallback_reason RETURN fallback_triggered, fallback_reason"},
				{"continuation", "CALL db.rretrieve({query:$query, limit:3, rerankTopK:3, minRRFScore:0, n:3}) YIELD page RETURN page"},
			} {
				t.Run(shape.name, func(t *testing.T) {
					callsMu.Lock()
					calls = nil
					callsMu.Unlock()
					result, err := exec.Execute(t.Context(), shape.statement, map[string]any{"query": query})
					require.NoError(t, err)
					require.NotEmpty(t, result.Rows)
					callsMu.Lock()
					lengths := append([]int(nil), calls...)
					callsMu.Unlock()
					t.Logf("PORT_RERANK_REQUESTS docs=%d route=%s query_bytes=%d received_bytes=%v", test.documents, shape.name, len(query), lengths)
					require.NotEmpty(t, lengths, "fixture must reach the reranker")
					for _, size := range lengths {
						require.LessOrEqual(t, size, 1024, "reranker received an over-limit query")
					}
				})
			}
		})
	}
}
