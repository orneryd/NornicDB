package nornicdb

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewR73PartialDocumentResult(t *testing.T) {
	for _, mode := range []string{"single", "batch", "resolved_single", "resolved_batch", "cached_single", "cached_batch", "cached_resolved_single", "cached_resolved_batch"} {
		for _, valid := range []bool{true, false} {
			name := mode + "/empty_later_vector"
			if valid {
				name = mode + "/valid_control"
			}
			t.Run(name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "/v1/contextualizedembeddings", r.URL.Path)
					var request struct {
						Inputs []string `json:"inputs"`
					}
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					data := make([]map[string]any, len(request.Inputs))
					for i := range data {
						var second []float32
						if valid {
							second = []float32{0, 1}
						}
						data[i] = map[string]any{"index": i, "data": []map[string]any{
							{"index": 0, "text": "first chunk", "embedding": []float32{1, 0}},
							{"index": 1, "text": "second chunk", "embedding": second},
						}}
					}
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": data, "model": "voyage-context-4", "chunker_version": "review", "usage": map[string]any{"total_tokens": 2}}))
				}))
				t.Cleanup(server.Close)
				provider, err := embed.NewVoyage(&embed.Config{Provider: "voyage", Mode: embed.VoyageModeContextualized, APIURL: server.URL, APIKey: "mock-only", Model: "voyage-context-4", Dimensions: 2, Timeout: time.Second})
				require.NoError(t, err)
				worker, queue, _ := nodeSaveRetryFixture(t, "whole", 1, 0)
				worker.config.MaxRetries = 2
				worker.config.ProviderRetryBackoff = time.Hour
				worker.config.ProviderRetryBackoffMax = time.Hour
				if strings.HasSuffix(mode, "single") {
					worker.config.EmbedBatchSize = 1
				}
				var selected embed.Embedder = provider
				if strings.HasPrefix(mode, "cached_") {
					selected = embed.NewCachedEmbedder(provider, 8)
				}
				worker.SetEmbedder(selected)
				if strings.Contains(mode, "resolved") {
					worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return selected, nil })
				}
				published := 0
				worker.SetOnEmbedded(func(*storage.Node) { published++ })
				require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
				saved, err := queue.GetNode("node-000")
				require.NoError(t, err)
				t.Logf("valid=%v processed=%d failed=%d published=%d retries=%d sidecar_writes=%d chunk_vectors=%v", valid, worker.processed.Load(), worker.failed.Load(), published, len(worker.nodeRetries), queue.attempts["node-000"], saved.ChunkEmbeddings)
				if valid {
					require.Equal(t, int64(1), worker.processed.Load())
					require.Equal(t, 1, published)
					require.Len(t, saved.ChunkEmbeddings, 2)
					return
				}
				assert.Zero(t, published, "a partially empty document cannot be published as completed")
				assert.Zero(t, worker.processed.Load())
				assert.Equal(t, int64(1), worker.failed.Load())
				assert.Len(t, worker.nodeRetries, 1, "an unusable provider result must enter the common retry owner")
				assert.Zero(t, queue.attempts["node-000"], "no partial sidecar should be accepted")
				assert.Empty(t, saved.ChunkEmbeddings)
			})
		}
	}
}
