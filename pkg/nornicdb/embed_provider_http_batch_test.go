package nornicdb_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise the real HTTP adapter and the public background worker together:
// a permanent input rejection must park only that node, after batch bisection.
func TestProviderHTTPWorkerPartialFailure(t *testing.T) {
	for _, provider := range []string{"voyage", "ollama", "openai", "orca"} {
		t.Run(provider, func(t *testing.T) {
			var mu sync.Mutex
			var sizes []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Input []string `json:"input"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				mu.Lock()
				sizes = append(sizes, len(request.Input))
				mu.Unlock()
				vectors := make([][]float32, len(request.Input))
				var data []map[string]any
				for i, text := range request.Input {
					if strings.Contains(text, "poison") {
						http.Error(w, "invalid document", 400)
						return
					}
					marker := float32(1)
					if strings.Contains(text, "third") {
						marker = 3
					}
					vectors[i] = []float32{marker, 1}
					data = append(data, map[string]any{"index": i, "embedding": vectors[i]})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "embeddings": vectors, "usage": map[string]int{"total_tokens": 7 * len(vectors)}, "prompt_eval_count": 7 * len(vectors)})
			}))
			t.Cleanup(server.Close)
			path := "/v1/embeddings"
			if provider == "ollama" {
				path = "/api/embeddings"
			}
			base, err := embed.NewEmbedder(context.Background(), &embed.Config{Provider: provider, Mode: "text", APIURL: server.URL, APIPath: path, APIKey: "mock-only", Model: "test", Dimensions: 2, Timeout: time.Second})
			require.NoError(t, err)
			engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			engine.SetEmbeddingsEnabled(true)
			store := storage.NewNamespacedEngine(engine, "http-batch")
			for _, id := range []storage.NodeID{"first", "poison", "third"} {
				_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": string(id)}})
				require.NoError(t, err)
			}
			worker := nornicdb.NewEmbedWorker(embed.NewTracedEmbedder(embed.NewCachedEmbedder(base, 16)), store, &nornicdb.EmbedWorkerConfig{NumWorkers: 1, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 32, DeferWorkerStart: true, ScanInterval: time.Minute})
			t.Cleanup(worker.Close)
			worker.StartWorkers()
			require.Eventually(t, func() bool {
				first, err1 := store.GetNode("first")
				third, err3 := store.GetNode("third")
				poison, err2 := store.GetNode("poison")
				return err1 == nil && err2 == nil && err3 == nil && len(first.ChunkEmbeddings) == 1 && len(third.ChunkEmbeddings) == 1 && poison.EmbedMeta["embedding_failed"] == true
			}, 5*time.Second, 10*time.Millisecond)
			worker.Close()
			for _, tc := range []struct {
				id     storage.NodeID
				marker float32
			}{{"first", 1}, {"third", 3}} {
				node, err := store.GetNode(tc.id)
				require.NoError(t, err)
				require.Equal(t, [][]float32{{tc.marker, 1}}, node.ChunkEmbeddings)
			}
			mu.Lock()
			got := append([]int(nil), sizes...)
			mu.Unlock()
			require.NotEmpty(t, got)
			require.Equal(t, 3, got[0], "the worker starts with an actual cross-document HTTP batch")
			require.Contains(t, got, 1, "the rejected input is isolated")
		})
	}
}

func TestProviderHTTPWorkerMultimodalChunkMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data":  []map[string]any{{"index": 0, "embedding": []float32{1, 1}}},
			"usage": map[string]int{"total_tokens": 7},
		})
	}))
	t.Cleanup(server.Close)
	base, err := embed.NewEmbedder(context.Background(), &embed.Config{Provider: "voyage", Mode: "multimodal", APIURL: server.URL, APIKey: "mock-only", Model: "test", Dimensions: 2, Timeout: time.Second})
	require.NoError(t, err)
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetEmbeddingsEnabled(true)
	store := storage.NewNamespacedEngine(engine, "http-multimodal")
	_, err = store.CreateNode(&storage.Node{ID: "document", Labels: []string{"Doc"}, Properties: map[string]any{
		"text":               "fallback",
		"_embedding_content": `[{"type":"text","text":"first passage"},{"type":"image_url","image_url":"https://example.test/mock.png"},{"type":"text","text":"later passage"}]`,
	}})
	require.NoError(t, err)
	worker := nornicdb.NewEmbedWorker(embed.NewTracedEmbedder(embed.NewCachedEmbedder(base, 16)), store, &nornicdb.EmbedWorkerConfig{NumWorkers: 1, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 32, DeferWorkerStart: true, ScanInterval: time.Minute})
	t.Cleanup(worker.Close)
	worker.StartWorkers()
	require.Eventually(t, func() bool {
		node, err := store.GetNode("document")
		return err == nil && len(node.ChunkEmbeddings) == 1
	}, 5*time.Second, 10*time.Millisecond)
	worker.Close()
	node, err := store.GetNode("document")
	require.NoError(t, err)
	require.EqualValues(t, 7, node.EmbedMeta["embedding_total_tokens"])
	chunks, err := json.Marshal(node.EmbedMeta["chunk_texts"])
	require.NoError(t, err)
	require.JSONEq(t, `["first passage\nlater passage"]`, string(chunks))
}
