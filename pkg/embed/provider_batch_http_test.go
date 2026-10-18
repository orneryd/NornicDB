package embed_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// The mock accepts the old single-input wire protocol as well, so the same
// public-path benchmark measures the regression on an unmodified baseline.
type batchHTTPMock struct {
	mu      sync.Mutex
	sizes   []int
	paths   []string
	status  int
	missing bool
	latency time.Duration
}

func (m *batchHTTPMock) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Input  []string        `json:"input"`
		Inputs json.RawMessage `json:"inputs"`
		Prompt string          `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	texts := req.Input
	if len(req.Inputs) > 0 {
		if err := json.Unmarshal(req.Inputs, &texts); err != nil {
			texts = nil // A failed slice decode can leave partially allocated entries.
			var inputs []struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal(req.Inputs, &inputs); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			for _, input := range inputs {
				texts = append(texts, input.Content[0].Text)
			}
		}
	}
	if texts == nil {
		texts = []string{req.Prompt}
	}
	m.mu.Lock()
	m.sizes = append(m.sizes, len(texts))
	m.paths = append(m.paths, r.URL.Path)
	status := m.status
	if status != 400 {
		m.status = 0
	}
	m.mu.Unlock()
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	if status != 0 {
		w.Header().Set("Retry-After", "2")
		http.Error(w, "mock failure", status)
		return
	}
	for _, text := range texts {
		if text == "bad" {
			http.Error(w, "invalid input", 400)
			return
		}
	}
	data := make([]map[string]any, 0, len(texts))
	vectors := make([][]float32, len(texts))
	for i, text := range texts {
		n, _ := strconv.Atoi(strings.SplitN(text, "|", 2)[0])
		vectors[i] = []float32{float32(n), 1}
	}
	for i := len(texts) - 1; i >= 0; i-- {
		if m.missing && i == len(texts)-1 {
			continue
		}
		item := map[string]any{"index": i, "embedding": vectors[i], "text": texts[i]}
		if r.URL.Path == "/v1/contextualizedembeddings" {
			item = map[string]any{"index": i, "data": []any{item}}
		}
		data = append(data, item)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": data, "embeddings": vectors, "embedding": vectors[0],
		"usage":             map[string]int{"total_tokens": 7 * len(texts), "prompt_tokens": 7 * len(texts)},
		"prompt_eval_count": 7 * len(texts),
	})
}

func newBatchProvider(tb testing.TB, provider, mode string, mock *batchHTTPMock) embed.Embedder {
	tb.Helper()
	srv := httptest.NewServer(http.HandlerFunc(mock.serve))
	tb.Cleanup(srv.Close)
	cfg := &embed.Config{Provider: provider, Mode: mode, APIURL: srv.URL, APIKey: "mock-only", Model: "test-model", Dimensions: 2, Timeout: 5 * time.Second, APIPath: "/v1/embeddings"}
	if provider == "ollama" {
		cfg.APIPath = "/api/embeddings"
	}
	e, err := embed.NewEmbedder(cfg)
	require.NoError(tb, err)
	return e
}

func batchTexts(n int) []string {
	texts := make([]string, n)
	for i := range texts {
		texts[i] = strconv.Itoa(i)
	}
	return texts
}

func TestProviderHTTPDocumentBatch(t *testing.T) {
	for _, provider := range []string{"voyage", "ollama", "openai", "orca"} {
		for _, path := range []string{"documents", "properties", "vectors"} {
			t.Run(provider+"/"+path, func(t *testing.T) {
				mock := &batchHTTPMock{}
				e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, provider, "text", mock), 4096))
				texts := batchTexts(32)
				var vectors [][]float32
				if path == "vectors" {
					var err error
					vectors, err = e.EmbedBatch(context.Background(), texts)
					require.NoError(t, err)
				} else {
					var results []*embed.DocumentChunkResult
					var err error
					if path == "properties" {
						results, err = e.EmbedDocumentPropertyBatchChunks(context.Background(), texts, make([]map[string]any, len(texts)), 512, 0)
					} else {
						results, err = e.EmbedDocumentBatchChunks(context.Background(), texts, 512, 0)
					}
					require.NoError(t, err)
					require.Len(t, results, len(texts))
					for i, result := range results {
						require.Equal(t, []string{texts[i]}, result.Chunks)
						require.Len(t, result.Embeddings, 1)
						vectors = append(vectors, result.Embeddings[0])
					}
				}
				require.Len(t, vectors, len(texts))
				for i, vector := range vectors {
					require.Equal(t, []float32{float32(i), 1}, vector)
				}
				require.Equal(t, []int{32}, mock.sizes, "a batch must be one HTTP request")
				if provider == "ollama" {
					require.Equal(t, []string{"/api/embed"}, mock.paths)
				}
			})
		}
	}
}

func TestProviderHTTPRequestLimits(t *testing.T) {
	for _, tc := range []struct {
		provider     string
		count, bytes int
		sizes        []int
	}{
		{"voyage", 1001, 0, []int{1000, 1}},
		{"openai", 2049, 0, []int{2048, 1}},
		{"orca", 2049, 0, []int{2048, 1}},
		{"voyage", 5, 30000, []int{3, 2}},
		{"openai", 40, 8000, []int{37, 3}},
	} {
		t.Run(fmt.Sprintf("%s/%d/%d", tc.provider, tc.count, tc.bytes), func(t *testing.T) {
			mock := &batchHTTPMock{}
			e := newBatchProvider(t, tc.provider, "text", mock)
			texts := batchTexts(tc.count)
			if tc.bytes > 0 {
				for i := range texts {
					texts[i] += "|" + strings.Repeat("x", tc.bytes-len(texts[i])-1)
				}
			}
			vectors, err := e.EmbedBatch(context.Background(), texts)
			require.NoError(t, err)
			require.Len(t, vectors, len(texts))
			for i, vector := range vectors {
				require.Equal(t, []float32{float32(i), 1}, vector)
			}
			require.Equal(t, tc.sizes, mock.sizes)
		})
	}
}

func TestProviderHTTPUsage(t *testing.T) {
	for _, provider := range []string{"voyage", "ollama", "openai", "orca"} {
		t.Run(provider, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(old) })
			e := newBatchProvider(t, provider, "text", &batchHTTPMock{})
			_, err := e.EmbedBatch(context.Background(), batchTexts(32))
			require.NoError(t, err)
			require.Contains(t, logs.String(), `"total_tokens":224`)
			require.Contains(t, logs.String(), `"provider":"`+provider+`"`)
		})
	}
	t.Run("single-document-result", func(t *testing.T) {
		e := newBatchProvider(t, "voyage", "text", &batchHTTPMock{})
		result, err := e.(embed.DocumentChunkEmbedder).EmbedDocumentChunks(context.Background(), "0", 512, 0)
		require.NoError(t, err)
		require.Equal(t, 7, result.TotalTokens)
	})
}

func TestProviderHTTPVoyageModes(t *testing.T) {
	for _, mode := range []string{"text", "contextualized", "multimodal"} {
		t.Run(mode, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(old) })
			mock := &batchHTTPMock{}
			e := embed.NewTracedEmbedder(embed.NewCachedEmbedder(newBatchProvider(t, "voyage", mode, mock), 4096))
			texts := batchTexts(1001)
			results, err := e.EmbedDocumentBatchChunks(context.Background(), texts, 512, 0)
			require.NoError(t, err)
			require.Len(t, results, len(texts))
			for i, result := range results {
				require.Equal(t, []string{texts[i]}, result.Chunks)
				require.Equal(t, [][]float32{{float32(i), 1}}, result.Embeddings)
			}
			require.Equal(t, []int{1000, 1}, mock.sizes)
			require.Equal(t, 2, strings.Count(logs.String(), `"total_tokens":`), "one usage record for each successful HTTP response")
			result, err := e.EmbedDocumentChunks(context.Background(), "2", 512, 0)
			require.NoError(t, err)
			require.Equal(t, 7, result.TotalTokens)
		})
	}
}

func TestProviderHTTPMissingItemsAndCancellation(t *testing.T) {
	for _, provider := range []string{"voyage", "openai", "orca"} {
		t.Run(provider, func(t *testing.T) {
			e := newBatchProvider(t, provider, "text", &batchHTTPMock{missing: true})
			_, err := e.EmbedBatch(context.Background(), []string{"0", "1"})
			require.Error(t, err)
			require.False(t, embed.IsRetryableError(err), "the worker must isolate an unusable input")
			mock := &batchHTTPMock{}
			e = newBatchProvider(t, provider, "text", mock)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = e.EmbedBatch(ctx, []string{"0"})
			require.ErrorIs(t, err, context.Canceled)
			require.Empty(t, mock.sizes)
		})
	}
}

func TestProviderHTTPPartialAndTransientErrors(t *testing.T) {
	for _, provider := range []string{"voyage", "ollama", "openai", "orca"} {
		t.Run(provider, func(t *testing.T) {
			e := newBatchProvider(t, provider, "text", &batchHTTPMock{})
			_, err := e.EmbedBatch(context.Background(), []string{"0", "bad", "2"})
			require.Error(t, err)
			require.False(t, embed.IsRetryableError(err))
			// A failed item must not poison its valid neighbours when the public
			// worker retries the permanent-error batch as smaller input groups.
			vectors, err := e.EmbedBatch(context.Background(), []string{"0", "2"})
			require.NoError(t, err)
			require.Equal(t, [][]float32{{0, 1}, {2, 1}}, vectors)
			mock := &batchHTTPMock{status: 429}
			e = newBatchProvider(t, provider, "text", mock)
			_, err = e.EmbedBatch(context.Background(), []string{"0", "1"})
			if provider == "voyage" {
				require.NoError(t, err)
				require.Equal(t, []int{2, 2}, mock.sizes)
			} else {
				require.Error(t, err)
				require.True(t, embed.IsRetryableError(err))
				require.Equal(t, []int{2}, mock.sizes)
			}
		})
	}
}

func BenchmarkProviderHTTPDocumentBatch(b *testing.B) {
	for _, provider := range []string{"voyage", "ollama", "openai", "orca"} {
		b.Run(provider, func(b *testing.B) {
			mock := &batchHTTPMock{latency: 2 * time.Millisecond}
			e := embed.NewTracedEmbedder(newBatchProvider(b, provider, "text", mock))
			texts := batchTexts(32)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.EmbedDocumentBatchChunks(context.Background(), texts, 512, 0); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(mock.sizes))/float64(b.N), "requests/op")
		})
	}
}
