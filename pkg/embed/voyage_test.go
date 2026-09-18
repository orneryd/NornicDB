package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestVoyageEmbedderUsesInputTypes(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/embeddings", r.URL.Path)
		require.Equal(t, "Bearer voyage-key", r.Header.Get("Authorization"))
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		requests = append(requests, req)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 0, "embedding": []float32{1, 0, 0}},
			},
			"model": "voyage-test",
			"usage": map[string]any{"total_tokens": 3},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{
		Provider:   "voyage",
		APIURL:     server.URL,
		APIKey:     "voyage-key",
		Model:      "voyage-test",
		Dimensions: 3,
		Timeout:    time.Second,
	})
	require.NoError(t, err)

	queryVec, err := embedder.Embed(context.Background(), "needle")
	require.NoError(t, err)
	docVecs, err := embedder.EmbedBatch(context.Background(), []string{"haystack"})
	require.NoError(t, err)

	require.Equal(t, []float32{1, 0, 0}, queryVec)
	require.Equal(t, [][]float32{{1, 0, 0}}, docVecs)
	require.Len(t, requests, 2)
	require.Equal(t, "query", requests[0]["input_type"])
	require.Equal(t, "document", requests[1]["input_type"])
	require.Equal(t, float64(3), requests[0]["output_dimension"])
	require.Equal(t, true, requests[0]["truncation"])
}

func TestVoyageEmbedderContextualizedDocumentChunks(t *testing.T) {
	var got struct {
		Inputs             []string `json:"inputs"`
		Model              string   `json:"model"`
		InputType          string   `json:"input_type"`
		OutputDimension    int      `json:"output_dimension"`
		EnableAutoChunking bool     `json:"enable_auto_chunking"`
		ChunkSize          int      `json:"chunk_size"`
		ChunkOverlap       int      `json:"chunk_overlap"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/contextualizedembeddings", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{
					"index": 0,
					"data": []map[string]any{
						{"index": 0, "text": "first chunk", "embedding": []float32{1, 0}},
						{"index": 1, "text": "second chunk", "embedding": []float32{0, 1}},
					},
				},
			},
			"model":           "voyage-context-4",
			"chunker_version": "voyage-chunker-test",
			"usage":           map[string]any{"total_tokens": 9},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{
		Provider:   "voyage",
		APIURL:     server.URL,
		APIKey:     "voyage-key",
		Dimensions: 2,
		Mode:       VoyageModeContextualized,
		Timeout:    time.Second,
	})
	require.NoError(t, err)

	result, err := embedder.EmbedDocumentChunks(context.Background(), "whole document", 0, 64)
	require.NoError(t, err)

	require.Equal(t, []string{"whole document"}, got.Inputs)
	require.Equal(t, "voyage-context-4", got.Model)
	require.Equal(t, "document", got.InputType)
	require.Equal(t, 2, got.OutputDimension)
	require.True(t, got.EnableAutoChunking)
	require.Equal(t, VoyageContextualizedDefaultChunkTokens, got.ChunkSize)
	require.Equal(t, 64, got.ChunkOverlap)
	require.Equal(t, []string{"first chunk", "second chunk"}, result.Chunks)
	require.Equal(t, [][]float32{{1, 0}, {0, 1}}, result.Embeddings)
	require.Equal(t, "voyage-context-4", result.Model)
	require.Equal(t, "voyage-chunker-test", result.ChunkerVersion)
	require.Equal(t, 9, result.TotalTokens)
}

func TestVoyageEmbedderContextualizedLongDocumentUsesBoundedAutoChunkedSegments(t *testing.T) {
	const safeRequestBytes = 96_000
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/contextualizedembeddings", r.URL.Path)
		var got struct {
			Inputs             []string `json:"inputs"`
			EnableAutoChunking bool     `json:"enable_auto_chunking"`
			ChunkSize          int      `json:"chunk_size"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.True(t, got.EnableAutoChunking)
		require.Equal(t, 128, got.ChunkSize)
		require.Len(t, got.Inputs, 1)
		require.LessOrEqual(t, len(got.Inputs[0]), safeRequestBytes)
		require.True(t, len(got.Inputs[0]) == 0 || !strings.HasPrefix(got.Inputs[0], " "), "segments must begin at a word boundary")
		requests = append(requests, got.Inputs[0])

		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "data": []map[string]any{
				{"index": 0, "text": got.Inputs[0], "embedding": []float32{1, 1}},
			}}},
			"model":           "voyage-context-4",
			"chunker_version": "voyage-auto-v1",
			"usage":           map[string]any{"total_tokens": len(got.Inputs[0]) / 2},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{
		Provider:   "voyage",
		APIURL:     server.URL,
		APIKey:     "voyage-key",
		Dimensions: 2,
		Mode:       VoyageModeContextualized,
		Timeout:    time.Second,
	})
	require.NoError(t, err)

	text := strings.Repeat("привет мир. ", 20_000)
	result, err := embedder.EmbedDocumentChunks(context.Background(), text, 128, -1)
	require.NoError(t, err)

	require.Greater(t, len(requests), 1, "an oversized document must be split across requests")
	require.Equal(t, text, strings.Join(requests, ""))
	require.Equal(t, requests, result.Chunks)
	require.Len(t, result.Embeddings, len(result.Chunks))
	require.Equal(t, "voyage-auto-v1", result.ChunkerVersion)
}

func TestVoyageContextualizedOverlapDistinguishesUnsetAndZero(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		requests = append(requests, got)
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"index": 0, "data": []map[string]any{{"index": 0, "text": "document", "embedding": []float32{1}}}}},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{Provider: "voyage", APIURL: server.URL, APIKey: "key", Dimensions: 1, Mode: VoyageModeContextualized})
	require.NoError(t, err)
	_, err = embedder.EmbedDocumentChunks(context.Background(), "document", 128, -1)
	require.NoError(t, err)
	_, err = embedder.EmbedDocumentChunks(context.Background(), "document", 128, 0)
	require.NoError(t, err)

	require.NotContains(t, requests[0], "chunk_overlap")
	require.Equal(t, float64(0), requests[1]["chunk_overlap"])
}

func TestVoyageContextualizedSegmentsPreserveUnicodeAndText(t *testing.T) {
	text := strings.Repeat("я", 101)
	segments := splitVoyageContextualizedSegments(text, 17)
	require.Greater(t, len(segments), 1)
	require.Equal(t, text, strings.Join(segments, ""))
	for _, segment := range segments {
		require.True(t, utf8.ValidString(segment))
		require.LessOrEqual(t, len(segment), 17)
	}
}

func TestVoyageContextualizedDocumentsShareRequest(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var got struct {
			Inputs []string `json:"inputs"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.Equal(t, []string{"first", "second", "third"}, got.Inputs)
		data := make([]map[string]any, len(got.Inputs))
		for i, input := range got.Inputs {
			data[i] = map[string]any{
				"index": i,
				"data":  []map[string]any{{"index": 0, "text": input, "embedding": []float32{float32(i), 1}}},
			}
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": data, "model": "voyage-context-4", "usage": map[string]any{"total_tokens": 3},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{
		Provider: "voyage", APIURL: server.URL, APIKey: "voyage-key",
		Dimensions: 2, Mode: VoyageModeContextualized, Timeout: time.Second,
	})
	require.NoError(t, err)

	results, err := embedder.EmbedDocumentBatchChunks(context.Background(), []string{"first", "second", "third"}, 512, 0)
	require.NoError(t, err)
	require.Equal(t, 1, requestCount)
	require.Len(t, results, 3)
	for i, result := range results {
		require.Equal(t, []string{[]string{"first", "second", "third"}[i]}, result.Chunks)
		require.Len(t, result.Embeddings, 1)
	}
}

func TestVoyageEmbedderContextualizedUsesTextModelForQueries(t *testing.T) {
	var paths []string
	var models []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var req map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		models = append(models, req["model"].(string))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"index": 0, "embedding": []float32{1, 0}},
			},
			"model": req["model"],
			"usage": map[string]any{"total_tokens": 1},
		}))
	}))
	t.Cleanup(server.Close)

	embedder, err := NewVoyage(&Config{
		Provider:   "voyage",
		APIURL:     server.URL,
		APIKey:     "voyage-key",
		Dimensions: 2,
		Mode:       VoyageModeContextualized,
		Timeout:    time.Second,
	})
	require.NoError(t, err)

	_, err = embedder.Embed(context.Background(), "query")
	require.NoError(t, err)

	require.Equal(t, []string{"/v1/embeddings"}, paths)
	require.Equal(t, []string{"voyage-4-large"}, models)
}

func TestVoyageEmbedderRejectsMultimodalManagedMode(t *testing.T) {
	_, err := NewVoyage(&Config{
		Provider:   "voyage",
		APIURL:     "http://127.0.0.1",
		APIKey:     "voyage-key",
		Dimensions: 2,
		Mode:       VoyageModeMultimodal,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "multimodal mode is not supported")
}

func TestVoyageContextualizedChunkSizeDefaultAndExplicit(t *testing.T) {
	require.Equal(t, VoyageContextualizedDefaultChunkTokens, voyageContextualizedChunkSize(0))
	require.Equal(t, VoyageContextualizedMaxChunkTokens, voyageContextualizedChunkSize(VoyageContextualizedMaxChunkTokens+1))
	require.Equal(t, 512, voyageContextualizedChunkSize(512))
}
