package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
		VoyageMode: VoyageModeContextualized,
		Timeout:    time.Second,
	})
	require.NoError(t, err)

	result, err := embedder.EmbedDocumentChunks(context.Background(), "whole document", 512, 64)
	require.NoError(t, err)

	require.Equal(t, []string{"whole document"}, got.Inputs)
	require.Equal(t, "voyage-context-4", got.Model)
	require.Equal(t, "document", got.InputType)
	require.Equal(t, 2, got.OutputDimension)
	require.True(t, got.EnableAutoChunking)
	require.Equal(t, 512, got.ChunkSize)
	require.Equal(t, 64, got.ChunkOverlap)
	require.Equal(t, []string{"first chunk", "second chunk"}, result.Chunks)
	require.Equal(t, [][]float32{{1, 0}, {0, 1}}, result.Embeddings)
	require.Equal(t, "voyage-context-4", result.Model)
	require.Equal(t, "voyage-chunker-test", result.ChunkerVersion)
	require.Equal(t, 9, result.TotalTokens)
}
