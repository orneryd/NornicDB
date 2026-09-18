package voyage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientEmbedTextRequestAndResponse(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/embeddings", r.URL.Path)
		require.Equal(t, "Bearer key", r.Header.Get("Authorization"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"model":  "voyage-4-large",
			"data": []map[string]any{
				{"index": 1, "embedding": []float32{0.3, 0.4}},
				{"index": 0, "embedding": []float32{0.1, 0.2}},
			},
			"usage": map[string]any{"total_tokens": 7},
		}))
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	resp, err := client.EmbedText(context.Background(), []string{"a", "b"}, EmbeddingOptions{
		Model:           "voyage-4-large",
		InputType:       "document",
		Truncation:      true,
		OutputDimension: 512,
	})
	require.NoError(t, err)
	require.Equal(t, "voyage-4-large", got["model"])
	require.Equal(t, "document", got["input_type"])
	require.Equal(t, true, got["truncation"])
	require.Equal(t, float64(512), got["output_dimension"])
	require.Equal(t, "float", got["output_dtype"])
	require.Equal(t, []EmbeddingData{
		{Index: 1, Embedding: []float32{0.3, 0.4}},
		{Index: 0, Embedding: []float32{0.1, 0.2}},
	}, resp.Data)
	require.Equal(t, 7, resp.Usage.TotalTokens)
}

func TestClientContextualizedAutoChunking(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/contextualizedembeddings", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"model":           "voyage-context-4",
			"chunker_version": "1.0.0",
			"usage":           map[string]any{"total_tokens": 11},
			"data": []map[string]any{
				{
					"index": 0,
					"data": []map[string]any{
						{"index": 0, "text": "chunk one", "embedding": []float32{0.1}},
						{"index": 1, "text": "chunk two", "embedding": []float32{0.2}},
					},
				},
			},
		}))
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	resp, err := client.EmbedContextualized(context.Background(), []string{"full document"}, ContextualizedOptions{
		Model:              "voyage-context-4",
		InputType:          "document",
		OutputDimension:    1024,
		EnableAutoChunking: true,
		ChunkSize:          512,
		ChunkOverlap:       0,
		ChunkOverlapSet:    true,
	})
	require.NoError(t, err)
	require.Equal(t, true, got["enable_auto_chunking"])
	require.Equal(t, "document", got["input_type"])
	require.Equal(t, float64(512), got["chunk_size"])
	require.Equal(t, float64(0), got["chunk_overlap"])
	require.Equal(t, "1.0.0", resp.ChunkerVersion)
	require.Len(t, resp.Data, 1)
	require.Equal(t, "chunk two", resp.Data[0].Data[1].Text)
}

func TestClientRerankParsesDataAndResultsShapes(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var got map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.Equal(t, "query", got["query"])
		require.Equal(t, float64(2), got["top_k"])
		require.Equal(t, false, got["return_documents"])
		require.Equal(t, true, got["truncation"])
		if calls == 1 {
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"data": []map[string]any{{"index": 1, "relevance_score": 0.9}},
			}))
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{"index": 0, "relevance_score": 0.8}},
		}))
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	first, err := client.Rerank(context.Background(), "query", []string{"a", "b"}, RerankOptions{
		Model:      "rerank-2.5",
		TopK:       2,
		Truncation: true,
	})
	require.NoError(t, err)
	require.Equal(t, []RerankResult{{Index: 1, RelevanceScore: 0.9}}, first.Data)
	second, err := client.Rerank(context.Background(), "query", []string{"a"}, RerankOptions{
		Model:      "rerank-2.5",
		TopK:       2,
		Truncation: true,
	})
	require.NoError(t, err)
	require.Equal(t, []RerankResult{{Index: 0, RelevanceScore: 0.8}}, second.Data)
}

func TestClientHonorsFalseTruncationOptions(t *testing.T) {
	var gotEmbedding, gotMultimodal, gotRerank map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		switch r.URL.Path {
		case "/v1/embeddings":
			gotEmbedding = got
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}}))
		case "/v1/multimodalembeddings":
			gotMultimodal = got
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}}))
		case "/v1/rerank":
			gotRerank = got
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}}))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	_, err = client.EmbedText(context.Background(), []string{"a"}, EmbeddingOptions{Model: "voyage-4-large", Truncation: false})
	require.NoError(t, err)
	_, err = client.EmbedMultimodal(context.Background(), []any{map[string]any{"content": []any{map[string]any{"type": "text", "text": "a"}}}}, MultimodalOptions{Model: "voyage-4-large", Truncation: false})
	require.NoError(t, err)
	_, err = client.Rerank(context.Background(), "query", []string{"a"}, RerankOptions{Model: "rerank-2.5", Truncation: false})
	require.NoError(t, err)

	require.Equal(t, false, gotEmbedding["truncation"])
	require.Equal(t, false, gotMultimodal["truncation"])
	require.Equal(t, false, gotRerank["truncation"])
}

func TestClientRetriesRetryableStatus(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "try again", http.StatusTooManyRequests)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{}}))
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: 1, RetryBackoff: time.Nanosecond})
	require.NoError(t, err)
	_, err = client.EmbedText(context.Background(), []string{"a"}, EmbeddingOptions{Model: "voyage-4-large", Truncation: true})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestClientNegativeRetriesDisableRetry(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "try again", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client, err := NewClient(Config{APIKey: "key", BaseURL: server.URL, MaxRetries: -1})
	require.NoError(t, err)
	_, err = client.EmbedText(context.Background(), []string{"a"}, EmbeddingOptions{Model: "voyage-4-large", Truncation: true})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}
