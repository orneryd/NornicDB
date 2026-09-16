package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Issue #381: a provider failure must remain visible after fail-open BM25
// fallback without exposing the provider's raw error to the caller.
func TestSearchTextChunksReportsAndLogsEmbeddingFailureFallback(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	response, err := SearchTextChunksWithErrorPolicy(
		context.Background(),
		"fallback query",
		&SearchOptions{Limit: 2},
		func(context.Context, string) ([]string, error) {
			return []string{"fallback", "query"}, nil
		},
		func(context.Context, string) ([]float32, error) {
			return nil, errors.New("provider rejected secret-key-value")
		},
		func(_ context.Context, _ string, embedding []float32, _ *SearchOptions) (*SearchResponse, error) {
			require.Empty(t, embedding)
			return &SearchResponse{SearchMethod: "fulltext", FallbackTriggered: true}, nil
		},
		ChunkedSearchErrorPolicy{Transport: "test"},
	)
	require.NoError(t, err)

	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(encoded, &payload))
	require.Equal(t, "query_embedding_failed", payload["fallback_reason"])
	require.NotContains(t, string(encoded), "secret-key-value")
	require.Contains(t, logs.String(), `"level":"WARN"`)
	require.Contains(t, logs.String(), `"fallback_reason":"query_embedding_failed"`)
	require.Contains(t, logs.String(), `"transport":"test"`)
	require.Contains(t, logs.String(), `"diagnostic":"embedding provider request failed"`)
	require.NotContains(t, logs.String(), "secret-key-value")
	require.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(`"fallback_reason":"query_embedding_failed"`)))
}

func TestSanitizedEmbeddingDiagnostic(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "embedding provider request timed out"},
		{name: "canceled", err: context.Canceled, want: "embedding provider request canceled"},
		{name: "network timeout", err: &net.DNSError{IsTimeout: true}, want: "embedding provider request timed out"},
		{name: "http status", err: errors.New("openai returned 429: sensitive response body"), want: "embedding provider returned HTTP status 429"},
		{name: "generic", err: errors.New("provider rejected secret-key-value"), want: "embedding provider request failed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostic := sanitizedEmbeddingDiagnostic(test.err)
			require.Equal(t, test.want, diagnostic)
			require.NotContains(t, diagnostic, "sensitive response body")
			require.NotContains(t, diagnostic, "secret-key-value")
		})
	}
}

func TestSearchTextChunksUsesOuterRRFAndBM25Fallback(t *testing.T) {
	t.Run("fuses independently searched chunks", func(t *testing.T) {
		var searchedQueries []string
		response, err := SearchTextChunks(
			context.Background(),
			"complete query",
			&SearchOptions{Limit: 3},
			func(context.Context, string) ([]string, error) {
				return []string{"chunk one", "chunk two", "chunk three"}, nil
			},
			func(_ context.Context, chunk string) ([]float32, error) {
				switch chunk {
				case "chunk one":
					return []float32{1, 0}, nil
				case "chunk two":
					return []float32{0, 1}, nil
				case "chunk three":
					return []float32{-1, 0}, nil
				default:
					return nil, errors.New("full query must not be embedded")
				}
			},
			func(_ context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
				searchedQueries = append(searchedQueries, query)
				require.NotNil(t, embedding)
				require.Equal(t, 10, opts.Limit)
				switch query {
				case "chunk one":
					return &SearchResponse{Results: []SearchResult{
						{NodeID: storage.NodeID("single"), Score: 0.99},
						{NodeID: storage.NodeID("repeated"), Score: 0.20, Properties: map[string]any{"representative_score": 0.20}},
					}}, nil
				case "chunk two":
					return &SearchResponse{Results: []SearchResult{
						{NodeID: storage.NodeID("other"), Score: 0.80},
						{NodeID: storage.NodeID("repeated"), Score: 0.70, Properties: map[string]any{"representative_score": 0.70}},
					}}, nil
				case "chunk three":
					return &SearchResponse{Results: []SearchResult{
						{NodeID: storage.NodeID("tie-b"), Score: 0.50},
						{NodeID: storage.NodeID("tie-a"), Score: 0.40},
					}}, nil
				default:
					t.Fatalf("unexpected search query %q", query)
					return nil, nil
				}
			},
		)
		require.NoError(t, err)
		require.Equal(t, []string{"chunk one", "chunk two", "chunk three"}, searchedQueries)
		require.Equal(t, "chunked_rrf_hybrid", response.SearchMethod)
		require.Equal(t, storage.NodeID("repeated"), response.Results[0].NodeID)
		require.Equal(t, 0.70, response.Results[0].Properties["representative_score"])
		require.InDelta(t, 2.0/62.0, response.Results[0].Score, 0.0000001)
	})

	t.Run("falls back once when no vector chunk succeeds", func(t *testing.T) {
		var searches int
		response, err := SearchTextChunks(
			context.Background(),
			"complete query",
			&SearchOptions{Limit: 2},
			func(context.Context, string) ([]string, error) {
				return []string{"chunk one", "chunk two"}, nil
			},
			func(context.Context, string) ([]float32, error) {
				return nil, errors.New("embedding unavailable")
			},
			func(_ context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
				searches++
				require.Equal(t, "complete query", query)
				require.Nil(t, embedding)
				require.Equal(t, 2, opts.Limit)
				return &SearchResponse{SearchMethod: "bm25"}, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, 1, searches)
		require.Equal(t, "bm25", response.SearchMethod)
	})

	t.Run("honors disabled fallback after empty vector results", func(t *testing.T) {
		fallbackEnabled := false
		searches := 0
		response, err := SearchTextChunks(
			context.Background(),
			"complete query",
			&SearchOptions{Limit: 2, FallbackEnabled: &fallbackEnabled},
			func(context.Context, string) ([]string, error) {
				return []string{"chunk one", "chunk two"}, nil
			},
			func(context.Context, string) ([]float32, error) {
				return []float32{1, 0}, nil
			},
			func(_ context.Context, _ string, embedding []float32, _ *SearchOptions) (*SearchResponse, error) {
				searches++
				require.NotNil(t, embedding, "disabled fallback must not issue a BM25 search")
				return &SearchResponse{SearchMethod: "rrf_hybrid"}, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, 2, searches)
		require.Equal(t, "chunked_rrf_hybrid", response.SearchMethod)
		require.Empty(t, response.Results)
	})
}
