package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddQueryChunkCounts(t *testing.T) {
	body := map[string]any{}
	addQueryChunkCounts(body, 0, 0)
	require.Empty(t, body)
	addQueryChunkCounts(body, 3, 5)
	require.Equal(t, map[string]any{"query_chunks_searched": 3, "query_chunks_total": 5}, body)
}
