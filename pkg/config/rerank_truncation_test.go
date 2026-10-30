package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// NORNICDB_SEARCH_RERANK_TRUNCATION / search_rerank.truncation: Voyage may
// shorten an over-long rerank query unless set to false (#907, I27).
func TestSearchRerankTruncationSetting(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_RERANK_TRUNCATION", "")
	require.False(t, LoadFromEnv().Features.SearchRerankDisableTruncation)

	t.Setenv("NORNICDB_SEARCH_RERANK_TRUNCATION", "false")
	require.True(t, LoadFromEnv().Features.SearchRerankDisableTruncation)
	t.Setenv("NORNICDB_SEARCH_RERANK_TRUNCATION", "true")
	require.False(t, LoadFromEnv().Features.SearchRerankDisableTruncation)

	t.Setenv("NORNICDB_SEARCH_RERANK_TRUNCATION", "")
	for yaml, disabled := range map[string]bool{
		"search_rerank:\n  truncation: false\n": true,
		"search_rerank:\n  truncation: true\n":  false,
		"search_rerank:\n  enabled: true\n":     false,
	} {
		path := filepath.Join(t.TempDir(), "nornicdb.yaml")
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))
		cfg, err := LoadFromFile(path)
		require.NoError(t, err)
		require.Equal(t, disabled, cfg.Features.SearchRerankDisableTruncation, yaml)
	}
}
