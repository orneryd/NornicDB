package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The public query path must preserve properties, constraints and updates when
// physical values use the fork codec. Run under both parser implementations.
func TestExperimentalStorageCypherRoundtrip(t *testing.T) {
	previous := config.IsExperimentalPropertyStorageEnabled()
	config.SetExperimentalPropertyStorageEnabled(true)
	t.Cleanup(func() { config.SetExperimentalPropertyStorageEnabled(previous) })
	native, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
	require.NoError(t, err)
	executor := NewStorageExecutor(storage.NewNamespacedEngine(native, "experiment"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err = executor.Execute(t.Context(), "CREATE CONSTRAINT doc_key IF NOT EXISTS FOR (n:Doc) REQUIRE n.key IS UNIQUE", nil)
	require.NoError(t, err)
	body := strings.Repeat("Documentation, preserved through compressed storage. ", 2048)
	numbers := make([]int64, 1024)
	for i := range numbers {
		numbers[i] = int64(i % 16)
	}
	_, err = executor.Execute(t.Context(), "CREATE (n:Doc {key: $key, body: $body, numbers: $numbers})", map[string]any{"key": "one", "body": body, "numbers": numbers})
	require.NoError(t, err)
	result, err := executor.Execute(t.Context(), "MATCH (n:Doc) WHERE n.body = $body RETURN n.body, n.numbers", map[string]any{"body": body})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, body, result.Rows[0][0])
	require.Equal(t, numbers, result.Rows[0][1])
	_, err = executor.Execute(t.Context(), "CREATE (n:Doc {key: $key, body: $body})", map[string]any{"key": "one", "body": body})
	require.Error(t, err, "physical encoding must not bypass unique constraints")
	replacement := strings.Repeat("Updated documentation. ", 4096)
	result, err = executor.Execute(t.Context(), "MATCH (n:Doc {key: $key}) SET n.body = $body RETURN n.body", map[string]any{"key": "one", "body": replacement})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, replacement, result.Rows[0][0])
	config.SetExperimentalPropertyStorageEnabled(false)
	result, err = executor.Execute(t.Context(), "MATCH (n:Doc {key: $key}) RETURN n.body, n.numbers", map[string]any{"key": "one"})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, replacement, result.Rows[0][0])
	require.Equal(t, numbers, result.Rows[0][1])
}
