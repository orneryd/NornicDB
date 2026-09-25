package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestShowSettingsRegistryMetadata(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	result, err := executor.Execute(context.Background(), "SHOW SETTING `db.nornic.memory.storage.mode`", nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"name", "description", "value", "isDynamic", "defaultValue", "startupValue",
		"validValues", "isExplicitlySet", "isDeprecated",
	}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "db.nornic.memory.storage.mode", result.Rows[0][0])
	require.Equal(t, "default", result.Rows[0][2])
	require.Equal(t, false, result.Rows[0][3])
	require.Equal(t, []string{"default", "low"}, result.Rows[0][6])
}

func TestShowSettingsSelectionAndUnsupportedComposition(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	result, err := executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.query_plan_cache.max_entries, db.memory.transaction.total.max", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "db.memory.transaction.total.max", result.Rows[0][0])
	require.Equal(t, "db.nornic.query_plan_cache.max_entries", result.Rows[1][0])

	// YIELD / WHERE / RETURN apply to SHOW SETTINGS like every SHOW command.
	result, err = executor.Execute(context.Background(), "SHOW SETTINGS YIELD name WHERE name = 'db.memory.transaction.total.max' RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.query_plan_cache.max_entries, db.memory.transaction.total.max YIELD name ORDER BY name DESC LIMIT 1", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, result.Columns)
	require.Equal(t, [][]interface{}{{"db.nornic.query_plan_cache.max_entries"}}, result.Rows)
}

func TestShowSettingsUsesResolvedValuesAndRedactsSecrets(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	executor.SetSettingsResolver(func() SettingsSnapshot {
		return SettingsSnapshot{
			Configured: map[string]string{
				"db.nornic.search.vector.warming": "lazy",
				"db.nornic.embedding.api.key":     "configured-secret",
			},
			Active: map[string]string{
				"db.nornic.search.vector.warming": "lazy",
				"db.nornic.embedding.api.key":     "active-secret",
			},
		}
	})

	result, err := executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.search.vector.warming, db.nornic.embedding.api.key", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "<REDACTED>", result.Rows[0][2])
	require.Equal(t, "<REDACTED>", result.Rows[0][5])
	require.Equal(t, true, result.Rows[0][7])
	require.Equal(t, "lazy", result.Rows[1][2])
	require.Equal(t, "lazy", result.Rows[1][5])
	require.Equal(t, true, result.Rows[1][7])
}
