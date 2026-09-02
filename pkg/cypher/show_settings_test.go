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

	_, err = executor.Execute(context.Background(), "SHOW SETTINGS YIELD name", nil)
	require.Error(t, err)
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
