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
	result, err := executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.memory.index.vector.max, db.memory.transaction.total.max", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "db.memory.transaction.total.max", result.Rows[0][0])
	require.Equal(t, "db.nornic.memory.index.vector.max", result.Rows[1][0])

	_, err = executor.Execute(context.Background(), "SHOW SETTINGS YIELD name", nil)
	require.Error(t, err)
}
