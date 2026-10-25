package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowSettingsRegistryMetadata(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	// Neo4j's default and full (YIELD *) column sets, in its order.
	result, err := executor.Execute(context.Background(), "SHOW SETTING `db.nornic.memory.storage.mode`", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name", "value", "isDynamic", "defaultValue", "description"}, result.Columns)
	result, err = executor.Execute(context.Background(), "SHOW SETTING `db.nornic.memory.storage.mode` YIELD *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		"name", "value", "isDynamic", "defaultValue", "description", "startupValue",
		"isExplicitlySet", "validValues", "isDeprecated",
	}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "db.nornic.memory.storage.mode", result.Rows[0][0])
	require.Equal(t, "default", result.Rows[0][1])
	require.Equal(t, false, result.Rows[0][2])
	require.Equal(t, []string{"default", "low"}, result.Rows[0][7])
}

func TestShowSettingsSelectionAndUnsupportedComposition(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
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
	const key = "db.nornic.embedding.api.key"
	for _, scenario := range []struct{ name, configured, active, want string }{
		{"distinct_runtime_values", "configured-secret", "active-secret", "<REDACTED>"},
		{"unset", "", "", ""},
		{"set", "synthetic-show-set-credential-813624", "synthetic-show-set-credential-813624", "<REDACTED>"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			executor := NewStorageExecutor(newTestMemoryEngine(t))
			configured := map[string]string{"db.nornic.search.vector.warming": "lazy", key: scenario.configured}
			active := map[string]string{"db.nornic.search.vector.warming": "lazy", key: scenario.active}
			executor.SetSettingsResolver(func() SettingsSnapshot { return SettingsSnapshot{Configured: configured, Active: active} })
			result, err := executor.Execute(context.Background(), "SHOW SETTINGS db.nornic.search.vector.warming, db.nornic.embedding.api.key YIELD name, value, startupValue, isExplicitlySet", nil)
			require.NoError(t, err)
			if scenario.name == "distinct_runtime_values" {
				require.Equal(t, [][]interface{}{{key, "<REDACTED>", "<REDACTED>", true}, {"db.nornic.search.vector.warming", "lazy", "lazy", true}}, result.Rows)
			}
			result, err = executor.Execute(context.Background(), "SHOW SETTINGS YIELD *", nil)
			require.NoError(t, err)
			columns := make(map[string]int)
			for i, column := range result.Columns {
				columns[column] = i
			}
			found := false
			for _, row := range result.Rows {
				if row[columns["name"]] != key {
					continue
				}
				found = true
				for _, column := range []string{"value", "startupValue"} {
					if row[columns[column]] != scenario.want {
						t.Error("SHOW changed unset state or disclosed a secret; row withheld")
					}
				}
				if row[columns["defaultValue"]] != "" {
					t.Error("SHOW changed the unset secret default")
				}
			}
			if !found {
				t.Error("secret setting row missing")
			}
			if active[key] != scenario.active || configured[key] != scenario.configured {
				t.Error("SHOW changed runtime settings")
			}
		})
	}
}
