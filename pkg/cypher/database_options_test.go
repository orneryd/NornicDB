package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type optionsTestManager struct{ *testDatabaseManagerAdapter }

func (a optionsTestManager) DatabaseOptions(name string) (map[string]string, error) {
	return a.manager.DatabaseOptions(name)
}

func (a optionsTestManager) SetDatabaseOption(ctx context.Context, name, key, value string) error {
	return a.manager.SetDatabaseOption(ctx, name, key, value)
}

// CREATE DATABASE … OPTIONS, ALTER DATABASE … SET / REMOVE OPTION and SHOW
// DATABASES' options column (#937): txLogEnrichment is OFF, DIFF or FULL;
// the other options Neo4j defines aren't supported, others are unknown, and
// a failing option creates nothing. Options persist with the database.
func TestDatabaseOptions(t *testing.T) {
	inner := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetDatabaseManager(optionsTestManager{&testDatabaseManagerAdapter{manager: manager}})
	ctx := context.Background()
	options := func(name string) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, "SHOW DATABASES YIELD name, options WHERE name = $name RETURN options", map[string]interface{}{"name": name})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0][0]
	}

	_, err = exec.Execute(ctx, "CREATE DATABASE cdcdb OPTIONS {txLogEnrichment: 'full'}", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"txLogEnrichment": "FULL"}, options("cdcdb"))
	_, err = exec.Execute(ctx, "ALTER DATABASE cdcdb SET OPTION txLogEnrichment 'DIFF'", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"txLogEnrichment": "DIFF"}, options("cdcdb"))

	reloaded, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	require.NoError(t, err)
	persisted, err := reloaded.DatabaseOptions("cdcdb")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"txLogEnrichment": "DIFF"}, persisted)

	_, err = exec.Execute(ctx, "ALTER DATABASE cdcdb REMOVE OPTION txLogEnrichment", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{}, options("cdcdb"))
	_, err = exec.Execute(ctx, "ALTER DATABASE cdcdb SET LIMIT max_nodes = 10", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE DATABASE `with.dot` IF NOT EXISTS OPTIONS {txLogEnrichment: $mode}", map[string]interface{}{"mode": "OFF"})
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"txLogEnrichment": "OFF"}, options("with.dot"))

	_, err = exec.Execute(ctx, "ALTER DATABASE missing IF EXISTS SET OPTION txLogEnrichment 'FULL'", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "ALTER DATABASE missing SET OPTION txLogEnrichment 'FULL'", nil)
	require.Error(t, err)

	for _, statement := range []string{
		"CREATE DATABASE bad OPTIONS {txLogEnrichment: 'SOME'}",
		"CREATE DATABASE bad OPTIONS {seedURI: 's3://bucket/backup'}",
		"CREATE DATABASE bad OPTIONS {noSuchOption: 1}",
		"ALTER DATABASE cdcdb SET OPTION txLogEnrichment 'SOME'",
		"ALTER DATABASE cdcdb SET OPTION storeFormat 'block'",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	}
	require.False(t, manager.Exists("bad"))
	for _, statement := range []string{
		"CREATE DATABASE bad OPTIONS txLogEnrichment",
		"ALTER DATABASE `` SET OPTION txLogEnrichment 'FULL'",
		"ALTER DATABASE cdcdb SET OPTION txLogEnrichment",
		"ALTER DATABASE cdcdb REMOVE OPTION seedURI",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.Error(t, err, statement)
	}

	// Typed CREATE carries initial options even without the optional ALTER/SHOW
	// options interface; that interface still gates standalone option mutation.
	plain := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	plain.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	_, err = plain.Execute(ctx, "CREATE DATABASE plain OPTIONS {txLogEnrichment: 'FULL'}", nil)
	require.NoError(t, err)
	require.True(t, manager.Exists("plain"))
	initial, err := manager.DatabaseOptions("plain")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"txLogEnrichment": "FULL"}, initial)
	_, err = plain.Execute(ctx, "ALTER DATABASE cdcdb SET OPTION txLogEnrichment 'FULL'", nil)
	require.Error(t, err)
	result, err := plain.Execute(ctx, "SHOW DATABASES YIELD name, options WHERE name = 'cdcdb' RETURN options", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, result.Rows)
}
