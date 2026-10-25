package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A custom procedure is one for the whole server, so a call of it is never
// answered from a database's result cache: replacing the procedure from
// another database (which writes nothing to the caller's graph) changes what
// the next call runs, also when the call is a statement apoc.cypher.run runs.
func TestReplacedCustomProcedureIsNotServedFromTheCache(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	caller := NewStorageExecutor(storage.NewNamespacedEngine(base, "vault"), QueryCachePolicy{MaxEntries: 100, TTL: time.Minute})
	installer := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"), QueryCachePolicy{MaxEntries: 100, TTL: time.Minute})
	install := func(body string) {
		t.Helper()
		_, err := installer.Execute(ctx, "CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', '"+body+"', 'nornic', 'read')", nil)
		require.NoError(t, err)
	}
	call := func(query string) [][]interface{} {
		t.Helper()
		result, err := caller.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}

	install("RETURN 1 AS c")
	for _, query := range []string{
		"CALL custom.report() YIELD c RETURN c",
		"CALL apoc.cypher.run('CALL custom.report() YIELD c RETURN c', {}) YIELD value RETURN value.c AS c",
	} {
		require.Equal(t, [][]interface{}{{int64(1)}}, call(query), query)
		require.Equal(t, [][]interface{}{{int64(1)}}, call(query), query)
	}
	install("RETURN 2 AS c")
	for _, query := range []string{
		"CALL custom.report() YIELD c RETURN c",
		"CALL apoc.cypher.run('CALL custom.report() YIELD c RETURN c', {}) YIELD value RETURN value.c AS c",
	} {
		require.Equal(t, [][]interface{}{{int64(2)}}, call(query), "the replaced body runs: %s", query)
	}

	require.True(t, isCacheableReadQuery("CALL apoc.cypher.run('RETURN 1 AS c', {}) YIELD value RETURN value"), "a query calling no custom procedure is cached")
}

// Cache recognition uses registered names independently of execution-key casing.
func TestCallsUserProcedureMatchesAnyLetterCase(t *testing.T) {
	registry := NewProcedureRegistry()
	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "custom.Tally", Mode: ProcedureModeRead},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	for _, query := range []string{
		"CALL custom.Tally() YIELD c RETURN c",
		"CALL custom.tally() YIELD c RETURN c",
		"CALL apoc.cypher.run('CALL custom.Tally() YIELD c RETURN c', {}) YIELD value RETURN value",
	} {
		require.True(t, registry.callsUserProcedure(query), query)
	}
	require.False(t, registry.callsUserProcedure("CALL db.labels() YIELD label RETURN label"))
}

func TestCallsUserProcedureWithExactRegistryKeys(t *testing.T) {
	registry := NewProcedureRegistry()
	require.NoError(t, registry.RegisterUser(ProcedureSpec{Name: "custom.CacheTick", Mode: ProcedureModeRead},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	if procedure, ok := registry.user["custom.cachetick"]; ok {
		delete(registry.user, "custom.cachetick")
		registry.user["custom.CacheTick"] = procedure
	}
	for _, query := range []string{
		"CALL custom.CacheTick() YIELD v RETURN v",
		"CALL `custom`.`CacheTick`() YIELD v RETURN v",
		"CALL apoc.cypher.run('CALL custom.CacheTick() YIELD v RETURN v', {}) YIELD value RETURN value",
	} {
		require.True(t, registry.callsUserProcedure(query), query)
	}
	_, executable := registry.Get("custom.cachetick")
	require.False(t, executable)
}

// Installing, replacing or dropping a custom procedure needs the admin
// entitlement, as CREATE and DROP PROCEDURE do: SHOW PROCEDURES lists the
// apoc.custom procedures that change the registry as admin-only, and the
// executor refuses them to a caller without admin, directly and in a
// statement apoc.cypher.run runs.
func TestCustomProcedureDefinitionNeedsAdmin(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	for _, name := range []string{"apoc.custom.installProcedure", "apoc.custom.declareProcedure", "apoc.custom.dropProcedure"} {
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found, name)
		require.True(t, procedure.Spec.Admin, name)
	}
	for _, name := range []string{"apoc.custom.list", "apoc.custom.show"} {
		procedure, found := globalProcedureRegistry.Get(name)
		require.True(t, found, name)
		require.False(t, procedure.Spec.Admin, name)
	}

	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	editor := WithPermissionChecker(ctx, func(permission string) bool { return permission != "admin" && permission != "schema" })
	for _, query := range []string{
		"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'RETURN 1 AS c', 'nornic', 'read')",
		"CALL apoc.custom.declareProcedure('report() :: (c :: INTEGER)', 'RETURN 1 AS c')",
		"CALL apoc.custom.dropProcedure('report')",
		"CREATE OR REPLACE PROCEDURE custom.report() MODE READ AS RETURN 1 AS c",
		"CALL apoc.cypher.run(\"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'RETURN 1 AS c')\", {}) YIELD value RETURN value",
	} {
		_, err := exec.Execute(editor, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), (&PermissionDeniedError{Permission: "admin"}).Error(), query)
	}
	_, found := globalProcedureRegistry.Get("custom.report")
	require.False(t, found, "nothing was installed")

	admin := WithPermissionChecker(ctx, func(string) bool { return true })
	_, err := exec.Execute(admin, "CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'RETURN 1 AS c', 'nornic', 'read')", nil)
	require.NoError(t, err)
	result, err := exec.Execute(editor, "CALL custom.report() YIELD c RETURN c", nil)
	require.NoError(t, err, "anyone with read runs an installed read procedure")
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
