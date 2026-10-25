package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every CALL of a query is classified, not just the first: a DBMS/admin
// procedure called after a read procedure's YIELD, inside a subquery, or with
// a backticked or differently-cased name needs the admin permission.
func TestQueryPermissionRequirementsClassifiesEveryCall(t *testing.T) {
	for name, testCase := range map[string]struct {
		query      string
		admin      bool
		procedures []string
	}{
		"chained after a read procedure": {query: "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 5) YIELD id AS change RETURN count(*) AS n", admin: true, procedures: []string{"db.cdc.earliest", "db.cdc.query"}},
		"single admin procedure":         {query: "CALL db.cdc.query()", admin: true, procedures: []string{"db.cdc.query"}},
		"backticked name parts":          {query: "CALL db.cdc.earliest() YIELD id CALL `db`.`cdc`.`query`(id) YIELD id AS c RETURN c", admin: true, procedures: []string{"db.cdc.earliest", "db.cdc.query"}},
		"backticked whole name":          {query: "CALL db.cdc.earliest() YIELD id CALL `db.cdc.query`(id) YIELD id AS c RETURN c", admin: true, procedures: []string{"db.cdc.earliest", "db.cdc.query"}},
		"inside a CALL subquery":         {query: "MATCH (n) CALL { CALL db.cdc.query() YIELD id RETURN id } RETURN id", admin: true, procedures: []string{"db.cdc.query"}},
		"inside a scoped subquery":       {query: "WITH 1 AS x CALL (x) { CALL db.cdc.query() YIELD id RETURN id } RETURN id", admin: true, procedures: []string{"db.cdc.query"}},
		"other case":                     {query: "call db.cdc.earliest() yield id call DB.CDC.QUERY(id) yield id as c return c", admin: true, procedures: []string{"db.cdc.earliest", "db.cdc.query"}},
		"read-only chain":                {query: "CALL db.cdc.earliest() YIELD id RETURN id", admin: false, procedures: []string{"db.cdc.earliest"}},
		"in a string literal":            {query: "RETURN 'CALL db.cdc.query()' AS s", admin: false},
		"in a comment":                   {query: "// CALL db.cdc.query()\nRETURN 1", admin: false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.admin, QueryPermissionRequirements(t.Context(), testCase.query).Admin)
			var names []string
			for _, procedure := range RegisteredProceduresForCalls(t.Context(), testCase.query) {
				names = append(names, procedure.Name)
			}
			require.Equal(t, testCase.procedures, names)
		})
	}
}

func TestStorageExecutorEnforcesPermissionsOfChainedCalls(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "authorization")
	executor := NewStorageExecutor(store)
	readOnlyCtx := WithPermissionChecker(context.Background(), func(permission string) bool {
		return permission == "read"
	})
	for _, query := range []string{
		"CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 5) YIELD id AS change RETURN count(*) AS n",
		"CALL db.cdc.earliest() YIELD id CALL `db`.`cdc`.`query`(id) YIELD id AS change RETURN count(*) AS n",
		"CALL db.cdc.earliest() YIELD id CALL dbms.listConfig() YIELD name RETURN name",
		"call db.cdc.earliest() yield id call DB.CDC.QUERY(id) yield id as c return c",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := executor.Execute(readOnlyCtx, query, nil)
			var denied *PermissionDeniedError
			require.True(t, errors.As(err, &denied), "a viewer must not run an admin procedure chained after a read one: %v", err)
			require.Equal(t, "admin", denied.Permission)
		})
	}
}

func TestProcedureCaseAuthorizationPreservesExactContracts(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	names := []string{"caseAuthorization.Read", "caseAuthorization.read"}
	for i, name := range names {
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeRead, Admin: i == 1}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{}, nil
		}))
	}
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		defer globalProcedureRegistry.mu.Unlock()
		for _, name := range names {
			delete(globalProcedureRegistry.user, name)
		}
	})
	require.False(t, QueryPermissionRequirements(t.Context(), "CALL caseAuthorization.Read()").Admin)
	require.True(t, QueryPermissionRequirements(t.Context(), "CALL caseAuthorization.read()").Admin)
	unknownCase := QueryPermissionRequirements(t.Context(), "CALL CASEAUTHORIZATION.READ()")
	require.True(t, unknownCase.Admin)
	require.Equal(t, []string{"caseAuthorization.read"}, unknownCase.BoostedProcedures)
	_, found := globalProcedureRegistry.Get("CASEAUTHORIZATION.READ")
	require.False(t, found, "authorization fallback must not register an executable alias")
}
