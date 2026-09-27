package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQueryPermissionRequirementsRecognizesCypherKeywords(t *testing.T) {
	for name, testCase := range map[string]struct {
		query  string
		write  bool
		schema bool
		admin  bool
	}{
		"read":                   {query: "MATCH (n) RETURN n"},
		"set after match":        {query: "MATCH (n) SET n.value = 1", write: true},
		"set before newline":     {query: "MATCH (n) SET\nn.value = 1", write: true},
		"remove before tab":      {query: "MATCH (n) REMOVE\tn.value", write: true},
		"create after unwind":    {query: "UNWIND [1] AS value CREATE ({value: value})", write: true},
		"schema":                 {query: "CREATE INDEX example FOR (n:Example) ON (n.value)", schema: true},
		"commented schema":       {query: "/* migration */ CREATE INDEX example FOR (n:Example) ON (n.value)", schema: true},
		"admin database DDL":     {query: "DROP DATABASE restricted", admin: true},
		"commented admin DDL":    {query: "// maintenance\nDROP DATABASE restricted", admin: true},
		"show users":             {query: "SHOW USERS", admin: true},
		"show transactions":      {query: "SHOW TRANSACTIONS", admin: true},
		"terminate transactions": {query: "/* audit */ TERMINATE TRANSACTIONS 'db-transaction-1'", admin: true},
		"show current user":      {query: "SHOW CURRENT USER"},
		"write procedure":        {query: "CALL db.create.setNodeVectorProperty('id', 'embedding', [1.0])", write: true},
		"keyword in string":      {query: "RETURN 'SET value' AS text"},
		"keyword in identifier":  {query: "MATCH (n:`CREATE`) RETURN n"},
		"keyword property":       {query: "MATCH (n) RETURN n.set"},
		"keyword map key":        {query: "RETURN {create: true} AS value"},
		"keyword in comment":     {query: "// DELETE n\nMATCH (n) RETURN n"},
	} {
		t.Run(name, func(t *testing.T) {
			requirements := QueryPermissionRequirements(testCase.query)
			require.True(t, requirements.Read)
			require.Equal(t, testCase.write, requirements.Write)
			require.Equal(t, testCase.schema, requirements.Schema)
			require.Equal(t, testCase.admin, requirements.Admin)
		})
	}
}

func TestStorageExecutorEnforcesQueryPermissions(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "authorization")
	executor := NewStorageExecutor(store)
	readOnlyCtx := WithPermissionChecker(context.Background(), func(permission string) bool {
		return permission == "read"
	})

	result, err := executor.Execute(readOnlyCtx, "MATCH (n) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])

	for name, query := range map[string]string{
		"direct create":  "CREATE (:DeniedDirect)",
		"match delete":   "MATCH (n) DETACH DELETE n",
		"optional match": "OPTIONAL MATCH (n) SET n.denied = true",
		"unwind create":  "UNWIND [1] AS value CREATE (:DeniedUnwind {value: value})",
		"with create":    "WITH 1 AS value CREATE (:DeniedWith {value: value})",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := executor.Execute(readOnlyCtx, query, nil)
			var denied *PermissionDeniedError
			require.True(t, errors.As(err, &denied))
			require.Equal(t, "write", denied.Permission)
		})
	}

	t.Run("schema", func(t *testing.T) {
		_, err := executor.Execute(readOnlyCtx, "DROP INDEX denied_index IF EXISTS", nil)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied))
		require.Equal(t, "schema", denied.Permission)
	})

	t.Run("admin procedure", func(t *testing.T) {
		_, err := executor.Execute(readOnlyCtx, "CALL dbms.info()", nil)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied))
		require.Equal(t, "admin", denied.Permission)
	})

	for _, query := range []string{"SHOW USERS", "SHOW TRANSACTIONS", "TERMINATE TRANSACTIONS 'authorization-transaction-1'"} {
		t.Run(query, func(t *testing.T) {
			_, err := executor.Execute(readOnlyCtx, query, nil)
			var denied *PermissionDeniedError
			require.ErrorAs(t, err, &denied)
			require.Equal(t, "admin", denied.Permission)
		})
	}

	t.Run("dynamic procedure statement", func(t *testing.T) {
		_, err := executor.Execute(readOnlyCtx, "CALL apoc.cypher.run('CREATE (:DeniedDynamic)', {})", nil)
		var denied *PermissionDeniedError
		require.True(t, errors.As(err, &denied))
		require.Equal(t, "write", denied.Permission)
	})
}

func TestDatabasePermissionResolverFollowsExecutionDatabase(t *testing.T) {
	ctx := WithDatabasePermissionResolver(context.Background(), "writable", func(database, permission string) bool {
		return permission == "read" || permission == "write" && database == "writable"
	})
	require.NoError(t, AuthorizeQuery(ctx, "CREATE (:Allowed)"))

	ctx = withExecutionDatabase(ctx, "readonly")
	var denied *PermissionDeniedError
	require.True(t, errors.As(AuthorizeQuery(ctx, "CREATE (:Denied)"), &denied))
	require.Equal(t, "write", denied.Permission)
}
