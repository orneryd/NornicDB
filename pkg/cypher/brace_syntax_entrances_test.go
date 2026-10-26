package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Statements with a grammar of their own hand their braces to it. A policy's
// APPLY and ON ACCESS blocks hold commands, not map entries; the expressions
// the policy parser extracts from them still get the map check.
func TestMapPreflightGrammarHandoffs(t *testing.T) {
	for _, query := range []string{
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.meta = {k: 1} } }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY {\n WHEN n.tags = {k: 1} APPLY PROFILE boost\n}",
		"CREATE CONSTRAINT c FOR (n:X) REQUIRE { n.x IS NOT NULL }",
		"CREATE /* ddl */ CONSTRAINT c FOR (n:X) REQUIRE { n.x IS NOT NULL }",
		"CREATE RANGE INDEX i FOR (n:X) ON (n.x) OPTIONS {indexProvider: 'range-1.0'}",
		"CREATE /* ddl */ VECTOR INDEX i FOR (n:X) ON (n.e) OPTIONS {indexConfig: {`vector.dimensions`: 3}}",
	} {
		require.NoError(t, validateStaticMapKeys(query), query)
	}
	for _, query := range []string{
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY { ON ACCESS { SET n.meta = {k} } }",
		"CREATE PROMOTION POLICY p FOR (n:Fact) APPLY {\n WHEN n.tags = {.k} APPLY PROFILE boost\n}",
	} {
		err := validateStaticMapKeys(query)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "map literal keys must be valid symbolic names", query)
	}
	err := validateStaticMapKeys("CREATE (n:X {k}) RETURN n")
	require.ErrorContains(t, err, "pattern property maps require key-value entries")
}

// Fabric checks the whole statement's maps before any fragment plans or runs.
// A fragment that receives no input rows never validates itself, so without
// that check a malformed map in it would be accepted.
func TestFabricStatementMapsCheckedBeforeFragmentsRun(t *testing.T) {
	inner := newTestMemoryEngine(t)
	ctx := context.Background()
	manager, err := multidb.NewDatabaseManager(ctx, inner, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "map_leaf_a"}))
	require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "map_leaf_b"}))
	require.NoError(t, manager.CreateCompositeDatabase(ctx, "map_cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "map_leaf_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "map_leaf_b", Type: "local", AccessMode: "read_write"},
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})

	for _, query := range []string{
		"CALL { USE map_cmp.a MATCH (n:MapFabricNothing) RETURN n } CALL { USE map_cmp.b WITH 1 AS a RETURN {a} AS out } RETURN out",
		"CALL { USE map_cmp.a MATCH (n:MapFabricNothing) RETURN n } CALL { USE map_cmp.b RETURN {1} AS out } RETURN out",
		"CALL { USE map_cmp.a MATCH (n:MapFabricNothing) RETURN n } CALL { USE map_cmp.b MATCH (m {bad}) RETURN m AS out } RETURN out",
		"CALL { USE map_cmp.a UNWIND [] AS a RETURN {a} AS out } RETURN out",
	} {
		t.Run(query, func(t *testing.T) {
			require.True(t, exec.shouldUseFabricPlanner(ctx, query))
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, "%v", err)
			requireSemanticDetail(t, err, "UnexpectedSyntax")
		})
	}

	// Nothing runs before the statement is rejected: the write in the first
	// fragment leaves no node behind.
	_, err = exec.Execute(ctx, "CALL { USE map_cmp.a CREATE (n:MapFabricBefore) RETURN 1 AS one } CALL { USE map_cmp.a MATCH (n:MapFabricNothing) RETURN {bad} AS out } RETURN one, out", nil)
	require.Error(t, err)
	leaf, err := manager.GetStorage("map_leaf_a")
	require.NoError(t, err)
	nodes, err := leaf.GetNodesByLabel("MapFabricBefore")
	require.NoError(t, err)
	require.Empty(t, nodes)

	for _, query := range []string{
		"CALL { USE map_cmp.a UNWIND [] AS a RETURN {k: a} AS out } RETURN out",
		"CALL { USE map_cmp.a WITH {a: 1} AS n, 2 AS b RETURN n {.a, b, c: 3} AS out } RETURN out",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
		})
	}
}
