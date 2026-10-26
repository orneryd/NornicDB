package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRowFunctionContextNestedElementIDNamespace(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%t", normalization), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_labels"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:A {id:1})-[r:R]->(b:B {id:2})", nil)
			require.NoError(t, err)
			for _, c := range []struct{ name, query, kind string }{
				{"node_list", "MATCH (n:A) WITH n RETURN elementId(n) AS direct, [elementId(n)] AS nested", "list"},
				{"edge_list", "MATCH ()-[r:R]->() WITH r RETURN elementId(r) AS direct, [elementId(r)] AS nested", "list"},
				{"node_argument", "MATCH (n:A) WITH n RETURN elementId(n) AS direct, toString(elementId(n)) AS nested", "string"},
				{"node_map", "MATCH (n:A) WITH n RETURN elementId(n) AS direct, {eid:elementId(n)} AS nested", "map"},
				{"edge_argument", "MATCH ()-[r:R]->() WITH r RETURN elementId(r) AS direct, toString(elementId(r)) AS nested", "string"},
				{"edge_map", "MATCH ()-[r:R]->() WITH r RETURN elementId(r) AS direct, {eid:elementId(r)} AS nested", "map"},
			} {
				t.Run(c.name, func(t *testing.T) {
					for range 3 {
						result, err := exec.Execute(ctx, c.query, nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1)
						nested := result.Rows[0][1]
						switch c.kind {
						case "list":
							list, ok := nested.([]interface{})
							require.True(t, ok, "%#v", result.Rows)
							require.Len(t, list, 1)
							nested = list[0]
						case "map":
							object, ok := nested.(map[string]interface{})
							require.True(t, ok, "%#v", result.Rows)
							nested = object["eid"]
						}
						require.Equal(t, result.Rows[0][0], nested, "query=%s rows=%#v", c.query, result.Rows)
					}
				})
			}
		})
	}
}

func TestRowFunctionContextExecutionState(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%t", normalization), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			t.Run("selected_database", testRowFunctionSelectedDatabase)
			t.Run("routing", testRowFunctionDatabaseRouting)
			t.Run("storage", testRowFunctionStorage)
			t.Run("catalog", testRowFunctionGraphCatalog)
			t.Run("version", testRowFunctionVersion)
		})
	}
}

func TestRowFunctionContextBindingDoesNotEscapeEvaluation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_context_storage"))
	row := pipelineRow{"n": &storage.Node{ID: "n"}, "x": "a"}
	for _, database := range []string{"first_context", "second_context", "first_context"} {
		ctx := withExpressionFailureSlot(context.WithValue(context.Background(), ctxKeyUseDatabase, database))
		for range 3 {
			value, ok := exec.evaluateRowExpressionWithContext(ctx, originalBraceView("[elementId(n)]"), row)
			require.True(t, ok)
			require.Equal(t, []interface{}{"4:" + database + ":n"}, value)
			require.NotContains(t, row, rowStatementContextKey)
			predicate := "head([elementId(n)]) STARTS WITH '4:" + database + ":'"
			require.True(t, exec.evaluateRowPredicate(ctx, originalBraceView(predicate), row))
			require.NotContains(t, row, rowStatementContextKey)
			require.NoError(t, getExpressionFailure(ctx))
		}
	}
	// An enclosing evaluation owns its binding, even when a nested caller
	// supplies another context. It must not be overwritten or removed.
	outer := context.WithValue(context.Background(), ctxKeyUseDatabase, "outer_context")
	row[rowStatementContextKey] = outer
	inner := context.WithValue(context.Background(), ctxKeyUseDatabase, "inner_context")
	value, ok := exec.evaluateRowExpressionWithContext(inner, originalBraceView("[elementId(n)]"), row)
	require.True(t, ok)
	require.Equal(t, []interface{}{"4:outer_context:n"}, value)
	require.Same(t, outer, row[rowStatementContextKey])
	require.True(t, exec.evaluateRowPredicate(inner, expressionBraceView("toLower(x) = 'a'"), row))
	require.Same(t, outer, row[rowStatementContextKey])
	delete(row, rowStatementContextKey)
	// Error returns also release the temporary binding.
	ctx := withExpressionFailureSlot(context.Background())
	_, _ = exec.evaluateRowExpressionWithContext(ctx, expressionBraceView("substring('abc', 0, -1)"), row)
	require.Error(t, getExpressionFailure(ctx))
	require.NotContains(t, row, rowStatementContextKey)
	require.Equal(t, pipelineRow{"n": &storage.Node{ID: "n"}, "x": "a"}, row)
}

func testRowFunctionSelectedDatabase(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_context_storage"))
	// USE/Fabric can select an execution identity different from the storage
	// wrapper's namespace. Exercise that context through the public entry.
	ctx := context.WithValue(context.Background(), ctxKeyUseDatabase, "row_context_selected")
	_, err := exec.Execute(ctx, "CREATE (:A)-[:R]->(:B)", nil)
	require.NoError(t, err)
	for range 3 {
		result, err := exec.Execute(ctx, "MATCH (n:A)-[r:R]->() WITH n,r RETURN elementId(n) AS node, elementId(r) AS edge, [elementId(n),elementId(r)] AS nested, {node:elementId(n),edge:elementId(r)} AS mapped, toString(elementId(n)) AS node_string, toString(elementId(r)) AS edge_string", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		row := result.Rows[0]
		require.True(t, strings.HasPrefix(row[0].(string), "4:row_context_selected:"))
		require.True(t, strings.HasPrefix(row[1].(string), "5:row_context_selected:"))
		require.Equal(t, []interface{}{row[0], row[1]}, row[2])
		require.Equal(t, map[string]interface{}{"node": row[0], "edge": row[1]}, row[3])
		require.Equal(t, row[0], row[4])
		require.Equal(t, row[1], row[5])
	}
}

func testRowFunctionDatabaseRouting(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "row_context_target"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "row_context_composite", []multidb.ConstituentRef{
		{Alias: "data", DatabaseName: "row_context_target", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "USE row_context_target CREATE (:A)-[:R]->(:B)", nil)
	require.NoError(t, err)
	body := "MATCH (n:A)-[r:R]->() WITH n,r RETURN elementId(n) AS node, elementId(r) AS edge, [elementId(n),elementId(r)] AS nested"
	for _, query := range []string{
		"USE row_context_target " + body,
		"PROFILE USE row_context_target " + body,
		"USE row_context_composite CALL { USE row_context_composite.data " + body + " } RETURN node,edge,nested",
	} {
		for range 3 {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 1)
			row := result.Rows[0]
			require.Equal(t, []interface{}{row[0], row[1]}, row[2], query)
		}
	}
}

// Every registry function that reads Storage is compared in two namespaces,
// with both graph projections using the same name and different node values.
func testRowFunctionStorage(t *testing.T) {
	for _, database := range []string{"row_context_one", "row_context_two"} {
		t.Run(database, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), database))
			ctx := context.Background()
			value := int64(11)
			if database == "row_context_two" {
				value = 111
			}
			params := map[string]interface{}{"a": value, "b": value + 1}
			_, err := exec.Execute(ctx, "CREATE (a:N {v:$a}),(b:N {v:$b}),(c:N {v:0}),(a)-[:R]->(c),(b)-[:R]->(c)", params)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CALL gds.graph.project('row_context_projection','N','R',{nodeProperties:['v']})", nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := exec.Execute(ctx, "CALL gds.graph.drop('row_context_projection')", nil)
				require.NoError(t, err)
			})
			expressions := []string{
				"gds.graph.exists('row_context_projection')",
				"gds.util.nodeProperty('row_context_projection',id(a),'v')",
				"gds.util.asNode(id(a)).v",
				"gds.util.asNodes([id(a)])[0].v",
			}
			for _, function := range []string{"adamicAdar", "commonNeighbors", "preferentialAttachment", "resourceAllocation", "totalNeighbors"} {
				expressions = append(expressions, "gds.alpha.linkprediction."+function+"(a,b)")
			}
			for _, expression := range expressions {
				query := "MATCH (a:N {v:$a}),(b:N {v:$b}) WITH a,b RETURN " + expression + " AS direct, [" + expression + "] AS nested, {value:" + expression + "} AS mapped"
				for range 2 {
					result, err := exec.Execute(ctx, query, params)
					require.NoError(t, err, query)
					require.Len(t, result.Rows, 1)
					row := result.Rows[0]
					require.NotNil(t, row[0], expression)
					require.Equal(t, []interface{}{row[0]}, row[1], expression)
					require.Equal(t, map[string]interface{}{"value": row[0]}, row[2], expression)
				}
			}
		})
	}
}

func testRowFunctionGraphCatalog(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "row_context_graph"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "row_context_catalog", []multidb.ConstituentRef{
		{Alias: "data", DatabaseName: "row_context_graph", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage("row_context_catalog")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, expression := range []string{"graph.names()", "graph.propertiesByName('row_context_catalog.data')"} {
		result, err := exec.Execute(context.Background(), "WITH 1 AS seed RETURN "+expression+" AS direct, ["+expression+"] AS nested, {value:"+expression+"} AS mapped", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		row := result.Rows[0]
		require.Equal(t, []interface{}{row[0]}, row[1])
		require.Equal(t, map[string]interface{}{"value": row[0]}, row[2])
	}
}

func testRowFunctionVersion(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "row_context_version"))
	ctx := context.Background()
	for _, version := range []string{"", "CYPHER 25 "} {
		for _, expression := range []string{
			"trim('leading', s)", "toString(1)", "toStringOrNull(1)", "toStringList([1])",
			"toInteger('1')", "toInt('1')", "toIntegerOrNull('1')", "toIntegerList(['1'])",
			"toFloat('1')", "toFloatOrNull('1')", "toFloatList(['1'])",
			"toBoolean('true')", "toBooleanOrNull('true')", "toBooleanList(['true'])",
		} {
			result, err := exec.Execute(ctx, version+"WITH ' x ' AS s RETURN "+expression+" AS direct, ["+expression+"] AS nested, {value:"+expression+"} AS mapped", nil)
			require.NoError(t, err)
			row := result.Rows[0]
			require.Equal(t, []interface{}{row[0]}, row[1])
			require.Equal(t, map[string]interface{}{"value": row[0]}, row[2])
		}
		for _, expression := range []string{"left(s,-1)", "right(s,-1)", "substring(s,-1)"} {
			_, err := exec.Execute(ctx, version+"WITH 'abc' AS s RETURN ["+expression+"] AS nested", nil)
			require.Error(t, err)
			code := "Neo.DatabaseError.Statement.ExecutionFailed"
			if version != "" {
				code = "Neo.ClientError.Statement.ArgumentError"
			}
			requireStatusCode(t, err, code)
		}
	}
}
