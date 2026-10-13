package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SHOW FUNCTIONS lists the built-in apoc.* functions as Neo4j with APOC lists
// APOC's (APOC's signature and description, not built in), NornicDB's own
// apoc.* names marked as extensions, and a loaded plugin's functions with a
// signature from their Go type (#1168).
func TestShowFunctionsListsApocFunctions(t *testing.T) {
	previous := PluginFunctionList
	PluginFunctionList = func() []PluginFunctionInfo {
		return []PluginFunctionInfo{
			{Name: "zz.text.camelCase", Category: "text", Description: "Camel-cases text", Signature: "zz.text.camelCase(arg1 :: STRING) :: STRING"},
			// A name the catalog lists keeps the catalog's row.
			{Name: "apoc.coll.sum", Category: "coll", Description: "plugin sum", Signature: "apoc.coll.sum(arg1 :: LIST<ANY>) :: FLOAT"},
			// Plugin names are case-sensitive: a case-distinct name is listed.
			{Name: "apoc.coll.SUM", Category: "coll", Description: "plugin SUM", Signature: "apoc.coll.SUM(arg1 :: LIST<ANY>) :: FLOAT"},
		}
	}
	t.Cleanup(func() { PluginFunctionList = previous })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "show_functions_apoc"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "SHOW FUNCTIONS YIELD name, description, signature, isBuiltIn, argumentDescription WHERE name IN ['apoc.coll.flatten', 'apoc.meta.type', 'zz.text.camelCase', 'toUpper'] RETURN name, description, signature, isBuiltIn, [a IN argumentDescription | a.description] AS arguments ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		// APOC 5.26's row, as Neo4j with APOC lists it.
		{"apoc.coll.flatten", "Flattens the given `LIST<ANY>` (to flatten nested `LIST<ANY>` values, set recursive to true).", "apoc.coll.flatten(coll :: LIST<ANY>, recursive = false :: BOOLEAN) :: LIST<ANY>", false, []interface{}{"The list to flatten.", "Whether nested list items should also be flattened."}},
		{"apoc.meta.type", "Returns the type name of the given value. (NornicDB extension; APOC 5's is apoc.meta.cypher.type)", "apoc.meta.type(value :: ANY) :: STRING", false, []interface{}{"The value to return the type name of."}},
		{"toUpper", "Returns the given `STRING` in uppercase.", "toUpper(input :: STRING) :: STRING", true, []interface{}{"A string to be converted into uppercase."}},
		{"zz.text.camelCase", "Camel-cases text", "zz.text.camelCase(arg1 :: STRING) :: STRING", false, []interface{}{""}},
	}, result.Rows)

	// Every built-in apoc.* function is listed, once.
	result, err = exec.Execute(ctx, "SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc.' RETURN count(*) AS listed, count(DISTINCT name) AS names", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(37), int64(37)}}, result.Rows, "the 36 built-ins and the plugin's apoc.coll.SUM")
	result, err = exec.Execute(ctx, "SHOW FUNCTIONS YIELD name, description WHERE name = 'apoc.coll.sum' RETURN description", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`."}}, result.Rows)

	// No plugin listing: the catalog's rows only.
	PluginFunctionList = nil
	require.Nil(t, pluginFunctionRows())
}
