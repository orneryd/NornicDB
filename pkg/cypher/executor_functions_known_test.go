package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The functions the query engine evaluates itself pass the unknown-function
// check when a plugin lookup is configured and doesn't have them, as on a
// server without the APOC plugin (on Windows, plugins can't load).
func TestExecutorImplementedFunctionsAreKnown(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(string) (interface{}, bool) { return nil, false }
	t.Cleanup(func() { PluginFunctionLookup = previous })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "executor_functions_known"))
	ctx := context.Background()
	for _, name := range []string{
		"apoc.coll.avg",
		"apoc.coll.contains",
		"apoc.coll.containsall",
		"apoc.coll.containsany",
		"apoc.coll.flatten",
		"apoc.coll.frequencies",
		"apoc.coll.indexof",
		"apoc.coll.intersection",
		"apoc.coll.max",
		"apoc.coll.min",
		"apoc.coll.occurrences",
		"apoc.coll.pairs",
		"apoc.coll.partition",
		"apoc.coll.reverse",
		"apoc.coll.sort",
		"apoc.coll.sortnodes",
		"apoc.coll.split",
		"apoc.coll.subtract",
		"apoc.coll.sum",
		"apoc.coll.toset",
		"apoc.coll.union",
		"apoc.coll.unionall",
		"apoc.coll.zip",
		"apoc.convert.fromjsonlist",
		"apoc.convert.fromjsonmap",
		"apoc.convert.tojson",
		"apoc.create.uuid",
		"apoc.map.clean",
		"apoc.map.fromlists",
		"apoc.map.frompairs",
		"apoc.map.merge",
		"apoc.map.removekey",
		"apoc.map.setkey",
		"apoc.meta.istype",
		"apoc.meta.type",
		"apoc.text.join",
		"date.day",
		"date.dayofweek",
		"date.dayofyear",
		"date.month",
		"date.ordinalday",
		"date.quarter",
		"date.week",
		"date.weekyear",
		"date.year",
		"datetime.day",
		"datetime.hour",
		"datetime.minute",
		"datetime.month",
		"datetime.second",
		"datetime.year",
		"point.crs",
		"point.height",
		"point.latitude",
		"point.longitude",
		"point.srid",
		"point.withindistance",
		"point.x",
		"point.y",
		"point.z",
	} {
		_, err := exec.Execute(ctx, "RETURN "+name+"([1, 2]) AS v", nil)
		if err != nil {
			require.NotContains(t, strings.ToLower(err.Error()), "unknown function", name)
		}
	}
	result, err := exec.Execute(ctx, "RETURN apoc.convert.fromJsonList('[1,2]') AS a, apoc.text.join(['a', 'b'], '-') AS b, apoc.convert.toJson([1]) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(1), int64(2)}, "a-b", "[1]"}}, result.Rows)
	_, err = exec.Execute(ctx, "RETURN apoc.no.suchFunction(1) AS v", nil)
	require.ErrorContains(t, err, "unknown function")
}
