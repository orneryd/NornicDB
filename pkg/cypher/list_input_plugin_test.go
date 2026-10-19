package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

// Exercise the real registered plugin handlers through public Cypher. The
// plugin's comparison algorithms remain its own; only representation parity is
// asserted here. Procedure-only split and predicate-taking partition are covered
// by their existing API tests; fill consumes a scalar rather than a list.
func TestListInputPluginRepresentations(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	handlers := map[string]interface{}{}
	for _, name := range apoc.List() {
		collection := strings.HasPrefix(name, "apoc.coll.") && name != "apoc.coll.partition" && name != "apoc.coll.split"
		conversion := strings.HasPrefix(name, "apoc.convert.to") && (strings.HasSuffix(name, "List") || name == "apoc.convert.toSet")
		if collection || conversion {
			info, ok := apoc.GetInfo(name)
			require.True(t, ok)
			handlers[name] = info.Handler
		}
	}
	withPluginFunctions(t, handlers)
	queries := listInputPluginQueries()
	for name := range handlers {
		if name != "apoc.coll.fill" {
			require.Contains(t, strings.Join(queries, "\n"), name+"(", "missing registered plugin list consumer")
		}
	}
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_lists")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1)
		encoded, err := json.Marshal(result.Rows[0][0])
		require.NoError(t, err)
		var value interface{}
		require.NoError(t, json.Unmarshal(encoded, &value))
		return value
	}
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range listinputs.Inputs() {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"PluginList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				for _, template := range queries {
					t.Run(template, func(t *testing.T) {
						want := read(t, listinputs.Query(template, input.Literal), nil)
						require.Equal(t, want, read(t, listinputs.Query(template, "$xs"), params))
						require.Equal(t, want, read(t, "MATCH (n:PluginList {name:$name}) "+listinputs.Query(template, "n.xs"), params))
					})
				}
			})
		}
	}
	rowValueProgramsDisabled.Store(false)
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
}

func listInputPluginQueries() []string {
	return []string{
		"apoc.coll.sum(%s)", "apoc.coll.avg(%s)", "apoc.coll.min(%s)", "apoc.coll.max(%s)",
		"apoc.coll.sort(%s)", "apoc.coll.sortMaps(%s,'rank')", "apoc.coll.reverse(%s)",
		"apoc.coll.contains(%s,head(%s))", "apoc.coll.containsAll(%s,%s)", "apoc.coll.containsAny(%s,%s)",
		"apoc.coll.containsSorted(%s,head(%s))", "apoc.coll.containsDuplicates(%s)",
		"apoc.coll.union(%s,[9])", "apoc.coll.union([9],%s)", "apoc.coll.unionAll(%s,[9])",
		"coll.sort(apoc.coll.intersection(%s,%s))", "apoc.coll.subtract(%s,[9])", "apoc.coll.disjunction(%s,[9])",
		"apoc.coll.flatten(%s,true)", "apoc.coll.flatten([%s],true)", "apoc.coll.toSet(%s)",
		"apoc.coll.indexOf(%s,head(%s))", "apoc.coll.pairs(%s)", "apoc.coll.pairsMin(%s)",
		"apoc.coll.zip(%s,[4,5,6])", "apoc.coll.frequencies(%s)", "apoc.coll.frequenciesAsMap(%s)",
		"apoc.coll.occurrences(%s,head(%s))", "coll.sort(apoc.coll.duplicates(%s))", "coll.sort(apoc.coll.duplicatesWithCount(%s))",
		"apoc.coll.dropDuplicateNeighbors(%s)", "apoc.coll.insert(%s,1,9)", "apoc.coll.insertAll(%s,1,%s)",
		"apoc.coll.remove(%s,1)", "apoc.coll.removeAll(%s,head(%s))", "apoc.coll.set(%s,1,9)",
		"apoc.coll.slice(%s,1,3)", "coll.sort(apoc.coll.shuffle(%s))",
		"apoc.coll.contains(%s,apoc.coll.randomItem(%s))", "size(apoc.coll.randomItems(%s,2))",
		"apoc.coll.isEmpty(%s)", "apoc.coll.isNotEmpty(%s)", "apoc.coll.different(%s,[9])", "apoc.coll.sumLongs(%s)",
		"apoc.convert.toList(%s)", "apoc.convert.toSet(%s)", "apoc.convert.toIntList(%s)",
		"apoc.convert.toFloatList(%s)", "apoc.convert.toStringList(%s)", "apoc.convert.toBooleanList(%s)",
		"apoc.convert.toNodeList(%s)", "apoc.convert.toRelationshipList(%s)",
	}
}
