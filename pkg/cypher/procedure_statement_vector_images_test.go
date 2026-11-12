package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicWriteProceduresKeepNodeVectorsForInnerConsumers(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		withPluginFunctions(t, map[string]interface{}{
			"nor413.vectorComponent": func(value interface{}) interface{} { return nor413VectorComponent(value) },
		})
		for _, test := range []struct {
			name string
			call string
		}{
			{"single statement", "CALL apoc.cypher.doIt($statement, {node: n}) YIELD value RETURN value.vector"},
			{"multiple statements", "CALL apoc.cypher.runMany($statement, {node: n}, {statistics: false}) YIELD result RETURN result.vector"},
		} {
			t.Run(test.name, func(t *testing.T) {
				graph := newNOR413ColdDocGraph(t)
				before, err := graph.store.NodeCount(t.Context())
				require.NoError(t, err)
				result := graph.run(t, "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) "+test.call,
					map[string]interface{}{"statement": "RETURN nor413.vectorComponent($node) AS vector"})
				require.Equal(t, [][]interface{}{{float64(1)}}, result.Rows,
					"runtime statement text must receive the original full node image")
				after, err := graph.store.NodeCount(t.Context())
				require.NoError(t, err)
				require.Equal(t, before, after, "WRITE admission does not invent mutations for a read body")
			})
		}
	})
}
