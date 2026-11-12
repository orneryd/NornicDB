package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/stretchr/testify/require"
)

func TestNativeScalarFunctionReceivesWholeNodeImages(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		handler, err := registry.Compile(func(value interface{}) interface{} { return nor413VectorComponent(value) }, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "image.component" {
				return handler, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		for _, explicit := range []bool{false, true} {
			for _, argument := range []string{"n", "[n]", "{node:n}"} {
				t.Run(fmt.Sprintf("explicit-%v/%s", explicit, argument), func(t *testing.T) {
					graph := newNOR413ColdDocGraph(t)
					if explicit {
						graph.run(t, "BEGIN", nil)
						t.Cleanup(func() { graph.run(t, "ROLLBACK", nil) })
					}
					result := graph.run(t, "MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) RETURN image.component("+argument+")", nil)
					require.Equal(t, [][]interface{}{{float64(1)}}, result.Rows)
				})
			}
		}
	})
}

func TestNativeFunctionInputsFollowScopedAliases(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		component, err := registry.Compile(func(value interface{}) interface{} { return nor413VectorComponent(value) }, registry.OpaqueReads)
		require.NoError(t, err)
		echo, err := registry.Compile(func(value interface{}) interface{} { return value }, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			switch name {
			case "image.component":
				return component, true
			case "image.echo":
				return echo, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		const match = "MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) "
		for _, tc := range []struct {
			query                string
			want                 interface{}
			metadata, nodeResult bool
		}{
			{match + "WITH n.title AS value RETURN image.echo(value)", "doc 3", true, false},
			{match + "WITH {node:n} AS n RETURN image.component(n.node)", float64(1), false, false},
			{match + "CALL (n) { RETURN n.title AS value } RETURN image.echo(value)", "doc 3", true, false},
			{"CALL { MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) RETURN n } RETURN image.component(n)", float64(1), false, false},
			{match + "RETURN n.title AS n ORDER BY image.echo(n)", "doc 3", true, false},
			{"WITH 'old' AS n MATCH (:Hub)-[:HAS]->(m:Doc {k:3}) RETURN m AS n ORDER BY image.component(n)", float64(1), false, true},
			{"CYPHER 25 " + match + "RETURN image.echo(n.title)", "doc 3", true, false},
			{"MATCH (:Hub)-[:HAS]->(`n`:Doc {k:3}) RETURN image.echo(`n`.title)", "doc 3", true, false},
			{"WITH 'outer' AS x " + match + "RETURN any(x IN [n] WHERE image.component(x) = 1)", true, false, false},
			{"WITH 'outer' AS x " + match + "RETURN reduce(total=0,x IN [n] | total+image.component(x))", float64(1), false, false},
			{"WITH 'outer' AS x " + match + "RETURN [x IN [n] | image.component(x)][0]", float64(1), false, false},
		} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit-%v/%s", explicit, tc.query), func(t *testing.T) {
					graph := newNOR413ColdDocGraph(t)
					spy := storagefault.NewQueryNodeReadSpy(graph.store)
					if !explicit {
						graph.exec = NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
					}
					if explicit {
						graph.run(t, "BEGIN", nil)
						t.Cleanup(func() { graph.run(t, "ROLLBACK", nil) })
					}
					result := graph.run(t, tc.query, nil)
					require.Len(t, result.Rows, 1)
					value := result.Rows[0][0]
					if tc.nodeResult {
						value = nor413VectorComponent(value)
					}
					require.Equal(t, tc.want, value)
					if tc.metadata && !explicit {
						require.Zero(t, spy.Full.Load(), "scalar-only native inputs must not require full storage nodes")
						require.Positive(t, spy.Query.Load())
					}
				})
			}
		}
	})
}

func TestNativeFunctionMetadataProofUsesExactSource(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		echo, err := registry.Compile(func(value interface{}) interface{} { return value }, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "image.echo" {
				return echo, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		graph := newNOR413ColdDocGraph(t)
		spy := storagefault.NewQueryNodeReadSpy(graph.store)
		graph.exec = NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
		for index, query := range []string{
			"MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) RETURN image.echo(n.title)",
			"/* leading comment */ MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) RETURN image.echo(n.title)",
			"MATCH (:Hub)-[:HAS]->(n:Doc {k: 3})\nRETURN image.echo(n.title)",
			"MATCH (:Hub)-[:HAS]->(n:Doc {k:2}) RETURN image.echo(n.title)",
			"MATCH (:Hub)-[:HAS]->(n:Doc {k:3}) RETURN image.echo(n.title)",
		} {
			spy.Full.Store(0)
			spy.Query.Store(0)
			result := graph.run(t, query, nil)
			require.Len(t, result.Rows, 1)
			want := "doc 3"
			if index == 3 {
				want = "doc 2"
			}
			require.Equal(t, want, result.Rows[0][0])
			require.Zero(t, spy.Full.Load(), query)
			require.Positive(t, spy.Query.Load(), query)
		}
	})
}
