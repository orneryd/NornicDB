package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAPOCFunctionsIntegration tests APOC functions work end-to-end
func TestAPOCFunctionsIntegration(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', age: 30})
		CREATE (b:Person {name: 'Bob', age: 25})
		CREATE (c:Person {name: 'Carol', age: 35})
		CREATE (a)-[:KNOWS]->(b)
		CREATE (b)-[:KNOWS]->(c)
		CREATE (a)-[:KNOWS]->(c)
	`, nil)
	require.NoError(t, err)

	t.Run("nornicdb.version", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.version()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Version: %v", result.Rows[0])
	})

	t.Run("nornicdb.stats", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL nornicdb.stats()", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		t.Logf("Stats: %v", result.Rows[0])
	})

	t.Run("db.labels", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.labels()", nil)
		require.NoError(t, err)
		assert.True(t, len(result.Rows) > 0)
		t.Logf("Labels: %v", result.Rows)
	})

	t.Run("db.relationshipTypes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL db.relationshipTypes()", nil)
		require.NoError(t, err)
		t.Logf("Relationship Types: %v", result.Rows)
	})

	t.Run("apoc.algo.pageRank", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.pageRank(nodes, {iterations: 20})
			YIELD node, score
			RETURN node.name as name, score
			ORDER BY score DESC
		`, nil)
		require.NoError(t, err)
		t.Logf("PageRank results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.algo.betweenness", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (n:Person)
			WITH collect(n) as nodes
			CALL apoc.algo.betweenness(nodes)
			YIELD node, score
			RETURN node.name as name, score
		`, nil)
		require.NoError(t, err)
		t.Logf("Betweenness results: %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v: score=%v", row[0], row[1])
		}
	})

	t.Run("apoc.neighbors.tohop", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'})
			CALL apoc.neighbors.tohop(start, 'KNOWS>', 2)
			YIELD node
			RETURN node.name as neighbor
		`, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, [][]interface{}{{"Bob"}, {"Carol"}}, result.Rows)
		t.Logf("Neighbors within 2 hops: %d", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  %v", row[0])
		}
	})

	t.Run("dbms.procedures", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL dbms.procedures()", nil)
		require.NoError(t, err)
		t.Logf("Available procedures: %d", len(result.Rows))
		// Show first 10
		for i, row := range result.Rows {
			if i >= 10 {
				t.Logf("  ... and %d more", len(result.Rows)-10)
				break
			}
			t.Logf("  %v", row[0])
		}
	})
}

func TestAPOCNeighborsTypedContracts(t *testing.T) {
	for _, procedure := range []struct {
		name       string
		yield      string
		projection string
	}{
		{name: "tohop", yield: "node", projection: "[node.name]"},
		{name: "byhop", yield: "nodes", projection: "[neighbor IN nodes | neighbor.name]"},
	} {
		t.Run(procedure.name, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, `
				CREATE (a:Neighbor {name:'A'}), (b:Neighbor {name:'B'}),
				       (c:Neighbor {name:'C'}), (d:Neighbor {name:'D'}), (i:Neighbor {name:'I'})
				CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c),
				       (c)-[:KNOWS]->(a), (i)-[:KNOWS]->(a), (a)-[:KNOWS]->(a),
				       (a)-[:OTHER]->(d), (d)-[:BACK]->(a)
			`, nil)
			require.NoError(t, err)
			call := "CALL apoc.neighbors." + procedure.name
			tail := " YIELD " + procedure.yield + " RETURN " + procedure.projection + " AS names"
			for _, testCase := range []struct {
				name      string
				arguments string
				filter    string
				distance  int64
				groups    [][]string
			}{
				{name: "outgoing_suffix", arguments: "start, $filter, $distance", filter: "KNOWS>", distance: 2, groups: [][]string{{"A", "B"}, {"C"}}},
				{name: "outgoing_prefix", arguments: "start, $filter, $distance", filter: ">KNOWS", distance: 2, groups: [][]string{{"A", "B"}, {"C"}}},
				{name: "incoming_suffix", arguments: "start, $filter, $distance", filter: "KNOWS<", distance: 2, groups: [][]string{{"A", "C", "I"}, {"B"}}},
				{name: "incoming_prefix", arguments: "start, $filter, $distance", filter: "<KNOWS", distance: 2, groups: [][]string{{"A", "C", "I"}, {"B"}}},
				{name: "undirected", arguments: "start, $filter, $distance", filter: "KNOWS", distance: 1, groups: [][]string{{"A", "B", "C", "I"}}},
				{name: "bidirectional_markers", arguments: "start, $filter, $distance", filter: "<KNOWS>", distance: 1, groups: [][]string{{"A", "C", "I"}}}, // the leading mark decides, as in APOC
				{name: "mixed_directions", arguments: "start, $filter, $distance", filter: "KNOWS>|<BACK", distance: 1, groups: [][]string{{"A", "B", "D"}}},
				{name: "outgoing_wildcard", arguments: "start, $filter, $distance", filter: ">", distance: 1, groups: [][]string{{"A", "B", "D"}}},
				{name: "unknown_type", arguments: "start, $filter, $distance", filter: "MISSING", distance: 2, groups: [][]string{{}, {}}},
				{name: "exhausted_frontier", arguments: "start, $filter, $distance", filter: "KNOWS>", distance: 4, groups: [][]string{{"A", "B"}, {"C"}, {}, {}}},
				{name: "empty_filter", arguments: "start, $filter, $distance", filter: "", distance: 1},
				{name: "zero_hops", arguments: "start, $filter, $distance", filter: "", distance: 0},
				{name: "negative_hops", arguments: "start, $filter, $distance", filter: "", distance: -1},
				{name: "default_distance", arguments: "start, 'KNOWS>'", groups: [][]string{{"A", "B"}}},
				{name: "default_filter_and_distance", arguments: "start"},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, "MATCH (start:Neighbor {name:'A'}) "+call+"("+testCase.arguments+")"+tail,
						map[string]interface{}{"filter": testCase.filter, "distance": testCase.distance})
					require.NoError(t, err)
					require.Equal(t, []string{"names"}, result.Columns)
					if procedure.name == "tohop" {
						var expected [][]interface{}
						for _, group := range testCase.groups {
							for _, name := range group {
								if name != "A" {
									expected = append(expected, []interface{}{[]interface{}{name}})
								}
							}
						}
						require.ElementsMatch(t, expected, result.Rows)
					} else {
						require.Len(t, result.Rows, len(testCase.groups))
						for index, group := range testCase.groups {
							require.Len(t, result.Rows[index], 1)
							var expected []interface{}
							for _, name := range group {
								expected = append(expected, name)
							}
							require.ElementsMatch(t, expected, result.Rows[index][0])
						}
					}
				})
			}
			t.Run("staged_write_visibility", func(t *testing.T) {
				result, err := exec.Execute(ctx, "CREATE (start:StagedNeighbor)-[:KNOWS]->(:StagedNeighbor {name:'staged'}) WITH start "+call+"(start, 'KNOWS>', 1)"+tail, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{[]interface{}{"staged"}}}, result.Rows)
			})
			t.Run("canonical_output_columns", func(t *testing.T) {
				nodes, err := store.GetNodesByLabel("Neighbor")
				require.NoError(t, err)
				var start *storage.Node
				for _, node := range nodes {
					if node.Properties["name"] == "A" {
						start = node
					}
				}
				require.NotNil(t, start)
				registered, ok := globalProcedureRegistry.Get("apoc.neighbors." + procedure.name)
				require.True(t, ok)
				result, err := registered.Handler(ctx, exec, "", []interface{}{start, "KNOWS>", int64(1)})
				require.NoError(t, err)
				require.Equal(t, []string{procedure.yield}, result.Columns)
				result, err = exec.Execute(ctx, call+"($start, 'KNOWS>', 1)", map[string]interface{}{"start": start})
				require.NoError(t, err)
				require.Equal(t, []string{procedure.yield}, result.Columns)
				require.Len(t, result.Rows, 1)
			})
		})
	}
}

func TestAPOCNeighborsTerminalErrors(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	start := &storage.Node{ID: "start"}
	_, err := store.CreateNode(start)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "end"})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "edge", StartNode: "start", EndNode: "end", Type: "KNOWS"}))
	for _, procedure := range []struct {
		name string
		call func(*StorageExecutor, context.Context, []interface{}) (*ExecuteResult, error)
	}{
		{name: "tohop", call: (*StorageExecutor).callApocNeighborsTohop},
		{name: "byhop", call: (*StorageExecutor).callApocNeighborsByhop},
	} {
		t.Run(procedure.name, func(t *testing.T) {
			for _, testCase := range []struct {
				name string
				args []interface{}
			}{
				{name: "missing_node"},
				{name: "extra_argument", args: []interface{}{start, "KNOWS", int64(1), true}},
				{name: "wrong_node", args: []interface{}{"start", "KNOWS", int64(1)}},
				{name: "nil_node", args: []interface{}{(*storage.Node)(nil), "KNOWS", int64(1)}},
				{name: "wrong_filter", args: []interface{}{start, int64(1), int64(1)}},
				{name: "wrong_distance", args: []interface{}{start, "KNOWS", "1"}},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := procedure.call(NewStorageExecutor(store), context.Background(), testCase.args)
					require.Error(t, err)
					require.Nil(t, result)
				})
			}
			sentinel := errors.New("neighbor storage failure")
			for _, testCase := range []struct {
				name   string
				engine storage.Engine
			}{
				{name: "outgoing_failure", engine: &callOutgoingErrEngine{Engine: store, err: sentinel}},
				{name: "incoming_failure", engine: &callIncomingErrEngine{Engine: store, err: sentinel}},
				{name: "node_failure", engine: &callGetNodeErrEngine{Engine: store, err: sentinel}},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := procedure.call(NewStorageExecutor(testCase.engine), context.Background(), []interface{}{start, "KNOWS", int64(1)})
					require.ErrorIs(t, err, sentinel)
					require.Nil(t, result)
				})
			}
			t.Run("cancelled", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				result, err := procedure.call(NewStorageExecutor(store), ctx, []interface{}{start, "KNOWS", int64(1)})
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			})
		})
	}
}


