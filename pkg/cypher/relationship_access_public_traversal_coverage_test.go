package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The statement owner must survive path aggregation, optional rows, nested
// projections and correlated reads. Assert results through public execution;
// EXPLAIN and PROFILE consume the same admitted statement as ordinary runtime.
func TestRelationshipAccessPublicTraversalConsumers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_traversal_consumers"))
	exec.cache = nil
	for _, query := range []string{
		"CREATE (a:CoverageRoute {name:'a'}), (b:CoverageRoute {name:'b'}), (c:CoverageRoute {name:'c'}), (a)-[:COVERAGE_ROUTE {key:'ab',amount:1}]->(b), (b)-[:COVERAGE_ROUTE {key:'bc',amount:2}]->(c), (a)-[:COVERAGE_ROUTE {key:'ac',amount:3}]->(c)",
		"CREATE RANGE INDEX coverage_route_key FOR ()-[r:COVERAGE_ROUTE]-() ON (r.key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		name, query string
		rows        [][]interface{}
	}{
		{"outgoing", "MATCH (a)-[r:COVERAGE_ROUTE {key:$key}]->(b) RETURN a.name, r.amount, b.name", [][]interface{}{{"a", int64(1), "b"}}},
		{"incoming", "MATCH (b)<-[r:COVERAGE_ROUTE]-(a) WHERE r.key=$key RETURN a.name, r.amount, b.name", [][]interface{}{{"a", int64(1), "b"}}},
		{"type_union", "MATCH (a)-[r:COVERAGE_ROUTE|COVERAGE_UNUSED]->(b) WHERE r.key=$key RETURN r.key", [][]interface{}{{"ab"}}},
		{"path_aggregation", "MATCH p=(a:CoverageRoute {name:'a'})-[:COVERAGE_ROUTE*1..2]->(b) RETURN count(p), sum(length(p)), min(length(p)), max(length(p)), avg(length(p))", [][]interface{}{{int64(3), int64(4), int64(1), int64(2), float64(4) / 3}}},
		{"grouped_path_aggregation", "MATCH p=(a:CoverageRoute {name:'a'})-[:COVERAGE_ROUTE*1..2]->(b) RETURN b.name AS name, count(p), sum(length(p)) ORDER BY name", [][]interface{}{{"b", int64(1), int64(1)}, {"c", int64(2), int64(3)}}},
		{"path_collection", "MATCH p=(a:CoverageRoute {name:'a'})-[:COVERAGE_ROUTE*1..2]->(b) RETURN size(collect(p))", [][]interface{}{{int64(3)}}},
		{"optional_empty", "MATCH (a:CoverageRoute {name:'c'}) OPTIONAL MATCH (a)-[r:COVERAGE_ROUTE]->(b) RETURN count(r)", [][]interface{}{{int64(0)}}},
		{"optional_nonempty", "MATCH (a:CoverageRoute {name:'a'}) OPTIONAL MATCH (a)-[r:COVERAGE_ROUTE]->(b) RETURN count(r), sum(r.amount)", [][]interface{}{{int64(2), int64(4)}}},
		{"pattern_projection", "MATCH (a:CoverageRoute {name:'a'}) RETURN size([(a)-[r:COVERAGE_ROUTE]->(b) | b.name])", [][]interface{}{{int64(2)}}},
		{"correlated_expression", "MATCH (a:CoverageRoute {name:'a'}) RETURN EXISTS { MATCH (a)-[:COVERAGE_ROUTE]->() } AS found, COUNT { MATCH (a)-[:COVERAGE_ROUTE]->() } AS amount", [][]interface{}{{true, int64(2)}}},
		{"computed_projection", "MATCH (a)-[r:COVERAGE_ROUTE {key:$key}]->(b) WITH r, {label:b.name} AS target RETURN {edge:r.key,target:target,nested:[x IN [1,2] | {value:x+r.amount}]} AS value", [][]interface{}{{map[string]interface{}{"edge": "ab", "target": map[string]interface{}{"label": "b"}, "nested": []interface{}{map[string]interface{}{"value": int64(2)}, map[string]interface{}{"value": int64(3)}}}}}},
		{"nested_call", "CALL { CALL { MATCH (a)-[r:COVERAGE_ROUTE {key:$key}]->(b) RETURN r.key AS key, b.name AS name } RETURN {key:key,name:name} AS value } RETURN value", [][]interface{}{{map[string]interface{}{"key": "ab", "name": "b"}}}},
		{"shortest_value", "MATCH (a:CoverageRoute {name:'a'}), (c:CoverageRoute {name:'c'}) RETURN length(shortestPath((a)-[:COVERAGE_ROUTE*]->(c)))", [][]interface{}{{int64(1)}}},
	} {
		for _, mode := range []string{"", "PROFILE ", "EXPLAIN "} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), mode+tc.query, map[string]interface{}{"key": "ab"})
				require.NoError(t, err, mode+tc.query)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
					require.NotEmpty(t, planStringFromResult(t, result))
					return
				}
				require.Equal(t, tc.rows, result.Rows, mode+tc.query)
			})
		}
	}
}
