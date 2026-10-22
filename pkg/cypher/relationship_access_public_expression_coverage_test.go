package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every expression is reached with a real matched relationship and two rows.
// Nested executable expressions keep their source through the shared function
// owner; scalar answers below are independent of the physical access route.
func TestRelationshipAccessPublicExpressionCoverage(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "public_expression_coverage"))
	exec.cache = nil
	withPluginFunctions(t, map[string]interface{}{
		"inspection.coverageidentity": func(value interface{}) interface{} { return value },
	})
	for _, query := range []string{
		"CREATE (:CoverageNode {name:'a', x:0, y:0})-[:COVERAGE_EDGE {key:'hit', x:3, y:4}]->(:CoverageNode {name:'b'})",
		"CREATE RANGE INDEX coverage_expression_key FOR ()-[r:COVERAGE_EDGE]-() ON (r.key)",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
	}
	const count = "COUNT { MATCH ()-[edge:COVERAGE_EDGE {key:$key}]->() RETURN edge }"
	const collected = "COLLECT { MATCH ()-[edge:COVERAGE_EDGE {key:$key}]->() RETURN edge.key }"
	for _, tc := range []struct {
		name, expression string
		want             interface{}
	}{
		{"sin", "sin(" + count + "-1)", 0.0},
		{"cos", "cos(" + count + "-1)", 1.0},
		{"tan", "tan(" + count + "-1)", 0.0},
		{"asin", "asin(" + count + "-1)", 0.0},
		{"acos", "acos(" + count + ")", 0.0},
		{"atan", "atan(" + count + "-1)", 0.0},
		{"atan2", "atan2(" + count + "-1,1)", 0.0},
		{"exp", "exp(" + count + "-1)", 1.0},
		{"log", "log(" + count + ")", 0.0},
		{"log10", "log10(" + count + ")", 0.0},
		{"sqrt", "sqrt(" + count + "+3)", 2.0},
		{"degrees", "degrees(" + count + "-1)", 0.0},
		{"haversin", "haversin(" + count + "-1)", 0.0},
		{"sinh", "sinh(" + count + "-1)", 0.0},
		{"cosh", "cosh(" + count + "-1)", 1.0},
		{"tanh", "tanh(" + count + "-1)", 0.0},
		{"power", "power(" + count + "+1,3)", 8.0},
		{"ceil", "ceil(" + count + "+0.2)", 2.0},
		{"floor", "floor(" + count + "+0.2)", 1.0},
		{"round", "round(" + count + "+0.2)", 1.0},
		{"tail", "tail(" + collected + ")", []interface{}{}},
		{"replace", "replace('xhitx',head(" + collected + "),'ok')", "xokx"},
		{"split", "split('hit|other','|')", []interface{}{"hit", "other"}},
		{"head", "head(" + collected + ")", "hit"},
		{"last", "last(" + collected + ")", "hit"},
		{"reverse", "reverse(" + collected + ")", []interface{}{"hit"}},
		{"lpad", "lpad(r.key,5,'0')", "00hit"},
		{"rpad", "rpad(r.key,5,'0')", "hit00"},
		{"point_node", "point(a).x", 0.0},
		{"point_relationship", "point(r).x", 3.0},
		{"point_x", "point.x(point({x:" + count + ",y:2}))", 1.0},
		{"point_y", "point.y(point({x:1,y:" + count + "+1}))", 2.0},
		{"point_z", "point.z(point({x:1,y:2,z:3}))", 3.0},
		{"point_srid", "point.srid(point({x:1,y:2}))", int64(7203)},
		{"point_crs", "point.crs(point({x:1,y:2}))", "cartesian"},
		{"point_latitude", "point.latitude(point({latitude:0,longitude:0}))", 0.0},
		{"point_longitude", "point.longitude(point({latitude:0,longitude:0}))", 0.0},
		{"distance", "distance(point(a),point(r))", 5.0},
		{"point_distance", "point.distance(point(a),point(r))", 5.0},
		{"point_within_bbox", "point.withinBBox(point({x:1,y:1}),point({x:0,y:0}),point({x:2,y:2}))", true},
		{"point_within_distance", "point.withinDistance(point(a),point(r),5)", true},
		{"kalman_default", "kalman.state(kalman.init())", 0.0},
		{"kalman_config", "kalman.state(kalman.init({processNoise:0.2}))", 0.0},
		{"kalman_reset", "kalman.state(kalman.reset(kalman.init()))", 0.0},
		{"apoc_map_pairs", "apoc.map.fromPairs([['key',r.key]])", map[string]interface{}{"key": "hit"}},
		{"apoc_map_merge", "apoc.map.merge({key:r.key},{number:" + count + "})", map[string]interface{}{"key": "hit", "number": int64(1)}},
		{"apoc_map_set", "apoc.map.setKey({key:r.key},'number'," + count + ")", map[string]interface{}{"key": "hit", "number": int64(1)}},
		{"apoc_map_clean", "apoc.map.clean({key:r.key,drop:1},['drop'],[])", map[string]interface{}{"key": "hit"}},
		{"apoc_join", "apoc.text.join(" + collected + ",'|')", "hit"},
		{"apoc_flatten", "apoc.coll.flatten([" + collected + "],true)", []interface{}{"hit"}},
		{"nested_map", "{answer:head(" + collected + "), count:" + count + "}", map[string]interface{}{"answer": "hit", "count": int64(1)}},
		{"nested_case", "CASE WHEN " + count + "=1 THEN head(" + collected + ") ELSE 'wrong' END", "hit"},
		{"null_function", "sin(null)", nil},
	} {
		for _, mode := range []string{"execute", "profile", "call", "function"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				expression := tc.expression
				if mode == "function" {
					expression = "inspection.coverageidentity(" + expression + ")"
				}
				query := "UNWIND [1,2] AS pass MATCH (a:CoverageNode)-[r:COVERAGE_EDGE {key:$key}]->(b) RETURN " + expression + " AS value"
				if mode == "profile" {
					query = "PROFILE " + query
				}
				if mode == "call" {
					query = "CALL { " + query + " } RETURN value"
				}
				result, err := exec.Execute(context.Background(), query, map[string]interface{}{"key": "hit"})
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{tc.want}, {tc.want}}, result.Rows, query)
			})
		}
	}
	for _, tc := range []struct {
		expression string
		want       interface{}
	}{
		{"length(p)", int64(1)},
		{"nodes(p)[0].name", "a"},
		{"relationships(p)[0].key", "hit"},
		{"startNode(head(relationships(p))).name", "a"},
		{"endNode(head(relationships(p))).name", "b"},
	} {
		t.Run("path/"+tc.expression, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), "MATCH p=(a:CoverageNode {name:'a'})-[r:COVERAGE_EDGE {key:$key}]->(b) RETURN "+tc.expression+" AS value", map[string]interface{}{"key": "hit"})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
		})
	}
}
