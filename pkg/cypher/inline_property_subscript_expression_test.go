package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInlinePropertyMapSubscriptAndArithmeticValues pins property-map values
// that subscript a list by a row variable (['b0','b1'][i], $ids[i * 20 + 1],
// $ws[0..2][i]) in node and relationship patterns of MATCH, OPTIONAL MATCH,
// MERGE and CREATE. A '[' or '*' inside a property map is part of the value
// expression, not a relationship bracket or a variable-length marker. The
// expected results are Neo4j 5.26's.
func TestInlinePropertyMapSubscriptAndArithmeticValues(t *testing.T) {
	ids := make([]interface{}, 22)
	for index := range ids {
		ids[index] = "b" + strconv.Itoa(index)
	}
	params := map[string]interface{}{
		"ids": ids,
		"ws":  []interface{}{int64(0), int64(1), int64(0), int64(21)},
	}
	counts := []struct {
		name  string
		query string
		want  int64
	}{
		{"list literal subscript", "UNWIND range(0,1) AS i MATCH (n:R {id: ['b0','b1'][i]})-[l]->() RETURN count(*)", 1},
		{"parameter subscript with arithmetic", "UNWIND range(0,1) AS i MATCH (n:R {id: $ids[i * 20 + 1]})-[l]->() RETURN count(*)", 2},
		{"list literal subscript with multiplication", "UNWIND range(0,1) AS i MATCH (n:R {id: ['b0','b21'][i * 1]})-[l]->() RETURN count(*)", 1},
		{"list literal subscript with subtraction", "UNWIND range(0,1) AS i MATCH (n:R {id: ['b1','b0'][1 - i]})-[l]->() RETURN count(*)", 1},
		{"arithmetic in a call", "UNWIND range(0,1) AS i MATCH (n:R {id: 'b' + toString(i * 20 + 1)})-[l]->() RETURN count(*)", 2},
		{"single node", "UNWIND range(0,1) AS i MATCH (n:R {id: ['b0','b1'][i]}) RETURN count(*)", 2},
		{"target node", "UNWIND range(0,1) AS i MATCH (:X)<-[l]-(n:R {id: ['b0','b1'][i]}) RETURN count(*)", 1},
		{"optional match", "UNWIND range(0,1) AS i OPTIONAL MATCH (n:R {id: ['b0','b1'][i]})-[l]->() RETURN count(l)", 1},
		{"relationship list literal subscript", "UNWIND range(0,1) AS i MATCH (n:R)-[l:T {w: [5, 1][i]}]->() RETURN count(*)", 1},
		{"relationship parameter subscript with arithmetic", "UNWIND range(0,1) AS i MATCH (n:R)-[l:T {w: $ws[i * 2 + 1]}]->() RETURN count(type(l))", 2},
		{"relationship slice subscript", "UNWIND range(0,1) AS i MATCH (n:R)-[l:T {w: $ws[0..2][i]}]->() RETURN count(*)", 1},
		{"node slice subscript", "UNWIND range(0,1) AS i MATCH (n:R {id: $ids[0..2][i]})-[l]->() RETURN count(*)", 1},
		{"optional match node and relationship", "UNWIND range(0,1) AS i OPTIONAL MATCH (n:R {id: $ids[i * 20 + 1]})-[l:T {w: $ws[i * 2 + 1]}]->() RETURN count(l)", 2},
		{"node and relationship list literal subscripts", "UNWIND range(0,1) AS i MATCH (n:R {id: ['b0','b1'][i]})-[l:T {w: [5, 1][i]}]->() RETURN count(*)", 1},
		{"node inline where", "UNWIND range(0,1) AS i MATCH (n:R WHERE n.id = $ids[i * 20 + 1])-[l]->() RETURN count(*)", 2},
		{"relationship inline where", "UNWIND range(0,1) AS i MATCH (n:R)-[l:T WHERE l.w = $ws[i * 2 + 1]]->() RETURN count(type(l))", 2},
		{"variable-length relationship property map", "UNWIND range(0,1) AS i MATCH (n:R)-[l:T*1..1 {w: $ws[i * 2 + 1]}]->() RETURN count(size(l))", 2},
		{"unwind list literal subscript", "UNWIND [['b0','b1'],['x']][0] AS id MATCH (n:R {id: id})-->() RETURN count(*)", 1},
		{"where list literal subscript", "UNWIND range(0,1) AS i MATCH (n:R) WHERE n.id = ['b0','b1'][i] MATCH (n)-->() RETURN count(*)", 1},
	}
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			name := "scan"
			if indexed {
				name = "index"
			}
			if explicit {
				name += "/explicit transaction"
			} else {
				name += "/autocommit"
			}
			t.Run(name, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "inline_property_subscript"))
				ctx := context.Background()
				if indexed {
					_, err := exec.Execute(ctx, "CREATE INDEX r_id FOR (n:R) ON (n.id)", nil)
					require.NoError(t, err)
				}
				_, err := exec.Execute(ctx, "CREATE (:R {id:'b0'}), (:R {id:'b1'})-[:T {w: 1}]->(:X), (:R {id:'b21'})-[:T {w: 21}]->(:X)", nil)
				require.NoError(t, err)
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}

				for _, tc := range counts {
					t.Run(tc.name, func(t *testing.T) {
						result, err := exec.Execute(ctx, tc.query, params)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
					})
				}

				set, err := exec.Execute(ctx, "MATCH (n:R {id:'b0'}) SET n.tag = ['x','y'][1] RETURN n.tag", nil)
				if assert.NoError(t, err) {
					assert.Equal(t, [][]interface{}{{"y"}}, set.Rows)
				}

				_, err = exec.Execute(ctx, "UNWIND range(0,1) AS i CREATE (:Q {id: $ids[i * 20 + 1]})", params)
				assert.NoError(t, err)
				created, err := exec.Execute(ctx, "MATCH (q:Q) RETURN q.id ORDER BY q.id", nil)
				require.NoError(t, err)
				assert.Equal(t, [][]interface{}{{"b1"}, {"b21"}}, created.Rows)

				for _, query := range []string{
					"UNWIND range(0,1) AS i MERGE (n:R {id: ['b0','b1'][i]}) RETURN count(*)",
					"UNWIND range(0,1) AS i MERGE (n:R {id: $ids[i * 20 + 1]}) RETURN count(*)",
					"UNWIND range(0,1) AS i MATCH (n:R {id: $ids[i * 20 + 1]}) MERGE (n)-[l:T {w: $ws[i * 2 + 1]}]->(x:X) RETURN count(*)",
				} {
					result, err := exec.Execute(ctx, query, params)
					if assert.NoError(t, err, query) {
						assert.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, query)
					}
				}
				// MERGE matched the existing nodes and relationships.
				totals, err := exec.Execute(ctx, "MATCH (n:R) WITH count(n) AS r MATCH ()-[t:T]->() RETURN r, count(t)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(3), int64(2)}}, totals.Rows)

				if explicit {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

// TestRelationshipRangeReadersSkipPropertyMaps pins the pattern readers on
// their own: a '[', '*' or '..' in a node or relationship property map is no
// relationship range, and an unclosed relationship bracket stays invalid.
func TestRelationshipRangeReadersSkipPropertyMaps(t *testing.T) {
	for pattern, invalid := range map[string]bool{
		"(n {id: $ids[i * 20 + 1]})-[r]->()":  false,
		"(n)-[r:T {w: $ws[0..2][i]}]->()":     false,
		"(n)-[r:T*1..2 {w: $ws[i * 2]}]->()":  false,
		"(n)-[r:T {w: 'a*b'}]->()":            false,
		"(n)-[:T..]->()":                      true,
		"(n)-[:T*x]->()":                      true,
		"(n)-[r:T*1..2->()":                   true,
		"(n {k: ['a', 'b'][i]})-[r:T*2]->(m)": false,
	} {
		assert.Equal(t, invalid, invalidRelationshipPattern(pattern), pattern)
	}
	assert.Equal(t,
		map[string]struct{}{"s": {}},
		variableLengthRelationshipVariableSet("(n {k: [x * 2]})-[r:T {w: $ws[i * 2]}]->()-[s*2]->()"))
	open, end := readRelationshipBracket("(a)-[r->(b)", 0, requireClosedRelationship)
	assert.Equal(t, []int{-1, -1}, []int{open, end})
}

// TestDynamicRelationshipTypeExpressionKeepsItsRange pins a variable-length
// relationship whose dynamic type is an expression with a map literal, a
// subscript or arithmetic ($({k: t}.k), $(['PR', 'X'][i - 1])): the '{', '*'
// or '..' inside $(...) is part of the type expression, and the '*' after it
// still makes the variable a relationship list. The expected results are
// Neo4j 5.26's.
func TestDynamicRelationshipTypeExpressionKeepsItsRange(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_type_range"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {id: 'a'})-[:PR]->(:B)-[:PR]->(:C)", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"UNWIND ['PR'] AS t MATCH (a:A)-[r:$({k: t}.k)*1..2]->(b) RETURN size(r) AS hops ORDER BY hops", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"UNWIND ['PR'] AS t MATCH (a:A)-[r:$({k: t}.k)*1..2]->(b) RETURN r[0].x IS NULL AS first ORDER BY first", [][]interface{}{{true}, {true}}},
		{"UNWIND ['PR'] AS t MATCH (a:A)-[r:$({k: t}.k)]->(b) RETURN type(r) AS ty", [][]interface{}{{"PR"}}},
		{"UNWIND [1] AS i MATCH (a:A)-[r:$(['PR', 'X'][i - 1])*2]->(b) RETURN size(r) AS hops", [][]interface{}{{int64(2)}}},
		{"UNWIND [['PR']] AS ts MATCH (a:A)-[r:$(ts[0..1][0])]->(b) RETURN type(r) AS ty", [][]interface{}{{"PR"}}},
		{"UNWIND [1] AS i MATCH (a:A)-[r:$(['X', 'PR'][i * 1])]->(b) RETURN type(r) AS ty", [][]interface{}{{"PR"}}},
		{"UNWIND [1] AS i MATCH (a:A)-[r:$(['X', 'PR'][i * 1])*2]->(b) RETURN size(r) AS hops", [][]interface{}{{int64(2)}}},
		{"UNWIND ['PR'] AS t MATCH p = (a:A)-[r:$({k: t}.k)*1..2]->(b) RETURN [x IN r | type(x)] AS ts ORDER BY size(ts)", [][]interface{}{{[]interface{}{"PR"}}, {[]interface{}{"PR", "PR"}}}},
		{"UNWIND ['PR'] AS t MATCH (a:A) OPTIONAL MATCH (a)-[r:$({k: t}.k)]->(b) RETURN type(r) AS ty", [][]interface{}{{"PR"}}},
		{"UNWIND ['PR'] AS t MATCH (a:A) OPTIONAL MATCH (a)-[r:$({k: t}.k)*1..2]->(b) RETURN size(r) AS hops ORDER BY hops", [][]interface{}{{int64(1)}, {int64(2)}}},
		{"UNWIND [1] AS i MATCH (a:A) OPTIONAL MATCH (a)-[r:$(['X', 'PR'][i * 1])]->(b) RETURN type(r) AS ty", [][]interface{}{{"PR"}}},
		{"UNWIND [1] AS i MATCH (a:A) OPTIONAL MATCH (a)-[r:$(['X', 'PR'][i * 1])*2]->(b) RETURN size(r) AS hops", [][]interface{}{{int64(2)}}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		if assert.NoError(t, err, tc.query) {
			assert.Equal(t, tc.want, result.Rows, tc.query)
		}
	}

	for pattern, invalid := range map[string]bool{
		"(a)-[r:$({k: t}.k)*1..2]->(b)":     false,
		"(a)-[r:$(t * 2)]->(b)":             false,
		"(a)-[r:$(['A', 'B'][0..1])]->(b)":  false,
		"(a)-[r:$({k: t}.k)*x]->(b)":        true,
		"(a)-[r:$({k: t}.k) {w: 1}]->(b)":   false,
		"(a)-[r:$({k: t}.k)*2 {w: 1}]->(b)": false,
	} {
		assert.Equal(t, invalid, invalidRelationshipPattern(pattern), pattern)
	}
	assert.Equal(t,
		map[string]struct{}{"r": {}},
		variableLengthRelationshipVariableSet("(a)-[r:$({k: t}.k)*1..2]->(b)-[s:$(t * 2)]->(c)"))
}

// TestDynamicRelationshipTypeExpressionInWrites pins CREATE and MERGE (with
// ON CREATE / ON MATCH) of a relationship whose dynamic type holds a '*', a
// map literal or a slice ($(['X', 'DZ'][i * 1]), $({k: t}.k),
// $(ts[0..1][0])): the relationship declaration reader takes none of them for
// a variable length or the property map. The expected results are Neo4j
// 5.26.30's.
func TestDynamicRelationshipTypeExpressionInWrites(t *testing.T) {
	statements := []struct {
		query string
		want  [][]interface{}
	}{
		{"WITH 1 AS i CREATE ()-[r:$(['X','DZ'][i * 1])]->() RETURN type(r) AS t", [][]interface{}{{"DZ"}}},
		{"WITH 'DZ' AS t CREATE (:W)-[r:$({k: t}.k)]->(:W) RETURN type(r) AS t", [][]interface{}{{"DZ"}}},
		{"WITH ['DZ', 'X'] AS ts CREATE (:W)-[r:$(ts[0..1][0])]->(:W) RETURN type(r) AS t", [][]interface{}{{"DZ"}}},
		{"WITH 'DZ' AS t CREATE (:W)-[r:$({k: t}.k) {w: 1}]->(:W) RETURN type(r) AS t, r.w AS w", [][]interface{}{{"DZ", int64(1)}}},
		{"WITH 1 AS i MERGE (a:M {id: 1})-[r:$(['X','DM'][i * 1]) {w: 1}]->(b:M {id: 2}) ON CREATE SET r.c = 'created' ON MATCH SET r.m = 'matched' RETURN type(r) AS t, r.c AS c, r.m AS m", [][]interface{}{{"DM", "created", nil}}},
		{"WITH 'DM' AS t MERGE (a:M {id: 1})-[r:$({k: t}.k) {w: 1}]->(b:M {id: 2}) ON CREATE SET r.c = 'again' ON MATCH SET r.m = 'matched' RETURN type(r) AS t, r.c AS c, r.m AS m", [][]interface{}{{"DM", "created", "matched"}}},
		{"WITH ['DM'] AS ts MERGE (a:M {id: 1})-[r:$(ts[0..1][0])]->(b:M {id: 2}) RETURN type(r) AS t, r.m AS m", [][]interface{}{{"DM", "matched"}}},
		{"MATCH (a:M {id: 1}), (b:M {id: 2}) WITH a, b, 1 AS i MERGE (a)-[r:$(['X','DM'][i * 1])]->(b) ON MATCH SET r.n = 'bound' RETURN type(r) AS t, r.n AS n", [][]interface{}{{"DM", "bound"}}},
		{"MATCH (:M)-[r]->(:M) RETURN count(r) AS rels", [][]interface{}{{int64(1)}}},
		{"MATCH ()-[r:DZ]->() RETURN count(r) AS dz", [][]interface{}{{int64(4)}}},
		// UNWIND ... MERGE between bound nodes, and to a new node.
		{"UNWIND ['DU', 'DU'] AS t MATCH (a:M {id: 1}), (b:M {id: 2}) MERGE (a)-[r:$({k: t}.k)]->(b) RETURN type(r) AS t", [][]interface{}{{"DU"}, {"DU"}}},
		{"UNWIND [1, 1] AS i MATCH (a:M {id: 1}), (b:M {id: 2}) MERGE (a)-[r:$(['X', 'DV'][i * 1]) {w: i}]->(b) ON CREATE SET r.c = 1 ON MATCH SET r.c = r.c + 1 RETURN type(r) AS t", [][]interface{}{{"DV"}, {"DV"}}},
		{"MATCH ()-[r:DV]->() RETURN r.c AS c", [][]interface{}{{int64(2)}}},
		{"UNWIND ['DW'] AS t MATCH (a:M {id: 1}) MERGE (a)-[r:$({k: t}.k)]->(b:M {id: 3}) RETURN type(r) AS t", [][]interface{}{{"DW"}}},
		{"MATCH (:M)-[r]->(:M) RETURN type(r) AS t, count(r) AS n ORDER BY t", [][]interface{}{{"DM", int64(1)}, {"DU", int64(1)}, {"DV", int64(1)}, {"DW", int64(1)}}},
	}
	for _, explicit := range []bool{false, true} {
		name := "autocommit"
		if explicit {
			name = "explicit transaction"
		}
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_type_writes"))
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			for _, tc := range statements {
				result, err := exec.Execute(ctx, tc.query, nil)
				if assert.NoError(t, err, tc.query) {
					assert.Equal(t, tc.want, result.Rows, tc.query)
				}
			}
			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}

	type declared struct {
		variable, typeText, length, properties string
		hasLength                              bool
	}
	for inner, want := range map[string]declared{
		"r:$(['X','DZ'][i * 1])":     {variable: "r", typeText: "$(['X','DZ'][i * 1])"},
		"r:$({k: t}.k)":              {variable: "r", typeText: "$({k: t}.k)"},
		"r:$(ts[0..1][0])":           {variable: "r", typeText: "$(ts[0..1][0])"},
		"r:$({k: t}.k) {w: 1}":       {variable: "r", typeText: "$({k: t}.k)", properties: "{w: 1}"},
		"r:$({k: t}.k)*1..2":         {variable: "r", typeText: "$({k: t}.k)", hasLength: true, length: "1..2"},
		"r:$(t * 2)*2 {w: $ws[i*2]}": {variable: "r", typeText: "$(t * 2)", hasLength: true, length: "2", properties: "{w: $ws[i*2]}"},
	} {
		got := relationshipDeclarationOf(inner)
		assert.Equal(t, want, declared{got.variable, got.typeText, got.length, got.properties, got.hasLength}, inner)
	}
}
