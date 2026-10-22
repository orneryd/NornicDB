package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MATCH pattern's property map may read a variable the same pattern
// declares ((a)-->(b {foo: a.foo})): the entry is the equality b.foo = a.foo.
// The rows are Neo4j 5.26.30's (#907, #509). A property key or a string
// that spells a variable's name isn't a read of it.
func TestMatchMapReadsSamePattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "same_pattern_maps"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:MW {foo: 1, a: 9})-[:MT {w: 1}]->(:MW {foo: 1, k: 'a'}), (:MW {foo: 2})-[:MT {w: 5}]->(:MW {foo: 3})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:MW)-[:MT]->(b:MW {foo: a.foo}) RETURN a.foo AS a, b.foo AS b":         {{int64(1), int64(1)}},
		"MATCH (a:MW)-[r:MT {w: a.foo}]->(b) RETURN r.w AS w":                           {{int64(1)}},
		"MATCH (a:MW)-[:MT]->(:MW {foo: a.foo}) RETURN count(*) AS c":                   {{int64(1)}},
		"MATCH (a:MW {a: 9})-[:MT]->(b:MW {k: 'a'}) RETURN count(*) AS c":               {{int64(1)}},
		"MATCH (a:MW)-[:MT]->(b:MW {foo: a.foo + 1}) RETURN a.foo AS a":                 {{int64(2)}},
		"MATCH (a:MW)-[:MT]->(b:MW {foo: a.foo, k: 'a'}) RETURN b.k AS k":               {{"a"}},
		"MATCH (a:MW)-[:MT]->(b:MW {foo: $v}) RETURN b.foo AS b":                        {{int64(3)}},
		"OPTIONAL MATCH (a:MW {foo: 2})-[:MT]->(b:MW {foo: a.foo}) RETURN a.foo, b.foo": {{nil, nil}},
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"v": int64(3)})
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	require.False(t, mayReadPatternInPropertyMap("MATCH (n {k: 1}) RETURN {a: n.k}"))
	require.True(t, mayReadPatternInPropertyMap("MATCH (a)-->(b {k: a.k}) RETURN b"))
	require.Nil(t, declaredPatternVariables("(:L {k: 1})"))
}

// A MERGE of a relationship pattern with an endpoint no earlier clause binds
// matches every existing path (each a row, ON MATCH applied to each) or
// creates the whole pattern; a bound endpoint is reused. Rows and counters
// are Neo4j 5.26.30's (#509).
func TestMergeWholePathMatchesEveryPath(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_whole_path"))
	ctx := context.Background()
	run := func(query string) *ExecuteResult {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result
	}
	created := run("MERGE (:SM {id: 1})-[:R]->(:SN {id: 1})")
	require.Equal(t, 2, created.Stats.NodesCreated)
	created = run("MERGE (:SM {id: 1})-[:R {w: 1}]->(:SN {id: 1})")
	require.Equal(t, 2, created.Stats.NodesCreated, "the whole pattern is created; no node is reused")
	require.Equal(t, 1, created.Stats.RelationshipsCreated)

	matched := run("MERGE (a:SM {id: 1})-[r:R]->(b:SN {id: 1}) ON MATCH SET r.seen = true RETURN properties(r) AS p ORDER BY p.w")
	require.Equal(t, [][]interface{}{
		{map[string]interface{}{"w": int64(1), "seen": true}},
		{map[string]interface{}{"seen": true}},
	}, matched.Rows)
	require.Equal(t, 2, matched.Stats.PropertiesSet)

	bound := run("MATCH (a:SM {id: 1}) WITH a LIMIT 1 MERGE (a)-[r:R2]->(b:SN {id: 2}) RETURN b.id AS b")
	require.Equal(t, [][]interface{}{{int64(2)}}, bound.Rows)
	require.Equal(t, 1, bound.Stats.NodesCreated)
	again := run("MATCH (a:SM {id: 1}) WITH a LIMIT 1 MERGE (a)-[r:R2]->(b:SN {id: 2}) RETURN b.id AS b")
	require.Equal(t, [][]interface{}{{int64(2)}}, again.Rows)
	require.Zero(t, again.Stats.NodesCreated)

	loop := run("MERGE (a:SL {id: 10})-[:R]->(a) ON CREATE SET a.created = 1 RETURN a.created AS c")
	require.Equal(t, [][]interface{}{{int64(1)}}, loop.Rows)
	loop = run("MERGE (a:SL {id: 10})-[:R]->(a) ON CREATE SET a.created = 2 ON MATCH SET a.matched = 1 RETURN a.created AS c, a.matched AS m")
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}}, loop.Rows)

	require.Equal(t, [][]interface{}{{"SL", int64(1)}, {"SM", int64(2)}, {"SN", int64(3)}},
		run("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c ORDER BY l").Rows)
	require.Equal(t, [][]interface{}{{"R", int64(3)}, {"R2", int64(1)}},
		run("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY t").Rows)

	// The relationship route takes only bound endpoints.
	fixtureAdmission1392Context, fixtureAdmission1392 := traversalTestContext(ctx, "(a)-[:R]->(b)", "", nil)
	_ = fixtureAdmission1392Context
	_, err := exec.executeMergeRelationshipWithContext(fixtureAdmission1392, fixtureAdmission1392Context, "(a)-[:R]->(b)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.Error(t, err)
	fixtureAdmission1393Context, fixtureAdmission1393 := traversalTestContext(ctx, "(a)-[:R]->(b)", "", nil)
	_ = fixtureAdmission1393Context
	_, err = exec.executeMergeRelationshipWithContext(fixtureAdmission1393, fixtureAdmission1393Context, "(a)-[:R]->(b)", map[string]*storage.Node{"a": {ID: "x"}}, map[string]*storage.Edge{})
	require.Error(t, err)
}
