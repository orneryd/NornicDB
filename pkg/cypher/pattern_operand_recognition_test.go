package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An AND / OR operand is a relationship pattern only when it is one whole
// relationship chain: a parenthesised expression whose subqueries hold
// patterns is evaluated as the expression it is. Recorded on Neo4j 5.26.30;
// the first two are from a user's report (reduced).
func TestPatternOperandIsAWholeChain(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pattern_operand"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:ZD {id: 'a'})-[:ZR {kind: 'same'}]->(b:ZD {id: 'b'})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:ZD {id: 'a'})-[r:ZR]->(b) WHERE false OR (NOT EXISTS { MATCH (h:ZPC)-[:C]->(x) } AND NOT EXISTS { MATCH (o:ZPC)-[:C]->(y) }) RETURN b.id AS v":                               {{"b"}},
		"MATCH (a:ZD {id: 'a'})-[r:ZR]->(b) WHERE type(r) <> 'ZR' OR (r.kind = 'same' AND NOT EXISTS { MATCH (h:ZPC)-[:C]->(x) } AND NOT EXISTS { MATCH (o:ZPC)-[:C]->(y) }) RETURN b.id AS v": {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR ((a)-[:ZR]->(b)) RETURN b.id AS v":                                                                                            {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR (a)-[:ZR]->(b) RETURN b.id AS v":                                                                                              {{"b"}},
		"MATCH (a:ZD {id: 'a'}), (b:ZD {id: 'b'}) WHERE false OR (b)-[:ZR]->(a) RETURN b.id AS v":                                                                                              {},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// NOT (NOT x) is x only when the inner NOT covers the whole parenthesised
// operand: NOT (NOT a OR b) negates the OR. Recorded on Neo4j 5.26.30.
func TestNotOfParenthesisedChainStartingWithNot(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "not_not_chain"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})-[:R]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"RETURN NOT (NOT false OR true) AS v":                                            {{false}},
		"RETURN NOT (NOT true AND false) AS v":                                           {{true}},
		"RETURN NOT (NOT true) AS v":                                                     {{true}},
		"RETURN NOT (NOT null OR false) AS v":                                            {{nil}},
		"WITH true AS t, false AS f RETURN NOT (NOT f OR t) AS v":                        {{false}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE NOT (NOT (a)<--() OR (a)-->()) RETURN a.id AS x": {},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE NOT ((a)-->() OR NOT (a)<--()) RETURN a.id AS x": {},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE NOT (NOT (a)<--()) RETURN a.id AS x":             {{int64(2)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// XOR binds between OR and AND in a compiled MATCH WHERE: a.id = 1 XOR true
// is (a.id = 1) XOR true. Recorded on Neo4j 5.26.30.
func TestMatchWhereXorPrecedence(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "where_xor"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})-[:R]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE a.id = 1 XOR true RETURN a.id AS x":                         {{int64(2)}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE false OR (a.id = 1 XOR true) RETURN a.id AS x":              {{int64(2)}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE a.id = 1 XOR b.id = 3 OR false RETURN a.id AS x ORDER BY x": {{int64(1)}, {int64(2)}},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE a.id = 1 XOR b.id = 2 OR false RETURN a.id AS x":            {},
		"MATCH (a:Q)-[r:R]->(b:Q) WHERE a.id = 1 XOR null RETURN a.id AS x":                         {},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
}

// XOR in a compiled WHERE, through both compilers: the graph-independent
// one (two bound values) and the executor's (an operand that is a
// relationship pattern). Null XOR anything is null, which filters the row.
func TestCompiledBindingWhereXor(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "compiled_xor")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(ctx, "CREATE (:XA {age: 30})-[:XR]->(:XB {age: 40})", nil)
	require.NoError(t, err)
	as, err := store.GetNodesByLabel("XA")
	require.NoError(t, err)
	bs, err := store.GetNodesByLabel("XB")
	require.NoError(t, err)
	require.Len(t, as, 1)
	require.Len(t, bs, 1)
	bind := binding{"a": as[0], "b": bs[0]}

	for clause, want := range map[string]bool{
		"a.age = 30 XOR b.age = 30": true,
		"a.age = 30 XOR b.age = 40": false,
		"a.age = 31 XOR b.age = 41": false,
		"a.age = 30 XOR b.none = 1": false,
	} {
		truth, ok := exec.getCompiledBindingWhereTruthIfSupported(ctx, clause)
		require.True(t, ok, clause)
		require.Equal(t, want, truth(bind, nil) == truthTrue, clause)
	}
	truth, ok := exec.getCompiledBindingWhereTruthIfSupported(ctx, "a.age = 30 XOR b.none = 1")
	require.True(t, ok)
	require.Equal(t, truthUnknown, truth(bind, nil))
	for clause, want := range map[string]bool{
		"a.age = 30 XOR (a)-[:XR]->(b)": false,
		"a.age = 31 XOR (a)-[:XR]->(b)": true,
		"a.age = 31 XOR (b)-[:XR]->(a)": false,
	} {
		predicate, ok := exec.tryCompileExecutorBindingWhere(ctx, clause)
		require.True(t, ok, clause)
		require.Equal(t, want, predicate(bind, nil), clause)
	}
	// An XOR whose operand neither compiler supports isn't compiled; the
	// generic evaluator answers it.
	_, ok = exec.getCompiledBindingWhereTruthIfSupported(ctx, "a.age = 30 XOR (")
	require.False(t, ok)
	_, ok = exec.tryCompileExecutorBindingWhere(ctx, "a.age = 30 XOR (")
	require.False(t, ok)
}

func TestStartsWithRelationshipFragment(t *testing.T) {
	for text, want := range map[string]bool{
		"": false, "->()": true, ">(b)": true, "<--(a)": true, "-[r]->(b)": true, "--(b)": true, "-1": false, "a.x": false,
	} {
		require.Equal(t, want, startsWithRelationshipFragment(text), text)
	}
}
