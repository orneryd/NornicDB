package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Expected values were checked against Neo4j 5.26.30. The decoys and the
// second MERGE distinguish a missing predicate from an incorrectly read value.
func TestInlineNestedMapPropertyExpressions(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, entity := range []string{"node", "relationship"} {
			for _, clause := range []string{"CREATE", "MERGE", "MATCH", "OPTIONAL MATCH"} {
				t.Run(entity+"/"+clause+map[bool]string{false: "/autocommit", true: "/transaction"}[explicit], func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested_inline"))
					ctx := context.Background()
					pattern := func(value string) string {
						if entity == "node" {
							return "(n:Nested {k: " + value + "})"
						}
						return "(:NA)-[n:Nested {k: " + value + "}]->(:NB)"
					}
					if clause != "CREATE" {
						seed := "CREATE (:Nested {k:17}), (:Nested {k:999})"
						if entity == "relationship" {
							seed = "CREATE (:NA)-[:Nested {k:17}]->(:NB), (:NA)-[:Nested {k:999}]->(:NB)"
						}
						_, err := exec.Execute(ctx, seed, nil)
						require.NoError(t, err)
					}
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					query := "WITH 1 AS i " + clause + " " + pattern("{outer:{xs:[11,17]}}.outer.xs[i]") + " RETURN n.k"
					for repeat := 0; repeat < 3; repeat++ {
						result, err := exec.Execute(ctx, query, nil)
						if assert.NoError(t, err, query) {
							assert.Equal(t, [][]interface{}{{int64(17)}}, result.Rows)
						}
					}
					if clause == "MERGE" {
						result, err := exec.Execute(ctx, "MATCH "+pattern("17")+" RETURN count(n)", nil)
						require.NoError(t, err)
						assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					}
					if explicit {
						_, err := exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestNestedMapPropertyExpressionEvaluators(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, expression := range []string{
		"{outer:{xs:[11,17]}}.outer.xs[i]",
		"[{xs:[11,17]}][0].xs[i]",
		"head([{xs:[11,17]}]).xs[i]",
		"({outer:{xs:[11,17]}}).outer.xs[i]",
	} {
		text, textOK, textErr := exec.evaluateRowValueText(expression, map[string]interface{}{"i": int64(1)})
		compiled, _ := (&rowValueCompiler{}).compile(expression)
		value, ok, err := compiled.eval(exec, map[string]interface{}{"i": int64(1)}, nil)
		assert.NoError(t, textErr, expression)
		assert.NoError(t, err, expression)
		assert.True(t, textOK, expression)
		assert.True(t, ok, expression)
		assert.Equal(t, int64(17), text, expression)
		assert.Equal(t, int64(17), value, expression)
	}
}

// The dynamic forms must decline the static UNWIND chain optimization and
// retain MERGE identity, optional traversal and clause-by-clause visibility.
func TestRelationshipReaderExecutionProbes(t *testing.T) {
	for _, expression := range []string{
		"{ts:['R*{..|:quote']}.ts[0..1][0]",
		"['X','R*{..|:quote'][i * 1]",
		"{outer:{ts:['R*{..|:quote']}}.outer.ts[0]",
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(expression+map[bool]string{false: "/autocommit", true: "/transaction"}[explicit], func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reader_probes"))
				ctx := context.Background()
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				query := "UNWIND [1,1] AS i MERGE (a:RR {id:1}) MERGE (b:RR {id:2}) MERGE (a)-[r:$(" + expression + ") {w:i}]->(b) ON CREATE SET r.n=1 ON MATCH SET r.n=r.n+1 RETURN type(r)"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				assert.Equal(t, [][]interface{}{{"R*{..|:quote"}, {"R*{..|:quote"}}, result.Rows)
				result, err = exec.Execute(ctx, "MATCH (:RR)-[r]->(:RR) RETURN count(r), sum(r.n)", nil)
				require.NoError(t, err)
				assert.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
				query = "WITH 1 AS i MATCH (a:RR {id:1}) OPTIONAL MATCH (a)-[r:$(" + expression + ")*1..2 {w:i}]->(b) RETURN size(r)"
				for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
					result, err = exec.Execute(ctx, prefix+query, nil)
					require.NoError(t, err, prefix+query)
					if prefix == "EXPLAIN " {
						assert.Empty(t, result.Rows)
					} else {
						assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					}
				}
				query = "UNWIND [1,2] AS k WITH k, 1 AS i CREATE (:WA)-[:$(" + expression + ") {w:k}]->(:WB) WITH k MATCH (:WA)-[r:`R*{..|:quote`]->(:WB) RETURN k, count(r) ORDER BY k"
				result, err = exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				assert.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}}, result.Rows)
				if explicit {
					_, err := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestDynamicCaseRelationshipTypes(t *testing.T) {
	for _, clause := range []string{"CREATE", "MERGE", "MATCH", "OPTIONAL MATCH"} {
		for _, explicit := range []bool{false, true} {
			t.Run(clause+map[bool]string{false: "/autocommit", true: "/transaction"}[explicit], func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_type"))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:DA)-[:DY {k:17}]->(:DB)-[:DY {k:17}]->(:DC)", nil)
				require.NoError(t, err)
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				for _, expression := range []string{
					"CASE WHEN i * 1 = 1 THEN {ts:['DY']}.ts[0..1] ELSE ['X'] END",
					"CASE i WHEN 1 THEN 'DY' ELSE 'X' END",
				} {
					query := "WITH 1 AS i " + clause + " (a:DA)-[r:$(" + expression + ") {k:17}]->(b:DB) RETURN type(r)"
					result, err := exec.Execute(ctx, query, nil)
					if assert.NoError(t, err, query) {
						assert.Equal(t, [][]interface{}{{"DY"}}, result.Rows)
					}
					if clause == "MATCH" || clause == "OPTIONAL MATCH" {
						query = "WITH 1 AS i MATCH (a:DA) " + clause + " (a)-[r:$(" + expression + ")*1..2 {k:17}]->(b) RETURN count(*), sum(size(r))"
						result, err = exec.Execute(ctx, query, nil)
						if assert.NoError(t, err, query) {
							assert.Equal(t, [][]interface{}{{int64(2), int64(3)}}, result.Rows)
						}
					}
				}
				if explicit {
					_, err := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestDynamicCaseTokenSignatures(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_signatures"))
	for _, query := range []string{
		"WITH 1 AS i CREATE (n:$(CASE WHEN i=1 THEN ['CaseLabel'] ELSE ['Other'] END)) RETURN labels(n)",
		"WITH 1 AS i CREATE (n:Key) SET n[CASE WHEN i=1 THEN 'k' ELSE 'other' END]=17 RETURN n.k",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		if query[len(query)-1] == ')' {
			assert.Equal(t, [][]interface{}{{[]interface{}{"CaseLabel"}}}, result.Rows)
		} else {
			assert.Equal(t, [][]interface{}{{int64(17)}}, result.Rows)
		}
	}
	for _, expression := range []string{"123", "true", "[1]", "CASE WHEN i=1 THEN [1] ELSE [2] END"} {
		assert.Error(t, staticDynamicTokenError(expression, staticTypeScope{}, dynamicTokenLabel), expression)
	}
}

func TestProfileDynamicRelationshipWrites(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_types"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "PROFILE WITH 'P*{' AS t CREATE ()-[r:$(t)]->() RETURN type(r)", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{"P*{"}}, result.Rows)
	_, err = exec.Execute(ctx, "PROFILE UNWIND [1,0] AS divisor CREATE (n:ProfileAtomic) SET n.x=1/divisor RETURN n.x", nil)
	require.Error(t, err)
	result, err = exec.Execute(ctx, "MATCH (n:ProfileAtomic) RETURN count(n)", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestRelationshipAuxiliaryReaders(t *testing.T) {
	for _, typeText := range []string{"$({ts:['R*{..|:quote']}.ts[0..1][0])", "`R*{..|:quote`"} {
		pattern := "(a)-[r:" + typeText + "*1..2 {w: 17}]->(b)"
		assert.Equal(t, []string{"r"}, extractRelationshipVariables(pattern))
		var tokens stretchTokens
		addStretchPattern(pattern, &tokens, false)
		assert.True(t, tokens.anyRelationship)
		assert.Equal(t, map[string]struct{}{"w": {}}, tokens.keys)
	}
	assert.Equal(t, map[string][]string{"w": {"17"}}, appendPatternPropertyExpressions(nil, "(a)-[:`R{*..` {w: 17}]->(b)", "R{*.."))
	assert.Empty(t, appendPatternPropertyExpressions(nil, "(a)-[:$({R: 'X'}.R) {w: 17}]->(b)", "$(@"))
	_, ok := parseUnwindMergeRelationshipClause("MERGE (a)-[r:R*1..2]->(b)")
	assert.False(t, ok)
	for _, typeText := range []string{"$({k: row.t}.k)", "$(row.ts[0..1][0])", "$(['X','R'][row.i * 1])"} {
		_, ok := parseUnwindMergeRelationshipClause("MERGE (a)-[r:" + typeText + " {w: row.w}]->(b)")
		assert.False(t, ok, "dynamic types must use the row-aware executor")
	}
}
