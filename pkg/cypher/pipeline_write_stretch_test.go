package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A clause reads what every row of an earlier clause wrote, and nothing a
// later row will write, as in Neo4j 5.26.30 (#907).
func TestWriteStretchReadsAreClauseByClause(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "write_stretch"))
	ctx := context.Background()
	inRolledBackTransaction := func(query string) [][]interface{} {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return result.Rows
	}
	_, err := exec.Execute(ctx, "CREATE (:A {n: 1}), (:Done {flag: false})", nil)
	require.NoError(t, err)

	require.Equal(t, [][]interface{}{{int64(1), int64(3)}, {int64(2), int64(3)}, {int64(3), int64(3)}},
		inRolledBackTransaction("UNWIND [1, 2, 3] AS k CREATE (w:W {k: k}) WITH w MATCH (v:W) RETURN w.k AS k, count(v) AS c ORDER BY k"))
	// Every row's MATCH runs before any row's CREATE: 2 rows, 2 nodes made.
	require.Equal(t, [][]interface{}{{int64(2)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (a:A) CREATE (:A) RETURN count(*) AS c"))
	require.Equal(t, [][]interface{}{{int64(3)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (a:A) CREATE (:A) WITH 1 AS one MATCH (x:A) RETURN count(DISTINCT x) AS c"))
	// A property a later clause matches on: every row sees every SET.
	require.Equal(t, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}},
		inRolledBackTransaction("UNWIND [1, 2] AS k MATCH (d:Done) SET d.flag = (k = 2) WITH k MATCH (x:Done {flag: true}) RETURN k, count(x) AS c ORDER BY k"))
	// MERGE sees the rows before it within its own clause.
	require.Equal(t, [][]interface{}{{int64(1)}},
		inRolledBackTransaction("UNWIND [1, 1, 1] AS k MERGE (m:M {k: k}) WITH DISTINCT m RETURN count(m) AS c"))
}

// Reads the conflict check must see however they are written: a label test
// or labels() in WHERE, a pattern in EXISTS, a quoted label or key, a
// subscript, a map projection, and a RETURN after the stretch (Neo4j
// 5.26.30, #907).
func TestWriteStretchSeesEveryReadForm(t *testing.T) {
	for _, testCase := range []struct {
		setup, query string
		rows         [][]interface{}
	}{
		{"CREATE (:EA)", "UNWIND [1, 2] AS k MATCH (a:EA) WHERE NOT a:EB SET a:EB RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"CREATE (:EA)", "UNWIND [1, 2] AS k MATCH (a:EA) WITH a, k WHERE size(labels(a)) = 1 SET a:EB RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"CREATE (:EA)", "UNWIND [1, 2] AS k MATCH (a:EA) WHERE NOT EXISTS { (:EA)-[:ER]->() } CREATE (a)-[:ER]->(:EB) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
		{"CREATE (:EA {k: 1}), (:EA {k: 2})", "UNWIND [1, 2] AS k MATCH (a:EA {k: k}) SET a:`EX Y` WITH k MATCH (m:`EX Y`) RETURN k, count(m) AS c ORDER BY k", [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}}},
		{"CREATE (:EDone {flag: false})", "UNWIND [1, 2] AS k MATCH (d:EDone) SET d.flag = (k = 2) WITH k MATCH (x:EDone) WHERE x.`flag` = true RETURN k, count(x) AS c ORDER BY k", [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}},
		{"CREATE (:EDone {flag: false})", "UNWIND [1, 2] AS k MATCH (d:EDone) SET d.flag = (k = 2) WITH k MATCH (x:EDone) WHERE x['flag'] = true RETURN k, count(x) AS c ORDER BY k", [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}},
		{"CREATE (:EDone {flag: false})", "UNWIND [1, 2] AS k MATCH (d:EDone) SET d.flag = (k = 2) WITH k MATCH (x:EDone) RETURN k, x {.flag} AS v ORDER BY k", [][]interface{}{{int64(1), map[string]interface{}{"flag": true}}, {int64(2), map[string]interface{}{"flag": true}}}},
		{"CREATE (:EA {k: 1}), (:EA {k: 2})", "UNWIND [1, 2] AS k MATCH (a:EA {k: k}) SET a.v = k WITH k MATCH (m:EA) WHERE m.v IS NOT NULL RETURN k, count(m) AS c ORDER BY k", [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "write_stretch_reads"))
			_, err := exec.Execute(context.Background(), testCase.setup, nil)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
}

func TestStretchReadAnalysis(t *testing.T) {
	var tokens stretchTokens
	addStretchExpressionReads("WHERE a:B:`C D` AND x.k = 1 AND m {k: 1}.k = 1 AND 'n:Q' = s", &tokens)
	require.Contains(t, tokens.labels, "B")
	require.Contains(t, tokens.labels, "C D")
	require.NotContains(t, tokens.labels, "Q")
	require.Contains(t, tokens.keys, "k")
	require.False(t, tokens.anyNode)

	for _, text := range []string{"labels(a) = []", "(a)-[:R]->()", "a:A|B", "a:$(x)", "COUNT { (a) } > 0"} {
		var any stretchTokens
		addStretchExpressionReads(text, &any)
		require.True(t, any.anyNode, text)
	}
	for _, text := range []string{"x.`k`", "x . k", "x['k']", "x[$p]", "x {.k}", "keys(x)", "x.*"} {
		var any stretchTokens
		addStretchPropertyReads(text, &any)
		require.True(t, any.anyKey, text)
	}
	var numbers stretchTokens
	addStretchPropertyReads("1.5 + x.k + [1..2]", &numbers)
	require.False(t, numbers.anyKey)
	require.Equal(t, map[string]struct{}{"k": {}}, numbers.keys)

	var chain stretchTokens
	addStretchLabelChain(":", &chain)
	require.True(t, chain.anyNode)

	var types stretchTokens
	addStretchPattern("(a)-[:`R S`]->(b)", &types, false)
	require.True(t, types.anyRelationship)

	// A read-only procedure only reads; an unknown one may write.
	var reads, writes stretchTokens
	analyzeStretchClause(pipelineClause{kind: pipelineClauseCall, text: "CALL db.labels() YIELD label"}, &reads, &writes)
	require.True(t, reads.everything)
	require.False(t, writes.everything)
	reads, writes = stretchTokens{}, stretchTokens{}
	analyzeStretchClause(pipelineClause{kind: pipelineClauseCall, text: "CALL no.such() YIELD x"}, &reads, &writes)
	require.True(t, writes.everything)

	// A RETURN after the stretch that reads what it writes.
	clauses, ok, _ := parsePipelineClauses("MATCH (d:Done) SET d.flag = true RETURN d.flag")
	require.True(t, ok)
	require.True(t, pipelineWriteStretchConflicts(clauses[:2], clauses[2:]))
	require.False(t, pipelineWriteStretchConflicts(clauses[:2], []pipelineClause{{kind: pipelineClauseReturn, text: "RETURN count(*)"}}))
}

func TestPipelineWriteStretchConflicts(t *testing.T) {
	conflicts := func(query string) bool {
		clauses, ok, _ := parsePipelineClauses(query)
		require.True(t, ok, query)
		return pipelineWriteStretchConflicts(clauses, nil)
	}
	// Bulk-load shapes stay row by row: nothing read is written.
	for _, query := range []string{
		"MATCH (a:Person {id: row.a}) MATCH (b:Person {id: row.b}) CREATE (a)-[:KNOWS]->(b)",
		"MERGE (n:Person {id: row.id}) SET n.name = row.name, n.age = row.age",
		"CREATE (n:Doc {id: row.id}) SET n.title = row.title",
		"MATCH (a:Person {id: row.a}) CREATE (a)-[:WROTE]->(:Post {title: row.t})",
	} {
		require.False(t, conflicts(query), query)
	}
	for _, query := range []string{
		"CREATE (w:W {k: k}) WITH w MATCH (v:W)",
		"MATCH (a:A) CREATE (:A)",
		"MATCH (a) CREATE (:B)",
		"MATCH (a)-[:R]->(b) CREATE (b)-[:R]->(:C)",
		"MATCH ()-[r]->() CREATE (:C)-[:T]->(:D)",
		"MATCH (n:Done {flag: false}) SET n.flag = true WITH n MATCH (x {flag: true})",
		"MERGE (n:Person {id: row.id}) SET n += row.props",
		"MATCH (n:A) SET n:B WITH n MATCH (m:B)",
		"MATCH (n:A) REMOVE n:A WITH n MATCH (m:A)",
		"MATCH (n:A) REMOVE n.k WITH n MATCH (m {k: 1})",
		"MATCH (n:A) DELETE n",
		"MATCH (n:A) SET n:$(row.label) WITH n MATCH (m:B)",
		"MERGE (a:P {id: row.a}) MERGE (b:P {id: row.b})",
		"MATCH (n:A) SET n.k = 1 WITH n, properties(n) AS p",
	} {
		require.True(t, conflicts(query), query)
	}
}

func TestStretchAnalysisEdgeBranches(t *testing.T) {
	var tokens stretchTokens
	tokens.add(&tokens.labels, "")
	require.Nil(t, tokens.labels)

	// A REMOVE item that is neither a property nor a label stands for any write.
	var reads, writes stretchTokens
	analyzeStretchClause(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n"}, &reads, &writes)
	require.True(t, writes.everything)

	// A variable-length relationship reads its types without the length.
	var pattern stretchTokens
	addStretchPattern("(a)-[:R*1..3]->(b)", &pattern, false)
	require.Contains(t, pattern.types, "R")

	// A map that doesn't close names no keys.
	var keys stretchTokens
	addStretchMapKeys("{k: 1", &keys)
	require.Nil(t, keys.keys)
}
