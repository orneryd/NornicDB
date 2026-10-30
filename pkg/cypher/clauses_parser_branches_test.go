package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseUnwindSimpleAssignments_Branches(t *testing.T) {
	assignments, ok := parseUnwindSimpleMergeMatchAssignments(expressionBraceView("{id: row.id, name: row.name}"))
	require.True(t, ok)
	require.Len(t, assignments, 2)
	require.Equal(t, "id", assignments[0].prop)
	require.Equal(t, "row.id", assignments[0].expr)

	_, ok = parseUnwindSimpleMergeMatchAssignments(expressionBraceView("{}"))
	require.False(t, ok)
	_, ok = parseUnwindSimpleMergeMatchAssignments(expressionBraceView("{id}"))
	require.False(t, ok)

	setAssignments, ok := parseUnwindSimpleSetAssignments("n.name = row.name, n += row", "n")
	require.True(t, ok)
	require.Len(t, setAssignments, 2)
	require.Equal(t, "name", setAssignments[0].prop)
	require.Equal(t, "row.name", setAssignments[0].expr)
	require.True(t, setAssignments[1].mergeMap)
	require.Equal(t, "row", setAssignments[1].expr)

	setAssignments, ok = parseUnwindSimpleSetAssignments("", "n")
	require.True(t, ok)
	require.Nil(t, setAssignments)

	_, ok = parseUnwindSimpleSetAssignments("m.name = row.name", "n")
	require.False(t, ok)
	_, ok = parseUnwindSimpleSetAssignments("n = row", "n")
	require.False(t, ok)
	_, ok = parseUnwindSimpleSetAssignments("n +=", "n")
	require.False(t, ok)

	_, ok = parseUnwindSimpleMergeMatchAssignments(expressionBraceView("{id row.id}"))
	require.False(t, ok)

	setAssign, ok := parseUnwindSimpleSetAssignments("n.k = row.k, n += row.props", "n")
	require.True(t, ok)
	require.Len(t, setAssign, 2)
	require.Equal(t, "k", setAssign[0].prop)
	require.True(t, setAssign[1].mergeMap)

	_, ok = parseUnwindSimpleSetAssignments("m.k = row.k", "n")
	require.False(t, ok)
	_, ok = parseUnwindSimpleSetAssignments("n +=", "n")
	require.False(t, ok)

	varName, labels, matchAssign, anyLabel, ok := parseUnwindNodePatternClauseInternal(originalBraceView("MATCH (n:LabelA|LabelB {id: row.id})"), "MATCH", true)
	require.True(t, ok)
	require.Equal(t, "n", varName)
	require.Equal(t, []string{"LabelA", "LabelB"}, labels)
	require.True(t, anyLabel)
	require.Len(t, matchAssign, 1)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal(originalBraceView("MATCH (n:LabelA|LabelB {id: row.id})"), "MATCH", false)
	require.False(t, ok)

	withPlan, ok := parseUnwindWithClause(t.Context(), originalBraceView("WITH n, row.score AS score"))
	require.True(t, ok)
	require.Len(t, withPlan.assignments, 1)
	require.Equal(t, "score", withPlan.assignments[0].alias)
	require.Equal(t, "row.score", withPlan.assignments[0].expr)

	_, ok = parseUnwindWithClause(t.Context(), originalBraceView("WITH n, row.score"))
	require.False(t, ok)

	wherePlan, ok := parseUnwindWhereClause("WHERE score > 0")
	require.True(t, ok)
	require.Equal(t, "score > 0", wherePlan.clause)
	_, ok = parseUnwindWhereClause("WHERE   ")
	require.False(t, ok)

	clauses, ok := splitUnwindMergeChainClauses(originalBraceView("MERGE (n:Node {id: row.id}) SET n.k = row.k"))
	require.True(t, ok)
	require.Len(t, clauses, 2)
	_, ok = splitUnwindMergeChainClauses(expressionBraceView("BAD (n)"))
	require.False(t, ok)

	rel, ok := parseUnwindMergeRelationshipClause(originalBraceView("MERGE (a)-[r:REL]->(b)"))
	require.True(t, ok)
	require.Equal(t, "a", rel.fromVar)
	require.Equal(t, "b", rel.toVar)
	require.Equal(t, "r", rel.relVar)
	require.Equal(t, "REL", rel.relType)

	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (a)-[:REL]->(b)-[:X]->(c)"))
	require.False(t, ok)

	stages, ok := splitUnwindCompoundMutationStages(
		originalBraceView("MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})"),
		"rows",
		"row",
	)
	require.True(t, ok)
	require.Len(t, stages, 2)

	_, ok = splitUnwindCompoundMutationStages(originalBraceView("MERGE (a:Node {id: row.id})"), "rows", "")
	require.False(t, ok)
}

func TestParseUnwindMergeRelationshipClause_Branches(t *testing.T) {
	plan, ok := parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[:REL]->(m)"))
	require.True(t, ok)
	require.Equal(t, "n", plan.fromVar)
	require.Equal(t, "m", plan.toVar)
	require.Equal(t, "REL", plan.relType)
	require.Equal(t, "", plan.relVar)

	plan, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[r:REL]->(m)"))
	require.True(t, ok)
	require.Equal(t, "r", plan.relVar)

	plan, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[r:REL {}]->(m)"))
	require.True(t, ok)
	require.Equal(t, "r", plan.relVar)
	require.Empty(t, plan.matchAssignments)
	plan, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[:REL {}]->(m)"))
	require.True(t, ok)
	require.Empty(t, plan.relVar)
	require.Empty(t, plan.matchAssignments)

	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("CREATE (n)-[:REL]->(m)"))
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE n-[:REL]->(m)"))
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[r]->(m)"))
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[:REL]-(m)"))
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause(originalBraceView("MERGE (n)-[:REL]->(m) RETURN n"))
	require.False(t, ok)
}

func TestGh713CountCompilersUseSharedProjectionPlans(t *testing.T) {
	for _, compiler := range []struct {
		name  string
		parse func(context.Context, string) (string, bool)
	}{
		{"merge count", func(ctx context.Context, clause string) (string, bool) {
			return parseSimpleCountReturn(ctx, originalBraceView(clause), "n")
		}},
		{"unwind count", func(ctx context.Context, clause string) (string, bool) {
			return parseUnwindBatchCountReturn(ctx, originalBraceView(clause))
		}},
	} {
		t.Run(compiler.name, func(t *testing.T) {
			for _, clause := range []string{
				"RETURN count(n) AS total", "RETURN count(n) AS `node total`",
				"RETURN count(n) AS `a``b`", "RETURN count(n)",
			} {
				t.Run(clause, func(t *testing.T) {
					alias, ok := compiler.parse(t.Context(), clause)
					require.True(t, ok)
					require.Equal(t, returnProjectionPlanFor(t.Context(), originalBraceView(clause)).columns[0], alias)
				})
			}
			for _, clause := range []string{
				"RETURN count(n) AS total LIMIT 0", "RETURN count(n) AS total SKIP 1",
				"RETURN count(n) AS total ORDER BY total", "RETURN DISTINCT count(n) AS total",
				"RETURN count(DISTINCT n) AS total", "RETURN count(n) AS",
				"RETURN *", "RETURN count(n), count(n) AS second",
				"RETURN avg(n) AS total", "RETURN count() AS total",
				"RETURN count(n) + 1 AS total", "RETURN count(n) AS ``",
				"RETURN count(n.value) AS total",
			} {
				t.Run("decline "+clause, func(t *testing.T) {
					_, ok := compiler.parse(t.Context(), clause)
					require.False(t, ok)
				})
			}
		})
	}
}

func TestGh713WithCompilerUsesSharedProjectionPlan(t *testing.T) {
	for _, clause := range []string{
		"WITH n, row.k AS k", "WITH n, row.k AS `k`",
		"WITH `n`, row.k AS `k`",
	} {
		t.Run(clause, func(t *testing.T) {
			compiled, ok := parseUnwindWithClause(t.Context(), originalBraceView(clause))
			require.True(t, ok)
			require.Len(t, compiled.assignments, 1)
			shared := returnProjectionPlanFor(t.Context(), originalBraceView("RETURN"+clause[len("WITH"):]))
			require.Equal(t, shared.projections[1].expr, compiled.assignments[0].expr)
			require.Equal(t, shared.columns[1], compiled.assignments[0].alias)
		})
	}
	for _, clause := range []string{
		"WITH count(n) AS total", "WITH count(DISTINCT n) AS total",
		"WITH count(n) + 1 AS total", "WITH collect(n) AS nodes",
		"WITH DISTINCT n", "WITH n AS node ORDER BY node",
		"WITH n AS node LIMIT 0", "WITH n AS node SKIP 1",
		"WITH *", "WITH n AS", "WITH row.k", "WITH",
	} {
		t.Run("decline "+clause, func(t *testing.T) {
			_, ok := parseUnwindWithClause(t.Context(), originalBraceView(clause))
			require.False(t, ok)
		})
	}
}

func TestGh713WithCompilerRoutesAndWriteAdmission(t *testing.T) {
	items := []interface{}{
		map[string]interface{}{"id": "a", "value": int64(1)},
		map[string]interface{}{"id": "b", "value": int64(2)},
	}
	for _, route := range []string{"batch", "autocommit", "explicit transaction"} {
		for _, alias := range []string{"score", "`score`"} {
			t.Run(route+"/"+alias, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				mutation := "MERGE (n:WithSeed {id: row.id}) WITH n, row, row.value AS " + alias +
					" MERGE (m:WithValue {id: row.id}) SET m.score = score"
				rest := mutation + " RETURN count(m) AS total"
				var result *ExecuteResult
				var err error
				if route == "batch" {
					var supported bool
					result, supported, err = exec.executeUnwindMergeChainBatch(ctx, "row", items, originalBraceView(mutation), originalBraceView("RETURN count(m) AS total"))
					require.True(t, supported)
				} else {
					if route == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err = exec.Execute(ctx, "UNWIND $rows AS row "+rest, map[string]interface{}{"rows": items})
				}
				require.NoError(t, err)
				require.Equal(t, []string{"total"}, result.Columns)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
				if route == "explicit transaction" {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (m:WithValue) RETURN m.id AS id, m.score AS score ORDER BY id", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"a", int64(1)}, {"b", int64(2)}}, stored.Rows)
			})
		}
	}
	for _, projection := range []string{
		"n, row, count(n) AS score", "n, row, collect(n) AS score",
		"DISTINCT n, row, row.value AS score", "n, row, row.value AS score LIMIT 0",
	} {
		t.Run("decline before writes/"+projection, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			mutation := "MERGE (n:WithSeed {id: row.id}) WITH " + projection +
				" MERGE (m:WithValue {id: row.id}) SET m.score = score"
			result, supported, err := exec.executeUnwindMergeChainBatch(ctx, "row", items, originalBraceView(mutation), originalBraceView("RETURN count(m) AS total"))
			require.NoError(t, err)
			require.False(t, supported)
			require.Nil(t, result)
			stored, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
}

func TestGh713CompiledWithItemsReadIncomingScope(t *testing.T) {
	items := []interface{}{map[string]interface{}{"id": "a", "value": int64(1)}}
	for _, route := range []string{"batch", "autocommit", "explicit transaction"} {
		for _, projection := range []string{"2 AS x, x AS previous", "x AS previous, 2 AS x"} {
			t.Run(route+"/"+projection, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				mutation := "MERGE (n:ScopeSeed {id: row.id}) WITH n, row, row.value AS x WITH n, row, " + projection +
					" MERGE (m:ScopeValue {id: row.id}) SET m.value = previous"
				var err error
				if route == "batch" {
					_, supported, batchErr := exec.executeUnwindMergeChainBatch(ctx, "row", items, originalBraceView(mutation), originalBraceView("RETURN count(m) AS total"))
					require.True(t, supported)
					err = batchErr
				} else {
					if route == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					_, err = exec.Execute(ctx, "UNWIND $rows AS row "+mutation+" RETURN count(m) AS total", map[string]interface{}{"rows": items})
				}
				require.NoError(t, err)
				if route == "explicit transaction" {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (m:ScopeValue) RETURN m.value AS value", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, stored.Rows)
			})
		}
	}
}

func TestGh713CompiledWithFailureRollsBackWrites(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx,
				"UNWIND $rows AS row MERGE (n:ScopeSeed {id: row.id}) WITH n, row, 1 / $zero AS value MERGE (m:ScopeValue {id: row.id}) SET m.value = value RETURN count(m) AS total",
				map[string]interface{}{"rows": []interface{}{map[string]interface{}{"id": "a"}}, "zero": int64(0)})
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Statement.ArithmeticError")
			if mode == "explicit transaction" {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
			stored, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS total", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stored.Rows)
		})
	}
}

func TestGh713CountReturnWindowsPreserveWrites(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit transaction"} {
		for _, test := range []struct {
			projection string
			rows       [][]interface{}
		}{
			{"count(n) AS total", [][]interface{}{{int64(2)}}},
			{"count(n) AS `node total`", [][]interface{}{{int64(2)}}},
			{"count(n) AS `a``b`", [][]interface{}{{int64(2)}}},
			{"count(n)", [][]interface{}{{int64(2)}}},
			{"count(n) AS total LIMIT 0", nil},
			{"count(n) AS total SKIP $skip", nil},
			{"count(n) AS total ORDER BY total LIMIT 1", [][]interface{}{{int64(2)}}},
			{"count(DISTINCT n) AS total", [][]interface{}{{int64(2)}}},
		} {
			t.Run(mode+"/"+test.projection, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE CONSTRAINT compiled_count_unique FOR (n:Counted) REQUIRE n.id IS UNIQUE", nil)
				require.NoError(t, err)
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				params := map[string]interface{}{
					"rows": []map[string]interface{}{{"id": "a", "value": int64(1)}, {"id": "b", "value": int64(2)}},
					"skip": int64(1),
				}
				result, err := exec.Execute(ctx, "UNWIND $rows AS row MERGE (n:Counted {id: row.id}) SET n.value = row.value RETURN "+test.projection, params)
				require.NoError(t, err)
				require.Equal(t, returnProjectionPlanFor(t.Context(), originalBraceView("RETURN "+test.projection)).columns, result.Columns)
				require.Len(t, result.Rows, len(test.rows))
				if len(test.rows) > 0 {
					require.Equal(t, test.rows, result.Rows)
				}
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				stored, err := exec.Execute(ctx, "MATCH (n:Counted) RETURN n.id AS id, n.value AS value ORDER BY id", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"a", int64(1)}, {"b", int64(2)}}, stored.Rows)
			})
		}
	}
}

func TestParseSimpleCountAndBatchReturn_Branches(t *testing.T) {
	alias, ok := parseSimpleCountReturn(t.Context(), originalBraceView("RETURN count(n) AS cnt"), "n")
	require.True(t, ok)
	require.Equal(t, "cnt", alias)

	alias, ok = parseSimpleCountReturn(t.Context(), originalBraceView("RETURN count(n) AS"), "n")
	require.False(t, ok)
	require.Empty(t, alias)

	_, ok = parseSimpleCountReturn(t.Context(), originalBraceView("RETURN count(m) AS cnt"), "n")
	require.False(t, ok)
	_, ok = parseSimpleCountReturn(t.Context(), expressionBraceView("count(n) AS cnt"), "n")
	require.False(t, ok)

	alias, ok = parseUnwindBatchCountReturn(t.Context(), originalBraceView("RETURN count(*) AS total"))
	require.True(t, ok)
	require.Equal(t, "total", alias)

	alias, ok = parseUnwindBatchCountReturn(t.Context(), originalBraceView("RETURN COUNT(id) AS total"))
	require.True(t, ok)
	require.Equal(t, "total", alias)

	_, ok = parseUnwindBatchCountReturn(t.Context(), originalBraceView("RETURN count(a.b) AS total"))
	require.False(t, ok)
	alias, ok = parseUnwindBatchCountReturn(t.Context(), originalBraceView("RETURN count(*)"))
	require.True(t, ok)
	require.Equal(t, "count(*)", alias)
	_, ok = parseUnwindBatchCountReturn(t.Context(), originalBraceView("MATCH (n) RETURN n"))
	require.False(t, ok)
}

func TestUnwindCollectDistinctUsesSharedProjection(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	res, err := exec.Execute(context.Background(), `UNWIND [{name:'a'}, {name:'a'}, {name:'b'}, {name:'c'}, {}] AS row
WITH COLLECT(DISTINCT row.name) AS names RETURN names`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"names"}, res.Columns)
	require.Equal(t, [][]interface{}{{[]interface{}{"a", "b", "c"}}}, res.Rows)
	res, err = exec.Execute(context.Background(), `UNWIND [] AS row WITH COLLECT(DISTINCT row.name) AS names RETURN names AS x`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Equal(t, [][]interface{}{{[]interface{}{}}}, res.Rows)
}
