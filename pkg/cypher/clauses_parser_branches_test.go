package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseUnwindSimpleAssignments_Branches(t *testing.T) {
	assignments, ok := parseUnwindSimpleMergeMatchAssignments("id: row.id, name: row.name")
	require.True(t, ok)
	require.Len(t, assignments, 2)
	require.Equal(t, "id", assignments[0].prop)
	require.Equal(t, "row.id", assignments[0].expr)

	_, ok = parseUnwindSimpleMergeMatchAssignments("")
	require.False(t, ok)
	_, ok = parseUnwindSimpleMergeMatchAssignments("id")
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
}

func TestParseUnwindMergeRelationshipClause_Branches(t *testing.T) {
	plan, ok := parseUnwindMergeRelationshipClause("MERGE (n)-[:REL]->(m)")
	require.True(t, ok)
	require.Equal(t, "n", plan.fromVar)
	require.Equal(t, "m", plan.toVar)
	require.Equal(t, "REL", plan.relType)
	require.Equal(t, "", plan.relVar)

	plan, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[r:REL]->(m)")
	require.True(t, ok)
	require.Equal(t, "r", plan.relVar)

	plan, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[r:REL {}]->(m)")
	require.True(t, ok)
	require.Equal(t, "r", plan.relVar)
	require.Empty(t, plan.matchAssignments)
	plan, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[:REL {}]->(m)")
	require.True(t, ok)
	require.Empty(t, plan.relVar)
	require.Empty(t, plan.matchAssignments)

	_, ok = parseUnwindMergeRelationshipClause("CREATE (n)-[:REL]->(m)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MERGE n-[:REL]->(m)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[r]->(m)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[:REL]-(m)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MERGE (n)-[:REL]->(m) RETURN n")
	require.False(t, ok)
}

func TestGh713CountCompilersUseSharedProjectionPlans(t *testing.T) {
	for _, compiler := range []struct {
		name  string
		parse func(string) (string, bool)
	}{
		{"merge count", func(clause string) (string, bool) { return parseSimpleCountReturn(clause, "n") }},
		{"unwind count", parseUnwindBatchCountReturn},
	} {
		t.Run(compiler.name, func(t *testing.T) {
			for _, clause := range []string{
				"RETURN count(n) AS total", "RETURN count(n) AS `node total`",
				"RETURN count(n) AS `a``b`", "RETURN count(n)",
			} {
				t.Run(clause, func(t *testing.T) {
					alias, ok := compiler.parse(clause)
					require.True(t, ok)
					require.Equal(t, returnProjectionPlanFor(clause).columns[0], alias)
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
					_, ok := compiler.parse(clause)
					require.False(t, ok)
				})
			}
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
				require.Equal(t, returnProjectionPlanFor("RETURN "+test.projection).columns, result.Columns)
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
	alias, ok := parseSimpleCountReturn("RETURN count(n) AS cnt", "n")
	require.True(t, ok)
	require.Equal(t, "cnt", alias)

	alias, ok = parseSimpleCountReturn("RETURN count(n) AS", "n")
	require.False(t, ok)
	require.Empty(t, alias)

	_, ok = parseSimpleCountReturn("RETURN count(m) AS cnt", "n")
	require.False(t, ok)
	_, ok = parseSimpleCountReturn("count(n) AS cnt", "n")
	require.False(t, ok)

	alias, ok = parseUnwindBatchCountReturn("RETURN count(*) AS total")
	require.True(t, ok)
	require.Equal(t, "total", alias)

	alias, ok = parseUnwindBatchCountReturn("RETURN COUNT(id) AS total")
	require.True(t, ok)
	require.Equal(t, "total", alias)

	_, ok = parseUnwindBatchCountReturn("RETURN count(a.b) AS total")
	require.False(t, ok)
	alias, ok = parseUnwindBatchCountReturn("RETURN count(*)")
	require.True(t, ok)
	require.Equal(t, "count(*)", alias)
	_, ok = parseUnwindBatchCountReturn("MATCH (n) RETURN n")
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
