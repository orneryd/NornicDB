package cypher

import (
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

func TestParseSimpleCountAndBatchReturn_Branches(t *testing.T) {
	alias, ok := parseSimpleCountReturn("RETURN count(n) AS cnt", "n")
	require.True(t, ok)
	require.Equal(t, "cnt", alias)

	alias, ok = parseSimpleCountReturn("RETURN count(n) AS", "n")
	require.True(t, ok)
	require.Equal(t, "count(n)", alias)

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
	_, ok = parseUnwindBatchCountReturn("RETURN count(*)")
	require.False(t, ok)
	_, ok = parseUnwindBatchCountReturn("MATCH (n) RETURN n")
	require.False(t, ok)
}

func TestParseUnwindCollectDistinctProjection_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	plan, ok := exec.parseUnwindCollectDistinctProjection("WITH COLLECT(DISTINCT row.name) AS names RETURN names")
	require.True(t, ok)
	require.Equal(t, "row", plan.srcVar)
	require.Equal(t, "name", plan.prop)
	require.Equal(t, "names", plan.alias)

	_, ok = exec.parseUnwindCollectDistinctProjection("WITH COLLECT(row.name) AS names RETURN names")
	require.False(t, ok)
	_, ok = exec.parseUnwindCollectDistinctProjection("WITH COLLECT(DISTINCT row.name) AS names RETURN names AS x")
	require.False(t, ok)
	_, ok = exec.parseUnwindCollectDistinctProjection("RETURN names")
	require.False(t, ok)

	res, matched := exec.executeUnwindWithCollectProjection("row", []interface{}{
		map[string]interface{}{"name": "a"},
		map[string]interface{}{"name": "a"},
		map[string]interface{}{"name": "b"},
		map[interface{}]interface{}{"name": "c"},
		123,
	}, "WITH COLLECT(DISTINCT row.name) AS names RETURN names")
	require.True(t, matched)
	require.NotNil(t, res)
	require.Equal(t, []string{"names"}, res.Columns)
	require.Equal(t, [][]interface{}{{[]interface{}{"a", "b", "c"}}}, res.Rows)

	res, matched = exec.executeUnwindWithCollectProjection("item", []interface{}{map[string]interface{}{"name": "a"}}, "WITH COLLECT(DISTINCT row.name) AS names RETURN names")
	require.False(t, matched)
	require.Nil(t, res)
}
