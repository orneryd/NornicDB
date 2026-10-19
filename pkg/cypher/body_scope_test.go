package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"

	"github.com/stretchr/testify/require"
)

// TestBodyScopeOfImplicitReads: an unscoped CALL body reads an outer name
// exactly when Neo4j 5.26.30 needs it imported (CALL (n) { … }) for the
// same answer; a name it uses only in pattern positions (MATCH, EXISTS /
// COUNT / COLLECT patterns, pattern comprehensions) is a new variable, as
// in CALL () { … }. The bodies are the CALL import matrix's
// (TestUnscopedCallImportsMatchNeo4j), whose answers were checked against
// Neo4j.
func TestBodyScopeOfImplicitReads(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, tc := range []struct {
		body  string
		reads bool
	}{
		{"MATCH (m:B) WHERE EXISTS { (m)-->(n) } RETURN count(m) AS c", false},
		{"MATCH (m:B) RETURN COUNT { (m)-->(n) } AS c", false},
		{"MATCH (m:B) RETURN size(COLLECT { MATCH (m)-->(n) RETURN n.id }) AS c", false},
		{"MATCH (m:B) RETURN size(COLLECT { MATCH (m)-->(x) RETURN COUNT { (x)<--(n) } AS k }) AS c", false},
		{"MATCH (m:B) WHERE m.n IS NULL AND EXISTS { (m)-->(n) } RETURN count(m) AS c", false},
		{"MATCH (m:B) WHERE 'n' = 'n' AND EXISTS { (m)-->(n) } RETURN count(m) AS c", false},
		{"MATCH (m:B) WHERE {n: 1}.n = 1 AND EXISTS { (m)-->(n) } RETURN count(m) AS c", false},
		{"RETURN COLLECT { MATCH (m:B) RETURN size([(m)-->(n) | n]) AS s }[0] AS c", false},
		{"RETURN size([n IN [1, 2] | n * 10]) AS c", false},
		{"RETURN reduce(s = 0, n IN [1, 2] | s + n) AS c", false},
		{"RETURN all(n IN [1, 2] WHERE n > 0) AS c", false},
		{"RETURN size(COLLECT { MATCH (n:C) RETURN n.id AS v UNION ALL MATCH (m:B) RETURN 0 AS v }) AS c", false},
		{"CALL () { MATCH (n:C) RETURN n.id AS v } RETURN v AS c", false},
		{"MATCH (n:C) RETURN n.id AS c", false},
		{"UNWIND [5] AS n RETURN n AS c", false},
		{"WITH 7 AS n RETURN n AS c", false},
		{"MATCH (m:B) RETURN COUNT { MATCH (m) ((x)-[:R]->(n)){1,1} } AS c", false},
		{"MATCH (m:B) /* n */ RETURN count(m) AS c", false},
		{"MATCH (m:B) WHERE EXISTS { (m)-->(`n`) } RETURN count(m) AS c", false},
		{"MATCH (m:B) WHERE NOT EXISTS { (m)-->(:n) } RETURN count(m) AS c", false},
		{"MATCH (m:B) WHERE NOT EXISTS { (m)-[:n]->() } RETURN count(m) AS c", false},
		{"RETURN COUNT { MATCH (n:C) RETURN 1 AS v UNION MATCH (n:B) RETURN 2 AS v } AS c", false},
		{"MATCH (m:B) WHERE EXISTS { MATCH (m)-->(n) WHERE n.id = 9 } RETURN count(m) AS c", false},
		{"MATCH (m:B) RETURN size([x IN COLLECT { MATCH (m)-->(n) RETURN n } | x]) AS c", false},
		// A pattern comprehension's pattern variable is its own when nothing
		// else reads the name (the R09 case: CALL () gives 1 in Neo4j).
		{"RETURN size([(n)-->() | 1]) AS c", false},
		{"RETURN n.id AS c", true},
		{"RETURN COLLECT { RETURN n.id }[0] AS c", true},
		{"MATCH (m:A) WHERE EXISTS { MATCH (m) WHERE m.id = n.id } RETURN count(m) AS c", true},
		{"RETURN COUNT { MATCH (m:A) WHERE m.id = n.id } AS c", true},
		{"RETURN COLLECT { MATCH (n:C) RETURN 10 AS v UNION ALL MATCH (m:A) WHERE m.id = n.id RETURN m.id AS v }[0] AS c", true},
		{"RETURN COLLECT { MATCH (m:A) WHERE m.id = n.id RETURN m.id AS v UNION ALL MATCH (n:C) RETURN 10 AS v }[0] AS c", true},
		{"RETURN COUNT { MATCH (n:C) RETURN 1 AS v UNION ALL MATCH (m:A) WHERE m.id = n.id RETURN 1 AS v } AS c", true},
		{"RETURN EXISTS { MATCH (n:C) RETURN 1 AS v UNION MATCH (m:A) WHERE m.id = n.id RETURN 1 AS v } AS c", true},
		{"RETURN [x IN [1] | n.id][0] AS c", true},
		{"CALL (n) { RETURN n.id AS v } RETURN v AS c", true},
		{"CALL { WITH n RETURN n.id AS v } RETURN v AS c", true},
		{"CALL (*) { RETURN n.id AS v } RETURN v AS c", true},
		{"RETURN COLLECT { MATCH (m:A) RETURN COUNT { MATCH (k:A) WHERE k.id = n.id } AS v }[0] AS c", true},
		{"MATCH (m:B) WHERE EXISTS { (m)-->(n) } WITH count(m) AS t RETURN t + n.id AS c", true},
		{"MATCH (m:A) WHERE EXISTS { (m {id: n.id}) } RETURN count(m) AS c", true},
		{"RETURN COLLECT { UNWIND [1] AS k RETURN n.id AS v }[0] AS c", true},
		{"RETURN EXISTS { MATCH (m:A) WHERE m.id = n.id } AS c", true},
		{"MATCH (m:A) RETURN sum(COUNT { MATCH (m) WHERE m = n }) AS c", true},
		{"RETURN COLLECT { MATCH (n:C) RETURN 10 AS v UNION ALL RETURN n.id AS v }[0] AS c", true},
		{"MATCH (m:A) WHERE m.id = `n`.id RETURN count(m) AS c", true},
		{"RETURN COUNT { MATCH (m:A {id: `n`.id}) } AS c", true},
		{"FOREACH (x IN n.list | CREATE (:L {v: x}))", true},
		{"FOREACH (n IN [1] | CREATE (:L {v: n}))", false},
	} {
		scope, err := exec.bodyScopeOf(tc.body, nil, false)
		require.NoError(t, err, tc.body)
		require.Equal(t, tc.reads, containsString(scope.Reads, "n"), "%s: reads %v", tc.body, scope.Reads)
	}
}

// TestBodyScopeOfShape: per UNION branch reads and declarations, importing
// WITH lists, visible names in pattern positions, procedure YIELDs, CALL
// scope clauses and the cache.
func TestBodyScopeOfShape(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	scope, err := exec.bodyScopeOf("MATCH (n:C) RETURN n.id AS c UNION ALL MATCH (m:A) WHERE m.id = n.id RETURN m.id AS c", nil, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)
	require.Len(t, scope.Branches, 2)
	require.Empty(t, scope.Branches[0].Reads)
	require.Equal(t, []string{"n", "c"}, scope.Branches[0].Declares)
	require.Equal(t, []string{"n"}, scope.Branches[1].Reads)
	require.Equal(t, []string{"m", "c"}, scope.Branches[1].Declares)

	scope, err = exec.bodyScopeOf("WITH a, b MATCH (x) WHERE x.v = a RETURN x, b", nil, false)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, scope.Branches[0].Importing)
	require.Equal(t, []string{"a", "b"}, scope.Reads)

	// A visible name in a pattern position is a read; without it, a
	// declaration.
	scope, err = exec.bodyScopeOf("(m)-->(n)", []string{"n"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)
	require.Equal(t, []string{"m"}, scope.Branches[0].Declares)
	scope, err = exec.bodyScopeOf("(m)-->(n)", []string{}, false)
	require.NoError(t, err)
	require.Empty(t, scope.Reads)
	require.Equal(t, []string{"m", "n"}, scope.Branches[0].Declares)
	// So in a pattern comprehension.
	scope, err = exec.bodyScopeOf("RETURN [(n)-->(x) | x.id] AS c", []string{"n"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)

	scope, err = exec.bodyScopeOf("CALL db.labels() YIELD label RETURN label, n.id AS c", nil, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)
	require.Equal(t, []string{"label", "c"}, scope.Branches[0].Declares)
	scope, err = exec.bodyScopeOf("CALL db.labels() YIELD label WHERE label <> 'ignored' RETURN label, n.id AS c", nil, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)
	require.Equal(t, []string{"label", "c"}, scope.Branches[0].Declares)

	scope, err = exec.bodyScopeOf("CALL (n) { RETURN 1 AS one } RETURN one", nil, false)
	require.NoError(t, err)
	require.Equal(t, []string{"n"}, scope.Reads)
	require.Equal(t, []string{"one"}, scope.Branches[0].Declares)

	again, err := exec.bodyScopeOf("CALL (n) { RETURN 1 AS one } RETURN one", nil, false)
	require.NoError(t, err)
	require.Equal(t, scope, again)
	require.NotEqual(t, bodyScopeKey("RETURN n", nil, false), bodyScopeKey("RETURN n", []string{}, false))
	require.NotEqual(t, bodyScopeKey("RETURN n", nil, false), bodyScopeKey("RETURN n", nil, true))
}
