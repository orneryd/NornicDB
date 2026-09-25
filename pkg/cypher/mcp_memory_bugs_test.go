package cypher

// Regression tests for the three Cypher defects reported by the
// mcp-neo4j-memory bug report (May 2026):
//
//  1. Property access on a map parameter / unwound item is stored as
//     literal text instead of evaluated.
//  2. Fulltext index ignores its declared label scope.
//  3. WHERE / WITH ... WHERE clauses placed after CALL ... YIELD return
//     zero rows from a bare aggregating RETURN.
//
// Each test reproduces the exact pattern from the report. The
// assertions are intentionally tight: the regression is a silent
// "wrong answer", not a panic, so we deeply assert the value, type,
// and row count.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMCPTestExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "mcp-test")
	return NewStorageExecutor(store)
}

// rowCell pulls (rowIndex, columnName) -> value out of an ExecuteResult,
// converting nil/missing into a typed sentinel so tight assertions read
// cleanly.
func rowCell(t *testing.T, res *ExecuteResult, rowIdx int, col string) any {
	t.Helper()
	require.NotNil(t, res, "result must not be nil")
	require.Greater(t, len(res.Rows), rowIdx, "row %d not present (have %d rows)", rowIdx, len(res.Rows))
	colIdx := -1
	for i, c := range res.Columns {
		if c == col {
			colIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, colIdx, 0, "column %q not present (have %v)", col, res.Columns)
	return res.Rows[rowIdx][colIdx]
}

// Bug 1a — WITH $m AS m CREATE (...) where the property value comes
// from m.name. The expected behavior is that m.name evaluates to
// "hello"; the bug stores the literal text "{name: 'hello'}.name".
func TestMCPBug1_MapParamPropertyAccess_InCreateValuePosition_WithBinding(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	res, err := exec.Execute(ctx,
		"WITH $m AS m CREATE (t:Tmp {name: m.name}) RETURN t.name AS name",
		map[string]any{"m": map[string]any{"name": "hello"}},
	)
	require.NoError(t, err)
	got := rowCell(t, res, 0, "name")
	assert.Equal(t, "hello", got,
		"WITH-binding map.prop access must evaluate, not stringify; got %T(%v)", got, got)
}

// Bug 1b — UNWIND [$r] AS r ... WHERE a.name = r.source. The reported
// failure is that the WHERE matches/creates nothing because r.source
// resolves to literal text after substitution.
func TestMCPBug1_MapParamPropertyAccess_InWhereAfterUnwind(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	// Seed two endpoints.
	_, err := exec.Execute(ctx, "CREATE (:X {name:'x'}), (:X {name:'y'})", nil)
	require.NoError(t, err)

	// Drive the bug-reproducer query verbatim.
	_, err = exec.Execute(ctx,
		"UNWIND [$r] AS r MATCH (a:X), (b:X) WHERE a.name = r.source AND b.name = r.target MERGE (a)-[:REL]->(b)",
		map[string]any{"r": map[string]any{"source": "x", "target": "y"}},
	)
	require.NoError(t, err)

	// Verify the edge exists.
	check, err := exec.Execute(ctx,
		"MATCH (a:X {name:'x'})-[:REL]->(b:X {name:'y'}) RETURN count(*) AS n",
		nil,
	)
	require.NoError(t, err)
	got := rowCell(t, check, 0, "n")
	assert.EqualValues(t, 1, got,
		"UNWIND map.prop in WHERE must match; expected 1 :REL edge, got %v", got)
}

// Bug 1c — UNWIND $list AS r where r is a map; r.prop in CREATE.
// Caller passes a list of maps; this is the common upsert pattern that
// MCP uses.
func TestMCPBug1_UnwindListOfMaps_PropertyAccess(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"UNWIND $rows AS r CREATE (n:Memory {name: r.name, type: r.type})",
		map[string]any{
			"rows": []any{
				map[string]any{"name": "alpha", "type": "Person"},
				map[string]any{"name": "beta", "type": "Concept"},
			},
		},
	)
	require.NoError(t, err)

	check, err := exec.Execute(ctx,
		"MATCH (n:Memory) RETURN n.name AS name, n.type AS type ORDER BY name",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, check.Rows, 2)

	// Tight assertions: every row must have a real value, never literal text.
	assert.Equal(t, "alpha", rowCell(t, check, 0, "name"))
	assert.Equal(t, "Person", rowCell(t, check, 0, "type"))
	assert.Equal(t, "beta", rowCell(t, check, 1, "name"))
	assert.Equal(t, "Concept", rowCell(t, check, 1, "type"))
}

// Bug 1d — MERGE (e:Memory {name: entity.name}) where entity comes from
// a WITH binding. This is the exact mcp-neo4j-memory create_entities
// shape; if Bug 1 still fires here, the upstream tool stores nodes
// whose `name` property is the literal string "{...}.name".
func TestMCPBug1_MergeWithMapParamBinding(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"WITH $entity AS entity MERGE (e:Memory {name: entity.name}) SET e.type = entity.type RETURN e.name AS name, e.type AS type",
		map[string]any{
			"entity": map[string]any{
				"name": "Alice",
				"type": "Person",
			},
		},
	)
	require.NoError(t, err)

	check, err := exec.Execute(ctx,
		"MATCH (n:Memory) RETURN n.name AS name, n.type AS type",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, check.Rows, 1, "expected exactly one :Memory node")
	assert.Equal(t, "Alice", rowCell(t, check, 0, "name"))
	assert.Equal(t, "Person", rowCell(t, check, 0, "type"))
}

// Bug 2 — fulltext index ignores its declared label scope. Create a
// :Memory node and a :Other node; the fulltext index is declared FOR
// (m:Memory) ONLY. queryNodes('search', '*') must return the :Memory
// node and exclude the :Other node.
func TestMCPBug2_FulltextIndexLabelScope(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	// Seed a :Memory node and an unrelated :Other node.
	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'mem-1', type:'Person', observations:'note'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE (:Other {name:'other-1', body:'note'})",
		nil)
	require.NoError(t, err)

	// Declare the fulltext index scoped to :Memory.
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	// Query should return only the :Memory node.
	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', '*') YIELD node RETURN labels(node) AS lbls, node.name AS name",
		nil)
	require.NoError(t, err)

	require.Greater(t, len(res.Rows), 0, "expected at least one :Memory hit, got 0 rows")
	for i := range res.Rows {
		labels := rowCell(t, res, i, "lbls")
		// labels can be []string or []any depending on storage round-trip;
		// the assertion is "Memory must be present, Other must NOT be".
		labelStrs := labelsToStrings(labels)
		assert.Contains(t, labelStrs, "Memory",
			"row %d: expected :Memory label, got %v", i, labelStrs)
		assert.NotContains(t, labelStrs, "Other",
			"row %d: fulltext index returned :Other node; index is scoped to :Memory only", i)
	}
}

// labelsToStrings normalizes the various shapes a labels() column might
// take ([]string, []any, etc.) into a comparable []string.
func labelsToStrings(v any) []string {
	switch xs := v.(type) {
	case []string:
		return xs
	case []any:
		out := make([]string, 0, len(xs))
		for _, x := range xs {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Bug 2b — the same wildcard handling must apply to
// queryRelationships, scoped to the declared RelationshipTypes. Seed
// two relationship types; the index only covers KNOWS; the wildcard
// query must surface KNOWS edges and exclude WORKS_WITH edges.
func TestMCPBug2_FulltextRelationshipIndexTypeScope(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (a:P {name:'a'}), (b:P {name:'b'}), (a)-[:KNOWS {note:'friend'}]->(b), (a)-[:WORKS_WITH {note:'colleague'}]->(b)",
		nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX rel_search IF NOT EXISTS FOR ()-[r:KNOWS]-() ON EACH [r.note]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryRelationships('rel_search', '*') YIELD relationship RETURN relationship",
		nil)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows, "wildcard must surface at least one matching relationship")
	for i := range res.Rows {
		rel := rowCell(t, res, i, "relationship")
		edge, ok := rel.(*storage.Edge)
		require.True(t, ok, "row %d: relationship must be a relationship, got %T", i, rel)
		assert.Equal(t, "KNOWS", edge.Type,
			"row %d: relationship-fulltext index scoped to KNOWS leaked %v", i, edge.Type)
	}
}

// Bug 2c — colon-form wildcard ("*:*") must behave identically to "*".
// Asserts row count via direct row enumeration (the count(...) post-YIELD
// aggregation is the separate Bug 3 family).
func TestMCPBug2_FulltextWildcard_ColonForm(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'mem-1', type:'Person', observations:'note'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', '*:*') YIELD node RETURN node",
		nil)
	require.NoError(t, err)
	assert.Len(t, res.Rows, 1, "*:* must surface every in-scope :Memory node")
}

// Bug 2d — Lucene field-presence wildcard `<prop>:*` returns every
// indexed node that carries a non-empty value for the named property.
// Two :Memory nodes are seeded; only one has `observations` populated.
// `observations:*` must surface only that one. The other :Memory node
// (no observations) and the :Other node (out of label scope) must
// both be excluded.
func TestMCPBug2_FulltextWildcard_FieldPresence_Node(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'with-obs', type:'Person', observations:'note'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE (:Memory {name:'no-obs', type:'Concept'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE (:Other {name:'other-1', observations:'should-be-excluded'})",
		nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', 'observations:*') YIELD node RETURN node.name AS name",
		nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1, "field-presence query must surface exactly the one :Memory node with a non-empty observations")
	assert.Equal(t, "with-obs", rowCell(t, res, 0, "name"))
}

// Bug 2e — `<prop>:*` against a property the index didn't declare must
// return zero rows. Even though the underlying graph has nodes with that
// property, the index's posting list doesn't cover it.
func TestMCPBug2_FulltextWildcard_FieldPresence_UndeclaredProperty(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'a', type:'t', observations:'o', secret:'shhh'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', 'secret:*') YIELD node RETURN node",
		nil)
	require.NoError(t, err)
	assert.Empty(t, res.Rows,
		"undeclared field must return zero rows (Lucene: no postings list)")
}

// Bug 2f — relationship-side `<prop>:*` field-presence query. Only
// edges that carry a non-empty value for the indexed property qualify.
func TestMCPBug2_FulltextWildcard_FieldPresence_Relationship(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (a:P {name:'a'}), (b:P {name:'b'}), (c:P {name:'c'}), (a)-[:KNOWS {note:'friend'}]->(b), (a)-[:KNOWS]->(c)",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX rel_search IF NOT EXISTS FOR ()-[r:KNOWS]-() ON EACH [r.note]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryRelationships('rel_search', 'note:*') YIELD relationship RETURN relationship",
		nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1,
		"field-presence on relationships must return only the edge with a non-empty note property")
	rel, ok := rowCell(t, res, 0, "relationship").(*storage.Edge)
	require.True(t, ok)
	assert.Equal(t, "KNOWS", rel.Type)
	assert.Equal(t, "friend", rel.Properties["note"])
}

// Bug 3 — a bare RETURN collect(...) after CALL ... YIELD ... WHERE
// must produce exactly one row. The bug report shows it returning
// zero rows, which is impossible for a collect() over an empty input —
// the grammar guarantees one row.
func TestMCPBug3_PostYieldWhere_AggregatingReturnAlwaysOneRow(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	// Seed one :Memory and one :Other so the index has data and the
	// WHERE has something to filter.
	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'a', type:'t', observations:'o'}), (:Other {name:'b'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	// The exact query shape from the bug report.
	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', '*') YIELD node AS entity, score WHERE entity:Memory RETURN collect({name: entity.name}) AS nodes",
		nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1,
		"bare RETURN collect(...) must produce exactly one row even when the WHERE filters every input")
	got := rowCell(t, res, 0, "nodes")
	require.NotNil(t, got, "collect must produce a non-nil list")

	// Assert the list is non-empty (the seeded :Memory node passes the
	// WHERE) and contains the expected name.
	rows, ok := got.([]any)
	if !ok {
		// Fall back to []map[string]any if collect uses a typed shape.
		if typed, ok2 := got.([]map[string]any); ok2 {
			rows = make([]any, len(typed))
			for i, m := range typed {
				rows[i] = m
			}
		} else {
			t.Fatalf("expected []any or []map, got %T", got)
		}
	}
	require.Len(t, rows, 1, "expected exactly one element after WHERE entity:Memory")
}

// Sub-bug — CALL dbms.components() must report the actual binary
// version (loaded from pkg/buildinfo), not the historical hard-coded
// "1.0.0". The bug report described this surfacing on the v1.1.0
// binary.
func TestMCPSubBug_DbmsComponentsReturnsBuildinfoVersion(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	res, err := exec.Execute(ctx, "CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	name := rowCell(t, res, 0, "name")
	assert.Equal(t, "NornicDB", name)

	versions := rowCell(t, res, 0, "versions")
	versionList, ok := versions.([]string)
	if !ok {
		// Some round-trip layers widen []string to []any.
		anyList, _ := versions.([]any)
		require.NotNil(t, anyList, "versions must be a list, got %T", versions)
		require.Len(t, anyList, 1)
		s, _ := anyList[0].(string)
		assert.NotEmpty(t, s, "version string must not be empty")
		assert.NotEqual(t, "1.0.0", s,
			"dbms.components() must report the buildinfo version, not the hard-coded 1.0.0 (got %q)", s)
		return
	}
	require.Len(t, versionList, 1)
	assert.NotEmpty(t, versionList[0], "version string must not be empty")
	// The current VERSION file in this tree is past 1.0.0, so the
	// hard-coded value would be a regression. We assert the value is
	// non-empty and not the legacy literal.
	assert.NotEqual(t, "1.0.0", versionList[0],
		"dbms.components() must report buildinfo.Version(), not the hard-coded 1.0.0")
}

// Bug 3b — WITH entity WHERE entity:Memory variant of the same shape.
// The report says either form fails identically.
func TestMCPBug3_PostYieldWithWhere_AggregatingReturnAlwaysOneRow(t *testing.T) {
	exec := newMCPTestExecutor(t)
	ctx := context.Background()

	_, err := exec.Execute(ctx,
		"CREATE (:Memory {name:'a', type:'t', observations:'o'}), (:Other {name:'b'})",
		nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx,
		"CREATE FULLTEXT INDEX search IF NOT EXISTS FOR (m:Memory) ON EACH [m.name, m.type, m.observations]",
		nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx,
		"CALL db.index.fulltext.queryNodes('search', '*') YIELD node AS entity WITH entity WHERE entity:Memory RETURN collect({name: entity.name}) AS nodes",
		nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	got := rowCell(t, res, 0, "nodes")
	require.NotNil(t, got)
}
