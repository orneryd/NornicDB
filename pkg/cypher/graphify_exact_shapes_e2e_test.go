// Code-of-record: every Cypher query graphify emits MUST have a matching
// exact-shape test below. "Exact" means byte-for-byte the same query text
// graphify produces — no simplifications, no adjacent rewrites, no
// "similar shapes".
//
// Inventory sources (commit-pin these when revisiting):
//   - graphify/graphify/export.py
//       * to_cypher()            — inline (.txt file) shapes 1 & 2
//       * push_to_neo4j()        — parameterised driver shapes 3 & 4
//       * push_to_falkordb()     — same parameterised shapes 3 & 4
//   - graphify/tests/test_falkordb_integration.py
//       * post-condition read shapes 5 & 6
//
// Graphify's surface is small and entirely MERGE/MATCH-by-property based; the
// queries are emitted per-node and per-edge with no UNWIND. This file pins:
//
//   Shape 1  — to_cypher node line:
//     MERGE (n:<FType> {id: '<id_esc>', label: '<label_esc>'});
//   Shape 2  — to_cypher edge line:
//     MATCH (a {id: '<u_esc>'}), (b {id: '<v_esc>'}) MERGE (a)-[:<REL> {confidence: '<conf_esc>'}]->(b);
//   Shape 3  — push_to_neo4j / push_to_falkordb node upsert:
//     MERGE (n:<FType> {id: $id}) SET n += $props
//   Shape 4  — push_to_neo4j / push_to_falkordb edge upsert:
//     MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:<REL>]->(b) SET r += $props
//   Shape 5  — node-count post-condition:
//     MATCH (n) RETURN count(n)
//   Shape 6  — edge-count post-condition:
//     MATCH ()-[r]->() RETURN count(r)
//
// Each test exercises the EXACT string graphify generates for at least one
// concrete value (e.g. a `_safe_label("Code")` → `Code`, `_safe_rel("calls")` →
// `CALLS`). When a shape has dynamic identifier-position substitutions (the
// `<FType>` / `<REL>` labels), we exercise the realistic graphify-produced
// values plus the documented fallback (`Entity`, `RELATED_TO`).
//
// Fast-path probe assertions:
//   - Shape 3 (node MERGE on id with SET n += $props) must drive the
//     MergeSchemaLookup fast path once a property index on (FType, id) exists,
//     which is what graphify users are expected to add for production loads.
//   - Shape 5/6 read-only count shapes don't have a dedicated fast path; we
//     just assert the count value is correct.

package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// graphifyExactShapeFixture mirrors the graphiti fixture: a fresh in-memory
// engine with the minimal graph shape graphify expects after a push_to_neo4j
// round-trip (Code/Document/Entity nodes carrying `id` + `label`, edges
// carrying `confidence` + arbitrary `<REL>` types).
type graphifyExactShapeFixture struct {
	exec *StorageExecutor
	ctx  context.Context
}

func newGraphifyExactShapeFixture(t *testing.T) *graphifyExactShapeFixture {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	// Graphify recommends an :Entity(id)/:Code(id) lookup index. Seeding it
	// proves the MERGE fast path lights up under realistic operator state.
	for _, q := range []string{
		"CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)",
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
		"CREATE INDEX document_id IF NOT EXISTS FOR (n:Document) ON (n.id)",
		"CREATE INDEX unknown_id IF NOT EXISTS FOR (n:Unknown) ON (n.id)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoErrorf(t, err, "fixture index: %s", q)
	}

	// Seed: two :Code nodes already linked by one :CALLS edge, plus a single
	// :Document node and a :Code -> :Document :REFERENCES edge, plus an
	// :Entity / :Entity :RELATES_TO edge (the to_cypher fallback label/rel).
	// Every node carries the {id, label} property set graphify emits.
	for _, q := range []string{
		`MERGE (n:Code {id: 'a.py::Foo', label: 'Foo'}) SET n += {id: 'a.py::Foo', label: 'Foo', file_type: 'code', source_file: 'a.py'}`,
		`MERGE (n:Code {id: 'b.py::Bar', label: 'Bar'}) SET n += {id: 'b.py::Bar', label: 'Bar', file_type: 'code', source_file: 'b.py'}`,
		`MERGE (n:Document {id: 'README.md::Intro', label: 'Intro'}) SET n += {id: 'README.md::Intro', label: 'Intro', file_type: 'document', source_file: 'README.md'}`,
		`MERGE (n:Entity {id: 'unknown::Thing', label: 'Thing'}) SET n += {id: 'unknown::Thing', label: 'Thing'}`,
		`MATCH (a {id: 'a.py::Foo'}), (b {id: 'b.py::Bar'}) MERGE (a)-[r:CALLS {confidence: 'EXTRACTED'}]->(b) SET r += {confidence: 'EXTRACTED'}`,
		`MATCH (a {id: 'a.py::Foo'}), (b {id: 'README.md::Intro'}) MERGE (a)-[r:REFERENCES {confidence: 'INFERRED'}]->(b) SET r += {confidence: 'INFERRED'}`,
		`MATCH (a {id: 'unknown::Thing'}), (b {id: 'a.py::Foo'}) MERGE (a)-[r:RELATES_TO {confidence: 'AMBIGUOUS'}]->(b) SET r += {confidence: 'AMBIGUOUS'}`,
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoErrorf(t, err, "fixture seed: %s", strings.TrimSpace(q[:min(len(q), 80)]))
	}
	return &graphifyExactShapeFixture{exec: exec, ctx: ctx}
}

func (f *graphifyExactShapeFixture) run(t *testing.T, query string, params map[string]interface{}) *ExecuteResult {
	t.Helper()
	res, err := f.exec.Execute(f.ctx, query, params)
	require.NoErrorf(t, err, "exact-shape query failed:\n%s", query)
	return res
}

func graphifyToInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	}
	t.Fatalf("not an int64: %T %v", v, v)
	return 0
}

// ============================================================================
// to_cypher() — Shape 1: per-node MERGE on inline (id, label) properties
// ============================================================================

// Exact line graphify writes for a code-typed node, with both id and label
// safely quoted as Cypher string literals.
func TestGraphifyExactShape_ToCypher_NodeMerge_CodeType(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Code {id: 'pkg/auth.py::Login', label: 'Login'});`, nil)
	cnt := f.run(t, "MATCH (n:Code {id: 'pkg/auth.py::Login'}) RETURN count(n)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// Same line for a document-typed node — exercises the `file_type='document'`
// branch through `_cypher_label("Document", "Entity")`.
func TestGraphifyExactShape_ToCypher_NodeMerge_DocumentType(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Document {id: 'docs/intro.md::Heading', label: 'Heading'});`, nil)
	cnt := f.run(t, "MATCH (n:Document {id: 'docs/intro.md::Heading'}) RETURN count(n)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// Fallback label path: graphify uses `_cypher_label(..., "Entity")` when the
// `file_type` is missing or unsanitisable, so this is the literal output for
// any non-alpha-leading or empty file_type.
func TestGraphifyExactShape_ToCypher_NodeMerge_EntityFallback(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Entity {id: 'bare-id', label: 'bare-id'});`, nil)
	cnt := f.run(t, "MATCH (n:Entity {id: 'bare-id'}) RETURN count(n)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// `_cypher_escape` swaps `\\`, `'`, `\n`, `\r` into their escape sequences so
// the inline string survives a literal-newline payload — the wire format must
// at minimum *parse* without breaking statement framing. We pin acceptance of
// the escape forms `\'`, `\\`, and `\n` inside single-quoted literals.
func TestGraphifyExactShape_ToCypher_NodeMerge_EscapedQuotesAndNewlines(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	// label = `Alice\nO'Connor` after _cypher_escape -> `Alice\\nO\\'Connor`
	// id    = `a\\\'b` after _cypher_escape -> `a\\\\\\'b`
	f.run(t, `MERGE (n:Code {id: 'a\\\'b', label: 'Alice\nO\'Connor'});`, nil)
	cnt := f.run(t, "MATCH (n:Code) WHERE n.label STARTS WITH 'Alice' RETURN count(n)", nil)
	require.GreaterOrEqual(t, graphifyToInt64(t, cnt.Rows[0][0]), int64(1),
		"escaped-quote/newline node MERGE must be accepted by the parser")
}

// Trailing semicolon — graphify emits `;` after every node MERGE so the
// statement framing for `cypher-shell` is preserved. NornicDB must accept a
// trailing semicolon as the statement terminator without producing a spurious
// empty-statement error.
func TestGraphifyExactShape_ToCypher_NodeMerge_TrailingSemicolonAccepted(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Code {id: 'semi.py::X', label: 'X'});`, nil)
}

// ============================================================================
// to_cypher() — Shape 2: per-edge MATCH (...), (...) MERGE [..] line
// ============================================================================

// Exact line graphify writes for an edge between two pre-existing nodes,
// inline relation type uppercased (`_cypher_label(rel.upper())`).
func TestGraphifyExactShape_ToCypher_EdgeMatchMerge_CallsRelation(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MATCH (a {id: 'a.py::Foo'}), (b {id: 'b.py::Bar'}) MERGE (a)-[:CALLS {confidence: 'EXTRACTED'}]->(b);`, nil)
	cnt := f.run(t, "MATCH (a {id:'a.py::Foo'})-[e:CALLS]->(b {id:'b.py::Bar'}) RETURN count(e)", nil)
	require.GreaterOrEqual(t, graphifyToInt64(t, cnt.Rows[0][0]), int64(1))
}

// `_cypher_label` fallback for unsanitisable relation types — graphify emits
// `RELATES_TO` as the default for any relation that doesn't survive the
// `[A-Za-z0-9_]` + must-start-with-letter filter.
func TestGraphifyExactShape_ToCypher_EdgeMatchMerge_RelatesToFallback(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MATCH (a {id: 'a.py::Foo'}), (b {id: 'README.md::Intro'}) MERGE (a)-[:RELATES_TO {confidence: 'INFERRED'}]->(b);`, nil)
	cnt := f.run(t, "MATCH (a {id:'a.py::Foo'})-[e:RELATES_TO]->(b {id:'README.md::Intro'}) RETURN count(e)", nil)
	require.GreaterOrEqual(t, graphifyToInt64(t, cnt.Rows[0][0]), int64(1))
}

// `confidence` default is `'EXTRACTED'` when the edge dict omits the key; we
// pin both the default and the `INFERRED`/`AMBIGUOUS` branches because they
// produce different exact strings.
func TestGraphifyExactShape_ToCypher_EdgeMatchMerge_ConfidenceVariants(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	for _, conf := range []string{"EXTRACTED", "INFERRED", "AMBIGUOUS"} {
		stmt := fmt.Sprintf(`MATCH (a {id: 'a.py::Foo'}), (b {id: 'b.py::Bar'}) MERGE (a)-[:CALLS {confidence: '%s'}]->(b);`, conf)
		f.run(t, stmt, nil)
	}
	// Relationship properties are part of the MERGE pattern, so the three
	// confidence identities remain distinct. Repeating any one statement is
	// idempotent for that exact pattern.
	cnt := f.run(t, "MATCH ({id:'a.py::Foo'})-[e:CALLS]->({id:'b.py::Bar'}) RETURN count(e)", nil)
	require.Equal(t, int64(3), graphifyToInt64(t, cnt.Rows[0][0]))
}

// ============================================================================
// push_to_neo4j() / push_to_falkordb() — Shape 3: parameterised node upsert
// ============================================================================

// Exact node-upsert shape with the recommended schema lookup index in place;
// must drive `MergeSchemaLookup` (not `MergeScanFallback`).
func TestGraphifyExactShape_Push_NodeMerge_FastPathSchemaLookup(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	res, err := f.exec.Execute(f.ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "a.py::Foo",
		"props": map[string]interface{}{
			"id":          "a.py::Foo",
			"label":       "Foo",
			"file_type":   "code",
			"source_file": "a.py",
			"community":   int64(0),
		},
	})
	require.NoError(t, err)
	require.True(t, f.exec.LastHotPathTrace().MergeSchemaLookupUsed, "node MERGE on indexed (Code, id) should hit the schema lookup fast path")
	require.False(t, f.exec.LastHotPathTrace().MergeScanFallbackUsed, "indexed MERGE must not fall back to full scan")
	_ = res
}

// Re-running the same MERGE upsert must NOT duplicate the node — pinning
// graphify's "MERGE so re-running is safe" contract.
func TestGraphifyExactShape_Push_NodeMerge_Idempotent(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	props := map[string]interface{}{
		"id": "a.py::Foo", "label": "Foo", "file_type": "code", "source_file": "a.py",
	}
	for i := 0; i < 3; i++ {
		_, err := f.exec.Execute(f.ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
			"id": "a.py::Foo", "props": props,
		})
		require.NoErrorf(t, err, "iteration %d", i)
	}
	cnt := f.run(t, "MATCH (n:Code {id: 'a.py::Foo'}) RETURN count(n)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// `SET n += $props` must layer additional properties without dropping
// pre-existing ones — graphify relies on this for incremental community
// re-labelling (community attr added after the initial push).
func TestGraphifyExactShape_Push_NodeMerge_PropsAreLayered(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	_, err := f.exec.Execute(f.ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "layered::1",
		"props": map[string]interface{}{
			"id": "layered::1", "label": "first", "file_type": "code",
		},
	})
	require.NoError(t, err)
	_, err = f.exec.Execute(f.ctx, `MERGE (n:Code {id: $id}) SET n += $props`, map[string]interface{}{
		"id": "layered::1",
		"props": map[string]interface{}{
			"id": "layered::1", "community": int64(7),
		},
	})
	require.NoError(t, err)
	res := f.run(t, "MATCH (n:Code {id:'layered::1'}) RETURN n.label, n.file_type, n.community", nil)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "first", res.Rows[0][0], "pre-existing label must survive the second upsert")
	require.Equal(t, "code", res.Rows[0][1], "pre-existing file_type must survive the second upsert")
	require.Equal(t, int64(7), graphifyToInt64(t, res.Rows[0][2]))
}

// Fallback label `Entity` — graphify uses this when `file_type` is empty after
// sanitising. The query string is identical apart from the inline `Entity`
// literal.
func TestGraphifyExactShape_Push_NodeMerge_EntityFallbackLabel(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	_, err := f.exec.Execute(f.ctx, `MERGE (n:Entity {id: $id}) SET n += $props`, map[string]interface{}{
		"id":    "bare::id",
		"props": map[string]interface{}{"id": "bare::id", "label": "Bare"},
	})
	require.NoError(t, err)
	cnt := f.run(t, "MATCH (n:Entity {id:'bare::id'}) RETURN count(n)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// ============================================================================
// push_to_neo4j() / push_to_falkordb() — Shape 4: parameterised edge upsert
// ============================================================================

// Exact edge-upsert shape. The MERGE matches an edge by *type and endpoints*
// only (no edge-key property): if two RELATES_TO edges already exist between
// the same pair, graphify will overwrite one — that's the documented contract.
func TestGraphifyExactShape_Push_EdgeMerge_BetweenExistingNodes(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	// Seed the two endpoints with the same shape graphify produces.
	f.run(t, `MERGE (n:Code {id: 'src::1'}) SET n += {id: 'src::1', label: 'src1'}`, nil)
	f.run(t, `MERGE (n:Code {id: 'tgt::1'}) SET n += {id: 'tgt::1', label: 'tgt1'}`, nil)

	_, err := f.exec.Execute(f.ctx, `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:CALLS]->(b) SET r += $props`, map[string]interface{}{
		"src": "src::1",
		"tgt": "tgt::1",
		"props": map[string]interface{}{
			"relation":   "calls",
			"confidence": "EXTRACTED",
		},
	})
	require.NoError(t, err)
	cnt := f.run(t, "MATCH ({id:'src::1'})-[e:CALLS]->({id:'tgt::1'}) RETURN count(e)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// `_safe_rel` sanitisation contract: any non `[A-Z0-9_]` collapses to `_`,
// empty → `RELATED_TO`. Pin both the sanitised and fallback exact strings.
func TestGraphifyExactShape_Push_EdgeMerge_RelatedToFallback(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Code {id: 'src::2'}) SET n += {id: 'src::2', label: 's2'}`, nil)
	f.run(t, `MERGE (n:Code {id: 'tgt::2'}) SET n += {id: 'tgt::2', label: 't2'}`, nil)
	_, err := f.exec.Execute(f.ctx, `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:RELATED_TO]->(b) SET r += $props`, map[string]interface{}{
		"src":   "src::2",
		"tgt":   "tgt::2",
		"props": map[string]interface{}{"confidence": "EXTRACTED"},
	})
	require.NoError(t, err)
	cnt := f.run(t, "MATCH ({id:'src::2'})-[e:RELATED_TO]->({id:'tgt::2'}) RETURN count(e)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// Re-running the edge MERGE upsert must not produce a duplicate edge between
// the same endpoint pair for the same type — graphify's idempotency promise.
func TestGraphifyExactShape_Push_EdgeMerge_Idempotent(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Code {id: 'idemp::s'}) SET n += {id: 'idemp::s', label: 's'}`, nil)
	f.run(t, `MERGE (n:Code {id: 'idemp::t'}) SET n += {id: 'idemp::t', label: 't'}`, nil)
	for i := 0; i < 3; i++ {
		_, err := f.exec.Execute(f.ctx, `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:CALLS]->(b) SET r += $props`, map[string]interface{}{
			"src":   "idemp::s",
			"tgt":   "idemp::t",
			"props": map[string]interface{}{"confidence": "EXTRACTED"},
		})
		require.NoErrorf(t, err, "iter %d", i)
	}
	cnt := f.run(t, "MATCH ({id:'idemp::s'})-[e:CALLS]->({id:'idemp::t'}) RETURN count(e)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// The edge MATCH endpoints are *label-less* (just `{id: $src}`) — pinning that
// graphify's emitted shape works regardless of which label is on the node.
// This is the only place graphify omits a label from a node match.
func TestGraphifyExactShape_Push_EdgeMerge_LabellessEndpointMatch(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	f.run(t, `MERGE (n:Code {id: 'mix::src'}) SET n += {id: 'mix::src'}`, nil)
	f.run(t, `MERGE (n:Document {id: 'mix::tgt'}) SET n += {id: 'mix::tgt'}`, nil)
	_, err := f.exec.Execute(f.ctx, `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:MENTIONS]->(b) SET r += $props`, map[string]interface{}{
		"src":   "mix::src",
		"tgt":   "mix::tgt",
		"props": map[string]interface{}{"confidence": "INFERRED"},
	})
	require.NoError(t, err)
	cnt := f.run(t, "MATCH (a:Code {id:'mix::src'})-[e:MENTIONS]->(b:Document {id:'mix::tgt'}) RETURN count(e)", nil)
	require.Equal(t, int64(1), graphifyToInt64(t, cnt.Rows[0][0]))
}

// ============================================================================
// post-condition read shapes — Shape 5 and Shape 6
// ============================================================================

// Exact node-count shape graphify's falkordb integration test uses.
func TestGraphifyExactShape_NodeCount_AllLabels(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	res := f.run(t, "MATCH (n) RETURN count(n)", nil)
	require.Len(t, res.Rows, 1)
	// fixture seeded 4 nodes (two Code, one Document, one Entity)
	require.Equal(t, int64(4), graphifyToInt64(t, res.Rows[0][0]))
}

// Exact edge-count shape graphify's falkordb integration test uses.
func TestGraphifyExactShape_EdgeCount_AllRelationships(t *testing.T) {
	f := newGraphifyExactShapeFixture(t)
	res := f.run(t, "MATCH ()-[r]->() RETURN count(r)", nil)
	require.Len(t, res.Rows, 1)
	// fixture seeded 3 edges (CALLS, REFERENCES, RELATES_TO)
	require.Equal(t, int64(3), graphifyToInt64(t, res.Rows[0][0]))
}
