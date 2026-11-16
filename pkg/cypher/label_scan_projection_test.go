package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// recordedNodeImage is one node image a label read delivered to the row
// pipeline: the identity every image carries, the decoded size of each
// property, and how many vectors it holds. A test reads which property bodies
// the statement acquired from this record; nothing here is timed.
type recordedNodeImage struct {
	id              storage.NodeID
	labels          []string
	propertySizes   map[string]int
	chunkEmbeddings int
}

// newRecordedNodeImage records what storage already materialized for one
// delivered node. A string property's decoded size is its length, so a test
// can name the bytes it did not want read; a non-string value records 0.
func newRecordedNodeImage(node *storage.Node) recordedNodeImage {
	image := recordedNodeImage{
		id:              node.ID,
		labels:          append([]string(nil), node.Labels...),
		propertySizes:   make(map[string]int, len(node.Properties)),
		chunkEmbeddings: len(node.ChunkEmbeddings),
	}
	for name, value := range node.Properties {
		text, _ := value.(string)
		image.propertySizes[name] = len(text)
	}
	return image
}

// recordedNodeScan is one label-match read a statement asked storage for: the
// user properties it named, the hydration it asked for, and the images it
// delivered. A nil projection is the whole-body demand (every stored property);
// an empty non-nil projection is the header-only demand (identity and labels, no
// user property).
type recordedNodeScan struct {
	projection []string
	queryRow   bool
	images     []recordedNodeImage
}

// recordingLabelScanEngine records the property projection each label scan
// asks storage for, and the property bodies it then delivers.
type recordingLabelScanEngine struct {
	*storage.MemoryEngine
	projections [][]string
	scans       []recordedNodeScan
}

// Pin the optional capability: a signature drift must fail compilation.
var _ storage.ScopedLabelNodeReader = (*recordingLabelScanEngine)(nil)

// beginScan appends one scan record and returns its index. The index, not a
// pointer, keeps the record addressable across later appends.
func (e *recordingLabelScanEngine) beginScan(projection []string, queryRow bool) int {
	e.scans = append(e.scans, recordedNodeScan{projection: projection, queryRow: queryRow})
	return len(e.scans) - 1
}

// recordImage records one image of the scan at index.
func (e *recordingLabelScanEngine) recordImage(index int, node *storage.Node) {
	if node == nil {
		return
	}
	e.scans[index].images = append(e.scans[index].images, newRecordedNodeImage(node))
}

func (e *recordingLabelScanEngine) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	e.projections = append(e.projections, properties)
	scan := e.beginScan(properties, false)
	return e.MemoryEngine.StreamNodesByLabelProjected(ctx, label, properties, func(node *storage.Node) error {
		e.recordImage(scan, node)
		return visit(node)
	})
}

func (e *recordingLabelScanEngine) StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*storage.Node) error) error {
	e.projections = append(e.projections, properties)
	scan := e.beginScan(properties, false)
	return e.MemoryEngine.StreamNodesByLabelProjectedInScope(ctx, scope, label, properties, func(node *storage.Node) error {
		e.recordImage(scan, node)
		return visit(node)
	})
}

// StreamNodesWithOptions is the other label read a statement may take: the
// options kernel, whose projection and query-row hydration say what it decodes.
func (e *recordingLabelScanEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	scan := e.beginScan(opts.Projection, opts.QueryNodes)
	return e.MemoryEngine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
		e.recordImage(scan, node)
		return visit(node)
	})
}

// readTailOf returns the pipeline clauses after the MATCH of query: the
// read-only tail a label scan narrows its property projection with.
func readTailOf(t *testing.T, query string) []pipelineClause {
	t.Helper()
	parsed, ok := pipelineClausesFor(t.Context(), query)
	require.True(t, ok, query)
	return parsed[1:]
}

// readTailTexts is the retained clause texts of tail, so an expectation can
// name the clauses a scan was allowed to read.
func readTailTexts(tail []pipelineClause) []string {
	if tail == nil {
		return nil
	}
	texts := make([]string, len(tail))
	for index, clause := range tail {
		texts[index] = clause.text
	}
	return texts
}

func TestPipelineReadOnlyTail(t *testing.T) {
	require.Equal(t, []string{"WITH p.a AS a", "RETURN a"},
		readTailTexts(pipelineReadOnlyTail(readTailOf(t, "MATCH (p:P) WITH p.a AS a RETURN a"))))
	require.Nil(t, pipelineReadOnlyTail(readTailOf(t, "MATCH (p:P) SET p.a = 1 RETURN p.a")))
	require.Nil(t, pipelineReadOnlyTail(readTailOf(t, "MATCH (p:P) RETURN *")))
	require.Nil(t, pipelineReadOnlyTail(readTailOf(t, "MATCH (p:P) WITH DISTINCT * RETURN p.a")))
	require.Nil(t, pipelineReadOnlyTail(readTailOf(t, "MATCH (p:P) WITH p.a AS a UNWIND [a] AS b WITH b")))
	require.Nil(t, pipelineReadOnlyTail(nil))
}

func TestPipelineLabelScanProjection(t *testing.T) {
	ctx := context.Background()
	pattern := nodePatternInfo{variable: "p", labels: []string{"P"}, properties: map[string]interface{}{"k": 1}}
	properties, ok := pipelineLabelScanProjection(ctx, pattern, "p.w > 1", readTailOf(t, "MATCH (p:P {k: 1}) WHERE p.w > 1 WITH p.b AS b RETURN b"))
	require.True(t, ok)
	require.Equal(t, []string{"b", "k", "w"}, properties)

	// A whole-node read keeps every property body: the variable is returned,
	// passes through a WITH into a RETURN of the variable, or is read by a
	// later pattern; a comparison against the variable is a whole-node read
	// too.
	whole := []string{
		"MATCH (p:P) RETURN p",
		"MATCH (p:P) WITH p AS q RETURN q",
		"MATCH (p:P) MATCH (p)-->(q) RETURN q.a",
		"MATCH (p:P) WITH p AS q RETURN properties(q)",
		"MATCH (p:P) WITH collect(p) AS ps RETURN size(ps)",
		"MATCH (p:P) RETURN custom.probe(p)",
		"MATCH (p:P) RETURN p {.a}",
		"MATCH (p:P) RETURN [p]",
		"MATCH (p:P) RETURN {node: p}",
		"MATCH (p:P) RETURN p.a + p",
		"MATCH (p:P) RETURN p['a' + 'b']",
	}
	for _, query := range whole {
		_, ok = pipelineLabelScanProjection(ctx, pattern, "", readTailOf(t, query))
		require.False(t, ok, query)
	}
	_, ok = pipelineLabelScanProjection(ctx, pattern, "p = $other", readTailOf(t, "MATCH (p:P) WHERE p = $other RETURN p.a"))
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(ctx, nodePatternInfo{variable: "p"}, "", readTailOf(t, "MATCH (p) RETURN p.a"))
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(ctx, pattern, "", nil)
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(WithTemporalViewport(ctx, CurrentTemporalViewport()), pattern, "", readTailOf(t, "MATCH (p:P) RETURN p.a"))
	require.False(t, ok)

	// A WITH carries the variable to the clauses after it under the item's own
	// name, and only what those clauses read is decoded: a rename is followed,
	// and identity, labels, existence and a list's size read the header alone.
	for _, tc := range []struct {
		query      string
		properties []string
	}{
		{"MATCH (p:P) WITH p RETURN p.a", []string{"a"}},
		{"MATCH (p:P) WITH p AS q RETURN q.a", []string{"a"}},
		{"MATCH (p:P) WITH p AS q, p.a AS a RETURN a, q.b", []string{"a", "b"}},
		{"MATCH (p:P) WITH p AS q RETURN id(q), labels(q), q IS NULL", []string{}},
		{"MATCH (p:P) WITH p AS q RETURN size([q]), size(collect(q))", []string{}},
		{"MATCH (p:P) WITH p RETURN 1", []string{}},
		{"MATCH (p:P) RETURN id(p), labels(p), p IS NOT NULL, p['a'], (p).a, head([p]).a, size([p])", []string{"a"}},
		{"MATCH (p:P) RETURN p.a AS a ORDER BY p.b DESC SKIP 0 LIMIT 1", []string{"a", "b"}},
		{"MATCH (p:P) RETURN id(p) ORDER BY id(p)", []string{}},
		// Quoted, commented and nested occurrences keep the boundaries they
		// have: a quoted member names that property, a comment is trivia, and
		// a chain or an element of a list still reads the named property.
		{"MATCH (p:P) /* the read */ RETURN p.a /* trailing */", []string{"a"}},
		{"MATCH (p:P) RETURN p.a.b, p['a b'], p.`a``.b`", []string{"a", "a b", "a`.b"}},
		{"MATCH (p:P) RETURN head([p, p]).a, collect(p.a)", []string{"a"}},
		// An unknown call, a procedure or native code owns its argument: the
		// property it is handed stays that property, the variable itself keeps
		// the whole body.
		{"MATCH (p:P) RETURN custom.probe(p.a), custom.probe(p.b)", []string{"a", "b"}},
		{"MATCH (p:P) WITH p AS q, count(*) AS c RETURN labels(q), c", []string{}},
		// A WITH's attached WHERE filters the rows the WITH produced, so it is
		// a consumer of the variable too.
		{"MATCH (p:P) WITH p AS q WHERE q.b = 1 RETURN id(q)", []string{"b"}},
		{"MATCH (p:P) WITH p AS q WHERE q IS NOT NULL RETURN labels(q)", []string{}},
	} {
		properties, ok := pipelineLabelScanProjection(ctx, nodePatternInfo{variable: "p", labels: []string{"P"}}, "", readTailOf(t, tc.query))
		require.True(t, ok, tc.query)
		require.Equal(t, tc.properties, properties, tc.query)
	}
}

// A label scan reads only the properties the statement uses through the
// variable; any whole-node use reads every property, and the answers are the
// same either way.
func TestLabelScanReadsOnlyUsedProperties(t *testing.T) {
	ctx := context.Background()
	store := &recordingLabelScanEngine{MemoryEngine: newTestMemoryEngine(t)}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(store, "test"))
	_, err := exec.Execute(ctx, "UNWIND range(1, 3) AS i CREATE (:P {a: i, b: i * 10, big: 'x'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Q {id: 1, other: 2})", nil)
	require.NoError(t, err)

	run := func(query string) ([][]interface{}, []string) {
		t.Helper()
		store.projections = nil
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.NotEmpty(t, store.projections, query)
		return result.Rows, store.projections[len(store.projections)-1]
	}

	rows, projection := run("MATCH (p:P) RETURN p.a % 2 AS k, count(*) AS c ORDER BY k")
	require.Equal(t, [][]interface{}{{int64(0), int64(1)}, {int64(1), int64(2)}}, rows)
	require.Equal(t, []string{"a"}, projection)

	rows, projection = run("MATCH (p:P) WITH p.b AS b RETURN b ORDER BY b")
	require.Equal(t, [][]interface{}{{int64(10)}, {int64(20)}, {int64(30)}}, rows)
	require.Equal(t, []string{"b"}, projection)

	store.projections = nil
	result, err := exec.Execute(ctx, "MATCH (p:P) RETURN p ORDER BY p.a LIMIT 1", nil)
	require.NoError(t, err)
	require.Empty(t, store.projections, "label IDs feed query reads without decoding discarded projected bodies")
	rows = result.Rows
	node := rows[0][0].(*storage.Node)
	require.Equal(t, "x", node.Properties["big"])

	rows, projection = run("MATCH (where:Q) RETURN where.id AS v")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Equal(t, []string{"id"}, projection)

	// A WITH that renames the variable carries it on: the clauses after the
	// WITH still decide the demand, under the name the WITH binds.
	rows, projection = run("MATCH (p:P) WITH p AS q RETURN q.b AS b ORDER BY b")
	require.Equal(t, [][]interface{}{{int64(10)}, {int64(20)}, {int64(30)}}, rows)
	require.Equal(t, []string{"b"}, projection)

	// Identity and labels are answered from the image's header, so a statement
	// that reads only those decodes no property body.
	rows, projection = run("MATCH (p:P) WITH p AS q RETURN labels(q) AS l")
	require.Equal(t, [][]interface{}{{[]interface{}{"P"}}, {[]interface{}{"P"}}, {[]interface{}{"P"}}}, rows)
	require.Empty(t, projection)
}

// A keyword-named variable followed by a property access is a reference.
func TestSemanticExpressionReferencesKeywordVariableProperty(t *testing.T) {
	type reference struct {
		variable, property string
		hasProperty        bool
	}
	for _, tc := range []struct {
		expression string
		want       []reference
	}{
		{"where.id + x", []reference{{"where", "id", true}, {"x", "", false}}},
		{"x IS NOT NULL AND true", []reference{{"x", "", false}}},
	} {
		var got []reference
		require.True(t, forEachSemanticExpressionReference(tc.expression, func(variable, property string, hasProperty bool, _ int) bool {
			got = append(got, reference{variable, property, hasProperty})
			return true
		}))
		require.Equal(t, tc.want, got)
	}
}
