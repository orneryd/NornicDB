package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// recordingLabelScanEngine records the property projection each label scan
// asks storage for.
type recordingLabelScanEngine struct {
	*storage.MemoryEngine
	projections [][]string
}

func (e *recordingLabelScanEngine) StreamNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	e.projections = append(e.projections, properties)
	return e.MemoryEngine.StreamNodesByLabelProjected(label, properties, visit)
}

func (e *recordingLabelScanEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*storage.Node) error) error {
	e.projections = append(e.projections, properties)
	return e.MemoryEngine.StreamNodesByLabelProjectedInScope(scope, label, properties, visit)
}

func TestPipelineReadOnlyTail(t *testing.T) {
	clauses := func(query string) []pipelineClause {
		parsed, ok := pipelineClausesFor(query)
		require.True(t, ok, query)
		return parsed[1:]
	}
	require.Equal(t, []string{"WITH p.a AS a", "RETURN a"}, pipelineReadOnlyTail(clauses("MATCH (p:P) WITH p.a AS a RETURN a")))
	require.Nil(t, pipelineReadOnlyTail(clauses("MATCH (p:P) SET p.a = 1 RETURN p.a")))
	require.Nil(t, pipelineReadOnlyTail(clauses("MATCH (p:P) RETURN *")))
	require.Nil(t, pipelineReadOnlyTail(clauses("MATCH (p:P) WITH DISTINCT * RETURN p.a")))
	require.Nil(t, pipelineReadOnlyTail(clauses("MATCH (p:P) WITH p.a AS a UNWIND [a] AS b WITH b")))
	require.Nil(t, pipelineReadOnlyTail(nil))
}

func TestPipelineLabelScanProjection(t *testing.T) {
	ctx := context.Background()
	pattern := nodePatternInfo{variable: "p", labels: []string{"P"}, properties: map[string]interface{}{"k": 1}}
	properties, ok := pipelineLabelScanProjection(ctx, pattern, "p.w > 1", []string{"WITH p.b AS b", "RETURN b, p.a"})
	require.True(t, ok)
	require.Equal(t, []string{"a", "b", "k", "w"}, properties)

	for _, tail := range [][]string{{"RETURN p"}, {"WITH p", "RETURN p.a"}, {"MATCH (p)-->(q)", "RETURN q.a"}} {
		_, ok = pipelineLabelScanProjection(ctx, pattern, "", tail)
		require.False(t, ok, "%v", tail)
	}
	_, ok = pipelineLabelScanProjection(ctx, pattern, "p = $other", []string{"RETURN p.a"})
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(ctx, nodePatternInfo{variable: "p"}, "", []string{"RETURN p.a"})
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(ctx, pattern, "", nil)
	require.False(t, ok)
	_, ok = pipelineLabelScanProjection(WithTemporalViewport(ctx, CurrentTemporalViewport()), pattern, "", []string{"RETURN p.a"})
	require.False(t, ok)
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

	rows, projection = run("MATCH (p:P) RETURN p ORDER BY p.a LIMIT 1")
	require.Nil(t, projection)
	node := rows[0][0].(*storage.Node)
	require.Equal(t, "x", node.Properties["big"])

	rows, projection = run("MATCH (where:Q) RETURN where.id AS v")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows)
	require.Equal(t, []string{"id"}, projection)
}

// A keyword-named variable followed by a property access is a reference.
func TestSemanticExpressionReferencesKeywordVariableProperty(t *testing.T) {
	require.Equal(t, []string{"where.id", "x"}, semanticExpressionReferences("where.id + x"))
	require.Equal(t, []string{"x"}, semanticExpressionReferences("x IS NOT NULL AND true"))
}
