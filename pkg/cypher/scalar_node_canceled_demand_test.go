package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The demand analysis a MATCH's narrowing depends on reads the statement's own
// context. A statement canceled while the semantic scan is reading has not read
// every call, so the demand is not complete: every read keeps the whole
// property body instead of narrowing on a partial analysis, and the query that
// owns the context reports the cancellation.
//
// The controls below are the same owners under a live context, which is the
// narrowing the statement asked for.
func TestCanceledDemandAnalysisDeclinesNarrowing(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	tail := readTailOf(t, "MATCH (p:P) RETURN p.a")
	pattern := nodePatternInfo{variable: "p", labels: []string{"P"}}
	match := &TraversalMatch{
		StartNode: nodePatternInfo{variable: "a", labels: []string{"P"}},
		EndNode:   nodePatternInfo{variable: "p", labels: []string{"Q"}},
	}
	items := []returnItem{{expr: "count(p)", alias: "c"}}

	live := t.Context()
	canceled, cancel := context.WithCancel(live)
	cancel()
	require.ErrorIs(t, canceled.Err(), context.Canceled)

	// The label scan's projection.
	properties, complete := pipelineLabelScanProjection(live, pattern, "", tail)
	require.True(t, complete, "a live analysis narrows the scan")
	require.Equal(t, []string{"a"}, properties)
	properties, complete = pipelineLabelScanProjection(canceled, pattern, "", tail)
	require.False(t, complete, "a canceled analysis keeps every body")
	require.Nil(t, properties)

	// The traversal endpoint's projection.
	require.Equal(t, []string{"a"}, traversalSourceProperties(live, pattern, "", tail, ""))
	require.Nil(t, traversalSourceProperties(canceled, pattern, "", tail, ""),
		"a canceled analysis keeps every body for the endpoint too")

	// The traversal's header-only decision: p.a is the only property the tail
	// reads, so p alone is read while a names none.
	require.Equal(t, map[string]bool{"a": true},
		e.traversalUnusedPropertyVariablesForTail(live, match, "", tail))
	require.Nil(t, e.traversalUnusedPropertyVariablesForTail(canceled, match, "", tail),
		"a canceled analysis reports no variable as unused")

	// The traversal's expression form of the same decision.
	require.Equal(t, map[string]bool{"a": true, "p": true},
		e.traversalUnusedPropertyVariables(live, match, items, ""))
	require.Nil(t, e.traversalUnusedPropertyVariables(canceled, match, items, ""),
		"a canceled analysis reports no variable as unused")

	// The canonical demand owner both forms ask.
	properties, complete = pipelineNodePropertyDemand(live, "p", "", nil, tail)
	require.True(t, complete)
	require.Equal(t, []string{"a"}, properties)
	properties, complete = pipelineNodePropertyDemand(canceled, "p", "", nil, tail)
	require.False(t, complete)
	require.Nil(t, properties)
}

// A statement that arrives already canceled never narrows: the public executor
// reports the caller's cancellation before it makes the narrowed label read the
// live statement makes. The demand analysis itself is proven not to narrow on a
// partial analysis by TestCanceledDemandAnalysisDeclinesNarrowing, which the
// live statement here reaches through the same owners.
func TestCanceledStatementKeepsWholePropertyBodies(t *testing.T) {
	for _, prefix := range scalarNodeStatementPrefixes {
		statement := prefix + "MATCH (n:EIBig) RETURN id(n)"

		// Control: the live statement reads the header only.
		graph := newScalarNodeDemandGraph(t, true)
		graph.run(t, statement)
		projection := graph.singleScan(t).projection
		require.NotNil(t, projection, statement)
		require.Empty(t, projection, statement)

		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		graph.store.scans = nil
		_, err := graph.exec.Execute(canceled, statement, nil)
		require.ErrorIs(t, err, context.Canceled, statement)
		require.Empty(t, graph.store.scans,
			"%sthe canceled statement is answered before any label read can narrow its projection", prefix)
	}
}
