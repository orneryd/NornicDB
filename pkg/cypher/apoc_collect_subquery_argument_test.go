package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// collectSubqueryApocArgument is an APOC call whose second argument is a
// COLLECT { } subquery expression with a relationship pattern in its body. It
// is one expression for the evaluator that owns the call, and the subquery's
// body is read where it stands by the row evaluator that owns subquery
// expressions: the two evaluators must not hand the same text to each other.
const collectSubqueryApocArgument = "apoc.coll.subtract(x, COLLECT { MATCH (o:PDCollection)<-[:MEMBER_OF]-(m:PDDocument) RETURN m.id AS id })"

// collectSubqueryApocSeed creates the graph the assertions read: document d1 is
// a member of collection d2, so the subquery's list is ['d1'] and subtracting
// it from ['d2', 'd1'] leaves ['d2']. A call that skipped the subquery would
// return both elements.
func collectSubqueryApocSeed(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(t.Context(), "CREATE (d:PDDocument {id: 'd1'})-[:MEMBER_OF]->(c:PDCollection {id: 'd2'})", nil)
	require.NoError(t, err)
}

// TestApocCollectSubqueryArgumentWithRelationshipPattern proves an APOC call
// with a COLLECT { } argument that has a relationship pattern returns the call's
// list, in a projection, in a CASE, and in a WITH projection, within the
// statement's deadline instead of passing the expression between evaluators
// without end (NOR-409).
func TestApocCollectSubqueryArgumentWithRelationshipPattern(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		collectSubqueryApocSeed(t, exec)

		for _, test := range []struct {
			name  string
			query string
		}{
			{
				// The reported statement, unchanged.
				name:  "reported projection",
				query: "WITH ['d2'] AS x RETURN " + collectSubqueryApocArgument + " AS y",
			},
			{
				name:  "projection filters the subquery's list",
				query: "WITH ['d2', 'd1'] AS x RETURN " + collectSubqueryApocArgument + " AS y",
			},
			{
				name:  "CASE branch",
				query: "WITH ['d2', 'd1'] AS x RETURN CASE WHEN true THEN " + collectSubqueryApocArgument + " ELSE [] END AS y",
			},
			{
				name:  "WITH projection keeping the scope",
				query: "WITH ['d2', 'd1'] AS x WITH x, " + collectSubqueryApocArgument + " AS y RETURN y",
			},
			{
				name:  "WITH projection replacing the scope",
				query: "WITH ['d2', 'd1'] AS x WITH " + collectSubqueryApocArgument + " AS y RETURN y",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				result, err := exec.Execute(ctx, test.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{[]interface{}{"d2"}}}, result.Rows)
			})
		}
	})
}

// TestApocCollectSubqueryArgumentControls proves the shapes that worked before
// the correction still work: the same call with the subquery bound to a
// variable first, with a subquery body that has no relationship pattern, and the
// subquery expression on its own.
func TestApocCollectSubqueryArgumentControls(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		collectSubqueryApocSeed(t, exec)

		for _, test := range []struct {
			name  string
			query string
			want  [][]interface{}
		}{
			{
				name:  "subquery bound to a variable first",
				query: "WITH ['d2', 'd1'] AS x, COLLECT { MATCH (o:PDCollection)<-[:MEMBER_OF]-(m:PDDocument) RETURN m.id AS id } AS open RETURN apoc.coll.subtract(x, open) AS y",
				want:  [][]interface{}{{[]interface{}{"d2"}}},
			},
			{
				name:  "subquery body without a relationship pattern",
				query: "WITH ['d2', 'd1'] AS x RETURN apoc.coll.subtract(x, COLLECT { MATCH (m:PDDocument) RETURN m.id AS id }) AS y",
				want:  [][]interface{}{{[]interface{}{"d2"}}},
			},
			{
				name:  "subquery expression on its own",
				query: "WITH ['d2', 'd1'] AS x RETURN COLLECT { MATCH (o:PDCollection)<-[:MEMBER_OF]-(m:PDDocument) RETURN m.id AS id } AS y",
				want:  [][]interface{}{{[]interface{}{"d1"}}},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				result, err := exec.Execute(ctx, test.query, nil)
				require.NoError(t, err)
				require.Equal(t, test.want, result.Rows)
			})
		}
	})
}

// TestApocCollectSubqueryArgumentStopsOnCancellation proves the call's
// evaluation observes the statement's cancellation instead of leaving its
// work running: a deadline already in effect fails the statement promptly with
// the context's error.
func TestApocCollectSubqueryArgumentStopsOnCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		collectSubqueryApocSeed(t, exec)
		expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()

		start := time.Now()
		_, err := exec.Execute(expired, "WITH ['d2', 'd1'] AS x RETURN "+collectSubqueryApocArgument+" AS y", nil)
		elapsed := time.Since(start)

		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled), "got %v", err)
		require.Less(t, elapsed, 5*time.Second, "canceled evaluation kept running")
	})
}

// TestRowPatternFallbackStopsAtDispatchBound proves the relationship-pattern
// fallback carries the same bounded dispatch as the value fallback: at the
// deepest handoff it reports the expression as unresolved through the checked
// contract rather than handing it over again.
func TestRowPatternFallbackStopsAtDispatchBound(t *testing.T) {
	exec, _ := newTestExecutor(t)
	values := pipelineRow{"x": []interface{}{"d2"}, rowStatementContextKey: context.Background()}
	ctx := context.WithValue(withValueBindings(t.Context(), values), expressionFailureKey{}, &expressionFailure{})
	ctx = context.WithValue(ctx, rowFallbackExpressionKey{}, rowFallbackState{depth: maxRowFallbackDepth})

	value, resolved := exec.evaluateRowExpressionWithContext(ctx, collectSubqueryApocArgument, values)

	require.False(t, resolved)
	require.Nil(t, value)
	require.Error(t, getExpressionFailure(ctx))
}

// Cancel from a function actually evaluated inside COLLECT, rather than from
// request admission. The statement must stop after evaluation has begun.
func TestApocCollectSubqueryArgumentStopsAfterInFlightCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		collectSubqueryApocSeed(t, exec)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		calls := 0
		var canceledAt time.Time
		PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
			if name == "audit.cancelCollect" {
				return func(value interface{}) interface{} {
					calls++
					if calls == 1 {
						canceledAt = time.Now()
						cancel()
					}
					return value
				}, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		})
		_, err := exec.Execute(ctx, "WITH ['d2', 'd1'] AS x RETURN apoc.coll.subtract(x, COLLECT { MATCH (o:PDCollection)<-[:MEMBER_OF]-(m:PDDocument) RETURN audit.cancelCollect(m.id) AS id }) AS y", nil)
		require.Equal(t, 1, calls, "cancellation must originate inside subquery work")
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(canceledAt), 100*time.Millisecond, "backend work exceeded the post-cancellation budget")
	})
}
