package cypher

import (
	"context"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPropertyDispatchApplicabilityAndResolution(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "dispatch_contract")
	e := NewStorageExecutor(store)
	for _, tc := range []struct {
		name, argument            string
		handled, resolved, failed bool
	}{
		{"unknown_dispatch_contract", "1 / 0", false, false, false},
		{"size", "missing", true, false, false},
		{"size", "properties(1)", true, false, false},
		{"size", "1 / 0", true, false, true},
		{"size", "null", true, true, false},
		{"properties", "{}", true, true, false},
		{"size", "[1]", true, true, false},
	} {
		fixtureAdmission1764Context, fixtureAdmission1764 := expressionTestContext(context.Background(), tc.argument)
		_ = fixtureAdmission1764Context
		_, handled, resolved, err := e.evaluateRowGraphFunction(fixtureAdmission1764, tc.name, tc.argument, pipelineRow{})
		require.Equal(t, tc.handled, handled, tc.name+tc.argument)
		require.Equal(t, tc.resolved, resolved, tc.name+tc.argument)
		require.Equal(t, tc.failed, err != nil, tc.name+tc.argument)
	}
	result, err := e.Execute(context.Background(), "RETURN point({x: COUNT { RETURN 1 }, y: 0}).x AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{float64(1)}}, result.Rows)
	_, err = e.Execute(context.Background(), "CREATE (:Seed {keep:1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"CREATE (:Attempt) CREATE (:Never {v:toString(properties(1))})",
		"CREATE (:Attempt) MERGE (:Never {v:size(properties(1))})",
		"MATCH (n:Seed) SET n.v = toString(properties(1))",
	} {
		before := propertyDurableSnapshot(t, store)
		_, err = e.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		require.Equal(t, before, propertyDurableSnapshot(t, store), query)
	}
}

// Shared readers must select syntax before evaluation and use the row that
// actually reaches the projection or MATCH endpoint.
func TestPropertyDispatchReaderScope(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "dispatch_readers")
	e := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:DispatchPath {id:1})-[:PATH]->(:DispatchPath {id:2})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"WITH 1 AS wanted MATCH p=shortestPath((a:DispatchPath {id:wanted})-[*]-(b:DispatchPath {id:2})) RETURN length(p)", [][]interface{}{{int64(1)}}},
		{"WITH [{k:'a',texts:[1]}] AS grouped,[{k:'a'}] AS rows UNWIND rows AS r WITH r,[g IN grouped WHERE g.k=r.k][0] AS hit RETURN hit.texts", [][]interface{}{{[]interface{}{int64(1)}}}},
	} {
		result, err := e.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
	for _, query := range []string{
		"CREATE (:Attempt) WITH 1 AS wanted MATCH p=shortestPath((a:DispatchPath {id:properties(wanted)})-[*]-(b:DispatchPath {id:2})) SET b.changed=true",
		"CREATE (:Attempt) WITH [1] AS rows UNWIND rows AS r WITH [g IN [1] WHERE properties(r)][0] AS hit RETURN hit",
	} {
		before := propertyDurableSnapshot(t, store)
		_, err := e.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Equal(t, before, propertyDurableSnapshot(t, store), query)
	}
}

// Callback failures belong to dispatch even if a handler discards its error;
// lazy handlers must still be free to leave an argument unevaluated.
func TestPropertyDispatchArgumentFailureBoundary(t *testing.T) {
	cypherfn.Register("property_dispatch_discard_error", func(ctx cypherfn.Context, args []string) (interface{}, error) {
		_, _ = ctx.Eval(args[0])
		return "discarded", nil
	})
	cypherfn.Register("property_dispatch_panic", func(cypherfn.Context, []string) (interface{}, error) {
		panic("registry defect")
	})
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dispatch_failure"))
	for _, tc := range []struct {
		argument string
		failed   bool
	}{
		{"missing", false},
		{"properties(1)", false},
		{"1 / 0", true},
	} {
		fixtureAdmission1765Context, fixtureAdmission1765 := expressionTestContext(context.Background(), tc.argument)
		_ = fixtureAdmission1765Context
		value, handled, resolved, err := e.evaluateRowGraphFunction(fixtureAdmission1765, "property_dispatch_discard_error", tc.argument, pipelineRow{})
		require.Nil(t, value)
		require.True(t, handled)
		require.False(t, resolved)
		require.Equal(t, tc.failed, err != nil)
	}
	fixtureAdmission1766Context, fixtureAdmission1766 := expressionTestContext(context.Background(), "1, 1 / 0")
	_ = fixtureAdmission1766Context
	value, handled, resolved, err := e.evaluateRowGraphFunction(fixtureAdmission1766, "coalesce", "1, 1 / 0", pipelineRow{})
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, resolved)
	require.Equal(t, int64(1), value)
	require.PanicsWithValue(t, "registry defect", func() {
		fixtureAdmission1767Context, fixtureAdmission1767 := expressionTestContext(context.Background(), "")
		_ = fixtureAdmission1767Context
		_, _, _, _ = e.evaluateRowGraphFunction(fixtureAdmission1767, "property_dispatch_panic", "", pipelineRow{})
	})
}
