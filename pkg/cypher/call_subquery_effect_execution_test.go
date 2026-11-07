package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// callSubqueryEffectFixture is a disposable in-memory procedure that reports
// how many times a statement invoked it, so a caller can observe whether an
// uncorrelated CALL body ran once or once per input row. A WRITE fixture also
// stores one node per invocation, so the observable state matches the
// invocation count.
type callSubqueryEffectFixture struct {
	calls atomic.Int64
	name  string
	mode  ProcedureMode
}

func (f *callSubqueryEffectFixture) register(t *testing.T) {
	t.Helper()
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	if f.mode == "" {
		f.mode = ProcedureModeWrite
	}
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name:      f.name,
		Signature: f.name + "() :: (v :: INTEGER)",
		Mode:      f.mode,
		MinArgs:   0,
		MaxArgs:   0,
		Returns:   []ProcedureColumn{{Name: "v", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		invocation := f.calls.Add(1)
		if f.mode == ProcedureModeWrite {
			if _, err := exec.executeUserStatement(ctx, "CREATE (:CallSubqueryEffect {call: $call})", map[string]interface{}{
				"call": int64(invocation),
			}); err != nil {
				return nil, err
			}
		}
		return &ExecuteResult{Columns: []string{"v"}, Rows: [][]interface{}{{int64(invocation)}}}, nil
	}))
}

// seedCallSubqueryEffectSeeds stores three rows for an uncorrelated CALL to
// read: the statement's row count is the invocation count the body must have.
func seedCallSubqueryEffectSeeds(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(t.Context(), "CREATE (:CallSubquerySeed {name: 'a'}), (:CallSubquerySeed {name: 'b'}), (:CallSubquerySeed {name: 'c'})", nil)
	require.NoError(t, err)
}

// uncorrelatedCallBody is the CALL body under test: a CALL of a registered
// procedure, projected for the outer rows. It imports nothing from the outer
// row, so the body is uncorrelated: nothing it reads changes per input row.
func uncorrelatedCallBody(procedure string) string {
	return "MATCH (n:CallSubquerySeed) CALL { CALL " + procedure + "() YIELD v RETURN v } RETURN n.name AS name, v"
}

// A registered WRITE procedure inside an uncorrelated CALL is a write of the
// body: the body runs once per input row, so each row both sees its own
// invocation's result and the state it stored.
func TestCallSubqueryUncorrelatedWriteProcedureRunsPerInputRow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := &callSubqueryEffectFixture{name: "callsubeffect.perrow"}
		fixture.register(t)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "callsubwrite"))
		seedCallSubqueryEffectSeeds(t, exec)

		result, err := exec.Execute(t.Context(), uncorrelatedCallBody(fixture.name), nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name", "v"}, result.Columns)
		require.Len(t, result.Rows, 3, "every input row keeps one row of its own")

		values := make([]int64, 0, len(result.Rows))
		for _, row := range result.Rows {
			require.NotNil(t, row[0], "the outer row is retained")
			value, ok := row[1].(int64)
			require.True(t, ok, "the call's own result for this row, got %T", row[1])
			values = append(values, value)
		}
		require.ElementsMatch(t, []int64{1, 2, 3}, values,
			"each input row runs the write procedure in its own invocation")
		require.Equal(t, int64(3), fixture.calls.Load(),
			"an uncorrelated CALL body with a registered WRITE procedure runs once per input row")

		state, err := exec.Execute(t.Context(), "MATCH (n:CallSubqueryEffect) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, state.Rows, "each input row keeps the state its invocation stored")
	})
}

// The READ control: an uncorrelated body of a registered READ procedure is no
// write, so the consumer keeps the single shared run of that body and every
// input row still receives its rows.
func TestCallSubqueryUncorrelatedReadProcedureStaysRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		fixture := &callSubqueryEffectFixture{name: "callsubeffect.readrows", mode: ProcedureModeRead}
		fixture.register(t)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "callsubread"))
		seedCallSubqueryEffectSeeds(t, exec)

		body := "CALL " + fixture.name + "() YIELD v RETURN v"
		info := mustAnalyze(t, NewQueryAnalyzer(8), t.Context(), body)
		require.False(t, info.Writes(), "a registered READ procedure body is no write")

		result, err := exec.Execute(t.Context(), uncorrelatedCallBody(fixture.name), nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3, "a shared read body still crosses every input row")
		for _, row := range result.Rows {
			require.Equal(t, int64(1), row[1], "the read body's single result reaches each input row")
		}
		require.Equal(t, int64(1), fixture.calls.Load(), "a read body keeps its single shared run")
	})
}

// The unknown-procedure control: a CALL body naming a procedure the registry
// cannot resolve has an unknown effect, so the consumer treats it as a write
// and the statement reports the missing procedure instead of serving rows.
func TestCallSubqueryUnknownProcedureBodyCountsAsWrite(t *testing.T) {
	const missing = "callsubeffect.unregistered"
	body := "CALL " + missing + "() YIELD v RETURN v"
	info := mustAnalyze(t, NewQueryAnalyzer(8), t.Context(), body)
	require.True(t, info.Writes(), "an unresolvable procedure has an unknown effect, so it is a write")

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "callsubunknown"))
	seedCallSubqueryEffectSeeds(t, exec)
	_, err := exec.Execute(t.Context(), uncorrelatedCallBody(missing), nil)
	require.Error(t, err, "an unresolvable procedure is reported, not run as a read")
	require.ErrorContains(t, err, missing)
}

// The cancellation control: a canceled call reports its cancellation and
// leaves no state behind for the rows the body never reached.
func TestCallSubqueryCanceledCallReportsCancellation(t *testing.T) {
	fixture := &callSubqueryEffectFixture{name: "callsubeffect.canceled"}
	fixture.register(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "callsubcancel"))
	seedCallSubqueryEffectSeeds(t, exec)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := exec.Execute(ctx, uncorrelatedCallBody(fixture.name), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, fixture.calls.Load(), "a canceled call runs no invocation")

	state, err := exec.Execute(t.Context(), "MATCH (n:CallSubqueryEffect) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, state.Rows, "a canceled call stores nothing")
	require.Nil(t, result)
}
