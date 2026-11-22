package cypher

// Author: NOR-315 reproduction lane (unit tests), 2026-11-06.
//
// NOR-315 records one pending defect beside the procedure-declaration work:
// "CALL with YIELD panics when a procedure returns (nil, nil)", found at
// pkg/cypher/call.go:1159 in executeProcedureCall's applyYieldFilter call.
// The approved contract for a handler that reports no failure and returns no
// result is an empty result or a clear error, never a panic; the exact rows
// are not invented.
//
// This file is reproduction-only: it adds no production behavior. It pins that
// contract on the public Execute entry point, on both parsers
// (config.ParserTypeNornic and config.ParserTypeANTLR), for each invocation
// form where YIELD is legal, with the two controls that separate an empty
// result from a suppressed one:
//
//   - a handler returning (nil, nil) serves the declaration's columns and no
//     rows through CALL, YIELD <column>, an aliased YIELD column, YIELD *, a
//     YIELD tail pipeline, a call inside a query pipeline and a call inside a
//     CALL subquery;
//   - a handler returning an explicit empty result keeps exactly its own
//     columns and no rows, so the empty-result contract only replaces a
//     missing result;
//   - a handler returning a non-nil error still fails the call instead of
//     becoming an empty result;
//   - EXPLAIN plans the call without invoking the handler, PROFILE invokes it
//     and serves the declared empty result.
//
// The handler counts its invocations, so each case shows whether the result
// came from the handler or from a call that never ran it. Every registration
// is scoped by the shared registration owner (scopeProcedureRegistration); no
// test clears the user registry.
//
// Declaration facts stay separate from the panic contract. Arity is decided by
// the compile-time owner (result_columns_compile.go, admitProcedureInvocation),
// so a wrong arity is rejected in every mode before the handler runs. The YIELD
// column is not: at this source the run-time decider in applyYieldFilter still
// answers it after the handler ran, and EXPLAIN plans the call. That remaining
// duplicate decider is NOR-315's consolidation work, not this issue's panic, so
// the cases below pin only what the approved contract requires there — a clear
// error for RUN and PROFILE, a plan or an error for EXPLAIN, and never a panic.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	portedNor315NilProcedureName     = "portedNor315.nilResult"
	portedNor315EmptyProcedureName   = "portedNor315.emptyResult"
	portedNor315FailingProcedureName = "portedNor315.nilResultError"
	portedNor315YieldColumn          = "value"
)

// portedNor315NilHandler returns no result and no error: the exact handler the
// pending defect reports.
func portedNor315NilHandler(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
	return nil, nil
}

// portedNor315EmptyHandler returns a valid, explicitly empty result over the
// declared column: the declared-empty control, distinct from (nil, nil).
func portedNor315EmptyHandler(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
	return &ExecuteResult{Columns: []string{portedNor315YieldColumn}, Rows: [][]interface{}{}}, nil
}

// portedNor315FailingHandler reports a failure: the control that keeps an error an
// error instead of an empty result.
func portedNor315FailingHandler(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
	return nil, errors.New("portedNor315 handler failed")
}

// portedNor315ProcedureSpec declares arity 0 and one INTEGER output named value, so
// the declaration-fact cases read one registration.
func portedNor315ProcedureSpec(name string) ProcedureSpec {
	return ProcedureSpec{
		Name:      name,
		Signature: name + "() :: (" + portedNor315YieldColumn + " :: INTEGER)",
		Mode:      ProcedureModeRead,
		MinArgs:   0,
		MaxArgs:   0,
		Returns:   []ProcedureColumn{{Name: portedNor315YieldColumn, Type: "INTEGER"}},
	}
}

// portedNor315Register publishes handler under name for the duration of the test
// through the scoped registration owner and returns its invocation counter.
func portedNor315Register(t *testing.T, name string, handler ProcedureHandler) *atomic.Int64 {
	t.Helper()
	var invocations atomic.Int64
	registerProcedureForTest(t, portedNor315ProcedureSpec(name), func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		invocations.Add(1)
		return handler(ctx, exec, cypher, args)
	})
	return &invocations
}

// portedNor315RequireEmptyResult runs query and requires the declared empty result:
// the procedure's columns and no rows, never a missing result (a panic fails
// the test run on its own).
func portedNor315RequireEmptyResult(t *testing.T, exec *StorageExecutor, query string, columns []string) {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	require.NotNil(t, result, "%s: a procedure call always serves a result", query)
	require.Equal(t, columns, result.Columns, query)
	require.Empty(t, result.Rows, query)
}

// A handler's (nil, nil) is the declared empty result in every invocation form
// where YIELD is legal, on both parsers.
func TestPortedNilResultProcedureCallServesDeclaredEmptyResultOnBothParsers(t *testing.T) {
	calls := portedNor315Register(t, portedNor315NilProcedureName, portedNor315NilHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		for _, tc := range []struct {
			name    string
			query   string
			columns []string
		}{
			{
				name:    "no yield",
				query:   "CALL " + portedNor315NilProcedureName + "()",
				columns: []string{portedNor315YieldColumn},
			},
			{
				name:    "yield column",
				query:   "CALL " + portedNor315NilProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
				columns: []string{portedNor315YieldColumn},
			},
			{
				name:    "yield column alias",
				query:   "CALL " + portedNor315NilProcedureName + "() YIELD " + portedNor315YieldColumn + " AS kept RETURN kept",
				columns: []string{"kept"},
			},
			{
				name:    "yield wildcard",
				query:   "CALL " + portedNor315NilProcedureName + "() YIELD *",
				columns: []string{portedNor315YieldColumn},
			},
			{
				name:    "yield tail pipeline",
				query:   "CALL " + portedNor315NilProcedureName + "() YIELD " + portedNor315YieldColumn + " WITH " + portedNor315YieldColumn + " WHERE " + portedNor315YieldColumn + " > 0 RETURN " + portedNor315YieldColumn + " ORDER BY " + portedNor315YieldColumn + " LIMIT 5",
				columns: []string{portedNor315YieldColumn},
			},
			{
				name:    "in query pipeline",
				query:   "UNWIND [1, 2] AS seed CALL " + portedNor315NilProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN seed, " + portedNor315YieldColumn,
				columns: []string{"seed", portedNor315YieldColumn},
			},
			{
				name:    "call subquery",
				query:   "CALL { CALL " + portedNor315NilProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn + " } RETURN " + portedNor315YieldColumn,
				columns: []string{portedNor315YieldColumn},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				before := calls.Load()
				portedNor315RequireEmptyResult(t, exec, tc.query, tc.columns)
				require.Greater(t, calls.Load(), before, "%s: the handler ran", tc.query)
			})
		}
	})
}

// The declared-empty control: a handler that returns a valid empty result of
// its own keeps exactly those columns and rows, on both parsers and in every
// invocation form.
func TestPortedEmptyResultProcedureCallControlOnBothParsers(t *testing.T) {
	portedNor315Register(t, portedNor315EmptyProcedureName, portedNor315EmptyHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		for _, tc := range []struct {
			name    string
			query   string
			columns []string
		}{
			{name: "no yield", query: "CALL " + portedNor315EmptyProcedureName + "()", columns: []string{portedNor315YieldColumn}},
			{name: "yield column", query: "CALL " + portedNor315EmptyProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn, columns: []string{portedNor315YieldColumn}},
			{name: "yield wildcard", query: "CALL " + portedNor315EmptyProcedureName + "() YIELD *", columns: []string{portedNor315YieldColumn}},
			{name: "yield tail pipeline", query: "CALL " + portedNor315EmptyProcedureName + "() YIELD " + portedNor315YieldColumn + " WITH " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn, columns: []string{portedNor315YieldColumn}},
			{name: "in query pipeline", query: "UNWIND [1] AS seed CALL " + portedNor315EmptyProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN seed, " + portedNor315YieldColumn, columns: []string{"seed", portedNor315YieldColumn}},
			{name: "call subquery", query: "CALL { CALL " + portedNor315EmptyProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn + " } RETURN " + portedNor315YieldColumn, columns: []string{portedNor315YieldColumn}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				portedNor315RequireEmptyResult(t, exec, tc.query, tc.columns)
			})
		}
	})
}

// The error control: a handler that reports a failure still fails the call on
// both parsers, with YIELD and without it, instead of becoming an empty
// result.
func TestPortedFailingProcedureHandlerStaysAnErrorOnBothParsers(t *testing.T) {
	portedNor315Register(t, portedNor315FailingProcedureName, portedNor315FailingHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		for _, query := range []string{
			"CALL " + portedNor315FailingProcedureName + "()",
			"CALL " + portedNor315FailingProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
			"CALL " + portedNor315FailingProcedureName + "() YIELD *",
			"CALL " + portedNor315FailingProcedureName + "() YIELD " + portedNor315YieldColumn + " WITH " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
			"UNWIND [1] AS seed CALL " + portedNor315FailingProcedureName + "() YIELD " + portedNor315YieldColumn + " RETURN seed, " + portedNor315YieldColumn,
		} {
			t.Run(query, func(t *testing.T) {
				result, err := exec.Execute(context.Background(), query, nil)
				require.ErrorContains(t, err, "portedNor315 handler failed", query)
				require.Nil(t, result, query)
			})
		}
	})
}

// EXPLAIN plans the call from the declaration without invoking the handler, and
// PROFILE invokes it and serves the same declared empty result: neither mode
// panics on the (nil, nil) handler, and neither invents rows.
func TestPortedNilResultProcedureExplainAndProfileOnBothParsers(t *testing.T) {
	calls := portedNor315Register(t, portedNor315NilProcedureName, portedNor315NilHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		t.Run("explain", func(t *testing.T) {
			before := calls.Load()
			result, err := exec.Execute(context.Background(),
				"EXPLAIN CALL "+portedNor315NilProcedureName+"() YIELD "+portedNor315YieldColumn+" RETURN "+portedNor315YieldColumn, nil)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, []string{portedNor315YieldColumn}, result.Columns)
			require.Empty(t, result.Rows)
			require.Equal(t, before, calls.Load(), "EXPLAIN must not invoke the handler")
		})
		t.Run("profile", func(t *testing.T) {
			before := calls.Load()
			result, err := exec.Execute(context.Background(),
				"PROFILE CALL "+portedNor315NilProcedureName+"() YIELD "+portedNor315YieldColumn+" RETURN "+portedNor315YieldColumn, nil)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, []string{portedNor315YieldColumn}, result.Columns)
			require.Empty(t, result.Rows)
			require.Greater(t, calls.Load(), before, "PROFILE runs the statement, so the handler ran")
		})
	})
}

// A wrong arity is the compile-time owner's rejection in every mode and on
// both parsers, before the handler runs: the (nil, nil) handler never turns a
// declaration error into an empty result.
func TestPortedNilResultProcedureArityRejectedBeforeHandlerOnBothParsers(t *testing.T) {
	calls := portedNor315Register(t, portedNor315NilProcedureName, portedNor315NilHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		for _, query := range []string{
			"CALL " + portedNor315NilProcedureName + "(1) YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
			"EXPLAIN CALL " + portedNor315NilProcedureName + "(1) YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
			"PROFILE CALL " + portedNor315NilProcedureName + "(1) YIELD " + portedNor315YieldColumn + " RETURN " + portedNor315YieldColumn,
			"UNWIND [1] AS seed CALL " + portedNor315NilProcedureName + "(1) YIELD " + portedNor315YieldColumn + " RETURN seed",
		} {
			t.Run(query, func(t *testing.T) {
				before := calls.Load()
				result, err := exec.Execute(context.Background(), query, nil)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				require.Nil(t, result, query)
				require.Equal(t, before, calls.Load(), "%s: rejected before the handler ran", query)
			})
		}
	})
}

// The YIELD declaration answers with a clear error (RUN, PROFILE, or a YIELD *
// that is not standalone) or, for EXPLAIN, a plan or an error, and never with a
// panic. See the file comment: the compile-time owner does not yet decide the
// YIELD column, so RUN and PROFILE answer this after the handler ran and
// EXPLAIN plans it; those orders are recorded here, not asserted as required.
func TestPortedNilResultProcedureYieldDeclarationAnswersWithoutPanicOnBothParsers(t *testing.T) {
	calls := portedNor315Register(t, portedNor315NilProcedureName, portedNor315NilHandler)

	forEachParser(t, func(t *testing.T) {
		exec := newCallScopeExecutor(t)
		for _, query := range []string{
			"CALL " + portedNor315NilProcedureName + "() YIELD missing RETURN missing",
			"PROFILE CALL " + portedNor315NilProcedureName + "() YIELD missing RETURN missing",
		} {
			t.Run(query, func(t *testing.T) {
				before := calls.Load()
				result, err := exec.Execute(context.Background(), query, nil)
				require.ErrorContains(t, err, "YIELD", query)
				require.Nil(t, result, query)
				if ran := calls.Load() - before; ran != 0 {
					t.Logf("run-time YIELD-column decider answered after the handler ran (%d invocation(s)); the compile-time owner does not decide this column yet", ran)
				}
			})
		}

		t.Run("yield wildcard in pipeline", func(t *testing.T) {
			query := "UNWIND [1] AS seed CALL " + portedNor315NilProcedureName + "() YIELD * RETURN seed"
			before := calls.Load()
			result, err := exec.Execute(context.Background(), query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			require.Nil(t, result, query)
			require.Equal(t, before, calls.Load(), "%s: rejected before the handler ran", query)
		})

		t.Run("explain unknown yield column", func(t *testing.T) {
			query := "EXPLAIN CALL " + portedNor315NilProcedureName + "() YIELD missing RETURN missing"
			before := calls.Load()
			result, err := exec.Execute(context.Background(), query, nil)
			require.Equal(t, before, calls.Load(), "%s: EXPLAIN must not invoke the handler", query)
			if err != nil {
				require.Nil(t, result, query)
				t.Logf("EXPLAIN rejected the unknown YIELD column: %v", err)
				return
			}
			require.NotNil(t, result, "%s: EXPLAIN served neither a plan nor an error", query)
			require.Empty(t, result.Rows, "%s: EXPLAIN must not execute the procedure", query)
			t.Logf("EXPLAIN planned the unknown YIELD column with columns %v; compile-time YIELD-column rejection is still open", result.Columns)
		})
	})
}
