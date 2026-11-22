package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// NOR-354: a registered procedure whose handler returns (nil, nil) is an empty
// result, not a missing one. The result owner is
// StorageExecutor.executeProcedureHandler (procedure_statement_stats.go): it
// supplies the procedure's declared columns and no rows in the one place a
// handler's result becomes a statement result, so YIELD, a query tail, a CALL
// subquery, a USE target and a Fabric constituent all consume a real result.
//
// This file is the reproduction sweep for that contract across the public
// Execute routes, under both statement parsers. Each route also carries its
// healthy non-nil control (the handler's own row, projected as the route
// states) and its error control (a handler that reports a failure still fails
// the statement instead of becoming an empty result). Before the owner above
// existed, the bare call returned a nil result and YIELD dereferenced it while
// validating its columns, so a panicking route is reported against its query
// instead of aborting the run.

const (
	nor354NilProcedureName     = "nor354.nilResult"
	nor354RowsProcedureName    = "nor354.nilResultRows"
	nor354FailingProcedureName = "nor354.nilResultFailure"
	nor354FailureMessage       = "nor354 handler failed"
	nor354DeclaredValue        = int64(7)
)

// nor354Route is one public statement that consumes a procedure call's result:
// the columns it serves, and the rows the healthy one-row control produces
// through it. A handler that returns no result serves the same columns and no
// rows.
type nor354Route struct {
	name    string
	query   string
	columns []string
	rows    [][]interface{}
}

// registerNor354Procedures publishes the three registrations the sweep uses:
// a handler that returns no result at all, one that returns its own one-row
// result, and one that reports a failure.
func registerNor354Procedures(t *testing.T) {
	t.Helper()
	registerProcedureForTest(t, nilResultProcedureSpec(nor354NilProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return nil, nil
		})
	registerProcedureForTest(t, nilResultProcedureSpec(nor354RowsProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{
				Columns: []string{"value"},
				Rows:    [][]interface{}{{nor354DeclaredValue}},
			}, nil
		})
	registerProcedureForTest(t, nilResultProcedureSpec(nor354FailingProcedureName),
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return nil, errors.New(nor354FailureMessage)
		})
}

// nor354Execute runs query through the public Execute path. A panic is reported
// against the query that caused it instead of aborting the test binary, so one
// panicking route cannot hide the routes that follow it.
func nor354Execute(t *testing.T, exec *StorageExecutor, query string) (result *ExecuteResult, err error) {
	t.Helper()
	defer func() {
		if panicked := recover(); panicked != nil {
			t.Fatalf("%s: the procedure call panicked instead of serving a result: %v", query, panicked)
		}
	}()
	return exec.Execute(context.Background(), query, nil)
}

// nor354AssertDeclaredEmptyResult requires the call's declared columns and no
// rows: a real result, never a missing one.
func nor354AssertDeclaredEmptyResult(t *testing.T, exec *StorageExecutor, query string, columns []string) {
	t.Helper()
	result, err := nor354Execute(t, exec, query)
	require.NoError(t, err, query)
	require.NotNil(t, result, "%s: a procedure call always serves a result", query)
	require.Equal(t, columns, result.Columns, query)
	require.Empty(t, result.Rows, query)
}

// nor354StatementRoutes are the statement routes for one procedure name: the
// bare call, YIELD in each standalone form, the query tail after YIELD, an
// in-query pipeline, CALL { } subqueries (plain and correlated), and a
// statement-executing procedure that runs the call.
func nor354StatementRoutes(procedure string) []nor354Route {
	call := "CALL " + procedure + "()"
	single := [][]interface{}{{nor354DeclaredValue}}
	return []nor354Route{
		{name: "bare call", query: call, columns: []string{"value"}, rows: single},
		{name: "standalone yield", query: call + " YIELD value", columns: []string{"value"}, rows: single},
		{name: "standalone yield star", query: call + " YIELD *", columns: []string{"value"}, rows: single},
		{name: "yield alias", query: call + " YIELD value AS v RETURN v", columns: []string{"v"}, rows: single},
		{name: "yield return tail", query: call + " YIELD value RETURN value", columns: []string{"value"}, rows: single},
		{name: "yield with tail", query: call + " YIELD value WITH value RETURN value", columns: []string{"value"}, rows: single},
		{name: "yield ordered paged tail", query: call + " YIELD value RETURN value ORDER BY value SKIP 0 LIMIT 2", columns: []string{"value"}, rows: single},
		{name: "unwind pipeline", query: "UNWIND [1, 2] AS seed " + call + " YIELD value RETURN seed, value", columns: []string{"seed", "value"}, rows: [][]interface{}{{int64(1), nor354DeclaredValue}, {int64(2), nor354DeclaredValue}}},
		{name: "call subquery", query: "CALL { " + call + " YIELD value RETURN value } RETURN value", columns: []string{"value"}, rows: single},
		{name: "correlated call subquery", query: "UNWIND [1, 2] AS seed CALL { WITH seed " + call + " YIELD value RETURN seed, value } RETURN seed, value", columns: []string{"seed", "value"}, rows: [][]interface{}{{int64(1), nor354DeclaredValue}, {int64(2), nor354DeclaredValue}}},
		{name: "statement procedure", query: "CALL apoc.cypher.run('" + call + " YIELD value RETURN value', {}) YIELD value RETURN value", columns: []string{"value"}, rows: [][]interface{}{{map[string]interface{}{"value": nor354DeclaredValue}}}},
	}
}

// nor354UseRoutes are the routes whose CALL runs on the graph a USE clause
// selects: on its own, behind a YIELD, and inside a CALL { } subquery (with and
// without an enclosing MATCH).
func nor354UseRoutes(procedure string) []nor354Route {
	call := "USE other CALL " + procedure + "()"
	single := [][]interface{}{{nor354DeclaredValue}}
	return []nor354Route{
		{name: "use bare call", query: call, columns: []string{"value"}, rows: single},
		{name: "use standalone yield", query: call + " YIELD value", columns: []string{"value"}, rows: single},
		{name: "use yield return tail", query: call + " YIELD value RETURN value", columns: []string{"value"}, rows: single},
		{name: "use call subquery", query: "CALL { " + call + " YIELD value RETURN value } RETURN value", columns: []string{"value"}, rows: single},
		{name: "use subquery after match", query: "MATCH (n:Home) CALL { " + call + " YIELD value RETURN value } RETURN value", columns: []string{"value"}, rows: single},
	}
}

// nor354FabricRoutes are the routes whose CALL runs on a composite database's
// local constituent: selected directly on its own and behind a YIELD, and
// selected inside a CALL { } subquery.
func nor354FabricRoutes(procedure string) []nor354Route {
	call := "USE pcomp.other CALL " + procedure + "()"
	single := [][]interface{}{{nor354DeclaredValue}}
	return []nor354Route{
		{name: "fabric constituent bare call", query: call, columns: []string{"value"}, rows: single},
		{name: "fabric constituent yield return tail", query: call + " YIELD value RETURN value", columns: []string{"value"}, rows: single},
		{name: "fabric constituent call subquery", query: "CALL { " + call + " YIELD value RETURN value } RETURN value", columns: []string{"value"}, rows: single},
	}
}

// nor354RouteFamilies is the route population: the three execution surfaces a
// procedure call's result reaches, each with its own fixture and its routes.
var nor354RouteFamilies = []struct {
	name        string
	newExecutor func(*testing.T) *StorageExecutor
	routes      func(string) []nor354Route
}{
	{name: "statement", newExecutor: newCallScopeExecutor, routes: nor354StatementRoutes},
	{name: "use target", newExecutor: newUseTargetFixture, routes: nor354UseRoutes},
	{name: "fabric constituent", newExecutor: newCompositeFixture, routes: nor354FabricRoutes},
}

// nor354ParserModes are the two statement parsers. Only admission differs
// between them; the CALL result contract must not.
var nor354ParserModes = []struct {
	name  string
	antlr bool
	setup func() func()
}{
	{name: "nornic", antlr: false, setup: config.WithNornicParser},
	{name: "antlr", antlr: true, setup: config.WithANTLRParser},
}

// forEachNor354RouteUnderBothParsers runs check for every route of every route
// family under both statement parsers, on a fresh executor per family.
func forEachNor354RouteUnderBothParsers(t *testing.T, procedure string, check func(t *testing.T, route nor354Route, exec *StorageExecutor)) {
	t.Helper()
	for _, mode := range nor354ParserModes {
		t.Run(mode.name, func(t *testing.T) {
			t.Cleanup(mode.setup())
			require.Equal(t, mode.antlr, config.IsANTLRParser(), "the %s parser is the one running", mode.name)
			for _, family := range nor354RouteFamilies {
				t.Run(family.name, func(t *testing.T) {
					exec := family.newExecutor(t)
					for _, route := range family.routes(procedure) {
						t.Run(route.name, func(t *testing.T) {
							check(t, route, exec)
						})
					}
				})
			}
		})
	}
}

// TestNor354NilProcedureResultServesDeclaredEmptyResultOnEveryRoute is the
// reproduction: a (nil, nil) handler is no rows on every public Execute route,
// under both parsers, and never a panic or a nil result.
func TestNor354NilProcedureResultServesDeclaredEmptyResultOnEveryRoute(t *testing.T) {
	registerNor354Procedures(t)
	forEachNor354RouteUnderBothParsers(t, nor354NilProcedureName,
		func(t *testing.T, route nor354Route, exec *StorageExecutor) {
			nor354AssertDeclaredEmptyResult(t, exec, route.query, route.columns)
		})
}

// TestNor354NilProcedureResultCountsAsZeroRows checks that the empty result is
// an ordinary empty row set for the clauses that aggregate over it: the call's
// rows are zero, on every route family, under both parsers.
func TestNor354NilProcedureResultCountsAsZeroRows(t *testing.T) {
	registerNor354Procedures(t)
	for _, mode := range nor354ParserModes {
		t.Run(mode.name, func(t *testing.T) {
			t.Cleanup(mode.setup())
			for _, family := range []struct {
				name        string
				newExecutor func(*testing.T) *StorageExecutor
				query       string
			}{
				{
					name:        "statement",
					newExecutor: newCallScopeExecutor,
					query:       "CALL " + nor354NilProcedureName + "() YIELD value RETURN count(*) AS total",
				},
				{
					name:        "use target",
					newExecutor: newUseTargetFixture,
					query:       "CALL { USE other CALL " + nor354NilProcedureName + "() YIELD value RETURN value } RETURN count(*) AS total",
				},
				{
					name:        "fabric constituent",
					newExecutor: newCompositeFixture,
					query:       "CALL { USE pcomp.other CALL " + nor354NilProcedureName + "() YIELD value RETURN value } RETURN count(*) AS total",
				},
			} {
				t.Run(family.name, func(t *testing.T) {
					result, err := nor354Execute(t, family.newExecutor(t), family.query)
					require.NoError(t, err, family.query)
					require.NotNil(t, result, family.query)
					require.Equal(t, []string{"total"}, result.Columns, family.query)
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, family.query)
				})
			}
		})
	}
}

// TestNor354ProcedureRowResultControls is the healthy control on the same
// routes: a handler that returns a row keeps that row, its value and the
// caller's projection, so the routes above are not passing by discarding
// results.
func TestNor354ProcedureRowResultControls(t *testing.T) {
	registerNor354Procedures(t)
	forEachNor354RouteUnderBothParsers(t, nor354RowsProcedureName,
		func(t *testing.T, route nor354Route, exec *StorageExecutor) {
			result, err := nor354Execute(t, exec, route.query)
			require.NoError(t, err, route.query)
			require.NotNil(t, result, route.query)
			require.Equal(t, route.columns, result.Columns, route.query)
			require.Equal(t, route.rows, result.Rows, route.query)
		})
}

// TestNor354ProcedureHandlerErrorControls is the error control on the same
// routes: a handler that reports a failure fails the statement with its own
// error, so the empty-result contract applies only to a handler that reports no
// failure.
func TestNor354ProcedureHandlerErrorControls(t *testing.T) {
	registerNor354Procedures(t)
	forEachNor354RouteUnderBothParsers(t, nor354FailingProcedureName,
		func(t *testing.T, route nor354Route, exec *StorageExecutor) {
			_, err := nor354Execute(t, exec, route.query)
			require.Error(t, err, route.query)
			require.ErrorContains(t, err, nor354FailingProcedureName, route.query)
			require.ErrorContains(t, err, nor354FailureMessage, route.query)
		})
}
