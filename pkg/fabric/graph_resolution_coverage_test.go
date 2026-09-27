package fabric

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// graphResolutionCatalog is a composite "cmp" with constituents cmp.a (on
// database dba) and cmp.b (on dbb), a composite "other" with constituent
// other.x (also on dbb), and a standalone database "abc".
func graphResolutionCatalog() *Catalog {
	catalog := NewCatalog()
	catalog.Register("abc", &LocationLocal{DBName: "abc"})
	catalog.Register("cmp", &LocationLocal{DBName: "cmp"})
	catalog.Register("cmp.a", &LocationLocal{DBName: "dba"})
	catalog.Register("cmp.b", &LocationLocal{DBName: "dbb"})
	catalog.Register("other", &LocationLocal{DBName: "other"})
	catalog.Register("other.x", &LocationLocal{DBName: "dbb"})
	return catalog
}

// literalGraphArguments evaluates graph reference arguments the way the
// Cypher executor would for these tests: 'text' is a string literal, $p a
// parameter, and any other expression a row variable held in params (the
// executor checks that each is a STRING; these tests pass strings).
func literalGraphArguments(_ context.Context, expressions []string, params map[string]interface{}) ([]string, error) {
	values := make([]string, len(expressions))
	for i, expression := range expressions {
		switch {
		case strings.HasPrefix(expression, "'") && strings.HasSuffix(expression, "'"):
			values[i] = strings.Trim(expression, "'")
		case strings.HasPrefix(expression, "$"):
			values[i], _ = params[expression[1:]].(string)
		default:
			values[i], _ = params[expression].(string)
		}
	}
	return values, nil
}

func dynamicExec(function string, args []string, scope string) *FragmentExec {
	return &FragmentExec{
		Input: &FragmentInit{},
		Query: "MATCH (n) RETURN n",
		Graph: &UseClause{Function: function, Args: args},
		Scope: scope,
	}
}

// TestResolveExecGraph_Dynamic pins how a dynamic graph reference resolves
// on composite "cmp": graph.byName names a constituent by its qualified name
// (case-insensitively), graph.byElementId finds the constituent of the
// scope whose database the element id names (a node "4:…" or relationship
// "5:…" id), and the argument may be a literal, a parameter or a row
// variable.
func TestResolveExecGraph_Dynamic(t *testing.T) {
	exec := NewFabricExecutor(graphResolutionCatalog(), nil, nil)
	exec.SetGraphArgumentEvaluator(literalGraphArguments)

	tests := []struct {
		name     string
		function string
		arg      string
		params   map[string]interface{}
		wantName string
		wantDB   string
	}{
		{name: "byName literal", function: "graph.byName", arg: "'cmp.a'", wantName: "cmp.a", wantDB: "dba"},
		{name: "byName parameter", function: "graph.byName", arg: "$g", params: map[string]interface{}{"g": "cmp.b"}, wantName: "cmp.b", wantDB: "dbb"},
		{name: "byName row variable", function: "graph.byName", arg: "g", params: map[string]interface{}{"g": "cmp.a"}, wantName: "cmp.a", wantDB: "dba"},
		{name: "byName other case", function: "graph.byName", arg: "'CMP.A'", wantName: "CMP.A", wantDB: "dba"},
		{name: "function name is case-insensitive", function: "GRAPH.BYNAME", arg: "'cmp.b'", wantName: "cmp.b", wantDB: "dbb"},
		{name: "byElementId of a node", function: "graph.byElementId", arg: "'4:dbb:17'", wantName: "cmp.b", wantDB: "dbb"},
		{name: "byElementId of a relationship", function: "graph.byElementId", arg: "$id", params: map[string]interface{}{"id": "5:dba:3"}, wantName: "cmp.a", wantDB: "dba"},
		{name: "byElementId database name is case-insensitive", function: "graph.byElementId", arg: "'4:DBA:1'", wantName: "cmp.a", wantDB: "dba"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, loc, err := exec.resolveExecGraph(context.Background(), dynamicExec(tt.function, []string{tt.arg}, "cmp"), tt.params)
			require.NoError(t, err)
			require.Equal(t, tt.wantName, name)
			require.Equal(t, tt.wantDB, loc.DatabaseName())
		})
	}
}

// TestResolveExecGraph_DynamicErrors pins the errors of a dynamic graph
// reference that can't be used, with Neo4j's status codes and messages:
// an unknown function or a wrong argument count (SyntaxError), a malformed
// element id (ArgumentError), and a name
// or element id that selects no constituent of the composite
// (DatabaseNotFound "Graph not found: …").
func TestResolveExecGraph_DynamicErrors(t *testing.T) {
	exec := NewFabricExecutor(graphResolutionCatalog(), nil, nil)
	exec.SetGraphArgumentEvaluator(literalGraphArguments)

	referenceErrors := []struct {
		name     string
		function string
		args     []string
		params   map[string]interface{}
		wantCode string
		want     localization.Message
		wantText string
	}{
		{name: "unknown function", function: "graph.byId", args: []string{"'cmp.a'"},
			wantCode: "Neo.ClientError.Statement.SyntaxError",
			want:     localization.CypherCommandRoutingGraphFunctionUnknown("graph.byId"),
			wantText: "Unknown function 'graph.byId'"},
		{name: "no argument", function: "graph.byName", args: nil,
			wantCode: "Neo.ClientError.Statement.SyntaxError",
			want:     localization.CypherCommandRoutingGraphFunctionArgumentCount("graph.byName", 0),
			wantText: "graph.byName takes 1 argument, got 0"},
		{name: "two arguments", function: "graph.byElementId", args: []string{"'a'", "'b'"},
			wantCode: "Neo.ClientError.Statement.SyntaxError",
			want:     localization.CypherCommandRoutingGraphFunctionArgumentCount("graph.byElementId", 2),
			wantText: "graph.byElementId takes 1 argument, got 2"},
		{name: "element id without a database", function: "graph.byElementId", args: []string{"'4::1'"},
			wantCode: "Neo.ClientError.Statement.ArgumentError",
			want:     localization.CypherCommandRoutingGraphElementIDInvalid("4::1"),
			wantText: "Element ID 4::1 has an unexpected format."},
		{name: "element id of an unknown kind", function: "graph.byElementId", args: []string{"'3:dba:1'"},
			wantCode: "Neo.ClientError.Statement.ArgumentError",
			want:     localization.CypherCommandRoutingGraphElementIDInvalid("3:dba:1")},
		{name: "element id with too few parts", function: "graph.byElementId", args: []string{"'17'"},
			wantCode: "Neo.ClientError.Statement.ArgumentError",
			want:     localization.CypherCommandRoutingGraphElementIDInvalid("17")},
	}
	for _, tt := range referenceErrors {
		t.Run(tt.name, func(t *testing.T) {
			name, loc, err := exec.resolveExecGraph(context.Background(), dynamicExec(tt.function, tt.args, "cmp"), tt.params)
			require.Error(t, err)
			require.Empty(t, name)
			require.Nil(t, loc)
			var refErr *GraphReferenceError
			require.True(t, errors.As(err, &refErr))
			require.Equal(t, tt.wantCode, refErr.Code)
			require.Equal(t, tt.want, refErr.Message)
			require.Equal(t, tt.want.Fallback, err.Error())
			if tt.wantText != "" {
				require.Equal(t, tt.wantText, err.Error())
			}
		})
	}

	notFound := []struct {
		name     string
		function string
		arg      string
		scope    string
		wantName string
	}{
		{name: "byName of a graph outside the composite", function: "graph.byName", arg: "'other.x'", scope: "cmp", wantName: "other.x"},
		{name: "byName of the composite itself", function: "graph.byName", arg: "'cmp'", scope: "cmp", wantName: "cmp"},
		{name: "byName of a standalone database", function: "graph.byName", arg: "'abc'", scope: "cmp", wantName: "abc"},
		{name: "byName with no composite scope", function: "graph.byName", arg: "'cmp.a'", scope: "", wantName: "cmp.a"},
		{name: "byName of an unregistered constituent", function: "graph.byName", arg: "'cmp.zzz'", scope: "cmp", wantName: "cmp.zzz"},
		{name: "byElementId of a database outside the composite", function: "graph.byElementId", arg: "'4:other:1'", scope: "cmp", wantName: "4:other:1"},
		{name: "byElementId of an unknown database", function: "graph.byElementId", arg: "'4:nope:1'", scope: "cmp", wantName: "4:nope:1"},
	}
	for _, tt := range notFound {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := exec.resolveExecGraph(context.Background(), dynamicExec(tt.function, []string{tt.arg}, tt.scope), nil)
			require.Error(t, err)
			var notFoundErr *GraphNotFoundError
			require.True(t, errors.As(err, &notFoundErr))
			require.Equal(t, tt.wantName, notFoundErr.Name)
			require.Equal(t, "Graph not found: "+tt.wantName, err.Error())
		})
	}
}

// TestResolveExecGraph_ElementIDPicksConstituentOfScope pins that when two
// composites both have a constituent on the database an element id names,
// graph.byElementId returns the constituent of the statement's composite.
func TestResolveExecGraph_ElementIDPicksConstituentOfScope(t *testing.T) {
	exec := NewFabricExecutor(graphResolutionCatalog(), nil, nil)
	exec.SetGraphArgumentEvaluator(literalGraphArguments)

	name, _, err := exec.resolveExecGraph(context.Background(), dynamicExec("graph.byElementId", []string{"'4:dbb:1'"}, "other"), nil)
	require.NoError(t, err)
	require.Equal(t, "other.x", name)

	name, _, err = exec.resolveExecGraph(context.Background(), dynamicExec("graph.byElementId", []string{"'4:dbb:1'"}, "CMP"), nil)
	require.NoError(t, err)
	require.Equal(t, "cmp.b", name)
}

// TestResolveExecGraph_EvaluatorFailures pins that a dynamic reference can't
// run without an argument evaluator, and that an evaluator's error is
// returned unchanged.
func TestResolveExecGraph_EvaluatorFailures(t *testing.T) {
	exec := NewFabricExecutor(graphResolutionCatalog(), nil, nil)
	_, _, err := exec.resolveExecGraph(context.Background(), dynamicExec("graph.byName", []string{"'cmp.a'"}, "cmp"), nil)
	require.EqualError(t, err, `dynamic graph reference graph.byName("cmp.a") can't be evaluated`)

	evalErr := errors.New("Variable `g` not defined")
	exec.SetGraphArgumentEvaluator(func(context.Context, []string, map[string]interface{}) ([]string, error) {
		return nil, evalErr
	})
	_, _, err = exec.resolveExecGraph(context.Background(), dynamicExec("graph.byName", []string{"g"}, "cmp"), nil)
	require.ErrorIs(t, err, evalErr)

	// An evaluator must return one value per argument expression.
	exec.SetGraphArgumentEvaluator(func(context.Context, []string, map[string]interface{}) ([]string, error) {
		return nil, nil
	})
	_, _, err = exec.resolveExecGraph(context.Background(), dynamicExec("graph.byName", []string{"g"}, "cmp"), nil)
	require.EqualError(t, err, "dynamic graph reference graph.byName(g): 1 arguments evaluated to 0 values")
}

// TestResolveExecGraph_Static pins the static path: a fragment without a
// dynamic reference runs on its GraphName, and an unknown GraphName is a
// routing error that wraps the catalog's error.
func TestResolveExecGraph_Static(t *testing.T) {
	exec := NewFabricExecutor(graphResolutionCatalog(), nil, nil)

	name, loc, err := exec.resolveExecGraph(context.Background(), &FragmentExec{GraphName: "cmp.a"}, nil)
	require.NoError(t, err)
	require.Equal(t, "cmp.a", name)
	require.Equal(t, "dba", loc.DatabaseName())

	_, _, err = exec.resolveExecGraph(context.Background(), &FragmentExec{GraphName: "missing"}, nil)
	require.EqualError(t, err, "cannot route query: graph 'missing' not found in fabric catalog")
	var notFoundErr *GraphNotFoundError
	require.False(t, errors.As(err, &notFoundErr))
}

// recordingCypherExecutor answers every query with the database it ran on.
type recordingCypherExecutor struct {
	databases []string
}

func (r *recordingCypherExecutor) ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	return r.ExecuteQueryWithRecord(ctx, dbName, engine, query, params, nil)
}

func (r *recordingCypherExecutor) ExecuteQueryWithRecord(_ context.Context, dbName string, _ storage.Engine, _ string, _ map[string]interface{}, _ map[string]interface{}) ([]string, [][]interface{}, error) {
	r.databases = append(r.databases, dbName)
	return []string{"db"}, [][]interface{}{{dbName}}, nil
}

// TestFabricExecutor_DynamicUseRunsOnResolvedConstituent pins the whole
// path of a dynamic USE on a composite: the planner keeps the reference,
// and each execution evaluates it with that execution's parameters and runs
// the query on the constituent it names.
func TestFabricExecutor_DynamicUseRunsOnResolvedConstituent(t *testing.T) {
	catalog := graphResolutionCatalog()
	fragment, err := NewFabricPlanner(catalog).Plan("USE graph.byName($g) MATCH (n) RETURN n", "cmp")
	require.NoError(t, err)

	recorder := &recordingCypherExecutor{}
	local := NewLocalFragmentExecutor(recorder, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
	exec := NewFabricExecutor(catalog, local, nil)
	exec.SetGraphArgumentEvaluator(literalGraphArguments)

	for _, tc := range []struct{ graph, db string }{{"cmp.a", "dba"}, {"cmp.b", "dbb"}} {
		result, err := exec.Execute(context.Background(), nil, fragment, map[string]interface{}{"g": tc.graph}, "")
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{tc.db}}, result.Rows)
	}
	require.Equal(t, []string{"dba", "dbb"}, recorder.databases)

	_, err = exec.Execute(context.Background(), nil, fragment, map[string]interface{}{"g": "other.x"}, "")
	require.EqualError(t, err, "Graph not found: other.x")
}
