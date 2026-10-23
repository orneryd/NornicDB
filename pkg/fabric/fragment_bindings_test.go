package fabric

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestRetainedFragmentBindings(t *testing.T) {
	ctx := context.Background()
	record := map[string]interface{}{"use": int64(7), "other": int64(9)}
	call := &resultmetadata.Call{Mode: resultmetadata.ImportListed, Retained: resultmetadata.KnownScope([]string{"use"})}
	ctx = withRetainedFragmentBindings(ctx, call, record)
	require.Equal(t, map[string]interface{}{"use": int64(7)}, RetainedFragmentBindings(ctx))
	ctx = withRetainedFragmentBindings(ctx, nil, record)
	require.Nil(t, RetainedFragmentBindings(ctx), "an unscoped fragment clears inherited retained imports")
	ctx = withRetainedFragmentBindings(ctx, &resultmetadata.Call{Mode: resultmetadata.ImportListed}, record)
	require.Nil(t, RetainedFragmentBindings(ctx), "an unknown retained scope cannot invent imports")
}

func TestGraphSelectionKeepsParametersIndependentOfImports(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp.one", &LocationLocal{DBName: "db1"})
	mock := &mockCypherExecutor{results: map[string]*ResultStream{"RETURN 7 AS k": {Columns: []string{"k"}, Rows: [][]interface{}{{int64(7)}}}}}
	exec := NewFabricExecutor(catalog, newTestLocalExecutor(mock), nil)
	params := map[string]interface{}{"use": int64(7)}
	record := map[string]interface{}{"use": int64(9)}
	exec.SetGraphArgumentEvaluator(func(ctx context.Context, _ []string, values map[string]interface{}) ([]string, error) {
		require.Equal(t, params, values, "USE arguments must receive caller parameters, without row substitution")
		actual, ok := RecordBindingsFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, record, actual)
		return []string{"comp.one"}, nil
	})
	fragment := &FragmentExec{Query: "RETURN 7 AS k", Graph: &UseClause{Function: "graph.byName", Args: []string{"$use"}}, Scope: "comp"}
	result, err := exec.Execute(WithRecordBindings(context.Background(), record), nil, fragment, params, "")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	require.Equal(t, map[string]interface{}{"use": int64(7)}, params)
}

func TestRetainedImportsAcrossRowCombination(t *testing.T) {
	columns := []string{"use", "return", "call", "other"}
	combiner := newCompiledRowCombiner(columns, buildColumnIndex([]string{"return"}), buildColumnIndex([]string{"call"})).withRetained(map[string]interface{}{
		"use": int64(7), "return": int64(8), "call": int64(9),
	})
	require.Equal(t, []interface{}{int64(7), nil, nil, nil}, combiner.combine([]interface{}{nil}, []interface{}{nil}),
		"missing retained columns survive; actual nulls from either row are preserved")
	require.Equal(t, []interface{}{int64(7), int64(1), int64(2), nil}, combiner.combine([]interface{}{int64(1)}, []interface{}{int64(2)}))
}

func TestLookupImportIdentity(t *testing.T) {
	for _, fragment := range []*FragmentExec{
		{Call: &resultmetadata.Call{Imports: []string{"use"}}},
		{Input: &FragmentInit{ImportColumns: []string{"use"}}},
		{Input: &FragmentInit{Columns: []string{"use"}}},
	} {
		for _, written := range []string{"use", "`use`"} {
			fragment.Query = "WITH " + written + " MATCH (n) RETURN n"
			name, remainder, ok := lookupImportPrefix(fragment)
			require.True(t, ok)
			require.Equal(t, "use", name)
			require.Equal(t, "MATCH (n) RETURN n", remainder)
		}
		fragment.Query = "WITH other MATCH (n) RETURN n"
		_, _, ok := lookupImportPrefix(fragment)
		require.False(t, ok, "the physical recognizer cannot invent an import")
		fragment.Query = "MATCH (n) RETURN n"
		_, _, ok = lookupImportPrefix(fragment)
		require.False(t, ok)
	}
	_, _, ok := lookupImportPrefix(&FragmentExec{Query: "WITH use MATCH (n) RETURN n"})
	require.False(t, ok)
	_, _, ok = lookupImportPrefix(&FragmentExec{Call: &resultmetadata.Call{Imports: []string{"use", "other"}}, Query: "WITH use MATCH (n) RETURN n"})
	require.False(t, ok, "a single-key lookup cannot absorb multiple imports")
}

func TestImportProjectionSerialization(t *testing.T) {
	for _, name := range []string{"as", "not", "distinct", "all", "use name", "ret`urn", "返す"} {
		call := &resultmetadata.Call{Imports: []string{name}}
		clause := resultmetadata.Clause{Kind: resultmetadata.ClauseWith, Projection: &resultmetadata.Projection{
			Expressions: []string{quoteCypherIdentifier(name)}, Names: resultmetadata.Known([]string{name}), Distinct: true, Modifiers: "LIMIT 1", Predicate: quoteCypherIdentifier(name) + " = 7", PassThrough: true,
		}}
		query, ok := serializeImportProjection(clause, call)
		require.True(t, ok)
		require.Equal(t, "WITH DISTINCT "+quoteCypherIdentifier(name)+" LIMIT 1 WHERE "+quoteCypherIdentifier(name)+" = 7", query)
		clause.Projection.Distinct, clause.Projection.Modifiers = false, ""
		clause.Projection.Predicate = ""
		query, ok = serializeImportProjection(clause, call)
		require.True(t, ok)
		require.Equal(t, "WITH "+quoteCypherIdentifier(name), query)
		clause.Projection.Expressions = []string{"1 + 6"}
		clause.Projection.PassThrough = false
		_, ok = serializeImportProjection(clause, call)
		require.False(t, ok, "a computed alias must keep its expression")
		clause.Projection.Expressions = []string{quoteCypherIdentifier(name)}
		clause.Projection.PassThrough = true
		_, ok = serializeImportProjection(clause, &resultmetadata.Call{Imports: []string{"other"}})
		require.False(t, ok, "local projections must keep their own source")
		clause.Projection.Names = resultmetadata.Known(nil)
		_, ok = serializeImportProjection(clause, call)
		require.False(t, ok)
	}
}

func TestFragmentBindingIdentity(t *testing.T) {
	for _, name := range []string{"return", "use", "all", "ret`urn", "use name", "返す"} {
		t.Run(name, func(t *testing.T) {
			fragment := &FragmentExec{
				Query: "RETURN $use AS k /* __fabric_import_0 */",
				In:    resultmetadata.KnownScope([]string{name}), Columns: []string{"k"},
			}
			params := map[string]interface{}{"use": int64(9), "__fabric_import_0_1": "caller"}
			query, actual := remoteImportStatement(fragment, params, map[string]interface{}{name: int64(7)})
			require.Equal(t, "WITH $__fabric_import_0_2 AS "+quoteCypherIdentifier(name)+" "+fragment.Query, query)
			require.Equal(t, int64(7), actual["__fabric_import_0_2"])
			require.Equal(t, int64(9), actual["use"])
			require.Equal(t, map[string]interface{}{"use": int64(9), "__fabric_import_0_1": "caller"}, params)
			fragment.Call = &resultmetadata.Call{Mode: resultmetadata.ImportListed}
			query, _ = remoteImportStatement(fragment, params, map[string]interface{}{name: int64(7)})
			require.Equal(t, "WITH $__fabric_import_0_2 AS "+quoteCypherIdentifier(name)+" CALL ("+quoteCypherIdentifier(name)+") { "+fragment.Query+" } RETURN `k`", query)
			fragment.Columns = nil
			query, _ = remoteImportStatement(fragment, params, map[string]interface{}{name: int64(7)})
			require.NotContains(t, query, "} RETURN")
		})
	}
	for _, scope := range []resultmetadata.Scope{resultmetadata.UnknownScope(), resultmetadata.KnownScope(nil)} {
		fragment := &FragmentExec{Query: "RETURN $use AS k", In: scope}
		query, params := remoteImportStatement(fragment, nil, nil)
		require.Equal(t, fragment.Query, query)
		require.Nil(t, params)
	}
	require.Equal(t, "__generated_2", freeName("__generated", "__generated", map[string]interface{}{"__generated_1": 7}))
}
