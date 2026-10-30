package fabric

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"testing"

	"github.com/stretchr/testify/require"
)

// collectPlannedExecs returns the executable fragments of a plan in
// execution order (APPLY input before inner, UNION left before right).
func collectPlannedExecs(fragment Fragment) []*FragmentExec {
	switch f := fragment.(type) {
	case *FragmentExec:
		return []*FragmentExec{f}
	case *FragmentApply:
		return append(collectPlannedExecs(f.Input), collectPlannedExecs(f.Inner)...)
	case *FragmentUnion:
		return append(collectPlannedExecs(f.LHS), collectPlannedExecs(f.RHS)...)
	default:
		return nil
	}
}

// requireDynamicUseProduct checks routing identity and the legacy evaluator
// projection. Original-coordinate extents have dedicated public product controls.
func requireDynamicUseProduct(t *testing.T, use *UseClause, function string, args []string) {
	t.Helper()
	require.NotNil(t, use)
	require.Empty(t, use.Name)
	require.Equal(t, function, use.Function)
	require.Equal(t, args, use.Args)
	require.Len(t, use.Arguments, len(args))
	for index, argument := range use.Arguments {
		require.Equal(t, args[index], argument.Text)
	}
}

func plannerUseCatalog() *Catalog {
	catalog := NewCatalog()
	catalog.Register("cmp", &LocationLocal{DBName: "cmp"})
	catalog.Register("cmp.a", &LocationLocal{DBName: "dba"})
	catalog.Register("cmp.b", &LocationLocal{DBName: "dbb"})
	return catalog
}

// TestPlan_DynamicUseKeepsReference pins that a leading dynamic USE plans
// one fragment that carries the graph reference (not a graph name) and the
// session's composite as the scope its constituents come from, for a
// session on the composite or on one of its constituents.
func TestPlan_DynamicUseKeepsReference(t *testing.T) {
	for _, sessionDB := range []string{"cmp", "cmp.a"} {
		planner := NewFabricPlanner(plannerUseCatalog(), testStatementWrites)
		fragment, err := planner.Plan(t.Context(), "USE graph.byName($g) MATCH (n) RETURN n", sessionDB, cyphertext.RawSource)
		require.NoError(t, err, sessionDB)
		exec, ok := fragment.(*FragmentExec)
		require.True(t, ok, "expected *FragmentExec, got %T", fragment)
		require.Empty(t, exec.GraphName)
		requireDynamicUseProduct(t, exec.Graph, "graph.byName", []string{"$g"})
		require.Equal(t, "cmp", exec.Scope)
		require.Equal(t, "MATCH (n) RETURN n", exec.Query)
		require.False(t, exec.IsWrite)
	}
}

// TestPlan_DynamicUseResolvedAtRunTime pins that the planner does not look
// up a dynamic reference's graph: a graph.byName of a constituent that
// doesn't exist plans, while the same name in a static USE is rejected at
// plan time.
func TestPlan_DynamicUseResolvedAtRunTime(t *testing.T) {
	planner := NewFabricPlanner(plannerUseCatalog(), testStatementWrites)

	fragment, err := planner.Plan(t.Context(), "USE graph.byName('cmp.zzz') RETURN 1", "cmp", cyphertext.RawSource)
	require.NoError(t, err)
	exec, ok := fragment.(*FragmentExec)
	require.True(t, ok, "expected *FragmentExec, got %T", fragment)
	require.Equal(t, "graph.byName", exec.Graph.Function)

	_, err = planner.Plan(t.Context(), "USE cmp.zzz RETURN 1", "cmp", cyphertext.RawSource)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid USE target 'cmp.zzz'")
}

// TestPlan_DynamicUseInUnionBranches pins that each UNION branch keeps its
// own dynamic reference.
func TestPlan_DynamicUseInUnionBranches(t *testing.T) {
	fragment, err := NewFabricPlanner(plannerUseCatalog(), testStatementWrites).Plan(t.Context(),
		"USE graph.byName('cmp.a') RETURN 1 AS x UNION USE graph.byElementId($id) RETURN 2 AS x", "cmp", cyphertext.RawSource)
	require.NoError(t, err)
	union, ok := fragment.(*FragmentUnion)
	require.True(t, ok, "expected *FragmentUnion, got %T", fragment)
	require.True(t, union.Distinct)

	execs := collectPlannedExecs(fragment)
	require.Len(t, execs, 2)
	requireDynamicUseProduct(t, execs[0].Graph, "graph.byName", []string{"'cmp.a'"})
	require.Equal(t, "RETURN 1 AS x", execs[0].Query)
	requireDynamicUseProduct(t, execs[1].Graph, "graph.byElementId", []string{"$id"})
	require.Equal(t, "RETURN 2 AS x", execs[1].Query)
	for _, exec := range execs {
		require.Empty(t, exec.GraphName)
		require.Equal(t, "cmp", exec.Scope)
	}
}

// TestParseLeadingWithUse_NameStartingWithUse pins that a name that only
// starts with the letters USE after an importing WITH is not a USE clause:
// the body is returned unchanged. The name here continues with a non-ASCII
// letter (א), which the WITH-clause scanner treats as the end of a word but
// the USE grammar treats as part of the name.
func TestParseLeadingWithUse_NameStartingWithUse(t *testing.T) {
	body := "WITH a USEא MATCH (n) RETURN n"
	use, rewritten, ok, err := parseLeadingWithUse(t.Context(), body, nil)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, UseClause{}, use)
	require.Equal(t, body, rewritten)
}

// TestPlan_DynamicUseSubqueryPositions ports the incoming PR #1525
// importing-CALL product onto the current planner API. The static-subquery
// sibling case already exists as TestPlan_DynamicTopLevelUseWithStaticSubquery
// in the releasing branch, so it is not duplicated here.
func TestPlan_DynamicUseSubqueryPositions(t *testing.T) {
	outerTable := []struct {
		name         string
		query        string
		importColumn string
		count        int
		check        func(*testing.T, []*FragmentExec)
	}{
		{
			name:         "TestPlan_DynamicUseInImportingCallSubquery",
			query:        "UNWIND $graphs AS g CALL { WITH g USE graph.byName(g) MATCH (n) RETURN count(n) AS c } RETURN g, c",
			importColumn: "g",
			count:        3,
			check: func(t *testing.T, execs []*FragmentExec) {
				// TestPlan_DynamicUseInImportingCallSubquery pins a CALL subquery whose
				// importing WITH is followed by a dynamic USE: the outer clauses run on the
				// session graph, and the subquery body, with its importing WITH kept and
				// the USE removed, runs on the reference resolved from the imported row.
				require.Equal(t, "UNWIND $graphs AS g RETURN *", execs[0].Query)
				require.Equal(t, "cmp", execs[0].GraphName)
				require.Nil(t, execs[0].Graph)

				require.Equal(t, "WITH g MATCH (n) RETURN count(n) AS c", execs[1].Query)
				require.Empty(t, execs[1].GraphName)
				requireDynamicUseProduct(t, execs[1].Graph, "graph.byName", []string{"g"})
				require.Equal(t, "cmp", execs[1].Scope)
				init, ok := execs[1].Input.(*FragmentInit)
				require.True(t, ok, "expected *FragmentInit, got %T", execs[1].Input)
				require.Equal(t, []string{"g"}, init.ImportColumns)

				require.Equal(t, "RETURN g, c", execs[2].Query)
				require.Equal(t, "cmp", execs[2].GraphName)
			},
		},
	}

	for _, tc := range outerTable {
		t.Run(tc.name, func(t *testing.T) {
			planner := NewFabricPlanner(plannerUseCatalog(), testStatementWrites)
			if tc.importColumn != "" {
				read, _ := recordedCallImports(tc.importColumn)
				planner.SetCallImportReader(read)
			}
			fragment, err := planner.Plan(t.Context(), tc.query, "cmp", cyphertext.RawSource)
			require.NoError(t, err)
			execs := collectPlannedExecs(fragment)
			require.Len(t, execs, tc.count)
			tc.check(t, execs)
		})
	}
}
