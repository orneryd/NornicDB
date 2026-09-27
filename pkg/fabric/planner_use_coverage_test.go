package fabric

import (
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
		fragment, err := NewFabricPlanner(plannerUseCatalog()).Plan("USE graph.byName($g) MATCH (n) RETURN n", sessionDB)
		require.NoError(t, err, sessionDB)
		exec, ok := fragment.(*FragmentExec)
		require.True(t, ok, "expected *FragmentExec, got %T", fragment)
		require.Empty(t, exec.GraphName)
		require.Equal(t, &UseClause{Function: "graph.byName", Args: []string{"$g"}}, exec.Graph)
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
	planner := NewFabricPlanner(plannerUseCatalog())

	fragment, err := planner.Plan("USE graph.byName('cmp.zzz') RETURN 1", "cmp")
	require.NoError(t, err)
	exec, ok := fragment.(*FragmentExec)
	require.True(t, ok, "expected *FragmentExec, got %T", fragment)
	require.Equal(t, "graph.byName", exec.Graph.Function)

	_, err = planner.Plan("USE cmp.zzz RETURN 1", "cmp")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid USE target 'cmp.zzz'")
}

// TestPlan_DynamicUseInUnionBranches pins that each UNION branch keeps its
// own dynamic reference.
func TestPlan_DynamicUseInUnionBranches(t *testing.T) {
	fragment, err := NewFabricPlanner(plannerUseCatalog()).Plan(
		"USE graph.byName('cmp.a') RETURN 1 AS x UNION USE graph.byElementId($id) RETURN 2 AS x", "cmp")
	require.NoError(t, err)
	union, ok := fragment.(*FragmentUnion)
	require.True(t, ok, "expected *FragmentUnion, got %T", fragment)
	require.True(t, union.Distinct)

	execs := collectPlannedExecs(fragment)
	require.Len(t, execs, 2)
	require.Equal(t, &UseClause{Function: "graph.byName", Args: []string{"'cmp.a'"}}, execs[0].Graph)
	require.Equal(t, "RETURN 1 AS x", execs[0].Query)
	require.Equal(t, &UseClause{Function: "graph.byElementId", Args: []string{"$id"}}, execs[1].Graph)
	require.Equal(t, "RETURN 2 AS x", execs[1].Query)
	for _, exec := range execs {
		require.Empty(t, exec.GraphName)
		require.Equal(t, "cmp", exec.Scope)
	}
}

// TestPlan_DynamicUseInImportingCallSubquery pins a CALL subquery whose
// importing WITH is followed by a dynamic USE: the outer clauses run on the
// session graph, and the subquery body, with its importing WITH kept and
// the USE removed, runs on the reference resolved from the imported row.
func TestPlan_DynamicUseInImportingCallSubquery(t *testing.T) {
	fragment, err := NewFabricPlanner(plannerUseCatalog()).Plan(
		"UNWIND $graphs AS g CALL { WITH g USE graph.byName(g) MATCH (n) RETURN count(n) AS c } RETURN g, c", "cmp")
	require.NoError(t, err)

	execs := collectPlannedExecs(fragment)
	require.Len(t, execs, 3)

	require.Equal(t, "UNWIND $graphs AS g", execs[0].Query)
	require.Equal(t, "cmp", execs[0].GraphName)
	require.Nil(t, execs[0].Graph)

	require.Equal(t, "WITH g MATCH (n) RETURN count(n) AS c", execs[1].Query)
	require.Empty(t, execs[1].GraphName)
	require.Equal(t, &UseClause{Function: "graph.byName", Args: []string{"g"}}, execs[1].Graph)
	require.Equal(t, "cmp", execs[1].Scope)
	init, ok := execs[1].Input.(*FragmentInit)
	require.True(t, ok, "expected *FragmentInit, got %T", execs[1].Input)
	require.Equal(t, []string{"g"}, init.ImportColumns)

	require.Equal(t, "RETURN g, c", execs[2].Query)
	require.Equal(t, "cmp", execs[2].GraphName)
}

// TestPlan_DynamicTopLevelUseWithStaticSubquery pins that after a dynamic
// top-level USE, a CALL subquery with a static USE runs on its constituent
// and the clauses after the subquery run on the dynamic reference.
func TestPlan_DynamicTopLevelUseWithStaticSubquery(t *testing.T) {
	fragment, err := NewFabricPlanner(plannerUseCatalog()).Plan(
		"USE graph.byName($g) CALL { USE cmp.a RETURN 1 AS one } RETURN one", "cmp")
	require.NoError(t, err)

	execs := collectPlannedExecs(fragment)
	require.Len(t, execs, 2)
	require.Equal(t, "cmp.a", execs[0].GraphName)
	require.Nil(t, execs[0].Graph)
	require.Equal(t, "RETURN 1 AS one", execs[0].Query)

	require.Equal(t, "RETURN one", execs[1].Query)
	require.Empty(t, execs[1].GraphName)
	require.Equal(t, &UseClause{Function: "graph.byName", Args: []string{"$g"}}, execs[1].Graph)
	require.Equal(t, "cmp", execs[1].Scope)
}

// TestParseLeadingWithUse_NameStartingWithUse pins that a name that only
// starts with the letters USE after an importing WITH is not a USE clause:
// the body is returned unchanged. The name here continues with a non-ASCII
// letter (א), which the WITH-clause scanner treats as the end of a word but
// the USE grammar treats as part of the name.
func TestParseLeadingWithUse_NameStartingWithUse(t *testing.T) {
	body := "WITH a USEא MATCH (n) RETURN n"
	use, rewritten, ok, err := parseLeadingWithUse(body)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, UseClause{}, use)
	require.Equal(t, body, rewritten)
}
