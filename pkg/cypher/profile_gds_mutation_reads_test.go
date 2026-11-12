package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileGDSProjectCountsDeliveredEntitiesBeforeProjection(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t,
			"CREATE (a:P {id:1}), (b:P {id:2}), (x:Other), (a)-[:K]->(b), (a)-[:Other]->(x)")
		gdsOwnProjections(t, exec, "profile-project")
		statement := "CALL gds.graph.project('profile-project', 'P', 'K') YIELD nodeCount, relationshipCount RETURN nodeCount, relationshipCount"
		plain, err := exec.Execute(t.Context(), statement, nil)
		require.NoError(t, err)
		_, err = exec.Execute(t.Context(), "CALL gds.graph.drop('profile-project')", nil)
		require.NoError(t, err)
		profiled, err := exec.Execute(t.Context(), "PROFILE "+statement, nil)
		require.NoError(t, err)
		require.Equal(t, plain.Columns, profiled.Columns)
		require.Equal(t, [][]interface{}{{int64(2), int64(1)}}, plain.Rows)
		require.Equal(t, plain.Rows, profiled.Rows)
		plan := executionPlanFromResult(t, profiled)
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		for _, expected := range []struct {
			kind       string
			rows, hits int64
		}{
			{profileKindAllNodesScan, 3, 3}, {profileKindAllRelationshipsScan, 2, 2}, {profileKindGraphCatalogSeek, 0, 1},
		} {
			read := planOperatorOfType(plan.Root, expected.kind)
			require.NotNil(t, read, expected.kind)
			require.EqualValues(t, expected.rows, read.ActualRows, expected.kind)
			require.EqualValues(t, expected.hits, read.DBHits, expected.kind)
		}
		require.EqualValues(t, 6, plan.TotalDBHits)
	})
}

func TestProfileGDSDropAccountsForPreflightAndActualLookups(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:P)")
		gdsOwnProjections(t, exec, "profile-drop")
		for _, preflight := range []bool{false, true} {
			_, err := exec.Execute(t.Context(), "CALL gds.graph.project('profile-drop', 'P', '*')", nil)
			require.NoError(t, err)
			flag := "false"
			if preflight {
				flag = "true"
			}
			result, err := exec.Execute(t.Context(), "PROFILE CALL gds.graph.drop(['profile-drop','profile-drop'],"+flag+") YIELD graphName RETURN graphName", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"profile-drop"}}, result.Rows, "duplicate names never duplicate output")
			plan := executionPlanFromResult(t, result)
			require.Empty(t, plan.Unobserved)
			require.True(t, plan.TotalDBHitsObserved)
			read := planOperatorOfType(plan.Root, profileKindGraphCatalogSeek)
			require.NotNil(t, read)
			rows, hits := int64(1), int64(2)
			if preflight {
				rows, hits = 3, 4
			}
			require.EqualValues(t, rows, read.ActualRows)
			require.EqualValues(t, hits, read.DBHits)
		}
		_, err := exec.Execute(t.Context(), "CALL gds.graph.project('profile-drop', 'P', '*')", nil)
		require.NoError(t, err)
		collector := newProfileRuntimeCollector()
		_, err = exec.Execute(withProfileRuntimeCollector(t.Context(), collector), "CALL gds.graph.drop(['profile-drop','missing'])", nil)
		require.Error(t, err)
		require.True(t, gdsProjectionPublished(exec, "profile-drop"), "failed preflight never removes the earlier graph")
		require.Equal(t, []gdsAccess{
			{kind: profileKindGraphCatalogSeek, detail: "profile-drop", rows: 1, hits: 1},
			{kind: profileKindGraphCatalogSeek, detail: "missing", rows: 0, hits: 1},
		}, profileGDSAccessesOfKinds(collector, profileKindGraphCatalogSeek))
	})
}

func TestProfileGDSDropKeepsInjectedPermissionResolverOpaque(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:P)")
		gdsOwnProjections(t, exec, "profile-authorized-drop")
		create := func() {
			_, err := exec.Execute(t.Context(), "CALL gds.graph.project('profile-authorized-drop','P','*')", nil)
			require.NoError(t, err)
		}
		create()
		collector := newProfileRuntimeCollector()
		var ctx context.Context
		calls := 0
		ctx = WithDatabasePermissionResolver(withProfileRuntimeCollector(t.Context(), collector), exec.databaseName(), func(string, string) bool {
			calls++
			_, _ = lookupGraphProjection(ctx, exec.databaseName(), "callback-catalog-miss")
			return true
		})
		statement := "CALL gds.graph.drop('profile-authorized-drop') YIELD graphName RETURN graphName"
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"profile-authorized-drop"}}, result.Rows)
		require.Positive(t, calls)
		require.Equal(t, []gdsAccess{{kind: profileKindGraphCatalogSeek, detail: "callback-catalog-miss", rows: 0, hits: int64(calls)},
			{kind: profileKindGraphCatalogSeek, detail: "profile-authorized-drop", rows: 2, hits: 2}},
			profileGDSAccessesOfKinds(collector, profileKindGraphCatalogSeek))
		unknown := false
		for _, access := range collector.accessObservation() {
			if access.Unknown && access.Detail == "permission resolver" {
				unknown = true
			}
		}
		require.True(t, unknown, "the same ledger retains opacity despite a real observed read inside the callback")

		// Separately establish the public PROFILE plan contract. The resolver has
		// no captured collector here, so the assertion concerns only its opacity.
		create()
		profileCtx := WithDatabasePermissionResolver(t.Context(), exec.databaseName(), func(string, string) bool { return true })
		result, err = exec.Execute(profileCtx, "PROFILE "+statement, nil)
		require.NoError(t, err)
		plan := executionPlanFromResult(t, result)
		require.Contains(t, plan.Unobserved, "database reads: permission resolver")
		require.False(t, plan.TotalDBHitsObserved)

		create()
		deniedCollector := newProfileRuntimeCollector()
		denied := WithDatabasePermissionResolver(withProfileRuntimeCollector(t.Context(), deniedCollector), exec.databaseName(), func(string, string) bool { return false })
		// Invoke the statement owner with the same context as the public admission
		// would supply after authorization, so denial happens inside drop itself.
		_, err = exec.callGdsGraphDrop(denied, []interface{}{"profile-authorized-drop"})
		require.Error(t, err)
		require.True(t, gdsProjectionPublished(exec, "profile-authorized-drop"))
		require.Empty(t, profileGDSAccessesOfKinds(deniedCollector, profileKindGraphCatalogSeek))
		unknown = false
		for _, access := range deniedCollector.accessObservation() {
			if access.Unknown && access.Detail == "permission resolver" {
				unknown = true
			}
		}
		require.True(t, unknown)
	})
}

func TestProfileGDSProjectReportsEmptyStreamsAndCatalogMiss(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		gdsOwnProjections(t, exec, "profile-empty-project")
		result, err := exec.Execute(t.Context(), "PROFILE CALL gds.graph.project('profile-empty-project','*','*') YIELD nodeCount,relationshipCount RETURN nodeCount,relationshipCount", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0), int64(0)}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.Empty(t, plan.Unobserved)
		require.True(t, plan.TotalDBHitsObserved)
		for _, kind := range []string{profileKindAllNodesScan, profileKindAllRelationshipsScan, profileKindGraphCatalogSeek} {
			read := planOperatorOfType(plan.Root, kind)
			require.NotNil(t, read, kind)
			require.Zero(t, read.ActualRows, kind)
			hits := int64(0)
			if kind == profileKindGraphCatalogSeek {
				hits = 1
			}
			require.EqualValues(t, hits, read.DBHits, kind)
		}
		require.EqualValues(t, 1, plan.TotalDBHits)
	})
}

// Use a long key so reconstructing it inside this read would allocate. Ordinary
// catalog reads must reuse the caller's key and allocate no observation state.
func TestGraphProjectionCatalogReadWithoutObserverDoesNotAllocate(t *testing.T) {
	exec, _ := newProfileRuntimeTestExecutor(t)
	name := strings.Repeat("catalog-read-", 16)
	gdsOwnProjections(t, exec, name)
	key := gdsCatalogKey(exec.databaseName(), name)
	projection := &GraphProjection{Name: name, Database: exec.databaseName()}
	projectionsMu.Lock()
	graphProjections[key] = projection
	projectionsMu.Unlock()
	ctx := t.Context()
	allocations := testing.AllocsPerRun(100, func() {
		projectionsMu.RLock()
		got, exists := readGraphProjectionLocked(ctx, key, name)
		projectionsMu.RUnlock()
		if !exists || got != projection {
			t.Fatal("catalog read changed its result")
		}
	})
	require.Zero(t, allocations)
}
