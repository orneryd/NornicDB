package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProfileDatabaseInfoReportsScalarCountRequests(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, populated := range []bool{false, true} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			nodes, edges := uint64(0), uint64(0)
			if populated {
				exec = tokenListingExecutor(t)
				nodes, edges = 2, 1
			}
			result, plan := profileEquivalentObservedStatement(t, exec, "CALL db.info() YIELD nodeCount, relationshipCount RETURN nodeCount, relationshipCount")
			require.EqualValues(t, nodes, result.Rows[0][0])
			require.EqualValues(t, edges, result.Rows[0][1])
			for _, kind := range []string{profileKindNodeCountRequest, profileKindRelationshipCountRequest} {
				read := planOperatorOfType(plan.Root, kind)
				require.NotNil(t, read, kind)
				require.EqualValues(t, 1, read.ActualRows)
				require.EqualValues(t, 1, read.DBHits)
			}
			require.EqualValues(t, 2, plan.TotalDBHits)
		}
	})
}

func TestProfileNamedIndexManagementReportsActualCatalog(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := profileReaderExecutor(t)
		want := len(exec.storage.GetSchema().GetIndexes())
		for _, query := range []string{"CALL db.awaitIndex('profile_person_email', 1) RETURN 7 AS seven", "CALL db.resampleIndex('profile_person_email') RETURN 7 AS seven"} {
			result, plan := profileEquivalentObservedStatement(t, exec, query)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
			read := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
			require.NotNil(t, read)
			require.EqualValues(t, want, read.ActualRows)
			require.EqualValues(t, 1, read.DBHits)
		}
		collector := newProfileRuntimeCollector()
		_, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), "CALL db.awaitIndex('profile_person_email', null)", nil)
		require.Error(t, err)
		require.Empty(t, collector.accessObservation(), "invalid arguments fail before catalog acquisition")
	})
}

func TestProfileOperationalProceduresDoNotInventGraphReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		for _, query := range []string{
			"CALL dbms.info() YIELD name RETURN name",
			"CALL dbms.listConnections() YIELD connectionId RETURN connectionId",
			"CALL dbms.listConfig('nornicdb.version') YIELD name RETURN name",
			"CALL db.awaitIndexes(1) RETURN 7 AS seven",
			"CALL db.clearQueryCaches() YIELD value RETURN value",
		} {
			_, plan := profileEquivalentObservedStatement(t, exec, query)
			require.Zero(t, plan.TotalDBHits, query)
		}
	})
}

func TestProfileSettingsResolverReportsOnlyActualInvocation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		calls := 0
		exec.SetSettingsResolver(func() SettingsSnapshot { calls++; return SettingsSnapshot{} })
		for _, query := range []string{"SHOW SETTINGS", "CALL dbms.listConfig('nornicdb.version') YIELD name RETURN name"} {
			calls = 0
			collector := newProfileRuntimeCollector()
			_, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), query, nil)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			if query == "SHOW SETTINGS" {
				accesses := collector.accessObservation()
				require.Len(t, accesses, 1)
				require.True(t, accesses[0].Unknown)
				require.Equal(t, "settings resolver", accesses[0].Detail)
				// PROFILE for this command is explicitly declined by the
				// existing result-column contract; observation does not add it.
				_, profileErr := exec.Execute(t.Context(), "PROFILE "+query, nil)
				require.Error(t, profileErr)
				require.Equal(t, unresolvedStatementError().Error(), profileErr.Error())
				require.Equal(t, 1, calls, "rejected preparation does not invoke the resolver")
				continue
			}
			result, err := exec.Execute(t.Context(), "PROFILE "+query, nil)
			require.NoError(t, err)
			observed := executionPlanFromResult(t, result)
			require.Equal(t, 2, calls, "one call per public execution")
			require.Contains(t, observed.Unobserved, "database reads: settings resolver")
			require.False(t, observed.TotalDBHitsObserved)
		}
	})
}

// A connection callback remains opaque even if it emits a legitimate observed
// nested read: observing part of a callback cannot certify its whole body.
func TestProfileConnectionsRetainsActualCallbackOpacity(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		calls := 0
		collector := newProfileRuntimeCollector()
		var ctx context.Context
		identity := &RequestIdentity{Connections: func() []ConnectionListing {
			calls++
			queryprofile.Access(ctx, profileKindNodeCountRequest, "").Add(1, 1)
			return []ConnectionListing{{ConnectionID: "connection-1", Username: "tester"}}
		}}
		ctx = WithRequestIdentity(withProfileRuntimeCollector(t.Context(), collector), identity)
		result, err := exec.Execute(ctx, "PROFILE CALL dbms.listConnections() YIELD connectionId RETURN connectionId", nil)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, [][]interface{}{{"connection-1"}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.Contains(t, plan.Unobserved, "database reads: connection snapshot provider")
		require.False(t, plan.TotalDBHitsObserved)
		require.Len(t, profileAccessesOfKind(collector, profileKindNodeCountRequest), 1, "the custom callback performed an observed inner read")
	})
}

type profileIdentityManager struct {
	*mockDatabaseManager
	identities int
	systems    int
}

func (m *profileIdentityManager) DatabaseIdentity(string) (string, time.Time) {
	m.identities++
	return "identity", time.Time{}
}
func (m *profileIdentityManager) ServerIdentity() (string, time.Time) { return "server", time.Time{} }

var _ DatabaseIdentityProvider = (*profileIdentityManager)(nil)

func (m *profileIdentityManager) SystemDatabaseName() string { m.systems++; return "system" }

func TestProfileInfoReportsActualInjectedIdentityCalls(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, name := range []string{"db.info", "dbms.info"} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			manager := &profileIdentityManager{mockDatabaseManager: newMockDatabaseManager(t)}
			exec.SetDatabaseManager(manager)
			result, err := exec.Execute(t.Context(), "PROFILE CALL "+name+"() YIELD id RETURN id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"identity"}}, result.Rows)
			require.Equal(t, 1, manager.identities)
			plan := executionPlanFromResult(t, result)
			require.Contains(t, plan.Unobserved, "database reads: database identity provider")
			require.False(t, plan.TotalDBHitsObserved)
			if name == "dbms.info" {
				require.Equal(t, 1, manager.systems)
				require.Contains(t, plan.Unobserved, "database reads: system database provider")
			} else {
				require.Zero(t, manager.systems)
			}
		}
	})
}

func TestProfileConnectionsObservesInjectedPermissionAndKeepsFiltering(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		identity := &RequestIdentity{User: &AuthenticatedUser{Name: "self"}, Connections: func() []ConnectionListing {
			return []ConnectionListing{
				{ConnectionID: "visible", Username: "self"}, {ConnectionID: "other", Username: "other"},
			}
		}}
		ctx := WithPermissionChecker(WithRequestIdentity(t.Context(), identity), func(permission string) bool { return permission != "admin" })
		query := "CALL dbms.listConnections() YIELD connectionId RETURN connectionId"
		plain, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		profiled, err := exec.Execute(ctx, "PROFILE "+query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"visible"}}, plain.Rows)
		require.Equal(t, plain.Rows, profiled.Rows)
		plan := executionPlanFromResult(t, profiled)
		require.Contains(t, plan.Unobserved, "database reads: permission resolver")
		require.Contains(t, plan.Unobserved, "database reads: connection snapshot provider")
		require.False(t, plan.TotalDBHitsObserved)
	})
}

func BenchmarkOperationalMetadataObservation(b *testing.B) {
	for _, procedure := range []string{"db.info()", "nornicdb.stats()", "db.awaitIndexes(1)", "dbms.listConfig('nornicdb.version')"} {
		b.Run(procedure, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "profile"), QueryCachePolicy{MaxEntries: 0})
			query := "CALL " + procedure
			_, err := exec.Execute(b.Context(), query, nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(b.Context(), query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
