package cypher

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReadBoundaryPendingUntypedMatch(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			wal, err := storage.NewWAL(filepath.Join(t.TempDir(), "wal"), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, wal.Close()) })
			engine := storage.NewTracedEngine(storage.NewWALEngine(storage.NewNamespacedEngine(base, "tenant"), wal))
			exec := NewStorageExecutor(engine)
			ctx := context.Background()
			query := "CREATE (:A)-[:LINK]->(:B) WITH 1 AS marker MATCH ()-[r]->() RETURN type(r)"
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				_, err = exec.Execute(ctx, "CREATE (:A)-[:LINK]->(:B)", nil)
				require.NoError(t, err)
				query = "MATCH ()-[r]->() RETURN type(r)"
			}
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"LINK"}}, result.Rows)
		})
	}
}

func TestReadBoundaryCompositePolicySource(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			_, manager := newCompositeIdentityExecutor(t, false)
			for index, database := range []string{"one", "two"} {
				engine, err := manager.GetStorage(database)
				require.NoError(t, err)
				base := unwrapBadgerEngine(engine)
				require.NotNil(t, base)
				base.SetDecayEnabled(true)
				profiles := map[string]*knowledgepolicy.DecayProfileBundle{}
				bindings := map[string]*knowledgepolicy.DecayProfileBinding{}
				for name, scope := range map[string]knowledgepolicy.ScopeType{"nodes": knowledgepolicy.ScopeNode, "edges": knowledgepolicy.ScopeEdge} {
					profile := database + "_" + name
					profiles[profile] = &knowledgepolicy.DecayProfileBundle{Name: profile, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromLastAccessed, Enabled: true, DecayEnabled: true}
					bindings[name] = &knowledgepolicy.DecayProfileBinding{Name: name, TargetLabels: []string{"P"}, ProfileRef: profile, IsEdge: scope == knowledgepolicy.ScopeEdge, TargetEdgeType: "R"}
				}
				table, err := knowledgepolicy.BuildBindingTable(profiles, bindings, nil, nil)
				require.NoError(t, err)
				base.GetSchemaForNamespace(database).SetBindingTable(table)
				for id, scope := range map[string]knowledgepolicy.ScopeType{"same": knowledgepolicy.ScopeNode, "same-rel": knowledgepolicy.ScopeEdge} {
					target := database + ":" + id
					require.NoError(t, base.PutAccessMeta(target, &knowledgepolicy.AccessMetaEntry{TargetID: target, TargetScope: scope, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: int64(index + 3), LastAccessedAt: time.Now().UnixNano()}}))
				}
			}
			composite, err := manager.GetStorage("comp")
			require.NoError(t, err)
			exec := NewStorageExecutor(storage.NewTracedEngine(composite))
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for index, database := range []string{"one", "two"} {
				t.Run(database, func(t *testing.T) {
					query := "USE comp CALL { USE comp." + database + " MATCH (n:P)-[r:R]->() RETURN n,r } RETURN policy(n).targetId,policy(n).accessCount,decay(n).policy,policy(r).targetId,policy(r).accessCount,decay(r).policy"
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{database + ":same", int64(index + 3), database + "_nodes", database + ":same-rel", int64(index + 3), database + "_edges"}}, result.Rows)
					// The ordinary scoped engine is an independent policy/decay
					// oracle for the imported participant and its metadata.
					source, err := manager.GetStorage(database)
					require.NoError(t, err)
					oracle := NewStorageExecutor(source)
					want, err := oracle.Execute(ctx, "MATCH (n:P)-[r:R]->() RETURN policy(n).targetId,policy(n).accessCount,decay(n).policy,policy(r).targetId,policy(r).accessCount,decay(r).policy", nil)
					require.NoError(t, err)
					require.Equal(t, want.Rows, result.Rows)
				})
			}
		})
	}
}
