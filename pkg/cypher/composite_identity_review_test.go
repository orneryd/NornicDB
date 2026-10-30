package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCompositeReviewEndpointAndPolicy(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			exec, _ := newCompositeIdentityExecutor(t, explicit)
			for _, tc := range []struct {
				query string
				want  [][]interface{}
			}{
				{"USE comp MATCH (n:P)-[r:R]->(m:Q) RETURN elementId(n),elementId(r),elementId(m) ORDER BY elementId(n)", [][]interface{}{{"4:one:same", "5:one:same-rel", "4:one:target"}, {"4:two:same", "5:two:same-rel", "4:two:target"}}},
				{"USE comp MATCH (n:P)-[r:R]->(m:Q) RETURN count(DISTINCT m)", [][]interface{}{{int64(2)}}},
				{"USE comp MATCH (n:P)-[r:R]->(m:Q) RETURN policy(n).targetId,policy(r).targetId,policy(r).targetScope ORDER BY elementId(n)", [][]interface{}{{"one:same", "one:same-rel", "EDGE"}, {"two:same", "two:same-rel", "EDGE"}}},
			} {
				t.Run(tc.query, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, result.Rows)
				})
			}
		})
	}
}

func TestCompositeReviewRelationshipWrites(t *testing.T) {
	for _, operation := range []string{"CREATE", "MERGE"} {
		for _, cross := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cross=%v", operation, cross), func(t *testing.T) {
				exec, mgr := newCompositeIdentityExecutor(t, false)
				source := "two"
				if cross {
					source = "one"
				}
				query := fmt.Sprintf("USE comp MATCH (n:P {db:'%s'}),(m:P {db:'two'}) %s (n)-[r:REVIEW_WRITE]->(m) RETURN elementId(r)", source, operation)
				result, err := exec.Execute(context.Background(), query, nil)
				if cross {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
				}
				for _, db := range []string{"one", "two"} {
					store, err := mgr.GetStorage(db)
					require.NoError(t, err)
					edges, err := store.GetEdgesByType("REVIEW_WRITE")
					require.NoError(t, err)
					want := 0
					if !cross && db == "two" {
						want = 1
					}
					require.Len(t, edges, want, "database %s", db)
				}
			})
		}
	}
}

func TestCompositeReviewImportedRelationshipDelete(t *testing.T) {
	exec, mgr := newCompositeIdentityExecutor(t, false)
	_, err := exec.Execute(context.Background(), "USE comp CALL { USE comp.two MATCH ()-[r:R]->() RETURN r } DELETE r", nil)
	require.NoError(t, err)
	one, err := mgr.GetStorage("one")
	require.NoError(t, err)
	two, err := mgr.GetStorage("two")
	require.NoError(t, err)
	first, err := one.GetEdgesByType("R")
	require.NoError(t, err)
	second, err := two.GetEdgesByType("R")
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Empty(t, second)
}

func TestCompositeReviewImportedPendingWrites(t *testing.T) {
	for _, terminal := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(terminal, func(t *testing.T) {
			exec, mgr := newCompositeIdentityExecutor(t, true)
			ctx := context.Background()
			result, err := exec.Execute(ctx, "USE comp CALL { USE comp.two MATCH (n:P) RETURN n } SET n.v=99 RETURN n.v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(99)}}, result.Rows)
			result, err = exec.Execute(ctx, "USE comp MATCH (n:P {db:'two'}) RETURN n.v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(99)}}, result.Rows)
			one, err := mgr.GetStorage("one")
			require.NoError(t, err)
			two, err := mgr.GetStorage("two")
			require.NoError(t, err)
			before, err := two.GetNode("same")
			require.NoError(t, err)
			require.Equal(t, int64(7), before.Properties["v"], "pending write must not escape its participant")
			_, err = exec.Execute(ctx, terminal, nil)
			require.NoError(t, err)
			first, err := one.GetNode("same")
			require.NoError(t, err)
			require.Equal(t, int64(7), first.Properties["v"])
			second, err := two.GetNode("same")
			require.NoError(t, err)
			want := int64(7)
			if terminal == "COMMIT" {
				want = 99
			}
			require.Equal(t, want, second.Properties["v"])
		})
	}
}

func TestCompositeReviewPolicyReadsSourceMetadata(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			exec, manager := newCompositeIdentityExecutor(t, explicit)
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
			result, err := exec.Execute(context.Background(), "USE comp MATCH (n:P)-[r:R]->() RETURN decay(n).policy,decay(r).policy,policy(n).accessCount,policy(r).accessCount ORDER BY elementId(n)", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			for index, database := range []string{"one", "two"} {
				row := result.Rows[index]
				require.Equal(t, database+"_nodes", row[0])
				require.Equal(t, database+"_edges", row[1])
				require.EqualValues(t, index+3, row[2])
				require.EqualValues(t, index+3, row[3])
			}
		})
	}
}
