package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Native temporal eligibility is part of a committed vector setter's search
// refresh. Historical versions must stay excluded without hiding its reads.
func TestProfileVectorRefreshObservesNativeTemporalEligibility(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, listKey := range []bool{false, true} {
			for _, historical := range []bool{false, true} {
				name := "current"
				if historical {
					name = "historical"
				}
				if listKey {
					name += "/list-buffered"
				}
				t.Run(name, func(t *testing.T) {
					base, err := testfixture.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: listKey, WriteBehindInterval: time.Hour}))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					store := storage.NewNamespacedEngine(base, "temporal_profile_refresh")
					require.NoError(t, store.GetSchema().AddConstraint(storage.Constraint{
						Name: "fact_temporal", Type: storage.ConstraintTemporal, Label: "FactVersion",
						Properties: []string{"fact_key", "valid_from", "valid_to"},
					}))
					start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
					end := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
					var key any = "k"
					if listKey {
						key = []interface{}{"k"}
					}
					for _, v := range []struct {
						id   string
						from time.Time
						to   any
					}{{"old", start, end}, {"current", end, nil}} {
						_, err := store.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(v.id), Labels: []string{"FactVersion"}, Properties: map[string]any{
							"fact_key": key, "valid_from": v.from, "valid_to": v.to, "embedding": []float64{0, 1, 0},
						}})
						require.NoError(t, err)
					}
					if listKey {
						for _, namespace := range []string{"temporal_profile_refresh", "other"} {
							other := storage.NewNamespacedEngine(base, namespace)
							_, err := other.CreateNode(t.Context(), &storage.Node{ID: "unrelated", Labels: []string{"FactVersion"}, Properties: map[string]any{
								"fact_key": []interface{}{"different"}, "valid_from": start,
							}})
							require.NoError(t, err)
						}
					}
					service := search.NewService(store, 3, nil)
					t.Cleanup(func() { require.NoError(t, service.Close()) })
					service.SetRuntimeStrategyTransitionsEnabled(false)
					require.NoError(t, service.BuildIndexes(t.Context()))
					exec := NewStorageExecutor(store)
					exec.SetSearchService(service)
					id := "current"
					if historical {
						id = "old"
					}
					result, err := exec.Execute(t.Context(), "PROFILE CALL db.create.setNodeVectorProperty($id,'embedding',[1.0,0.0,0.0]) RETURN 7 AS n", map[string]any{"id": id})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
					plan := executionPlanFromResult(t, result)
					require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
					eligibility := planOperatorOfType(plan.Root, "TemporalEligibilityRequest")
					require.NotNil(t, eligibility, "native search refresh must account for its eligibility lookup")
					require.EqualValues(t, 1, eligibility.DBHits)
					require.EqualValues(t, 1, eligibility.ActualRows, "a false eligibility decision is still a returned decision")
					scan := planOperatorOfType(plan.Root, queryprofile.KindNodeByLabelScan)
					if listKey {
						require.NotNil(t, scan, "the buffered list-key fallback actually scans")
						require.EqualValues(t, 4, scan.ActualRows, "the physical label acquisition precedes key and namespace filtering")
						require.EqualValues(t, 4, scan.DBHits)
					} else {
						require.Nil(t, scan, "authoritative scalar lookup must not invent a label scan")
					}
					require.Equal(t, 1, service.EmbeddingCount(), "the historical version must remain excluded")
					node, err := store.GetNode(t.Context(), storage.NodeID(id))
					require.NoError(t, err)
					actual, ok := toFloat64Slice(node.Properties["embedding"])
					require.True(t, ok)
					require.Equal(t, []float64{1, 0, 0}, actual)
				})
			}
		}
	})
}
