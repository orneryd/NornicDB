package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/search"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProfileVectorSettersObserveOnlyActualReadsWritesAndCallbacks(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, relationship := range []bool{false, true} {
			for _, supplied := range []bool{false, true} {
				for _, changed := range []bool{false, true} {
					name := "node"
					if relationship {
						name = "relationship"
					}
					if supplied {
						name += "/image"
					} else {
						name += "/id"
					}
					if changed {
						name += "/changed"
					} else {
						name += "/unchanged"
					}
					t.Run(name, func(t *testing.T) {
						exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:ProfileVector {embedding:[0.0,1.0,0.0]})-[:ProfileVectorRel {embedding:[0.0,1.0,0.0]}]->(:ProfileOther)")
						selection := "MATCH (n:ProfileVector) RETURN n"
						procedure := "db.create.setNodeVectorProperty"
						kind, write := profileKindNodeByIdSeek, profileKindNodeUpdateRequest
						if relationship {
							selection = "MATCH ()-[r:ProfileVectorRel]->() RETURN r"
							procedure = "db.create.setRelationshipVectorProperty"
							kind, write = queryprofile.KindRelationshipByIDSeek, profileKindRelationshipUpdateRequest
						}
						rows, err := exec.Execute(t.Context(), selection, nil)
						require.NoError(t, err)
						require.Len(t, rows.Rows, 1)
						entity := rows.Rows[0][0]
						var id string
						switch value := entity.(type) {
						case *storage.Node:
							id = string(value.ID)
						case *storage.Edge:
							id = string(value.ID)
						default:
							t.Fatalf("unexpected entity %T", entity)
						}
						input := entity
						if !supplied {
							input = id
						}
						vector := []interface{}{float64(0), float64(1), float64(0)}
						if changed {
							vector = []interface{}{float64(1), float64(0), float64(0)}
						}
						calls := 0
						if !relationship {
							exec.SetNodeMutatedCallback(func(string) { calls++ })
						}
						result, err := exec.Execute(t.Context(), "PROFILE CALL "+procedure+"($entity,'embedding',$vector) RETURN 7 AS n", map[string]interface{}{"entity": input, "vector": vector})
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
						plan := executionPlanFromResult(t, result)
						if !relationship && changed {
							require.Equal(t, 1, calls)
							require.False(t, plan.TotalDBHitsObserved)
							require.Contains(t, plan.Unobserved, "database reads: node mutation callback")
						} else {
							require.Zero(t, calls)
							require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
							require.Empty(t, plan.Unobserved)
						}
						point := planOperatorOfType(plan.Root, kind)
						if supplied {
							require.Nil(t, point, "a supplied image is not another storage read")
						} else {
							require.NotNil(t, point)
							require.EqualValues(t, 1, point.ActualRows)
							require.EqualValues(t, 1, point.DBHits)
						}
						update := planOperatorOfType(plan.Root, write)
						if changed {
							require.NotNil(t, update)
							require.EqualValues(t, 1, update.DBHits)
						} else {
							require.Nil(t, update, "unchanged vector issues no mutation")
						}
						var properties map[string]interface{}
						if relationship {
							edge, err := exec.storage.GetEdge(t.Context(), storage.EdgeID(id))
							require.NoError(t, err)
							properties = edge.Properties
						} else {
							node, err := exec.storage.GetNode(t.Context(), storage.NodeID(id))
							require.NoError(t, err)
							properties = node.Properties
						}
						stored, ok := toFloat64Slice(properties["embedding"])
						require.True(t, ok)
						wanted, _ := toFloat64Slice(vector)
						require.Equal(t, wanted, stored)
					})
				}
			}
		}
	})
}

func TestProfileVectorSetterReportsCommittedSearchRefreshPointRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, relationship := range []bool{false, true} {
			t.Run(map[bool]string{false: "node", true: "relationship"}[relationship], func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:RefreshVector {embedding:[0.0,1.0,0.0]})-[:RefreshVectorRel {embedding:[0.0,1.0,0.0]}]->(:RefreshOther)")
				service := search.NewService(exec.storage, 3, nil)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				service.SetRuntimeStrategyTransitionsEnabled(false)
				require.NoError(t, service.BuildIndexes(t.Context()))
				require.True(t, service.IsReady())
				exec.SetSearchService(service)
				selection := "MATCH (n:RefreshVector) RETURN n"
				procedure := "db.create.setNodeVectorProperty"
				kind := queryprofile.KindNodeByIDSeek
				if relationship {
					selection = "MATCH ()-[r:RefreshVectorRel]->() RETURN r"
					procedure = "db.create.setRelationshipVectorProperty"
					kind = queryprofile.KindRelationshipByIDSeek
				}
				selected, err := exec.Execute(t.Context(), selection, nil)
				require.NoError(t, err)
				require.Len(t, selected.Rows, 1)
				var id string
				switch entity := selected.Rows[0][0].(type) {
				case *storage.Node:
					id = string(entity.ID)
				case *storage.Edge:
					id = string(entity.ID)
				default:
					t.Fatalf("unexpected image %T", entity)
				}
				result, err := exec.Execute(t.Context(), "PROFILE CALL "+procedure+"($id,'embedding',[1.0,0.0,0.0]) RETURN 7 AS n", map[string]interface{}{"id": id})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
				plan := executionPlanFromResult(t, result)
				require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
				var rows, hits int64
				for _, point := range profilePlanOperators(plan.Root, kind) {
					rows += point.ActualRows
					hits += point.DBHits
				}
				require.EqualValues(t, 2, rows, "one setter read and one synchronous committed refresh read")
				require.EqualValues(t, 2, hits)
			})
		}
	})
}

func TestSearchRefreshObservationPreservesServiceCancellationOwnership(t *testing.T) {
	exec, _ := newProfileRuntimeTestExecutor(t)
	_, err := exec.storage.CreateNode(t.Context(), &storage.Node{ID: "refresh-owner", Properties: map[string]interface{}{"content": "beforeword"}})
	require.NoError(t, err)
	service := search.NewService(exec.storage, 3, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetRuntimeStrategyTransitionsEnabled(false)
	require.NoError(t, service.BuildIndexes(t.Context()))
	exec.SetSearchService(service)
	node, err := exec.storage.GetNode(t.Context(), "refresh-owner")
	require.NoError(t, err)
	node.Properties["content"] = "afterword"
	require.NoError(t, exec.storage.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	collector := newProfileRuntimeCollector()
	ctx, cancel := context.WithCancel(withProfileRuntimeCollector(t.Context(), collector))
	cancel()
	exec.notifyNodeMutated(ctx, "refresh-owner")
	require.Equal(t, 1, consumerHits(t, service, "afterword"))
	require.Zero(t, consumerHits(t, service, "beforeword"))
	var rows, hits int64
	for _, read := range collector.accessObservation() {
		if read.Kind == queryprofile.KindNodeByIDSeek {
			rows += read.Rows
			hits += read.Hits
		}
	}
	require.EqualValues(t, 1, rows)
	require.EqualValues(t, 1, hits)
}
