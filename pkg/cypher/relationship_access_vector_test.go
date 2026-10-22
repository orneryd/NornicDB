package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Independent storage observations: assertions must not trust a descriptor or a
// hot-path flag as evidence that an index was queried or that planning was pure.
type relationshipVectorReadCounter struct {
	storage.Engine
	edgeScans, edgeReads, nodeReads atomic.Int64
}

func (s *relationshipVectorReadCounter) GetInnerEngine() storage.Engine {
	return s.Engine
}

func (s *relationshipVectorReadCounter) AllEdges() ([]*storage.Edge, error) {
	s.edgeScans.Add(1)
	return s.Engine.AllEdges()
}
func (s *relationshipVectorReadCounter) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	s.edgeReads.Add(1)
	return s.Engine.GetEdge(id)
}
func (s *relationshipVectorReadCounter) AllNodes() ([]*storage.Node, error) {
	s.nodeReads.Add(1)
	return s.Engine.AllNodes()
}
func (s *relationshipVectorReadCounter) GetNodesByLabel(label string) ([]*storage.Node, error) {
	s.nodeReads.Add(1)
	return s.Engine.GetNodesByLabel(label)
}
func (s *relationshipVectorReadCounter) GetNode(id storage.NodeID) (*storage.Node, error) {
	s.nodeReads.Add(1)
	return s.Engine.GetNode(id)
}
func (s *relationshipVectorReadCounter) reset() {
	s.edgeScans.Store(0)
	s.edgeReads.Store(0)
	s.nodeReads.Store(0)
}

// Positive index-use fixtures need a populated search service as well as an
// index definition. Independent reads distinguish the seek from scan fallback.
func relationshipVectorIndexedFixture(t *testing.T, e *StorageExecutor, store *relationshipVectorReadCounter, dimensions int) {
	t.Helper()
	svc := search.NewServiceWithDimensions(store, dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))
	e.SetSearchService(svc)
	store.reset()
}

func TestRelationshipAccessProgramVectorPhysicalChoices(t *testing.T) {
	for _, serviceState := range []string{"absent", "unpopulated", "populated", "index_error"} {
		t.Run(serviceState, func(t *testing.T) {
			store := &relationshipVectorReadCounter{Engine: storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_choices")}
			e := NewStorageExecutor(store)
			for _, query := range []string{
				"CREATE ()-[:RELATES_TO {uuid:'r1',fact_embedding:[1.0,0.0,0.0]}]->()",
				"CREATE ()-[:RELATES_TO {uuid:'r2',fact_embedding:[0.0,1.0,0.0]}]->()",
				"CREATE ()-[:OTHER {fact_embedding:[1.0,0.0,0.0]}]->()",
				"CREATE (:QueryVector {v:[1.0,0.0,0.0], threshold:0.25})",
				"CREATE VECTOR INDEX vector_choices FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig:{`vector.dimensions`:3,`vector.similarity_function`:'cosine'}}",
			} {
				_, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err)
			}
			if serviceState != "absent" {
				var engine storage.Engine = store
				if serviceState == "index_error" {
					// A real service can retain vector metadata while its query
					// backend is unavailable. Its public query returns that error.
					engine = nil
				}
				svc := search.NewServiceWithDimensions(engine, 3)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				if serviceState == "populated" {
					require.NoError(t, svc.BuildIndexes(context.Background()))
				} else if serviceState == "index_error" {
					edges, err := store.AllEdges()
					require.NoError(t, err)
					for _, edge := range edges {
						require.NoError(t, svc.IndexEdge(edge))
					}
					_, err = svc.VectorQueryRelationships(context.Background(), []float32{1, 0, 0}, search.RelationshipVectorQuerySpec{Type: "RELATES_TO", Property: "fact_embedding", Limit: 1})
					require.Error(t, err, "independent runtime index-error control")
				}
				e.SetSearchService(svc)
				require.Equal(t, serviceState == "populated" || serviceState == "index_error", svc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))
			}
			for _, tc := range relationshipAccessGeneratedVectorPublicCases() {
				t.Run(tc.name, func(t *testing.T) {
					query, params, want := tc.query, tc.params, tc.rows
					order := "ASC"
					if tc.descending {
						order = "DESC"
					}
					index := serviceState == "populated" && tc.descending
					for _, mode := range []string{"EXPLAIN ", "", "PROFILE "} {
						store.reset()
						result, err := e.Execute(context.Background(), mode+query, params)
						require.NoError(t, err, mode)
						require.Equal(t, index && mode != "EXPLAIN ", e.LastHotPathTrace().CosineVectorIndexFastPath, mode)
						if mode == "EXPLAIN " {
							require.Empty(t, result.Rows)
							require.Zero(t, store.edgeScans.Load(), "EXPLAIN edge scan")
							require.Zero(t, store.edgeReads.Load(), "EXPLAIN relationship read")
							require.Zero(t, store.nodeReads.Load(), "EXPLAIN expression evaluation")
						} else {
							require.Equal(t, want, result.Rows, mode)
							if index {
								require.Zero(t, store.edgeScans.Load())
								require.Positive(t, store.edgeReads.Load())
							} else {
								require.EqualValues(t, 1, store.edgeScans.Load())
							}
						}
						if mode == "" {
							continue
						}
						root := result.Metadata["plan"].(*ExecutionPlan).Root
						seek := relationshipPlanOperator(root, "RelationshipVectorIndexSeek")
						plannedIndex := index || (mode == "EXPLAIN " && serviceState == "index_error" && order == "DESC")
						if plannedIndex {
							require.NotNil(t, seek, mode)
							require.Equal(t, "vector_choices", seek.Arguments["index"])
							if mode == "EXPLAIN " {
								require.Equal(t, true, seek.Arguments["conditional"])
								require.NotNil(t, relationshipPlanOperator(root, "RelationshipAccessChoice"))
								require.NotNil(t, relationshipPlanOperator(root, "Expand"))
							} else {
								require.EqualValues(t, 1, seek.Arguments["indexLookups"])
							}
						} else {
							require.Nil(t, seek, mode)
							scan := relationshipPlanOperator(root, "Expand")
							require.NotNil(t, scan)
							require.Empty(t, scan.Arguments["index"])
							if mode == "PROFILE " {
								require.EqualValues(t, 3, scan.Arguments["scanRelationships"])
								lookups := int64(0)
								if serviceState == "index_error" && order == "DESC" {
									lookups = 1
									for _, key := range []string{"failedIndex", "indexError"} {
										require.NotEmpty(t, scan.Arguments[key])
									}
								}
								require.EqualValues(t, lookups, scan.Arguments["indexLookups"])
								require.EqualValues(t, 3+lookups, scan.DBHits)
								require.EqualValues(t, 1, scan.ActualRows)
							}
						}
					}
					for _, mode := range []string{"EXPLAIN ", "", "PROFILE "} {
						store.reset()
						zero := query[:len(query)-1] + "0"
						result, err := e.Execute(context.Background(), mode+zero, params)
						require.NoError(t, err, mode)
						require.False(t, e.LastHotPathTrace().CosineVectorIndexFastPath, "zero limit performs no index access")
						require.Empty(t, result.Rows)
						require.Zero(t, store.edgeScans.Load())
						require.Zero(t, store.edgeReads.Load())
						require.Zero(t, store.nodeReads.Load(), "zero limit must not resolve the query vector")
						if mode != "" {
							root := result.Metadata["plan"].(*ExecutionPlan).Root
							require.Nil(t, relationshipPlanOperator(root, "RelationshipVectorIndexSeek"))
							empty := relationshipPlanOperator(root, "EmptyResult")
							require.NotNil(t, empty)
							require.Zero(t, empty.DBHits)
						}
					}
				})
			}
		})
	}
}
