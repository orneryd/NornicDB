package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProfileIndexProceduresObserveAdmissionDropAndPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, vector := range []bool{false, true} {
			for _, relationship := range []bool{false, true} {
				for _, wired := range []bool{false, true} {
					if wired && !vector {
						continue
					}
					name := "fulltext"
					if vector {
						name = "vector"
					}
					if relationship {
						name += "/relationship"
					} else {
						name += "/node"
					}
					if wired {
						name += "/service"
					} else {
						name += "/storage"
					}
					t.Run(name, func(t *testing.T) {
						var plainCreate, plainDrop *ExecuteResult
						for _, profiled := range []bool{false, true} {
							exec, _ := newProfileRuntimeTestExecutor(t, `CREATE
        (a:Indexed {name:'vector',text:'hotel',embedding:[1.0,0.0,0.0]}),
        (b:Indexed {name:'plain',text:'hotel'}),
        (c:Other {name:'other',embedding:[0.0,1.0,0.0]}),
        (a)-[:IndexedRel {name:'vector',text:'hotel',embedding:[1.0,0.0,0.0]}]->(b),
        (b)-[:IndexedRel {name:'plain',text:'hotel',embedding:[0.0,1.0,0.0]}]->(c),
        (c)-[:OtherRel {embedding:[0.0,1.0,0.0]}]->(a)`)
							schema := exec.storage.GetSchema()
							constraints, indexes := len(schema.GetAllConstraints()), len(schema.GetIndexes())
							var service *search.Service
							if wired {
								service = search.NewService(exec.storage, 3, nil)
								t.Cleanup(func() { require.NoError(t, service.Close()) })
								service.SetRuntimeStrategyTransitionsEnabled(false)
								require.NoError(t, service.BuildIndexes(t.Context()))
								require.True(t, service.IsReady())
								exec.SetSearchService(service)
								// Leave one actual stored member absent from the live service so
								// index creation must reconcile, not merely inspect a settled index.
								selection := "MATCH (n:Indexed {name:'vector'}) RETURN n"
								if relationship {
									selection = "MATCH ()-[r:IndexedRel {name:'vector'}]->() RETURN r"
								}
								selected, err := exec.Execute(t.Context(), selection, nil)
								require.NoError(t, err)
								require.Len(t, selected.Rows, 1)
								switch entity := selected.Rows[0][0].(type) {
								case *storage.Node:
									require.NoError(t, service.RemoveNode(entity.ID))
								case *storage.Edge:
									require.NoError(t, service.RemoveEdge(entity.ID))
								default:
									t.Fatalf("unexpected image %T", entity)
								}
							}
							target, kind, method := "Indexed", "fulltext", "createNodeIndex"
							if relationship {
								target = "IndexedRel"
								method = "createRelationshipIndex"
							}
							args := "'profile_mutation_index',['" + target + "'],['text']"
							if vector {
								kind = "vector"
								args = "'profile_mutation_index','" + target + "','embedding',3,'cosine'"
							}
							query := "CALL db.index." + kind + "." + method + "(" + args + ") RETURN 7 AS n"
							if profiled {
								query = "PROFILE " + query
							}
							created, err := exec.Execute(t.Context(), query, nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{int64(7)}}, created.Rows)
							if !profiled {
								plainCreate = created
							} else {
								require.Equal(t, plainCreate.Columns, created.Columns)
								require.Equal(t, plainCreate.Rows, created.Rows)
								plan := executionPlanFromResult(t, created)
								require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
								require.Empty(t, plan.Unobserved)
								catalog := planOperatorOfType(plan.Root, profileKindSchemaIndexCatalog)
								require.NotNil(t, catalog)
								require.EqualValues(t, indexes, catalog.ActualRows)
								require.EqualValues(t, 1, catalog.DBHits)
								constraintReads := profilePlanOperators(plan.Root, profileKindSchemaConstraintCatalog)
								require.NotEmpty(t, constraintReads)
								var rows, hits int64
								for _, read := range constraintReads {
									rows += read.ActualRows
									hits += read.DBHits
								}
								require.EqualValues(t, constraints, rows)
								require.GreaterOrEqual(t, hits, int64(1))
								if !wired {
									require.EqualValues(t, 2, plan.TotalDBHits)
								} else {
									populationKind := queryprofile.KindNodeByLabelScan
									pointKind := queryprofile.KindNodeByIDSeek
									if relationship {
										populationKind = "RelationshipByTypeScan"
										pointKind = queryprofile.KindRelationshipByIDSeek
									}
									population := planOperatorOfType(plan.Root, populationKind)
									require.NotNil(t, population)
									require.Equal(t, target, population.Description)
									require.EqualValues(t, 2, population.ActualRows)
									require.EqualValues(t, 2, population.DBHits)
									var rows, hits int64
									for _, point := range profilePlanOperators(plan.Root, pointKind) {
										rows += point.ActualRows
										hits += point.DBHits
									}
									require.EqualValues(t, 2, rows, "refresh plus settled-state reread of the missing live member")
									require.EqualValues(t, 2, hits)
								}
							}
							currentSchema := exec.storage.GetSchema()
							if vector {
								index, ok := currentSchema.GetVectorIndex("profile_mutation_index")
								require.True(t, ok)
								require.Equal(t, target, index.Label)
							} else {
								index, ok := currentSchema.GetFulltextIndex("profile_mutation_index")
								require.True(t, ok)
								if relationship {
									require.Contains(t, index.RelationshipTypes, target)
								} else {
									require.Contains(t, index.Labels, target)
								}
							}
							dropQuery := "CALL db.index." + kind + ".drop('profile_mutation_index') RETURN 7 AS n"
							if profiled {
								dropQuery = "PROFILE " + dropQuery
							}
							dropped, err := exec.Execute(t.Context(), dropQuery, nil)
							require.NoError(t, err)
							require.Equal(t, [][]interface{}{{int64(7)}}, dropped.Rows)
							if !profiled {
								plainDrop = dropped
							} else {
								require.Equal(t, plainDrop.Columns, dropped.Columns)
								require.Equal(t, plainDrop.Rows, dropped.Rows)
								plan := executionPlanFromResult(t, dropped)
								require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
								vectorLookup := planOperatorOfType(plan.Root, "VectorIndexByNameSeek")
								require.NotNil(t, vectorLookup)
								expectedHits, expectedRows := 1, 0
								if vector {
									expectedHits, expectedRows = 2, 2
								}
								require.EqualValues(t, expectedHits, vectorLookup.DBHits)
								require.EqualValues(t, expectedRows, vectorLookup.ActualRows)
								total := int64(2)
								if wired {
									total++
									population := planOperatorOfType(plan.Root, "VectorIndexPopulationByNameSeek")
									require.NotNil(t, population)
									require.EqualValues(t, 1, population.DBHits)
									// The drop reads the record of the population this service served:
									// the created index is ONLINE, so the lookup finds it.
									require.EqualValues(t, 1, population.ActualRows)
								}
								require.Equal(t, total, plan.TotalDBHits)
							}
							_, exists := exec.storage.GetSchema().GetVectorIndex("profile_mutation_index")
							require.False(t, exists)
							_, exists = exec.storage.GetSchema().GetFulltextIndex("profile_mutation_index")
							require.False(t, exists)
						}
					})
				}
			}
		}
	})
}

// Failed admissions observe the reader that actually ran, not the operation
// that would have followed. Use the public collector because failed PROFILE
// statements intentionally return no successful execution plan.
func TestProfileIndexMutationFailuresObserveOnlyAttemptedAcquisitions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, kind := range []string{"fulltext", "vector"} {
			for _, wrongKind := range []bool{false, true} {
				name := kind + "/missing"
				if wrongKind {
					name = kind + "/wrong-kind"
				}
				t.Run(name, func(t *testing.T) {
					exec, _ := newProfileRuntimeTestExecutor(t)
					if wrongKind {
						seed := "CREATE VECTOR INDEX other_kind FOR (n:Other) ON (n.emb) OPTIONS {indexConfig:{`vector.dimensions`:3}}"
						if kind == "vector" {
							seed = "CREATE FULLTEXT INDEX other_kind FOR (n:Other) ON EACH [n.text]"
						}
						_, err := exec.Execute(t.Context(), seed, nil)
						require.NoError(t, err)
					}
					before := exec.storage.GetSchema().GetIndexes()
					statement := "CALL db.index." + kind + ".drop('other_kind')"
					_, plainErr := exec.Execute(t.Context(), statement, nil)
					require.Error(t, plainErr)
					collector := newProfileRuntimeCollector()
					_, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, nil)
					require.EqualError(t, err, plainErr.Error())
					reads := collector.accessObservation()
					require.Len(t, reads, 1)
					expected := "FulltextIndexByNameSeek"
					if kind == "vector" {
						expected = "VectorIndexByNameSeek"
					}
					require.Equal(t, expected, reads[0].Kind)
					require.Zero(t, reads[0].Rows)
					require.EqualValues(t, 1, reads[0].Hits)
					require.False(t, reads[0].Unknown)
					require.ElementsMatch(t, before, exec.storage.GetSchema().GetIndexes())
				})
			}
			for _, relationship := range []bool{false, true} {
				method, target := "createNodeIndex", "Indexed"
				if relationship {
					method, target = "createRelationshipIndex", "IndexedRel"
				}
				for _, rejection := range []string{"argument", "constraint", "index"} {
					t.Run(kind+"/"+method+"/"+rejection, func(t *testing.T) {
						exec, _ := newProfileRuntimeTestExecutor(t)
						switch rejection {
						case "constraint":
							_, err := exec.Execute(t.Context(), "CREATE CONSTRAINT occupied FOR (n:Existing) REQUIRE n.id IS UNIQUE", nil)
							require.NoError(t, err)
						case "index":
							_, err := exec.Execute(t.Context(), "CREATE INDEX occupied FOR (n:Existing) ON (n.id)", nil)
							require.NoError(t, err)
						}
						schema := exec.storage.GetSchema()
						beforeIndexes, beforeConstraints := schema.GetIndexes(), schema.GetAllConstraints()
						args := "'occupied',['" + target + "'],['text']"
						if kind == "vector" {
							args = "'occupied','" + target + "','embedding',3,'cosine'"
						}
						if rejection == "argument" {
							args = "42" + args[len("'occupied'"):]
						}
						statement := "CALL db.index." + kind + "." + method + "(" + args + ")"
						_, plainErr := exec.Execute(t.Context(), statement, nil)
						require.Error(t, plainErr)
						collector := newProfileRuntimeCollector()
						_, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, nil)
						require.EqualError(t, err, plainErr.Error())
						reads := collector.accessObservation()
						if rejection == "argument" {
							require.Empty(t, reads)
						} else {
							expected := 1
							if rejection == "index" {
								expected = 2
							}
							require.Len(t, reads, expected)
							constraints := profileAccessesOfKind(collector, profileKindSchemaConstraintCatalog)
							require.Len(t, constraints, 1)
							require.EqualValues(t, len(beforeConstraints), constraints[0].Rows)
							require.EqualValues(t, 1, constraints[0].Hits)
							if rejection == "index" {
								indexes := profileAccessesOfKind(collector, profileKindSchemaIndexCatalog)
								require.Len(t, indexes, 1)
								require.EqualValues(t, len(beforeIndexes), indexes[0].Rows)
								require.EqualValues(t, 1, indexes[0].Hits)
							}
						}
						require.ElementsMatch(t, beforeIndexes, schema.GetIndexes())
						require.Equal(t, beforeConstraints, schema.GetAllConstraints())
					})
				}
			}
		}
		for _, relationship := range []bool{false, true} {
			procedure, kind := "db.create.setNodeVectorProperty", queryprofile.KindNodeByIDSeek
			if relationship {
				procedure, kind = "db.create.setRelationshipVectorProperty", queryprofile.KindRelationshipByIDSeek
			}
			for _, invalid := range []bool{false, true} {
				name := procedure + "/missing"
				if invalid {
					name = procedure + "/invalid"
				}
				t.Run(name, func(t *testing.T) {
					exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:Retained {v:1})-[:RETAINED {v:2}]->(:Retained {v:3})")
					beforeNodes, err := exec.storage.AllNodes(t.Context())
					require.NoError(t, err)
					beforeEdges, err := exec.storage.AllEdges(t.Context())
					require.NoError(t, err)
					callbacks := 0
					exec.SetNodeMutatedCallback(func(string) { callbacks++ })
					entity := "'absent'"
					if invalid {
						entity = "42"
					}
					statement := "CALL " + procedure + "(" + entity + ",'embedding',[1.0,0.0,0.0])"
					_, plainErr := exec.Execute(t.Context(), statement, nil)
					require.Error(t, plainErr)
					collector := newProfileRuntimeCollector()
					_, err = exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, nil)
					require.EqualError(t, err, plainErr.Error())
					reads := collector.accessObservation()
					if invalid {
						require.Empty(t, reads)
					} else {
						require.Len(t, reads, 1)
						require.Equal(t, kind, reads[0].Kind)
						require.EqualValues(t, 1, reads[0].Hits)
						require.Zero(t, reads[0].Rows)
						require.False(t, reads[0].Unknown)
					}
					require.Zero(t, callbacks)
					afterNodes, err := exec.storage.AllNodes(t.Context())
					require.NoError(t, err)
					afterEdges, err := exec.storage.AllEdges(t.Context())
					require.NoError(t, err)
					require.Equal(t, beforeNodes, afterNodes)
					require.Equal(t, beforeEdges, afterEdges)
				})
			}
		}
	})
}
