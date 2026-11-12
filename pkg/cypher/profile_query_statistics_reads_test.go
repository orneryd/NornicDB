package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestProfileQueryStatisticsReportsActualAcquisitions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, populated := range []bool{false, true} {
			t.Run(map[bool]string{false: "empty", true: "populated"}[populated], func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t)
				execute := func(query string) {
					t.Helper()
					_, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
				}
				if populated {
					execute("CREATE (:StatsA {id:1})-[:STATS]->(:StatsB {id:2})")
					execute("CREATE INDEX stats_id FOR (n:StatsA) ON (n.id)")
					execute("CREATE CONSTRAINT stats_required FOR (n:StatsA) REQUIRE n.id IS NOT NULL")
					execute("CREATE CONSTRAINT stats_type FOR (n:StatsA) REQUIRE n.id IS :: INTEGER")
				}
				execute("CALL db.stats.stop('QUERIES')")
				execute("CALL db.stats.clear('QUERIES')")
				if populated {
					execute("CALL db.stats.collect('QUERIES')")
					execute("RETURN 101 AS recorded")
					execute("RETURN 102 AS recorded")
					execute("CALL db.stats.stop('QUERIES')")
				}
				schema := exec.storage.GetSchema()
				catalogs := map[string]int{
					profileKindSchemaConstraintCatalog:             len(schema.GetAllConstraints()),
					profileKindSchemaPropertyTypeConstraintCatalog: len(schema.GetAllPropertyTypeConstraints()),
					profileKindSchemaIndexCatalog:                  len(schema.GetIndexes()),
				}
				// The two implicit lookup indexes are real catalog entries even
				// when no node or relationship exists.
				wantIndexes := 2
				if populated {
					wantIndexes++
				}
				require.Equal(t, wantIndexes, catalogs[profileKindSchemaIndexCatalog])
				queries := len(exec.queryStatistics.order)
				if populated {
					require.Equal(t, 2, queries)
				} else {
					require.Zero(t, queries)
				}
				for _, section := range []string{"QUERIES", "TOKENS", "META", "GRAPH COUNTS", "ALL", "ALL_ALIAS"} {
					t.Run(section, func(t *testing.T) {
						query := "CALL db.stats.retrieve('" + section + "', {maxInvocations:0}) YIELD section, data RETURN section"
						if section == "ALL_ALIAS" {
							query = "CALL db.stats.retrieveAllAnTheStats() YIELD section, data RETURN section"
							section = "ALL"
						}
						_, plan := profileEquivalentObservedStatement(t, exec, query)
						require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
						require.Empty(t, plan.Unobserved)
						var expected int64
						if section == "QUERIES" || section == "ALL" {
							state := planOperatorOfType(plan.Root, "QueryStatisticsState")
							require.NotNil(t, state)
							require.EqualValues(t, 1, state.ActualRows)
							require.EqualValues(t, 1, state.DBHits)
							catalog := planOperatorOfType(plan.Root, "QueryStatisticsCatalog")
							require.NotNil(t, catalog)
							require.EqualValues(t, queries, catalog.ActualRows)
							require.EqualValues(t, 1, catalog.DBHits)
							expected += 2
						}
						if section != "QUERIES" {
							nodes := planOperatorOfType(plan.Root, profileKindAllNodesScan)
							edges := planOperatorOfType(plan.Root, profileKindAllRelationshipsScan)
							require.NotNil(t, nodes)
							require.NotNil(t, edges)
							var n, r int64
							if populated {
								n, r = 2, 1
							}
							require.Equal(t, n, nodes.ActualRows)
							require.Equal(t, n, nodes.DBHits)
							require.Equal(t, r, edges.ActualRows)
							require.Equal(t, r, edges.DBHits)
							expected += n + r + 2
							tokens := profilePlanOperators(plan.Root, "SchemaTokenOrder")
							require.Len(t, tokens, 2)
							for _, token := range tokens {
								require.EqualValues(t, 1, token.DBHits)
							}
							if section == "GRAPH COUNTS" || section == "ALL" {
								expected += 3
								for _, kind := range []string{profileKindSchemaConstraintCatalog, profileKindSchemaPropertyTypeConstraintCatalog, profileKindSchemaIndexCatalog} {
									catalog := planOperatorOfType(plan.Root, kind)
									require.NotNil(t, catalog)
									require.EqualValues(t, 1, catalog.DBHits)
									require.EqualValues(t, catalogs[kind], catalog.ActualRows)
								}
							}
						}
						require.Equal(t, expected, plan.TotalDBHits)
					})
				}
			})
		}
	})
}

func TestProfileQueryStatisticsPreservesSystemAliasAcquisitions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "system"))
		rows, plan := profileEquivalentObservedStatement(t, exec, "CALL db.stats.retrieve('ALL') YIELD section, data RETURN section")
		require.Empty(t, rows.Rows, "the system database owns no query statistics collector")
		require.Zero(t, plan.TotalDBHits)
		rows, plan = profileEquivalentObservedStatement(t, exec, "CALL db.stats.retrieveAllAnTheStats() YIELD section, data RETURN section")
		require.Equal(t, [][]interface{}{{"GRAPH COUNTS"}, {"TOKENS"}, {"META"}}, rows.Rows)
		require.EqualValues(t, 5, plan.TotalDBHits, "two token order and three schema acquisitions; no graph entities")
		require.Nil(t, planOperatorOfType(plan.Root, "QueryStatisticsState"))
		require.Nil(t, planOperatorOfType(plan.Root, "QueryStatisticsCatalog"))
	})
}

func TestProfileQueryStatisticsLifecyclePreservesResultsAndState(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, action := range []string{"collect", "stop", "status", "clear"} {
			t.Run(action, func(t *testing.T) {
				var ordinary *ExecuteResult
				for _, profiled := range []bool{false, true} {
					exec, _ := newProfileRuntimeTestExecutor(t)
					if action == "collect" || action == "clear" {
						_, err := exec.Execute(t.Context(), "CALL db.stats.stop('QUERIES')", nil)
						require.NoError(t, err)
					}
					query := "CALL db.stats." + action + "('QUERIES')"
					if action == "status" {
						query = "CALL db.stats.status()"
					}
					if profiled {
						query = "PROFILE " + query
					}
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					if !profiled {
						ordinary = result
					} else {
						require.Equal(t, ordinary.Columns, result.Columns)
						require.Equal(t, ordinary.Rows, result.Rows)
						plan := executionPlanFromResult(t, result)
						require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
						require.Empty(t, plan.Unobserved)
						state := planOperatorOfType(plan.Root, "QueryStatisticsState")
						require.NotNil(t, state)
						require.EqualValues(t, 1, state.ActualRows)
						require.EqualValues(t, 1, state.DBHits)
						require.Nil(t, planOperatorOfType(plan.Root, "QueryStatisticsCatalog"))
						require.EqualValues(t, 1, plan.TotalDBHits)
					}
					require.Equal(t, action == "collect" || action == "status", exec.queryStatistics.active.Load())
					require.Empty(t, exec.queryStatistics.order, "statistics administration is not itself collected")
				}
			})
		}
	})
}
