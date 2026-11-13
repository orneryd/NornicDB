package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestNoOverlapConstraintNodeAndRelationshipContracts(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, relationship := range []bool{false, true} {
				t.Run(fmt.Sprint("relationship=", relationship), func(t *testing.T) {
					exec, engine := newConstraintGapExecutor(t)
					ctx := t.Context()
					variable, pattern, match := "n", "(n:Interval)", "MATCH (n:Interval)"
					create := "CREATE (:Interval %s)"
					if relationship {
						variable, pattern, match = "r", "()-[r:INTERVAL]-()", "MATCH ()-[r:INTERVAL]->()"
						_, err := exec.Execute(ctx, "CREATE (:Endpoint {id:1}), (:Endpoint {id:2})", nil)
						require.NoError(t, err)
						create = "MATCH (a:Endpoint {id:1}), (b:Endpoint {id:2}) CREATE (a)-[:INTERVAL %s]->(b)"
					}
					ddl := fmt.Sprintf("CREATE NO OVERLAP CONSTRAINT intervals FOR %s REQUIRE temporal.interval(%s.starts, %s.ends) GROUP BY %s.tenant, %s.key", pattern, variable, variable, variable, variable)
					_, err := exec.Execute(ctx, ddl, nil)
					require.NoError(t, err)
					run := func(q string) error { _, err := exec.Execute(ctx, q, nil); return err }
					properties := func(tenant, key, slot, start, end string) string {
						return fmt.Sprintf("{tenant:'%s', key:'%s', slot:'%s', starts:datetime('%s'), ends:datetime('%s')}", tenant, key, slot, start, end)
					}
					first := properties("a", "k", "first", "2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z")
					overlap := properties("a", "k", "overlap", "2026-01-05T00:00:00Z", "2026-01-15T00:00:00Z")
					adjacent := properties("a", "k", "adjacent", "2026-01-10T00:00:00Z", "2026-01-20T00:00:00Z")
					other := properties("b", "k", "other", "2026-01-05T00:00:00Z", "2026-01-15T00:00:00Z")
					require.NoError(t, run(fmt.Sprintf(create, first)))
					require.Error(t, run(fmt.Sprintf(create, overlap)), "same composite group must reject overlap")
					require.NoError(t, run(fmt.Sprintf(create, adjacent)), "touching intervals must be accepted")
					require.NoError(t, run(fmt.Sprintf(create, other)), "different tenant is a separate group")
					require.Error(t, run(fmt.Sprintf("%s WHERE %s.slot='adjacent' SET %s.starts=datetime('2026-01-05T00:00:00Z')", match, variable, variable)))
					require.Error(t, run(fmt.Sprintf("%s WHERE %s.slot='other' SET %s.tenant='a'", match, variable, variable)))
					result, err := exec.Execute(ctx, fmt.Sprintf("%s RETURN %s.slot AS slot, %s.tenant AS tenant ORDER BY slot", match, variable, variable), nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"adjacent", "a"}, {"first", "a"}, {"other", "b"}}, result.Rows)
					rendered := constraintCreateStatements(t, exec)["intervals"]
					require.True(t, strings.HasPrefix(rendered, "CREATE NO OVERLAP CONSTRAINT "), rendered)
					require.NotContains(t, rendered, "IS NO OVERLAP")
					recreated, recreatedEngine := newConstraintGapExecutor(t)
					_, err = recreated.Execute(ctx, rendered, nil)
					require.NoError(t, err)
					require.Equal(t, temporalConstraintNamed(t, engine, "intervals"), temporalConstraintNamed(t, recreatedEngine, "intervals"))
				})
			}
		})
	}
}

func TestNoOverlapConstraintArbitraryGroupingWidth(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, relationship := range []bool{false, true} {
				for _, width := range []int{1, 2, 4, 8, 16} {
					for _, reversed := range []bool{false, true} {
						t.Run(fmt.Sprintf("relationship=%t/width=%d/reversed=%t", relationship, width, reversed), func(t *testing.T) {
							exec, _ := newConstraintGapExecutor(t)
							ctx := t.Context()
							variable, pattern, create := "n", "(n:Interval)", "CREATE (:Interval $props)"
							if relationship {
								variable, pattern, create = "r", "()-[r:INTERVAL]-()", "MATCH (a:Endpoint {id:1}), (b:Endpoint {id:2}) CREATE (a)-[:INTERVAL $props]->(b)"
								_, err := exec.Execute(ctx, "CREATE (:Endpoint {id:1}), (:Endpoint {id:2})", nil)
								require.NoError(t, err)
							}
							keys := make([]string, width)
							for i := range keys {
								index := i
								if reversed {
									index = width - i - 1
								}
								keys[i] = fmt.Sprintf("%s.k%d", variable, index)
							}
							ddl := fmt.Sprintf("CREATE NO OVERLAP CONSTRAINT intervals FOR %s REQUIRE temporal.interval(%s.starts,%s.ends) GROUP BY %s", pattern, variable, variable, strings.Join(keys, ","))
							_, err := exec.Execute(ctx, ddl, nil)
							require.NoError(t, err)
							properties := func(changed int) map[string]interface{} {
								p := map[string]interface{}{"starts": "2026-01-01T00:00:00Z", "ends": "2026-01-10T00:00:00Z"}
								for i := 0; i < width; i++ {
									value := "same"
									if i == changed {
										value = "different"
									}
									p[fmt.Sprintf("k%d", i)] = value
								}
								return p
							}
							_, err = exec.Execute(ctx, create, map[string]interface{}{"props": properties(-1)})
							require.NoError(t, err)
							_, err = exec.Execute(ctx, create, map[string]interface{}{"props": properties(-1)})
							require.Error(t, err, "all grouping values equal must conflict")
							for component := 0; component < width; component++ {
								_, err = exec.Execute(ctx, create, map[string]interface{}{"props": properties(component)})
								require.NoError(t, err, "component %d must distinguish the group", component)
							}
						})
					}
				}
			}
		})
	}
}

func TestNoOverlapConstraintConcurrentWritesAdmitOneInterval(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, relationship := range []bool{false, true} {
				t.Run(fmt.Sprint("relationship=", relationship), func(t *testing.T) {
					owner, engine := newConstraintGapExecutor(t)
					variable, pattern, create := "n", "(n:Interval)", "CREATE (:Interval {tenant:'t', account:'a', region:'r', category:'c', starts:datetime('2026-01-01T00:00:00Z'), ends:datetime('2026-02-01T00:00:00Z')})"
					count := "MATCH (n:Interval) RETURN count(n) AS count"
					if relationship {
						variable, pattern = "r", "()-[r:INTERVAL]-()"
						_, err := owner.Execute(t.Context(), "CREATE (:Endpoint {id:1}), (:Endpoint {id:2})", nil)
						require.NoError(t, err)
						create = "MATCH (a:Endpoint {id:1}), (b:Endpoint {id:2}) CREATE (a)-[:INTERVAL {tenant:'t', account:'a', region:'r', category:'c', starts:datetime('2026-01-01T00:00:00Z'), ends:datetime('2026-02-01T00:00:00Z')}]->(b)"
						count = "MATCH ()-[r:INTERVAL]->() RETURN count(r) AS count"
					}
					_, err := owner.Execute(t.Context(), fmt.Sprintf("CREATE NO OVERLAP CONSTRAINT intervals FOR %s REQUIRE temporal.interval(%s.starts,%s.ends) GROUP BY %s.tenant,%s.account,%s.region,%s.category", pattern, variable, variable, variable, variable, variable, variable), nil)
					require.NoError(t, err)
					executors := []*StorageExecutor{NewStorageExecutor(engine), NewStorageExecutor(engine)}
					start := make(chan struct{})
					outcomes := make(chan error, 2)
					for _, executor := range executors {
						go func(e *StorageExecutor) { <-start; _, err := e.Execute(t.Context(), create, nil); outcomes <- err }(executor)
					}
					close(start)
					accepted := 0
					for range executors {
						if err := <-outcomes; err == nil {
							accepted++
						}
					}
					require.Equal(t, 1, accepted, "only one overlapping interval may be acknowledged")
					result, err := owner.Execute(t.Context(), count, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				})
			}
		})
	}
}
