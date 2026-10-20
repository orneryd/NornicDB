package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Keep OPTIONAL at the public USE boundary, including bodies whose output schema
// must survive an empty nested CALL or UNION. Reference queries omit only USE:
// each body is data independent or matches a deliberately absent label.
func TestFabricOptionalCallRows(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	targets := []string{"one"}
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		mgr := base.dbManager.(*testDatabaseManagerAdapter).manager
		require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{
			Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri,
		}))
		targets = append(targets, "remote")
	}
	for _, target := range targets {
		use := "USE comp." + target + " "
		for _, normalization := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/normalization_%v", target, normalization), func(t *testing.T) {
				restore := func() {}
				if !normalization {
					restore = config.WithCypherQueryNormalizationDisabled()
				}
				defer restore()
				for _, tt := range []struct{ name, query, want string }{
					{"missing_match", "WITH 7 AS x OPTIONAL CALL { " + use + "MATCH (n:FabricOptionalMissing) RETURN n } RETURN x,n", "[[7,null]]"},
					{"empty_unwind", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"multiple_exports", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v AS a, v AS b } RETURN x,a,b", "[[7,null,null]]"},
					{"multiple_outer", "UNWIND [1,2] AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v } RETURN x,v ORDER BY x", "[[1,null],[2,null]]"},
					{"matched_rows", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [1,2] AS v RETURN v } RETURN x,v ORDER BY v", "[[7,1],[7,2]]"},
					{"correlated_mixed", "UNWIND [1,2] AS x OPTIONAL CALL { " + use + "WITH x UNWIND [1,2] AS v WITH x,v WHERE x = 1 RETURN v } RETURN x,v ORDER BY x,v", "[[1,1],[1,2],[2,null]]"},
					{"correlated_lookup", "UNWIND [1,2] AS x OPTIONAL CALL { WITH x " + use + "MATCH (n:FabricOptionalMissing) WHERE n.key = x RETURN n.key AS v } RETURN x,v ORDER BY x", "[[1,null],[2,null]]"},
					{"null_lookup", "UNWIND [null,1] AS x OPTIONAL CALL { WITH x " + use + "MATCH (n:FabricOptionalMissing) WHERE n.key = x RETURN n.key AS v } RETURN x,v ORDER BY x", "[[1,null],[null,null]]"},
					{"scoped_import", "WITH 7 AS x OPTIONAL CALL (x) { " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"scoped_empty", "WITH 7 AS x OPTIONAL CALL () { " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"nested_optional", "WITH 7 AS x CALL { OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v } RETURN v } RETURN x,v", "[[7,null]]"},
					{"nested_empty", "WITH 7 AS x OPTIONAL CALL { CALL { " + use + "UNWIND [] AS a RETURN a } CALL { " + use + "RETURN 2 AS b } RETURN a,b } RETURN x,a,b", "[[7,null,null]]"},
					{"nested_star", "WITH 7 AS x OPTIONAL CALL { CALL { " + use + "UNWIND [] AS a RETURN a } CALL { " + use + "RETURN 2 AS b } RETURN * } RETURN x,a,b", "[[7,null,null]]"},
					{"nested_star_new_scope", "WITH 7 AS x OPTIONAL CALL { CALL { " + use + "UNWIND [] AS a RETURN a } CALL { " + use + "MATCH (p:FabricOptionalMissing) RETURN * } RETURN * } RETURN x,a,p", "[[7,null,null]]"},
					{"yield_empty", "WITH 7 AS x OPTIONAL CALL { " + use + "CALL db.labels() YIELD label WITH label WHERE label = 'FabricOptionalMissing' RETURN label AS v } RETURN x,v", "[[7,null]]"},
					{"optional_yield", "CALL { " + use + "WITH 7 AS x OPTIONAL CALL db.labels() YIELD label WHERE label = 'FabricOptionalMissing' RETURN x,label } RETURN x,label", "[[7,null]]"},
					{"union_empty", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v UNION ALL " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"union_partial", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v UNION ALL " + use + "RETURN 2 AS v } RETURN x,v", "[[7,2]]"},
					{"union_distinct", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v UNION " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"union_cross_constituent", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v UNION ALL USE comp.two UNWIND [] AS v RETURN v } RETURN x,v", "[[7,null]]"},
					{"aggregate_zero", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN count(*) AS c } RETURN x,c", "[[7,0]]"},
					{"empty_outer", "UNWIND [] AS x OPTIONAL CALL { " + use + "RETURN 1 AS v } RETURN x,v", "[]"},
					{"ordinary_control", "WITH 7 AS x CALL { " + use + "UNWIND [] AS v RETURN v } RETURN x,v", "[]"},
					{"optional_then_call", "WITH 7 AS x OPTIONAL CALL { " + use + "UNWIND [] AS v RETURN v } CALL { " + use + "WITH v RETURN coalesce(v,9) AS w } RETURN x,v,w", "[[7,null,9]]"},
				} {
					t.Run(tt.name, func(t *testing.T) {
						for _, explicit := range []bool{false, true} {
							t.Run(fmt.Sprintf("explicit_%v", explicit), func(t *testing.T) {
								if driver != nil {
									query := strings.NewReplacer(use, "", "USE comp.two ", "").Replace(tt.query)
									if explicit {
										session := driver.NewSession(ctx, neo4j.SessionConfig{})
										defer session.Close(ctx)
										tx, err := session.BeginTransaction(ctx)
										require.NoError(t, err)
										defer tx.Rollback(ctx)
										fabricCallRowsAssertReference(t, tx, query, tt.want)
									} else {
										ref, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
										require.NoError(t, err, query)
										rows := make([][]interface{}, 0, len(ref.Records))
										for _, record := range ref.Records {
											rows = append(rows, record.Values)
										}
										fabricDispatchRows(t, rows, tt.want)
									}
								}
								exec := base
								if explicit {
									exec = fabricCallRowsCompositeExecutor(t, base)
									_, err := exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
								}
								result, err := exec.Execute(ctx, "USE comp "+tt.query, nil)
								require.NoError(t, err)
								fabricDispatchRows(t, result.Rows, tt.want)
							})
						}
					})
				}
			})
		}
	}
}

// Exercise both lookup rewrites at their key/chunk boundaries and assert the
// public route keeps its batching, including null keys and LIMIT eliminating
// every match. An optional join must not disable the set-based fast path.
func TestFabricOptionalCallBatchedRows(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	base.cache = nil
	mgr := base.dbManager.(*testDatabaseManagerAdapter).manager
	store, err := mgr.GetStorage("one")
	require.NoError(t, err)
	seed := "CREATE (:FabricOptionalLookup {key:1}), (:FabricOptionalLookup {key:513}), (:FabricOptionalLookup {key:1025})"
	_, err = NewStorageExecutor(store).Execute(ctx, seed, nil)
	require.NoError(t, err)
	targets := []string{"one"}
	driver := fabricCallRowsReference(t)
	if driver != nil {
		_, err = neo4j.ExecuteQuery(ctx, driver, "MATCH (n:FabricOptionalLookup) DETACH DELETE n", nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		_, err = neo4j.ExecuteQuery(ctx, driver, seed, nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := neo4j.ExecuteQuery(ctx, driver, "MATCH (n:FabricOptionalLookup) DETACH DELETE n", nil, neo4j.EagerResultTransformer)
			require.NoError(t, err)
		})
		require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")}))
		targets = append(targets, "remote")
	}
	for _, target := range targets {
		for _, tt := range []struct{ name, keys, projection, want string }{
			{"streamed", "range(1,1025)", "RETURN n.key AS v", "[[1025,3]]"},
			{"materialized", "range(1,1025)", "RETURN n.key AS v ORDER BY v", "[[1025,3]]"},
			{"limited", "[1,2]", "RETURN n.key AS v ORDER BY v LIMIT 0", "[[2,0]]"},
			{"null_streamed", "[null,null]", "RETURN n.key AS v", "[[2,0]]"},
			{"null_materialized", "[null,null]", "RETURN n.key AS v ORDER BY v", "[[2,0]]"},
		} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/explicit_%v", target, tt.name, explicit), func(t *testing.T) {
					body := "UNWIND " + tt.keys + " AS x OPTIONAL CALL { WITH x USE comp." + target + " MATCH (n:FabricOptionalLookup) WHERE n.key = x " + tt.projection + " } RETURN count(*) AS rows,count(v) AS matches"
					if driver != nil {
						query := strings.ReplaceAll(body, "USE comp."+target+" ", "")
						session := driver.NewSession(ctx, neo4j.SessionConfig{})
						defer session.Close(ctx)
						tx, err := session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer tx.Rollback(ctx)
						fabricCallRowsAssertReference(t, tx, query, tt.want)
					}
					exec := base
					if explicit {
						exec = fabricCallRowsCompositeExecutor(t, base)
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
					}
					result, err := exec.Execute(ctx, "USE comp "+body, nil)
					require.NoError(t, err)
					fabricDispatchRows(t, result.Rows, tt.want)
					require.True(t, exec.LastHotPathTrace().FabricBatchedApplyRows, "optional lookup must retain batching")
				})
			}
		}
	}
}
