package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFabricStructuralWriteAndUnionOracle(t *testing.T) {
	ctx := context.Background()
	base := newFabricCallRowsExecutor(t)
	driver := fabricCallRowsReference(t)
	rowsEqual := func(t *testing.T, rows [][]interface{}, want string) {
		t.Helper()
		encoded, err := json.Marshal(rows)
		require.NoError(t, err)
		require.JSONEq(t, want, string(encoded))
	}
	for _, normalization := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization_%v", normalization), func(t *testing.T) {
			restore := func() {}
			if !normalization {
				restore = config.WithCypherQueryNormalizationDisabled()
			}
			defer restore()
			for _, explicit := range []bool{false, true} {
				for _, shape := range []struct {
					name, body, want string
					invalid          bool
				}{
					{"reorder_all", "RETURN 1 AS x,2 AS y UNION ALL RETURN 3 AS y,4 AS x", "[[1,2],[4,3]]", false},
					{"reorder_distinct", "RETURN 1 AS x,2 AS y UNION RETURN 2 AS y,1 AS x", "[[1,2]]", false},
					{"three_branches", "RETURN 1 AS x,2 AS y UNION ALL RETURN 3 AS y,4 AS x UNION ALL RETURN 5 AS x,6 AS y", "[[1,2],[4,3],[5,6]]", false},
					{"quoted_names", "RETURN 1 AS `UNION`,2 AS `CALL` UNION ALL RETURN 3 AS `CALL`,4 AS `UNION`", "[[1,2],[4,3]]", false},
					{"empty_branch", "UNWIND [] AS n RETURN n AS x, n AS y UNION ALL RETURN 3 AS y,4 AS x", "[[4,3]]", false},
					{"star_schema", "WITH 1 AS x,2 AS y RETURN * UNION ALL WITH 3 AS y,4 AS x RETURN y,x", "[[1,2],[4,3]]", false},
					{"write_reorder", "FOREACH (i IN [1] | CREATE (:FabricUnionWrite)) RETURN 1 AS x,2 AS y UNION ALL FOREACH (i IN [1] | CREATE (:FabricUnionWrite)) RETURN 3 AS y,4 AS x", "[[1,2],[4,3]]", false},
					{"mismatch_names", "RETURN 1 AS a UNION ALL RETURN 2 AS b", "", true},
					{"mismatch_count", "RETURN 1 AS a UNION RETURN 2 AS a,3 AS b", "", true},
					{"empty_mismatch", "UNWIND [] AS n RETURN n AS a UNION ALL RETURN 2 AS b", "", true},
				} {
					t.Run(fmt.Sprintf("%s/explicit_%v", shape.name, explicit), func(t *testing.T) {
						if driver != nil {
							ref, err := neo4j.ExecuteQuery(ctx, driver, shape.body, nil, neo4j.EagerResultTransformer)
							if shape.invalid {
								require.Error(t, err)
							} else {
								require.NoError(t, err)
								rows := make([][]interface{}, 0, len(ref.Records))
								for _, record := range ref.Records {
									rows = append(rows, record.Values)
								}
								rowsEqual(t, rows, shape.want)
							}
						}
						exec := base
						if explicit {
							exec = fabricCallRowsCompositeExecutor(t, base)
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
						}
						// The outer USE is inherited by both branches; the nested variant also
						// exercises APPLY's UNION iterator rather than only top-level execution.
						body := shape.body
						if shape.name == "write_reorder" {
							body = strings.ReplaceAll(body, " UNION ALL ", " UNION ALL USE comp.one ")
						}
						projection := "RETURN *"
						if shape.name == "quoted_names" {
							projection = "RETURN `UNION`, `CALL`"
						}
						for _, query := range []string{"USE comp.one " + body, "USE comp CALL { USE comp.one " + body + " } " + projection} {
							result, err := exec.Execute(ctx, query, nil)
							if shape.invalid {
								require.ErrorContains(t, err, "same columns")
								continue
							}
							require.NoError(t, err)
							rowsEqual(t, result.Rows, shape.want)
						}
					})
				}
			}
			for _, mutation := range []string{"CREATE (:FabricWritePipe)", "MERGE (:FabricWritePipe {v:7})", "SET n.v=7", "REMOVE n.v", "DETACH DELETE n", "FOREACH (j IN [1] | CREATE (:FabricWritePipe))"} {
				t.Run(mutation, func(t *testing.T) {
					body := "WITH {v:1} AS n FOREACH (x IN [1] | " + mutation + ") RETURN 7 AS k"
					if strings.HasPrefix(mutation, "SET") || strings.HasPrefix(mutation, "REMOVE") || strings.Contains(mutation, "DELETE") {
						seed := "MERGE (:FabricWritePipeSeed {v:1})"
						_, err := base.Execute(ctx, "USE comp.one "+seed, nil)
						require.NoError(t, err)
						if driver != nil {
							_, err = neo4j.ExecuteQuery(ctx, driver, seed, nil, neo4j.EagerResultTransformer)
							require.NoError(t, err)
						}
						body = "MATCH (n:FabricWritePipeSeed {v:1}) FOREACH (x IN [1] | " + mutation + ") RETURN 7 AS k"
					}
					if driver != nil {
						_, err := neo4j.ExecuteQuery(ctx, driver, body, nil, neo4j.EagerResultTransformer)
						require.NoError(t, err)
					}
					exec := fabricCallRowsCompositeExecutor(t, base)
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
					_, err = exec.Execute(ctx, "USE comp.one "+body, nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "USE comp.two CREATE (:FabricSecondWrite) RETURN 1 AS k", nil)
					require.Error(t, err, "FOREACH must reserve the first write constituent")
				})
			}
		})
	}
}
