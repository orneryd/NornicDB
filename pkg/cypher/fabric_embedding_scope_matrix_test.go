package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// Exercise the execution frame through the public entry, rather than teaching
// the embedding extension how each scope construct exports its variables.
type embeddingFrameForm struct{ name, prefix, suffix, value string }

func TestFabricEmbeddingExecutionFrameMatrix(t *testing.T) {
	forms := []embeddingFrameForm{
		{"with", "WITH 7 AS @ ", "", "@"},
		{"with_star", "WITH 7 AS @ WITH * ", "", "@"},
		{"unwind", "UNWIND [7] AS @ ", "", "@"},
		{"call_return", "CALL { WITH 7 AS @ RETURN @ } ", "", "@"},
		{"call_return_star", "CALL { WITH 7 AS @ RETURN * } ", "", "@"},
		{"call_empty_import", "CALL () { WITH 7 AS @ RETURN * } ", "", "@"},
		{"call_legacy_import", "WITH 7 AS @ CALL { WITH @ ", " } RETURN k", "@"},
		{"call_named_import", "WITH 7 AS @ CALL (@) { ", " } RETURN k", "@"},
		{"call_star_import", "WITH 7 AS @ CALL (*) { ", " } RETURN k", "@"},
		{"call_union", "CALL { WITH 7 AS @ RETURN @ UNION WITH 7 AS @ RETURN @ } ", "", "@"},
		{"call_union_all", "CALL { WITH 7 AS @ RETURN * UNION ALL WITH 7 AS @ RETURN * } ", "", "@"},
		{"scoped_union_import", "WITH 7 AS @ CALL (@) { ", " UNION ALL WITH 0 AS ignored WITH @ RETURN @ AS k } RETURN k", "@"},
		{"foreach_shadow", "WITH 7 AS @ FOREACH (@ IN [9] | CREATE (:FrameShadow)) ", "", "@"},
		{"exists_scope", "WITH 7 AS @ WITH @, EXISTS { WITH @ RETURN @ } AS found ", "", "@"},
		{"count_scope", "WITH 7 AS @ WITH @, COUNT { WITH @ RETURN @ } AS total ", "", "@"},
		{"pattern_scope", "CREATE (a:FramePattern)-[:FRAME]->(b:FramePattern) WITH 7 AS @, a WITH @, [(a)-->(z) | z] AS matches ", "", "@"},
		{"pattern_binding", "CREATE (@:FrameBinding {k:7}) ", "", "@.k"},
		{"match_binding", "CREATE (seed:FrameSource {id:$id,k:7}) WITH seed.id AS sid MATCH (@:FrameSource {id:sid}) ", "", "@.k"},
		{"optional_match_binding", "CREATE (seed:FrameSource {id:$id,k:7}) WITH seed.id AS sid OPTIONAL MATCH (@:FrameSource {id:sid}) ", "", "@.k"},
		{"list_comprehension_shadow", "WITH 7 AS @ WITH @, [@ IN [9] | @] AS list ", "", "@"},
		{"null_binding", "WITH null AS @ ", "", "coalesce(@,7)"},
	}
	runFabricEmbeddingExecutionFrames(t, forms)
}

func runFabricEmbeddingExecutionFrames(t *testing.T, forms []embeddingFrameForm) {
	mutations := []string{
		"CREATE (w:FrameMutation {id:$id,k:7})",
		"MERGE (w:FrameMutation {id:$id,k:7})",
		"INSERT (w:FrameMutation {id:$id,k:7})",
		"CREATE (w:FrameMutation {id:$id}) SET w.k=7",
		"CREATE (w:FrameMutation {id:$id}) REMOVE w.unused",
		"CREATE (w:FrameMutation {id:$id}) DELETE w",
		"CREATE (w:FrameMutation {id:$id}) DETACH DELETE w",
		"CREATE (w:FrameMutation {id:$id}) NODETACH DELETE w",
		"FOREACH (i IN [1] | CREATE (:FrameMutation {id:$id,k:7}))",
	}
	for _, normalize := range []bool{true, false} {
		for _, native := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalize_%v/native_%v/explicit_%v", normalize, native, explicit), func(t *testing.T) {
					if !normalize {
						defer config.WithCypherQueryNormalizationDisabled()()
					}
					exec := newFabricCallRowsExecutor(t)
					if explicit && !native {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					ctx := context.Background()
					for _, f := range forms {
						for _, name := range []string{"embedding", "EMBEDDING", "EmBeDdInG", "`embedding`"} {
							for m, mutation := range mutations {
								// Preserve release execution support in both routing modes.
								if strings.HasPrefix(mutation, "INSERT") || strings.Contains(mutation, "NODETACH") {
									continue
								}
								t.Run(fmt.Sprintf("%s/%s/update_%d", f.name, name, m), func(t *testing.T) {
									prefix, suffix, value := strings.ReplaceAll(f.prefix, "@", name), strings.ReplaceAll(f.suffix, "@", name), strings.ReplaceAll(f.value, "@", name)
									query := prefix + mutation + " WITH " + name + " RETURN " + value + " AS k" + suffix
									if !native {
										query = "USE comp.one " + query
									}
									if f.name == "let" || f.name == "for" {
										query = "CYPHER 25 " + query
									}
									if explicit {
										_, err := exec.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer exec.Execute(ctx, "ROLLBACK", nil)
									}
									result, err := exec.Execute(ctx, query, map[string]interface{}{"id": t.Name(), "frameName": symbolicNameValue(name)})
									if f.name == "yield_star" {
										// YIELD * followed by other clauses is rejected on both routes. The
										// release's scope check may report the projected name before the
										// YIELD * rule, with or without USE and for any name, so either
										// rejection is the release behavior here.
										require.Error(t, err, query)
										if !strings.Contains(err.Error(), "YIELD * is only valid for a standalone procedure call") {
											require.Contains(t, err.Error(), "Variable `"+strings.Trim(name, "`")+"` not defined", query)
										}
										return
									}
									// USE … LET / FOR is admitted: the clauses after a USE graph name
									// come from the shared grammar (NOR-300), so Fabric routing runs
									// these Cypher 25 frames like every other frame. Release rejected
									// them only because its follower list omitted LET and FOR.
									require.NoError(t, err, query)
									require.NotEmpty(t, result.Rows, query)
									for _, row := range result.Rows {
										require.EqualValues(t, 7, row[0], query)
									}
									if explicit {
										_, err = exec.Execute(ctx, "COMMIT", nil)
										require.NoError(t, err)
									}
								})
							}
						}
					}
					// USE belongs to each UNION branch. Both branches deliberately select
					// the same constituent so the write-domain rule is not the subject here.
					for _, all := range []string{"UNION", "UNION ALL"} {
						branch := "WITH 7 AS embedding CREATE (:FrameUnion) WITH embedding RETURN embedding AS k"
						if !native {
							branch = "USE comp.one " + branch
						}
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := exec.Execute(ctx, branch+" "+all+" "+branch, nil)
						require.NoError(t, err)
						for _, row := range result.Rows {
							require.EqualValues(t, 7, row[0])
						}
						if explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
					}
				})
			}
		}
	}
}

// Additional engine bindings from the complete reading/pattern inventory.
// LET/FOR are Nornic extensions; the Cypher 5 reference intentionally excludes
// them. Procedure output schemas, including YIELD *, belong to the operator.
func TestFabricEmbeddingAdditionalExecutionFrames(t *testing.T) {
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "review.frame", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{args[0].(string)}, Rows: [][]interface{}{{int64(7)}}}, nil
	}))
	defer ClearUserProcedures()
	runFabricEmbeddingExecutionFrames(t, []embeddingFrameForm{
		{"let", "LET @ = 7 ", "", "@"},
		{"for", "FOR @ IN [7] ", "", "@"},
		{"yield_alias", "CALL review.frame('value') YIELD value AS @ ", "", "@"},
		{"yield_star", "CALL review.frame($frameName) YIELD * ", "", "@"},
		{"relationship_binding", "CREATE ()-[@:FrameRelation {k:7}]->() ", "", "@.k"},
		{"path_binding", "CREATE @=(:FramePath)-[:FRAME]->(:FramePath) ", "", "length(@)+6"},
	})
}
