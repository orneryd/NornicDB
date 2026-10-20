package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// This guards the lowering boundary independently of row execution. New CALL
// syntax fields are retained wholesale, and no physical exec may lose its
// native compiler frame, even when no row will ever reach that fragment.
func TestFabricLogicalScopeSurvivesLowering(t *testing.T) {
	for _, query := range []string{
		"WITH 7 AS x OPTIONAL CALL { USE comp.one UNWIND [] AS v RETURN v } RETURN x,v",
		"WITH 7 AS x CALL (x) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v",
		"WITH 7 AS x CALL (*) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v",
		"WITH 7 AS x CALL () { USE comp.one RETURN 1 AS v } RETURN v",
		"UNWIND [1,2] AS x CALL (x) { USE comp.one UNWIND [1,2] AS y CREATE (:FabricFrameUnit {x:x,y:y}) } RETURN x",
		"WITH 7 AS embedding CALL (embedding) { USE comp.one WITH 1 AS y CREATE (:FabricFrameNode) WITH embedding RETURN 1 AS k } RETURN k",
		"WITH 7 AS x OPTIONAL CALL { CALL { USE comp.one UNWIND [] AS a RETURN a } CALL { USE comp.two MATCH (p:FabricFrameMissing) RETURN * } RETURN * } RETURN x,a,p",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			prepared, err := exec.prepareFabricExecution(context.Background(), "USE comp "+query)
			require.NoError(t, err)
			var visit func(fabric.Fragment)
			visit = func(fragment fabric.Fragment) {
				switch f := fragment.(type) {
				case *fabric.FragmentExec:
					require.NotNil(t, f.Frame)
					require.NotNil(t, f.Frame.InputTypes)
					require.NotNil(t, f.Frame.OutputTypes)
				case *fabric.FragmentApply:
					if !f.Projection {
						require.NotNil(t, f.Call)
						parsed := fabric.ReadQueryStructure(f.Call.Text)
						require.NoError(t, parsed.Err)
						native := parsed.First("CALL")
						require.NotNil(t, native)
						require.Equal(t, native.Optional, f.Call.Optional)
						require.Equal(t, native.Scoped, f.Call.Scoped)
						require.Equal(t, native.ScopeNames, f.Call.ScopeNames)
						require.Equal(t, native.InTransactions, f.Call.InTransactions)
						require.NotNil(t, f.Call.Nested)
					}
					visit(f.Input)
					visit(f.Inner)
				case *fabric.FragmentUnion:
					visit(f.LHS)
					visit(f.RHS)
				}
			}
			visit(prepared.fragment)
		})
	}
}

// A binding imported by name does not turn a literal expression into a variable.
// Exercise projection, aggregation and iteration through the native frame and
// the same frame lowered onto a constituent.
func TestFabricLogicalLiteralBindings(t *testing.T) {
	driver := fabricCallRowsReference(t)
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, literal := range []struct{ name, value, unwind, count string }{
					{"false", "[[false]]", "[[false]]", "[[1]]"},
					{"true", "[[true]]", "[[true]]", "[[1]]"},
					{"null", "[[null]]", "[]", "[[0]]"},
				} {
					t.Run(fmt.Sprintf("normalization_%v/composite_%v/explicit_%v/%s", normalize, composite, explicit, literal.name), func(t *testing.T) {
						config.SetCypherQueryNormalizationEnabled(normalize)
						exec := newFabricCallRowsExecutor(t)
						prefix, selection := "", ""
						if composite {
							exec = fabricCallRowsCompositeExecutor(t, exec)
							prefix, selection = "USE comp ", "USE comp.one "
						}
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
							if composite {
								_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
								require.True(t, ok)
							}
						}
						for _, sample := range []struct{ body, want string }{
							{"RETURN " + literal.name + " AS v", literal.value},
							{"WITH " + literal.name + " AS v RETURN v", literal.value},
							{"UNWIND " + literal.name + " AS v RETURN v", literal.unwind},
							{"RETURN count(" + literal.name + ") AS v", literal.count},
							{"RETURN " + projectionVariableText(literal.name) + " AS v", "[[7]]"},
						} {
							query := "WITH 7 AS " + literal.name + " CALL (" + literal.name + ") { " + selection + sample.body + " } RETURN v"
							if driver != nil {
								session := driver.NewSession(ctx, neo4j.SessionConfig{})
								tx, err := session.BeginTransaction(ctx)
								require.NoError(t, err)
								fabricCallRowsAssertReference(t, tx, strings.ReplaceAll(query, selection, ""), sample.want)
								require.NoError(t, tx.Rollback(ctx))
								require.NoError(t, session.Close(ctx))
							}
							result, err := exec.Execute(ctx, prefix+query, nil)
							require.NoError(t, err, query)
							fabricDispatchRows(t, result.Rows, sample.want)
						}
					})
				}
			}
		}
	}
}

func TestFabricLogicalProcedureSchemas(t *testing.T) {
	for _, query := range []string{
		"USE comp.one CALL db.labels()",
		"USE comp.one CALL db.labels() YIELD *",
	} {
		t.Run(query, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"label"}, result.Columns)
			require.Equal(t, [][]interface{}{{"P"}}, result.Rows)
		})
	}
}

func TestFabricLogicalYieldIdentityScope(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalization_%v/composite_%v/explicit_%v", normalize, composite, explicit), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					base := newFabricCallRowsExecutor(t)
					manager := base.dbManager.(*testDatabaseManagerAdapter).manager
					store, err := manager.GetStorage("one")
					require.NoError(t, err)
					exec := NewStorageExecutor(store)
					prefix, selection := "", ""
					if composite {
						exec = fabricCallRowsCompositeExecutor(t, base)
						prefix, selection = "USE comp ", "USE comp.one "
					}
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
						if composite {
							_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
							require.True(t, ok)
						}
					}
					query := prefix + "WITH 7 AS x CALL (x) { " + selection + "CALL db.labels() YIELD label WHERE COUNT { WITH ((label)) AS label RETURN label } = 1 WITH label, count(*) AS c RETURN label, c, x AS copied } RETURN x, label, c, copied"
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{int64(7), "P", int64(1), int64(7)}}, result.Rows)
				})
			}
		}
	}
}
