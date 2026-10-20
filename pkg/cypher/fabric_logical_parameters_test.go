package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalParameterBindings(t *testing.T) {
	ctx := context.Background()
	driver := fabricCallRowsReference(t)
	targets := []string{"native", "one"}
	if os.Getenv("NORNICDB_NEO4J_REFERENCE_URI") != "" {
		targets = append(targets, "remote")
	}
	for _, normalize := range []bool{false, true} {
		for _, target := range targets {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalization_%v/%s/explicit_%v", normalize, target, explicit), func(t *testing.T) {
					restore := func() {}
					if !normalize {
						restore = config.WithCypherQueryNormalizationDisabled()
					}
					defer restore()
					base := newFabricCallRowsExecutor(t)
					if target == "remote" {
						mgr := base.dbManager.(*testDatabaseManagerAdapter).manager
						require.NoError(t, mgr.AddConstituent("comp", multidb.ConstituentRef{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")}))
					}
					exec := base
					prefix, use := "", ""
					if target != "native" {
						prefix = "USE comp "
						use = "USE comp." + target + " "
						exec = fabricCallRowsCompositeExecutor(t, base)
					}
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
						if target != "native" {
							_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
							require.True(t, ok)
						}
					}
					params := map[string]interface{}{"x": int64(9), "__fabric_frame_records": "user"}
					for _, tt := range []struct{ query, want string }{
						{"WITH 7 AS x CALL (x) { " + use + "RETURN $x AS v } RETURN x,v", "[[7,9]]"},
						{"WITH 7 AS x CALL { " + use + "WITH x RETURN $x AS v } RETURN x,v", "[[7,9]]"},
						{"WITH 7 AS x CALL (x) { " + use + "WITH 1 AS y RETURN $x AS v, $__fabric_frame_records AS s } RETURN x,v,s", "[[7,9,\"user\"]]"},
						{"WITH 7 AS x CALL (x) { " + use + "WITH 1 AS y RETURN x+y+$x AS v } RETURN v", "[[17]]"},
						{"WITH 7 AS x CALL () { " + use + "RETURN $x AS v } RETURN x,v", "[[7,9]]"},
						{"WITH 7 AS x CALL (x) { " + use + "UNWIND [] AS y RETURN count(*) AS c } RETURN x,c", "[[7,0]]"},
					} {
						if driver != nil {
							ref, err := neo4j.ExecuteQuery(ctx, driver, strings.ReplaceAll(tt.query, use, ""), params, neo4j.EagerResultTransformer)
							require.NoError(t, err)
							rows := make([][]interface{}, 0, len(ref.Records))
							for _, record := range ref.Records {
								rows = append(rows, record.Values)
							}
							fabricDispatchRows(t, rows, tt.want)
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := exec.Execute(ctx, prefix+tt.query, params)
							require.NoError(t, err, tt.query)
							fabricDispatchRows(t, result.Rows, tt.want)
						}
					}
				})
			}
		}
	}
}

func TestFabricLogicalGraphParameterBindings(t *testing.T) {
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalization_%v/explicit_%v", normalize, explicit), func(t *testing.T) {
				restore := func() {}
				if !normalize {
					restore = config.WithCypherQueryNormalizationDisabled()
				}
				defer restore()
				exec := fabricCallRowsCompositeExecutor(t, newFabricCallRowsExecutor(t))
				if explicit {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, err := exec.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
					_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
					require.True(t, ok)
				}
				for _, tc := range []struct{ imports, argument, want string }{
					{"(g)", "$g", "[[\"comp.one\",\"two\"]]"},
					{"(g)", "g", "[[\"comp.one\",\"one\"]]"},
					{"", "$g", "[[\"comp.one\",\"two\"]]"},
					{"", "g", "[[\"comp.one\",\"one\"]]"},
				} {
					query := "WITH 'comp.one' AS g CALL " + tc.imports + " { USE graph.byName(" + tc.argument + ") MATCH (n:P) RETURN n.db AS db } RETURN g,db"
					result, err := exec.Execute(ctx, query, map[string]interface{}{"g": "comp.two"})
					require.NoError(t, err)
					fabricDispatchRows(t, result.Rows, tc.want)
				}
			})
		}
	}
}

func TestFabricLogicalUndispatchedGraphBinding(t *testing.T) {
	exec := fabricCallRowsCompositeExecutor(t, newFabricCallRowsExecutor(t))
	for _, query := range []string{
		"UNWIND [] AS x CALL { USE graph.byName(missing) RETURN 1 AS v } RETURN v",
		"WITH 'comp.one' AS g UNWIND [] AS x CALL () { USE graph.byName(g) RETURN 1 AS v } RETURN v",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
	}
}
