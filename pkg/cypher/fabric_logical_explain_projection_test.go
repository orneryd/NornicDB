package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// The first UNION branch has no rows and its unit CALL exports no hidden
// bindings. Planning and execution must nevertheless agree on RETURN *.
func TestFabricLogicalExplainProjection(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for _, optional := range []bool{false, true} {
					for _, unit := range []string{
						"CALL { RETURN 'inner' AS hidden }",
						"WITH 'local' AS local CALL { RETURN local AS hidden }",
					} {
						for _, operator := range []string{"UNION", "UNION ALL"} {
							for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
								t.Run(fmt.Sprintf("normalize_%v/composite_%v/explicit_%v/optional_%v/%s/%s/%s", normalize, composite, explicit, optional, unit, operator, strings.TrimSpace(mode)), func(t *testing.T) {
									exec := newFabricCallRowsExecutor(t)
									call := "CALL"
									if optional {
										call = "OPTIONAL CALL"
									}
									query := "UNWIND [] AS seed " + call + " { " + unit + " } RETURN * " + operator + " RETURN 'outer' AS seed"
									if composite {
										exec = fabricCallRowsCompositeExecutor(t, exec)
										query = "USE comp " + query
									}
									query = mode + query
									if explicit {
										_, err := exec.Execute(ctx, "BEGIN", nil)
										require.NoError(t, err)
										defer exec.Execute(ctx, "ROLLBACK", nil)
										if composite {
											_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
											require.True(t, ok)
										}
									}
									for repeat := 0; repeat < 2; repeat++ {
										result, err := exec.Execute(ctx, query, nil)
										require.NoError(t, err, query)
										require.Equal(t, []string{"seed"}, result.Columns, query)
										if mode == "EXPLAIN " {
											require.Empty(t, result.Rows, query)
										} else {
											require.Equal(t, [][]interface{}{{"outer"}}, result.Rows, query)
										}
									}
								})
							}
						}
					}
				}
			}
		}
	}
}
