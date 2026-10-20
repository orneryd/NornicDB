package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalCommandContinuationScopes(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("normalize_%v/composite_%v/explicit_%v", normalize, composite, explicit), func(t *testing.T) {
					base := newFabricCallRowsExecutor(t)
					exec, selection := base, ""
					if composite {
						exec = fabricCallRowsCompositeExecutor(t, base)
						selection = "USE comp.one "
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
					for _, tc := range []struct {
						query string
						want  interface{}
					}{
						{"SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' RETURN name", "abs"},
						{"SHOW FUNCTIONS YIELD name AS fn WITH fn WHERE fn = 'abs' RETURN fn", "abs"},
						{"SHOW FUNCTIONS YIELD name UNWIND [1, 2] AS i WITH name, i WHERE name = 'abs' RETURN sum(i) AS s", int64(3)},
						{"SHOW FUNCTIONS YIELD name CALL (name) { RETURN name AS m } WITH m WHERE m = 'abs' RETURN m", "abs"},
						{"SHOW FUNCTIONS YIELD name OPTIONAL CALL (name) { WITH name WHERE false RETURN name AS m } WITH name, m WHERE name = 'abs' RETURN m", nil},
						{"SHOW FUNCTIONS YIELD name, signature WITH name, collect(signature) AS s FILTER size(s) > 100 RETURN count(name) AS c", int64(0)},
						{"SHOW PROCEDURES YIELD name WITH name LIMIT 1 RETURN [(a:FabricAbsentCommand)-->(b) | b] AS xs", []interface{}{}},
					} {
						result, err := exec.Execute(ctx, "CYPHER 25 "+selection+tc.query, nil)
						require.NoError(t, err, tc.query)
						require.Equal(t, [][]interface{}{{tc.want}}, result.Rows, tc.query)
					}
				})
			}
		}
	}
}
