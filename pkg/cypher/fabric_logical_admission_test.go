package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compilation must validate a CALL body even if the enclosing stream is empty,
// and keep known import types across a constituent boundary.
func TestFabricLogicalFragmentCompileContract(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, tc := range []struct{ name, query string }{
				{"empty_outer", "UNWIND [] AS x CALL { USE comp.one RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v } RETURN v"},
				{"typed_import", "WITH [1] AS x CALL (x) { USE comp.one UNWIND [] AS ignored RETURN toUpper(x) AS v } RETURN v"},
				{"empty_outer_aggregate", "UNWIND [] AS unused CALL { USE comp.one MATCH (n:ReviewMissing) WITH n.v * 1.0 AS x CALL (*) { WITH count(*) AS c RETURN [1,2][x] AS v } RETURN v } RETURN v"},
				{"direct_empty_outer", "UNWIND [] AS x CALL { RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v } RETURN v"},
				{"direct_typed_import", "WITH [1] AS x CALL (x) { UNWIND [] AS ignored RETURN toUpper(x) AS v } RETURN v"},
			} {
				t.Run(fmt.Sprintf("normalize_%v/explicit_%v/%s", normalize, explicit, tc.name), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					composite := strings.Contains(tc.query, "USE comp.")
					if composite {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						if composite {
							_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
							require.True(t, ok)
						}
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := exec.Execute(ctx, tc.query, nil)
					assert.Error(t, err, "malformed query compiled: %s; result=%+v", tc.query, result)
				})
			}
		}
	}
}
