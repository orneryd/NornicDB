package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalCompositeTransaction(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, tc := range []struct {
			name, query string
			rows        [][]interface{}
			invalid     bool
		}{
			{"optional", "WITH 1 AS x OPTIONAL CALL { USE comp.one MATCH (n:ReviewNoNode) RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
			{"named_retained", "WITH 11 AS x CALL (x) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", [][]interface{}{{int64(11)}}, false},
			{"star_retained", "WITH 11 AS x CALL (*) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", [][]interface{}{{int64(11)}}, false},
			{"empty_scope", "WITH 11 AS x CALL () { USE comp.one RETURN x AS v } RETURN v", nil, true},
			{"empty_outer", "UNWIND [] AS x CALL { USE comp.one RETURN toUpper(CASE WHEN true THEN [1] ELSE ['a'] END) AS v } RETURN v", nil, true},
			{"typed_import", "WITH [1] AS x CALL (x) { USE comp.one UNWIND [] AS ignored RETURN toUpper(x) AS v } RETURN v", nil, true},
			{"aggregate_order", "WITH 11 AS x CALL (x) { USE comp.one UNWIND [] AS y RETURN count(*) AS c ORDER BY x } RETURN c", [][]interface{}{{int64(0)}}, false},
			{"numeric_case_empty", "USE comp.one UNWIND [] AS unused RETURN (CASE WHEN true THEN 1 ELSE 2.0 END) + {a:1} AS v", nil, true},
			{"asymmetric_import_filtered", "USE comp.one UNWIND [] AS unused WITH 1.0 AS x CALL (x) { MATCH (n:ReviewEmpty) WITH n.v * 1.0 AS x WITH count(*) AS c RETURN [1,2][x] AS v } RETURN v", nil, true},
		} {
			t.Run(fmt.Sprintf("normalize_%v/%s", normalize, tc.name), func(t *testing.T) {
				exec := fabricCallRowsCompositeExecutor(t, newFabricCallRowsExecutor(t))
				ctx := context.Background()
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer exec.Execute(ctx, "ROLLBACK", nil)
				_, ok := exec.txContext.tx.(*fabric.FabricTransaction)
				require.True(t, ok, "the test must exercise a real composite transaction")
				result, err := exec.Execute(ctx, tc.query, nil)
				if tc.invalid {
					assert.Error(t, err, "query=%s result=%+v", tc.query, result)
					return
				}
				if assert.NoError(t, err, tc.query) {
					assert.Equal(t, tc.rows, result.Rows, tc.query)
				}
			})
		}
	}
}
