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

func TestFabricLogicalCallMetadata(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, composite := range []bool{false, true} {
				for _, tc := range []struct {
					name, query string
					rows        [][]interface{}
					invalid     bool
				}{
					{"optional", "WITH 1 AS x OPTIONAL CALL { USE comp.one MATCH (n:ReviewNoNode) RETURN n } RETURN x,n", [][]interface{}{{int64(1), nil}}, false},
					{"named_retained", "WITH 11 AS x CALL (x) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", [][]interface{}{{int64(11)}}, false},
					{"star_retained", "WITH 11 AS x CALL (*) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", [][]interface{}{{int64(11)}}, false},
					{"empty_scope", "WITH 11 AS x CALL () { USE comp.one RETURN x AS v } RETURN v", nil, true},
					{"aggregate_order", "WITH 11 AS x CALL (x) { USE comp.one UNWIND [] AS y RETURN count(*) AS c ORDER BY x } RETURN c", [][]interface{}{{int64(0)}}, false},
					{"aggregate_order_expression", "WITH 11 AS x CALL (x) { USE comp.one UNWIND [] AS y RETURN count(*) AS c ORDER BY x+1 } RETURN c", [][]interface{}{{int64(0)}}, false},
				} {
					t.Run(fmt.Sprintf("normalize_%v/explicit_%v/composite_%v/%s", normalize, explicit, composite, tc.name), func(t *testing.T) {
						exec := newFabricCallRowsExecutor(t)
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
						query := tc.query
						if !composite {
							query = strings.ReplaceAll(query, "USE comp.one ", "")
						}
						result, err := exec.Execute(ctx, query, nil)
						if tc.invalid {
							assert.Error(t, err, "query=%s result=%+v", query, result)
							return
						}
						if assert.NoError(t, err, query) {
							assert.Equal(t, tc.rows, result.Rows, query)
						}
					})
				}
			}
		}
	}
}
