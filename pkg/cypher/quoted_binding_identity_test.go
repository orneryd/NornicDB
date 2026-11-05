package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQuotedBindingIdentityMatchesBareVariableRoles(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			prior := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(prior)
			for _, normalized := range []bool{true, false} {
				t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(normalized)
					require.Equal(t, normalized, config.IsCypherQueryNormalizationEnabled())
					for _, tc := range []struct {
						name, query string
						params      map[string]interface{}
						columns     []string
						rows        [][]interface{}
						label       bool
					}{
						{name: "mixed keyword references", query: "WITH 7 AS `call` RETURN call AS bare, `call` AS quoted", columns: []string{"bare", "quoted"}, rows: [][]interface{}{{int64(7), int64(7)}}},
						{name: "variable versus label property and parameter", query: "CREATE (n:`call` {call:3}) WITH n, 7 AS `call` RETURN n.call AS property, $call AS parameter, call AS bare, `call` AS quoted", params: map[string]interface{}{"call": int64(9)}, columns: []string{"property", "parameter", "bare", "quoted"}, rows: [][]interface{}{{int64(3), int64(9), int64(7), int64(7)}}, label: true},
						{name: "boolean literal stays literal", query: "WITH 7 AS `true` RETURN true AS literal, `true` AS variable", columns: []string{"literal", "variable"}, rows: [][]interface{}{{true, int64(7)}}},
						{name: "null literal stays literal", query: "WITH 7 AS `null` RETURN null AS literal, `null` AS variable", columns: []string{"literal", "variable"}, rows: [][]interface{}{{nil, int64(7)}}},
					} {
						t.Run(tc.name, func(t *testing.T) {
							exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quoted-binding-roles"))
							result, err := exec.Execute(t.Context(), tc.query, tc.params)
							require.NoError(t, err)
							require.Equal(t, tc.columns, result.Columns)
							require.Equal(t, tc.rows, result.Rows)
							if tc.label {
								result, err = exec.Execute(t.Context(), "MATCH (n:`call`) RETURN count(n) AS count", nil)
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
							}
						})
					}
				})
			}
		})
	}
}
