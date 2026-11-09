package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCallSubqueryScopeUsesCanonicalClauseRoles(t *testing.T) {
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
					for _, tc := range []struct{ name, query, column string }{
						{"path binding", "USE comp CALL { USE comp.one MATCH call = (n:FabricStructureName) RETURN n.v AS k } RETURN k", "k"},
						{"order operand", "USE comp CALL { USE comp.one WITH 7 AS call RETURN call AS k ORDER BY call DESC SKIP 0 LIMIT 1 } RETURN k", "k"},
						{"final order", "USE comp CALL { USE comp.one RETURN 7 AS call } RETURN call ORDER BY call DESC SKIP 0 LIMIT 1", "call"},
					} {
						t.Run(tc.name, func(t *testing.T) {
							for _, explicit := range []bool{false, true} {
								t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
									base := newFabricCallRowsExecutor(t)
									_, err := base.Execute(t.Context(), "USE comp.one CREATE (:FabricStructureName {v:7})", nil)
									require.NoError(t, err)
									exec := base
									if explicit {
										exec = fabricCallRowsCompositeExecutor(t, base)
										_, err = exec.Execute(t.Context(), "BEGIN", nil)
										require.NoError(t, err)
										t.Cleanup(func() { _, err := exec.Execute(context.Background(), "ROLLBACK", nil); require.NoError(t, err) })
									}
									result, err := exec.Execute(t.Context(), tc.query, nil)
									require.NoError(t, err, tc.query)
									require.Equal(t, []string{tc.column}, result.Columns)
									require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
								})
							}
						})
					}
				})
			}
		})
	}
}
