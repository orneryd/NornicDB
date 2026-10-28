package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFabricPrefixRetainsKeywordNamedBindings(t *testing.T) {
	for _, normalized := range []bool{true, false} {
		for _, parser := range []string{"nornic", "antlr"} {
			t.Run(parser+map[bool]string{true: "/normalized", false: "/raw"}[normalized], func(t *testing.T) {
				old := config.GetParserType()
				config.SetParserType(parser)
				defer config.SetParserType(old)
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
				config.SetCypherQueryNormalizationEnabled(normalized)
				for _, q := range []string{"USE comp WITH 7 AS return CALL (return) { USE comp.one RETURN return AS k } RETURN k", "USE comp WITH 7 AS with WITH with CALL (with) { USE comp.one RETURN with AS k } RETURN k", "USE comp WITH 7 AS as CALL (as) { USE comp.one RETURN as AS k } RETURN k", "USE comp WITH 7 AS as CALL (*) { USE comp.one RETURN as AS k } RETURN k"} {
					t.Run(q, func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						fabricRunModes(t, e, func(t *testing.T, run *StorageExecutor) {
							r, err := run.Execute(t.Context(), q, nil)
							require.NoError(t, err)
							require.Equal(t, []string{"k"}, r.Columns)
							require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
						})
					})
				}
			})
		}
	}
}
