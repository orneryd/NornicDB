package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestINPatternMutationRoleContracts(t *testing.T) {
	prior := config.GetParserType()
	defer config.SetParserType(prior)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			for _, norm := range []bool{true, false} {
				t.Run(fmt.Sprintf("normalization_%v", norm), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(norm)
					base := newFabricCallRowsExecutor(t)
					fabricRunModes(t, base, func(t *testing.T, exec *StorageExecutor) {
						const query = "USE comp CALL { USE comp.one CREATE (in {v:7}) SET in:FabricNameMutation RETURN in.v AS k } RETURN k"
						result, err := exec.Execute(t.Context(), query, nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
						read, err := exec.Execute(t.Context(), "USE comp CALL { USE comp.one MATCH (in:FabricNameMutation) RETURN in.v AS k } RETURN k", nil)
						require.NoError(t, err)
						require.NotEmpty(t, read.Rows)
						for _, row := range read.Rows {
							require.Equal(t, []interface{}{int64(7)}, row)
						}
					})
				})
			}
		})
	}
}

func TestINListPositionRoleControls(t *testing.T) {
	for _, tc := range []struct {
		query    string
		want     int
		mismatch bool
	}{
		{"CREATE (in {v:7}) SET in:Label RETURN in.v", 1, false},
		{"RETURN 7 IN {v:7}", 1, true},
		{"RETURN [x IN {v:7}|x]", 1, true},
		{"RETURN any(x IN {v:7} WHERE true)", 1, true},
		{"RETURN reduce(acc=0,x IN {v:7}|acc+x)", 1, true},
		{"RETURN [in IN [7]|in]", 2, false},
		{"RETURN reduce(acc=0,in IN [7]|acc+in)", 1, false},
		{"FOREACH (in IN [7]|CREATE (:N {v:in}))", 1, false},
		{"RETURN any(in IN [7] WHERE in=7)", 1, false},
		{"WITH {v:7} AS in RETURN in{.v}", 0, false},
		{"RETURN $in", 0, false},
		{"RETURN n.in", 0, false},
		{"RETURN n:IN", 0, false},
		{"WITH 7 AS in RETURN in", 0, false},
		{"RETURN [7] CONTAINS 7", 0, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			count := 0
			require.NoError(t, forEachListOperand(tc.query, func(start, in int) error { count++; return nil }))
			require.Equal(t, tc.want, count)
			err := validateListOperands(tc.query, nil)
			if tc.mismatch {
				require.ErrorContains(t, err, "expected List<T> but was Map")
				var classified interface {
					BoltErrorCode() string
					BoltErrorDetail() string
				}
				require.ErrorAs(t, err, &classified)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
				require.Equal(t, "InvalidArgumentType", classified.BoltErrorDetail())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
