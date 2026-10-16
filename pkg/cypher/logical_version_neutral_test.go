package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogicalVersionNeutralPreservesValue(t *testing.T) {
	for _, text := range []bool{false, true} {
		mode := "compiled"
		if text {
			mode = "text"
		}
		t.Run(mode, func(t *testing.T) {
			rowValueProgramsDisabled.Store(text)
			defer rowValueProgramsDisabled.Store(false)
			for _, value := range []struct {
				name, literal string
			}{
				{"integer", "7"},
				{"list", "[7]"},
				{"empty_list", "[]"},
			} {
				t.Run(value.name, func(t *testing.T) {
					exec, _ := perRowTestExecutor(t)
					exec.cache = nil
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:R3Neutral {v:"+value.literal+"})", nil)
					require.NoError(t, err)
					original, err := exec.Execute(ctx, "MATCH (n:R3Neutral) RETURN n.v AS v", nil)
					require.NoError(t, err)
					require.Len(t, original.Rows, 1)
					require.Len(t, original.Rows[0], 1)
					for _, tc := range []struct {
						expression, neutralVersion string
						deciding                   bool
						multi                      bool
					}{
						{"(9007199254740993 <> 9007199254740992.0) AND coalesce(n.v, COUNT { RETURN 1 })", "5", false, false},
						{"coalesce(n.v, COUNT { RETURN 1 }) AND (9007199254740993 <> 9007199254740992.0)", "5", false, false},
						{"(9007199254740993 <> 9007199254740992.0) OR coalesce(n.v, COUNT { RETURN 1 })", "25", true, false},
						{"coalesce(n.v, COUNT { RETURN 1 }) OR (9007199254740993 <> 9007199254740992.0)", "25", true, false},
						{"(9007199254740993 <> 9007199254740992.0) AND COUNT { RETURN 1 } > 0 AND coalesce(n.v, COUNT { RETURN 1 })", "5", false, true},
						{"coalesce(n.v, COUNT { RETURN 1 }) AND COUNT { RETURN 1 } > 0 AND (9007199254740993 <> 9007199254740992.0)", "5", false, true},
						{"(9007199254740993 <> 9007199254740992.0) OR COUNT { RETURN 1 } > 1 OR coalesce(n.v, COUNT { RETURN 1 })", "25", true, true},
						{"coalesce(n.v, COUNT { RETURN 1 }) OR COUNT { RETURN 1 } > 1 OR (9007199254740993 <> 9007199254740992.0)", "25", true, true},
					} {
						for _, versions := range [][]string{{"5", "25", "5", "25"}, {"25", "5", "25", "5"}} {
							expression := tc.expression
							if versions[0] == "25" {
								expression = strings.ReplaceAll(expression, "RETURN 1", "RETURN 2")
							}
							for _, version := range versions {
								t.Run(version+"/"+expression, func(t *testing.T) {
									want := interface{}(tc.deciding)
									if version == tc.neutralVersion {
										want = original.Rows[0][0]
										if tc.multi {
											want = value.name == "list"
										}
									}
									result, err := exec.Execute(ctx, "CYPHER "+version+" MATCH (n:R3Neutral) RETURN "+expression+" AS v", nil)
									if tc.multi && version == tc.neutralVersion && value.name == "integer" {
										requireRound3Neo4jError(t, err, "Type mismatch: expected Boolean but was Integer")
										return
									}
									require.NoError(t, err)
									require.Equal(t, [][]interface{}{{want}}, result.Rows)
								})
							}
						}
					}
				})
			}
		})
	}
}
