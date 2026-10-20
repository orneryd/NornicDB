package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A cropped operand must retain its admitted iteration types and literal
// roles on every public route, including the constituent continuation.
func TestAdmittedTypedFramePublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/norm_%v/tx_%v", route, normalize, explicit), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					if route == "composite" || route == "use" && explicit {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					wrap := func(query string) string {
						switch route {
						case "call":
							return "CALL { " + query + " } RETURN v"
						case "use":
							return "USE comp.one " + query
						case "composite":
							return "USE comp CALL { USE comp.one " + query + " } RETURN v"
						}
						return query
					}
					query := "FOREACH (`x x` IN [1,2] | CREATE (:AdmittedFrame {v:abs(`x x`)})) RETURN 1 AS v"
					result, err := exec.Execute(ctx, wrap(query), nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
					result, err = exec.Execute(ctx, wrap("MATCH (n:AdmittedFrame) RETURN sum(n.v) AS v"), nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
					for _, expression := range []string{"-NaN", "-Infinity", "Infinity", "NaN+1"} {
						result, err = exec.Execute(ctx, wrap("RETURN "+expression+" AS v"), nil)
						require.NoError(t, err, expression)
						require.Equal(t, []string{"v"}, result.Columns)
						require.Len(t, result.Rows, 1)
						value, ok := result.Rows[0][0].(float64)
						require.True(t, ok, expression)
						if expression == "-NaN" || expression == "NaN+1" {
							require.True(t, math.IsNaN(value), expression)
						} else {
							sign := 1
							if expression == "-Infinity" {
								sign = -1
							}
							require.True(t, math.IsInf(value, sign), expression)
						}
					}
				})
			}
		}
	}
}

// Parameters remain evaluation dependencies without becoming physical row
// reads. NestedCypherUpdateStats separately pins the public reuse contract.
func TestAdmittedParameterReadPurpose(t *testing.T) {
	for _, text := range []string{"$body", "[$body]", "coalesce($body,'fallback')"} {
		info := rowValueProgramForFacts(text).info
		require.True(t, info.pure, text)
		require.Contains(t, info.refs, "$body", text)
		require.Empty(t, info.readFootprint(), text)
		require.Empty(t, physicalReadReferences(text), text)
		require.Empty(t, nativePhysicalReadReferences(text), text)
	}
}
