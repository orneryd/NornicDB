package cypher

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pinned Neo4j distinguishes omitted-mode zero precision (ties toward positive
// infinity) from positive precision and explicitly requested HALF_UP.
func TestRoundZeroPrecisionPinnedNeo4j(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			if parser == "antlr" {
				defer config.WithANTLRParser()()
			} else {
				defer config.WithNornicParser()()
			}
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprint("transaction=", explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "round_zero"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:RoundZero)", nil)
					require.NoError(t, err)
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					for _, tc := range []struct {
						expression string
						want       float64
					}{
						{"round(-0.5, 0)", 0}, {"round(-0.5, 0.0)", 0}, {"round(-0.5, -0.5)", 0},
						{"round(-2.5, 0)", -2}, {"round(-2.5, 0.5)", -2}, {"round(-2.5, 0, 'HALF_UP')", -3},
						{"round(-2.55, 1)", -2.6}, {"round(-2.55, 1.0)", -2.6}, {"round(-2.55, 1.5)", -2.6},
						{"round(0.5, 0)", 1}, {"round(-2.5)", -2}, {"round(-2.5, 0, 'HALF_DOWN')", -2},
					} {
						for _, query := range []string{
							"RETURN " + tc.expression + " AS v",
							"MATCH (n:RoundZero) RETURN " + tc.expression + " AS v",
							"MATCH (n:RoundZero) WITH " + tc.expression + " AS v RETURN v",
						} {
							t.Run(query, func(t *testing.T) {
								result, err := exec.Execute(ctx, query, nil)
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
							})
						}
					}
					if explicit {
						_, err = exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

// Preserve exact boundary values through literal, parameter and stored-property
// routes. In particular, adding 0.5 must not change an already-integral double.
func TestRoundDefaultIEEEBoundaries(t *testing.T) {
	cases := []struct{ value, want float64 }{
		{0.49999999999999994, 0}, {4503599627370497, 4503599627370497},
		{-4503599627370497, -4503599627370497}, {9007199254740991, 9007199254740991},
		{-9007199254740991, -9007199254740991},
		{math.Nextafter(0.5, 0), 0}, {0.5, 1}, {math.Nextafter(0.5, 1), 1},
		{math.Nextafter(-0.5, -1), -1}, {-0.5, 0}, {math.Nextafter(-0.5, 0), 0},
		{math.Nextafter(1.5, 0), 1}, {1.5, 2}, {math.Nextafter(1.5, 2), 2},
		{math.Nextafter(-1.5, -2), -2}, {-1.5, -1}, {math.Nextafter(-1.5, 0), -1},
		{0, 0}, {math.Copysign(0, -1), 0}, {math.SmallestNonzeroFloat64, 0}, {-math.SmallestNonzeroFloat64, 0},
		{4503599627370495.5, 4503599627370496}, {-4503599627370495.5, -4503599627370495},
		{math.MaxFloat64, math.MaxFloat64}, {-math.MaxFloat64, -math.MaxFloat64},
	}
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			if parser == "antlr" {
				defer config.WithANTLRParser()()
			} else {
				defer config.WithNornicParser()()
			}
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprint("transaction=", explicit), func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "round_boundary"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					for i, tc := range cases {
						t.Run(fmt.Sprint(i), func(t *testing.T) {
							params := map[string]interface{}{"x": tc.value}
							_, err := exec.Execute(ctx, "MERGE (n:RoundBoundary {id:1}) SET n.x=$x", params)
							require.NoError(t, err)
							for _, precision := range []string{"", ", 0", ", 0.0", ", 0.5"} {
								literal := strconv.FormatFloat(tc.value, 'g', -1, 64)
								if !strings.ContainsAny(literal, ".eE") {
									literal += ".0"
								}
								queries := []string{"RETURN round(" + literal + precision + ") AS v", "RETURN round($x" + precision + ") AS v", "MATCH (n:RoundBoundary) RETURN round(n.x" + precision + ") AS v", "MATCH (n:RoundBoundary) WITH round(n.x" + precision + ") AS v RETURN v"}
								for _, q := range queries {
									t.Run(q, func(t *testing.T) {
										r, err := exec.Execute(ctx, q, params)
										require.NoError(t, err)
										require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
										if tc.want == 0 {
											require.False(t, math.Signbit(r.Rows[0][0].(float64)))
										}
									})
								}
							}
						})
					}
					if explicit {
						_, err := exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
			}
		})
	}
}
