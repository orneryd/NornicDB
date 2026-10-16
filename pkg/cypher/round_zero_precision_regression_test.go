package cypher

import (
	"context"
	"fmt"
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
