package cypher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The grammar admits expression syntax in epoch operands and grouping keys,
// but the schema owner requires directly bound properties. A query body in
// any of these positions must never become an executable schema expression.
func TestEpochConstraintRejectsQueryBodyOperandsWithoutEffects(t *testing.T) {
	withParserTypes(t, func(t *testing.T) {
		for _, body := range []string{
			"COUNT { MATCH (x) RETURN x }",
			"COLLECT { MATCH (x) RETURN x }",
			"EXISTS { MATCH (x) RETURN x }",
		} {
			for _, slot := range []string{"start", "end", "group"} {
				for _, block := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/block=%t", body, slot, block), func(t *testing.T) {
						exec, engine := newConstraintGapExecutor(t)
						_, err := exec.Execute(t.Context(), "CREATE (:Sentinel {id: 1})", nil)
						require.NoError(t, err)
						start, end, group := "n.starts", "n.ends", "n.key"
						switch slot {
						case "start":
							start = body
						case "end":
							end = body
						case "group":
							group = body
						}
						requirement := fmt.Sprintf("temporal.epoch(%s, %s) GROUP BY %s", start, end, group)
						if block {
							requirement = "{ n.id IS UNIQUE; " + requirement + "; n.hours > 0 }"
						}
						_, err = exec.Execute(t.Context(), "CREATE CONSTRAINT rejected FOR (n:Period) REQUIRE "+requirement, nil)
						require.Error(t, err)
						require.Empty(t, engine.GetSchema().GetAllConstraints(), "rejection cannot partially publish the block's unique constraint")
						contracts, err := exec.Execute(t.Context(), "SHOW CONSTRAINT CONTRACTS", nil)
						require.NoError(t, err)
						require.Empty(t, contracts.Rows)
						nodes, err := exec.Execute(t.Context(), "MATCH (n) RETURN n.id AS id", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(1)}}, nodes.Rows)
					})
				}
			}
		}
	})
}
