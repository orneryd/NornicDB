package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestReviewR8ProjectionFamily(t *testing.T) {
	queries := []string{
		"CALL { WITH 7 AS embedding RETURN * } CREATE (n:R8Probe) WITH embedding RETURN embedding AS k",
		"WITH 7 AS embedding CALL (embedding) { CREATE (n:R8Probe) WITH embedding RETURN embedding AS k UNION ALL CREATE (m:R8Probe) WITH embedding RETURN embedding AS k } RETURN k",
		"CREATE (embedding:R8Probe {k:7}) WITH embedding RETURN embedding.k AS k UNION ALL CREATE (EMBEDDING:R8Probe {k:7}) WITH EMBEDDING RETURN EMBEDDING.k AS k",
		"WITH 7 AS EmBeDdInG CALL { WITH EmBeDdInG CREATE (n:R8Probe) WITH EmBeDdInG AS retained WITH retained RETURN retained AS k } RETURN k",
		"CALL { CREATE (embedding:R8Probe {k:7}) WITH embedding RETURN embedding } SET embedding.k=8 WITH embedding RETURN embedding.k-1 AS k",
	}
	for _, prefix := range []string{"", "USE comp.one "} {
		for _, explicit := range []bool{false, true} {
			for _, query := range queries {
				t.Run(prefix+query+map[bool]string{false: "/auto", true: "/explicit"}[explicit], func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					if explicit && prefix != "" {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := exec.Execute(ctx, prefix+query, nil)
					if prefix != "" && strings.HasPrefix(query, "CREATE (embedding:") && strings.Contains(query, "UNION ALL CREATE") {
						if explicit {
							// UNION branches have separate USE scopes. The RHS inherits
							// composite session comp and has no physical write target.
							require.ErrorContains(t, err, "ambiguous composite write target")
							_, rollbackErr := exec.Execute(ctx, "ROLLBACK", nil)
							require.NoError(t, rollbackErr)
						} else {
							require.ErrorContains(t, err, "Writing to more than one database")
						}
						for _, database := range []string{"one", "two"} {
							check, checkErr := exec.Execute(ctx, "USE comp."+database+" MATCH (n:R8Probe) RETURN count(n) AS c", nil)
							require.NoError(t, checkErr)
							require.Equal(t, [][]interface{}{{int64(0)}}, check.Rows)
						}
						return
					}
					require.NoError(t, err)
					require.NotEmpty(t, result.Rows)
					for _, row := range result.Rows {
						require.EqualValues(t, 7, row[0])
					}
					if explicit {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
