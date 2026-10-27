package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestReviewR8ProjectionControls(t *testing.T) {
	queries := []string{
		"CALL { WITH 7 AS embedding RETURN * } CREATE (n:R8Probe) WITH embedding AS retained RETURN retained AS k",
		"CALL { WITH 7 AS ordinary RETURN * } CREATE (n:R8Probe) WITH ordinary RETURN ordinary AS k",
		"WITH 7 AS ordinary CALL (ordinary) { CREATE (n:R8Probe) WITH ordinary RETURN ordinary AS k UNION ALL CREATE (m:R8Probe) WITH ordinary RETURN ordinary AS k } RETURN k",
		"CREATE (ordinary:R8Probe {k:7}) WITH ordinary RETURN ordinary.k AS k UNION ALL CREATE (OTHER:R8Probe {k:7}) WITH OTHER RETURN OTHER.k AS k",
	}
	for _, prefix := range []string{"", "USE comp.one "} {
		for _, query := range queries {
			t.Run(prefix+query, func(t *testing.T) {
				exec := newFabricCallRowsExecutor(t)
				result, err := exec.Execute(context.Background(), prefix+query, nil)
				if prefix != "" && strings.HasPrefix(query, "CREATE (ordinary:") {
					require.ErrorContains(t, err, "Writing to more than one database")
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, result.Rows)
				require.EqualValues(t, 7, result.Rows[0][0])
			})
		}
	}
	for _, name := range []string{"embedding", "EMBEDDING", "EmBeDdInG", "`embedding`"} {
		for _, mutation := range []string{"CREATE (n:R8Probe)", "MERGE (n:R8Probe)", "FOREACH (i IN [1] | CREATE (:R8Probe))"} {
			query := "CALL { WITH 7 AS " + name + " RETURN * } " + mutation + " WITH " + name + " RETURN " + name + " AS k"
			t.Run(strings.ReplaceAll(query, "/", "_"), func(t *testing.T) {
				exec := newFabricCallRowsExecutor(t)
				result, err := exec.Execute(context.Background(), "USE comp.one "+query, nil)
				require.NoError(t, err)
				require.EqualValues(t, 7, result.Rows[0][0])
			})
		}
	}
}
