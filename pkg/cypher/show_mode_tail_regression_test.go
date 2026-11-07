package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShowConstraintsModeTailRetainsProjectedHeader(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, mode := range []string{"EXPLAIN", "PROFILE"} {
			for _, tail := range []string{"YIELD name", "YIELD name RETURN name", "YIELD name AS constraintName RETURN constraintName"} {
				query := mode + " SHOW CONSTRAINTS " + tail
				t.Run(query, func(t *testing.T) {
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					want := "name"
					if tail == "YIELD name AS constraintName RETURN constraintName" {
						want = "constraintName"
					}
					require.Equal(t, []string{want}, result.Columns)
					require.Empty(t, result.Rows)
				})
			}
		}
	})
}
