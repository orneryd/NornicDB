package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Neo4j acknowledges CREATE LOOKUP INDEX before population finishes. The
// reset must establish readiness before the next corpus query can use it.
func TestHTTPDifferentialResetWaitsForLookupPopulation(t *testing.T) {
	for _, failWait := range []bool{false, true} {
		name := "ready_before_return"
		if failWait {
			name = "readiness_error_stops_reset"
		}
		t.Run(name, func(t *testing.T) {
			pending, waits := 0, 0
			var statements []string
			err := resetHTTPDifferentialBackend(func(statement string) TransactionResponse {
				statements = append(statements, statement)
				switch {
				case statement == "SHOW CONSTRAINTS" || statement == "SHOW INDEXES":
					return TransactionResponse{Results: []QueryResult{{Columns: []string{"name"}}}}
				case strings.HasPrefix(statement, "CREATE LOOKUP INDEX "):
					pending++
				case statement == "CALL db.awaitIndexes(10)":
					waits++
					require.Equal(t, 2, pending, "wait follows creation of both lookup indexes")
					if failWait {
						return TransactionResponse{Errors: []QueryError{{Code: "Neo.ClientError.Schema.IndexCreationFailed", Message: "lookup population failed"}}}
					}
					pending = 0
				}
				return TransactionResponse{}
			})
			if failWait {
				require.ErrorContains(t, err, "lookup population failed")
				require.Equal(t, "CALL db.awaitIndexes(10)", statements[len(statements)-1], "readiness failure is not retried or hidden")
				return
			}
			require.NoError(t, err)
			require.Zero(t, pending, "reset cannot return while a lookup index is still populating")
			require.Equal(t, 1, waits)
			require.Equal(t, "CALL db.clearQueryCaches()", statements[len(statements)-1])
		})
	}
}
