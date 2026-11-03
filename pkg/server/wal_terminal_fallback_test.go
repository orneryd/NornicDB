// Author: Codex, NOR-180 HTTP consumer error-flow regression, 2026-10-27.
package server

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

var walFallbackProcedureID atomic.Uint64

func TestHTTPWALTerminalInvalidCallFallback(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			_, engine, request := walHTTPFixture(t)
			name := fmt.Sprintf("nor180.fallback%d", walFallbackProcedureID.Add(1))
			// A successful read keeps wrote=false, and closes the independent
			// WAL only after HTTP's initial explicit BEGIN has been logged.
			require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeRead, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
				if closed {
					if err := engine.WAL.Close(); err != nil {
						return nil, err
					}
				}
				return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, nil
			}))
			body := fmt.Sprintf(`{"statements":[{"statement":%q},{"statement":%q}]}`, "CALL "+name+"() YIELD value RETURN value", "UNWIND [1] AS i CALL (i) { CREATE (:WalWire {i:i}) } IN TRANSACTIONS OF 1 ROWS RETURN i")
			status, response := request(http.MethodPost, "/db/nornic/tx/commit", body)
			require.Equal(t, http.StatusOK, status)
			if closed {
				require.NotEmpty(t, response.Errors)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", response.Errors[0].Code)
				requireHTTPWALDiagnostic(t, response, true)
			} else {
				require.Empty(t, response.Errors)
			}
		})
	}
}
