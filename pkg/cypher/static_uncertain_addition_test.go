package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestStaticUncertainAdditionAdmission covers NOR-82's four recovered
// reference probes and their shared temporal producer boundaries. The original
// 64 inherited H2 cases remain unrecovered.
// Expected verdicts belong to the shared Neo4j matrix, not a second oracle.
func TestStaticUncertainAdditionAdmission(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, verdict := range staticUncertainAdditionVerdicts {
			query, rejection := verdict.query, verdict.rejection
			for _, mode := range []string{"auto", "tx"} {
				for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(query+"/"+mode+"/"+prefix, func(t *testing.T) {
						exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "uncertain_addition"))
						ctx := context.Background()
						if mode == "tx" {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() {
								_, err := exec.Execute(ctx, "ROLLBACK", nil)
								require.NoError(t, err)
							}()
						}
						if rejection != "" {
							requireStatus(t, exec, prefix+query, nil, "Neo.ClientError.Statement.SyntaxError", rejection)
							return
						}
						result, err := exec.Execute(ctx, prefix+query, nil)
						require.NoError(t, err)
						require.NotNil(t, result)
						if prefix == "" {
							require.Empty(t, result.Rows, "UNWIND [] must produce no rows")
						}
					})
				}
			}
		}
	})
}
