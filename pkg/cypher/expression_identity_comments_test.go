package cypher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/expressionidentitymatrix"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExpressionSubqueryIdentityComments(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, sample := range expressionidentitymatrix.Cases() {
				t.Run(fmt.Sprintf("normalize=%t/explicit=%t/%s", normalize, explicit, sample.ID), func(t *testing.T) {
					config.SetCypherQueryNormalizationEnabled(normalize)
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "identity"))
					ctx := context.Background()
					if explicit {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := exec.Execute(ctx, sample.Query, nil)
					require.NoError(t, err, sample.Query)
					rows, err := json.Marshal(result.Rows)
					require.NoError(t, err)
					require.JSONEq(t, sample.Rows, string(rows), sample.Query)
				})
			}
		}
	}
}

func TestExportExpressionIdentityOracle(t *testing.T) {
	if os.Getenv("NORNICDB_EXPORT_EXPRESSION_IDENTITY_ORACLE") == "" {
		t.Skip("explicit oracle export")
	}
	for _, sample := range expressionidentitymatrix.Cases() {
		query := "CALL { " + sample.Query + " } RETURN '" + sample.ID + "' AS id, v;"
		fmt.Println("EXPRESSION_IDENTITY_ORACLE " + base64.StdEncoding.EncodeToString([]byte(query)))
		fmt.Printf("EXPRESSION_IDENTITY_EXPECTED %s %s\n", sample.ID, sample.Rows)
	}
}
