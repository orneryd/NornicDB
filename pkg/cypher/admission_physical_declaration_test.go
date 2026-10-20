package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Native absence is an authoritative acquisition decision, not a declaration
// verdict. Physical aliases still pass through the canonical name reader.
func TestAdmissionPhysicalDeclarationProof(t *testing.T) {
	previous := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use"} {
			t.Run(fmt.Sprintf("%s/norm_%v", route, normalize), func(t *testing.T) {
				exec := newFabricCallRowsExecutor(t)
				prefix := ""
				if route == "call" {
					prefix = "CALL { RETURN 2 AS imported } "
				}
				if route == "use" {
					exec = fabricCallRowsCompositeExecutor(t, exec)
					prefix = "USE comp.one "
				}
				for _, alias := range []string{"plain", "`quoted`", "`name,part`", "`back``tick`"} {
					query := prefix + "WITH 7 AS " + alias + " RETURN " + alias
					for repeat := 0; repeat < 2; repeat++ {
						result, err := exec.Execute(context.Background(), query, nil)
						require.NoError(t, err, query)
						require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, query)
					}
				}
				for _, body := range []string{"WITH 1 AS alias extra RETURN alias", "RETURN 1 AS alias extra", "WITH 1 AS RETURN 1", "RETURN 1 AS"} {
					query := prefix + body
					for repeat := 0; repeat < 2; repeat++ {
						_, err := exec.Execute(context.Background(), query, nil)
						require.Error(t, err, query)
						requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
					}
				}
			})
		}
	}
}
