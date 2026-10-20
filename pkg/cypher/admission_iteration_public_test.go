package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdmissionIterationPredicatePublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/norm_%v/tx_%v", route, normalize, explicit), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					if route == "use" || route == "composite" {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					ctx := context.Background()

					for _, body := range []string{"UNWIND [] AS item WHERE true RETURN item", "MATCH (n:NoAdmissionRows) UNWIND labels(n) AS item WHERE item <> 'x' RETURN item"} {
						func() {
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer exec.Execute(ctx, "ROLLBACK", nil)
							}
							query := body
							switch route {
							case "call":
								query = "CALL { " + body + " } RETURN item"
							case "use":
								query = "USE comp.one " + body
							case "composite":
								query = "USE comp CALL { USE comp.one " + body + " } RETURN item"
							}
							_, err := exec.Execute(ctx, query, nil)
							require.Error(t, err, query)
							code, _ := nornicerrors.Neo4jStatus(err)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
							require.Contains(t, err.Error(), "Invalid input 'WHERE'", query)
						}()
					}
				})
			}
		}
	}
}
