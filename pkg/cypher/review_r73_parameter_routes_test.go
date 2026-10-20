package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReviewR73ParameterRoutes(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "use"} {
			for _, explicit := range []bool{false, true} {
				for _, empty := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/tx_%v/empty_%v/norm_%v", route, explicit, empty, normalize), func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						ctx := context.Background()
						query := "WITH $p AS x "
						if empty {
							query += "UNWIND [] AS stop "
						}
						query += "CALL (x) { WITH 2 AS y RETURN x.a AS v } RETURN v"
						if route == "use" {
							e = fabricCallRowsCompositeExecutor(t, e)
							query = "USE comp.one " + query
						}
						if explicit {
							_, err := e.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer e.Execute(ctx, "ROLLBACK", nil)
						}
						result, err := e.Execute(ctx, query, map[string]interface{}{"p": int64(1)})
						code := ""
						if err != nil {
							code, _ = nornicerrors.Neo4jStatus(err)
						}
						t.Logf("query=%q code=%s err=%v result=%v", query, code, err, result)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					})
				}
			}
		}
	}
}
