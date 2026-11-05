package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A CALL body that runs on a remote constituent receives the variables it
// imports through its leading WITH: the remote statement binds them, and its
// writes complete with the transaction.
func TestFabricRemoteBodyImports(t *testing.T) {
	for _, normalization := range []string{"on", "off"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			if normalization == "off" {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := fabricDispatchRemoteExecutor(t)
			ctx := context.Background()
			for _, kind := range []string{"CALL", "OPTIONAL CALL"} {
				for _, explicit := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/explicit=%v", kind, explicit), func(t *testing.T) {
						run := fmt.Sprintf("%s-%s-%v-%d", normalization, kind, explicit, time.Now().UnixNano())
						params := map[string]interface{}{"run": run}
						query := "USE comp UNWIND [1,2] AS x " + kind + " { WITH x UNWIND [1,2] AS y CALL { WITH x, y UNWIND [1,2] AS z CALL { WITH x, y, z USE comp.remote CREATE (:FabricRemoteImport {run: $run, x: x, y: y, z: z}) } } } RETURN x ORDER BY x"
						runExec := exec
						if explicit {
							runExec = fabricCallRowsCompositeExecutor(t, exec)
							_, err := runExec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						result, err := runExec.Execute(ctx, query, params)
						require.NoError(t, err)
						fabricDispatchRows(t, result.Rows, "[[1],[2]]")
						if explicit {
							_, err := runExec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						written, err := exec.Execute(ctx, "USE comp.remote MATCH (n:FabricRemoteImport {run: $run}) RETURN count(n) AS c, sum(n.x * 100 + n.y * 10 + n.z) AS s", params)
						require.NoError(t, err)
						fabricDispatchRows(t, written.Rows, "[[8,1332]]")
						_, err = exec.Execute(ctx, "USE comp.remote MATCH (n:FabricRemoteImport {run: $run}) DETACH DELETE n", params)
						require.NoError(t, err)
					})
				}
			}
			// A returning remote body reads its imports too.
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("returning/explicit=%v", explicit), func(t *testing.T) {
					runExec := exec
					if explicit {
						runExec = fabricCallRowsCompositeExecutor(t, exec)
						_, err := runExec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, _ = runExec.Execute(context.Background(), "ROLLBACK", nil) }()
					}
					result, err := runExec.Execute(ctx, "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.remote WITH x, x * 10 AS w RETURN w + 1 AS v } RETURN x, v ORDER BY x", nil)
					require.NoError(t, err)
					encoded, err := json.Marshal(result.Rows)
					require.NoError(t, err)
					require.JSONEq(t, "[[1,11],[2,21]]", string(encoded))
					// A leading WITH item that projects a parameter is not an import.
					result, err = runExec.Execute(ctx, "USE comp UNWIND [1,2] AS x CALL { WITH x USE comp.remote WITH x, $k AS k RETURN x + k AS v } RETURN x, v ORDER BY x", map[string]interface{}{"k": int64(10)})
					require.NoError(t, err)
					encoded, err = json.Marshal(result.Rows)
					require.NoError(t, err)
					require.JSONEq(t, "[[1,11],[2,12]]", string(encoded))
				})
			}
		})
	}
}
