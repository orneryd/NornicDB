package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A retained input node must keep properties used by a later MATCH, through
// native execution, constituent routing and a decomposed composite CALL.
func TestPhysicalPatternReadPublicRoutes(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "constituent", "composite"} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/norm_%v/explicit_%v", route, normalize, explicit), func(t *testing.T) {
					exec := newFabricCallRowsExecutor(t)
					seed := "CREATE (:ReadPattern {a:1,b:42}), (:ReadPattern {a:2,b:99})"
					if route != "native" {
						seed = "USE comp.one " + seed
					}
					_, err := exec.Execute(ctx, seed, nil)
					require.NoError(t, err)
					if route != "native" {
						exec = fabricCallRowsCompositeExecutor(t, exec)
					}
					if explicit {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer exec.Execute(ctx, "ROLLBACK", nil)
					}
					for _, query := range []string{
						"MATCH (p:ReadPattern) MATCH (p {a:1}) RETURN p.b AS v",
						"MATCH (p:ReadPattern) WITH p MATCH (p {a:1}) RETURN p.b AS v",
					} {
						if route == "constituent" {
							query = "USE comp.one " + query
						}
						if route == "composite" {
							query = "USE comp CALL { USE comp.one " + query + " } RETURN v"
						}
						for repeat := 0; repeat < 2; repeat++ {
							result, err := exec.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows, query)
						}
					}
				})
			}
		}
	}
}
