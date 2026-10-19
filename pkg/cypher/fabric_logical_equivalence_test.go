package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalUnionEquivalence(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	driver := fabricCallRowsReference(t)
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, route := range []string{"native", "use", "call"} {
				for _, tc := range []struct{ name, query string }{
					{"numeric", "RETURN 1 AS v UNION RETURN 1.0 AS v"},
					{"list", "RETURN [1] AS v UNION RETURN [1.0] AS v"},
					{"map", "RETURN {k:1} AS v UNION RETURN {k:1.0} AS v"},
				} {
					t.Run(fmt.Sprintf("normalize_%v/explicit_%v/%s/%s", normalize, explicit, route, tc.name), func(t *testing.T) {
						if driver != nil {
							reference, err := neo4j.ExecuteQuery(ctx, driver, tc.query, nil, neo4j.EagerResultTransformer)
							require.NoError(t, err)
							require.Len(t, reference.Records, 1, "reference equivalence")
						}
						exec := newFabricCallRowsExecutor(t)
						query := tc.query
						if route != "native" {
							exec = fabricCallRowsCompositeExecutor(t, exec)
							query = "USE comp.one " + strings.ReplaceAll(query, " UNION ", " UNION USE comp.one ")
						}
						if route == "call" {
							query = "CALL { " + query + " } RETURN v"
						}
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
						}
						result, err := exec.Execute(ctx, query, nil)
						if assert.NoError(t, err, query) {
							assert.Len(t, result.Rows, 1, "query=%s rows=%v", query, result.Rows)
						}
					})
				}
			}
		}
	}
}
