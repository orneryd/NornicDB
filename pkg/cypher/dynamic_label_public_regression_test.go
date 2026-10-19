package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/dynamiclabels"
	"github.com/stretchr/testify/require"
)

func TestDynamicLabelPublicExecute(t *testing.T) {
	for _, normalization := range []string{"true", "false"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			t.Setenv("NORNICDB_CYPHER_QUERY_NORMALIZATION", normalization)
			for _, c := range dynamiclabels.Cases() {
				t.Run(c.Name, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "dynamic_public"))
					ctx := context.Background()
					_, err := exec.Execute(ctx, dynamiclabels.Fixture, nil)
					require.NoError(t, err)
					for range 2 {
						result, err := exec.Execute(ctx, c.Query, c.Params)
						if c.Code != "" { require.Error(t, err); requireStatusCode(t, err, c.Code); continue }
						require.NoError(t, err, c.Query)
						require.Equal(t, c.Rows, result.Rows, c.Query)
					}
				})
			}
		})
	}
}
