package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFabricLogicalScopedEmbeddingFrame(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, composite := range []bool{false, true} {
				for _, name := range []string{"embedding", "ordinary"} {
					t.Run(fmt.Sprintf("normalize_%v/explicit_%v/composite_%v/%s", normalize, explicit, composite, name), func(t *testing.T) {
						exec := newFabricCallRowsExecutor(t)
						if composite {
							exec = fabricCallRowsCompositeExecutor(t, exec)
						}
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
						}
						query := "WITH 7 AS embedding CALL (embedding) { USE comp.one WITH 1 AS y CREATE (:ReviewXhighEmbedding) WITH embedding RETURN 1 AS k } RETURN k"
						query = strings.ReplaceAll(query, "embedding", name)
						if !composite {
							query = strings.ReplaceAll(query, "USE comp.one ", "")
						}
						result, err := exec.Execute(ctx, query, nil)
						if assert.NoError(t, err, query) {
							assert.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, query)
						}
					})
				}
			}
		}
	}
}
