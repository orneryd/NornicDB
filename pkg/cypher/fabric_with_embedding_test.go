package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFabricWithEmbeddingLocalConstituent(t *testing.T) {
	for _, normalization := range []bool{true, false} {
		for separatorIndex, separator := range []string{" ", "  ", "\n", " /* option */ "} {
			for _, mode := range []string{"autocommit", "explicit"} {
				for _, shape := range []struct {
					name, body string
					returns    bool
				}{
					{"create", "CREATE (n:FabricEmbedded {id:'doc', content:'hello world'}) WITH%sEMBEDDING RETURN n.id AS id", true},
					{"create_without_return", "CREATE (n:FabricEmbedded {id:'doc', content:'hello world'}) WITH%sEMBEDDING", false},
					{"merge_set", "MERGE (n:FabricEmbedded {id:'doc'}) SET n.content='hello world' WITH%sEMBEDDING RETURN n.id AS id", true},
				} {
					t.Run(fmt.Sprintf("normalization_%v/separator_%d/%s/%s", normalization, separatorIndex, mode, shape.name), func(t *testing.T) {
						if !normalization {
							defer config.WithCypherQueryNormalizationDisabled()()
						}
						exec := newFabricCallRowsExecutor(t)
						if mode == "explicit" {
							exec = fabricCallRowsCompositeExecutor(t, exec)
						}
						exec.SetEmbedder(&inlineEmbeddingTestEmbedder{model: "fabric-inline", dims: 3})
						ctx := context.Background()
						if mode == "explicit" {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer exec.Execute(ctx, "ROLLBACK", nil)
						}
						result, err := exec.Execute(ctx, "USE comp.one "+fmt.Sprintf(shape.body, separator), nil)
						require.NoError(t, err)
						if shape.returns {
							require.Equal(t, [][]interface{}{{"doc"}}, result.Rows)
						}
						if mode == "explicit" {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						store, err := exec.dbManager.(*testDatabaseManagerAdapter).manager.GetStorage("one")
						require.NoError(t, err)
						nodes, err := store.GetNodesByLabel("FabricEmbedded")
						require.NoError(t, err)
						require.Len(t, nodes, 1)
						require.NotEmpty(t, nodes[0].ChunkEmbeddings)
						require.Equal(t, true, nodes[0].EmbedMeta["has_embedding"])
					})
				}
			}
		}
	}
}

func TestFabricEmbeddingKeywordNameControls(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		restore := func() {}
		if disabled {
			restore = config.WithCypherQueryNormalizationDisabled()
		}
		exec := newFabricCallRowsExecutor(t)
		for _, query := range []string{"WITH 7 AS embedding WITH embedding RETURN embedding AS k", "WITH $embedding AS embedding RETURN embedding AS k", "WITH 7 AS `EMBEDDING` RETURN `EMBEDDING` AS k"} {
			result, err := exec.Execute(context.Background(), "USE comp.one "+query, map[string]interface{}{"embedding": int64(7)})
			require.NoError(t, err)
			require.EqualValues(t, int64(7), result.Rows[0][0])
		}
		restore()
	}
}

func TestFabricWithEmbeddingFailureRollsBack(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t)
			if mode == "explicit" {
				exec = fabricCallRowsCompositeExecutor(t, exec)
			}
			ctx := context.Background()
			if mode == "explicit" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, "USE comp.one CREATE (:FabricEmbeddingFailure {content:'text'}) WITH EMBEDDING", nil)
			require.Error(t, err)
			if mode == "explicit" {
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
			store, err := exec.dbManager.(*testDatabaseManagerAdapter).manager.GetStorage("one")
			require.NoError(t, err)
			nodes, err := store.GetNodesByLabel("FabricEmbeddingFailure")
			require.NoError(t, err)
			require.Empty(t, nodes, "embedding failure must not publish the mutation")
		})
	}
}
