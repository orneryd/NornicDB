package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestDB_ValidateQueryEmbeddingBatchDimensions(t *testing.T) {
	db := &DB{
		dbConfigResolver: func(dbName string) (int, float64, string) {
			require.Equal(t, "tenant", dbName)
			return 3, 0.5, ""
		},
	}

	err := db.validateQueryEmbeddingBatchDimensions("tenant", [][]float32{{1, 2, 3}, {}})
	require.NoError(t, err)

	err = db.validateQueryEmbeddingBatchDimensions("tenant", [][]float32{{1, 2}})
	require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
	require.Contains(t, err.Error(), "index dims 3")
	require.Contains(t, err.Error(), "query dims 2")
}

func TestDB_EmbedQueryChunksForDBWithEmbedder(t *testing.T) {
	t.Run("propagates chunking error", func(t *testing.T) {
		db := &DB{}
		emb := &coverageQueryEmbedder{chunkErr: errors.New("chunk failed")}

		chunks, embs, err := db.embedQueryChunksForDBWithEmbedder(context.Background(), "tenant", emb, "query")
		require.Error(t, err)
		require.Contains(t, err.Error(), "chunk failed")
		require.Nil(t, chunks)
		require.Nil(t, embs)
	})

	t.Run("returns mismatch error from batch validation", func(t *testing.T) {
		db := &DB{
			dbConfigResolver: func(dbName string) (int, float64, string) { return 3, 0, "" },
		}
		emb := &coverageQueryEmbedder{
			chunks: []string{"c1", "c2"},
			batch:  [][]float32{{1, 2}, {1, 2}},
		}

		chunks, embs, err := db.embedQueryChunksForDBWithEmbedder(context.Background(), "tenant", emb, "query")
		require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
		require.Nil(t, chunks)
		require.Nil(t, embs)
	})

	t.Run("returns chunks and embeddings when valid", func(t *testing.T) {
		db := &DB{
			dbConfigResolver: func(dbName string) (int, float64, string) { return 2, 0, "" },
		}
		emb := &coverageQueryEmbedder{
			chunks: []string{"c1", "c2"},
			batch:  [][]float32{{1, 2}, {3, 4}},
		}

		chunks, embs, err := db.embedQueryChunksForDBWithEmbedder(context.Background(), "tenant", emb, "query")
		require.NoError(t, err)
		require.Equal(t, []string{"c1", "c2"}, chunks)
		require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embs)
	})
}

func TestDB_EmbedQueryChunksForDB_UsesRegistryEmbedderWhenConfigured(t *testing.T) {
	defaultEmb := &coverageQueryEmbedder{
		chunks: []string{"default"},
		batch:  [][]float32{{9, 9}},
	}
	specialEmb := &coverageQueryEmbedder{
		chunks: []string{"special-1", "special-2"},
		batch:  [][]float32{{1, 2}, {3, 4}},
	}
	cfg := &embed.Config{Provider: "ollama", Model: "special", Dimensions: 2}

	db := &DB{
		cypherExecutor: queryEmbeddingTestExecutor(t, defaultEmb),
		embedConfigForDB: func(dbName string) (*embed.Config, error) {
			require.Equal(t, "tenant", dbName)
			return cfg, nil
		},
		embedderRegistry: map[string]embed.Embedder{embedConfigKey(cfg): specialEmb},
		dbConfigResolver: func(dbName string) (int, float64, string) { return 2, 0, "" },
	}

	chunks, embs, err := db.EmbedQueryChunksForDB(context.Background(), "tenant", "query")
	require.NoError(t, err)
	require.Equal(t, []string{"special-1", "special-2"}, chunks)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embs)
}

func TestDB_EmbedQueryChunksForDB_FallbackPaths(t *testing.T) {
	t.Run("registry enabled but no global queue returns nil result", func(t *testing.T) {
		db := &DB{
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				return &embed.Config{Provider: "ollama", Model: "m", Dimensions: 2}, nil
			},
		}

		chunks, embs, err := db.EmbedQueryChunksForDB(context.Background(), "tenant", "query")
		require.NoError(t, err)
		require.Nil(t, chunks)
		require.Nil(t, embs)
	})

	t.Run("global fallback enforces resolver dimensions", func(t *testing.T) {
		db := &DB{
			cypherExecutor: queryEmbeddingTestExecutor(t, &coverageQueryEmbedder{
				chunks: []string{"g1", "g2"},
				batch:  [][]float32{{1, 2, 3}, {4, 5, 6}},
			}),
			dbConfigResolver: func(dbName string) (int, float64, string) { return 2, 0, "" },
		}

		chunks, embs, err := db.EmbedQueryChunksForDB(context.Background(), "tenant", "query")
		require.ErrorIs(t, err, ErrQueryEmbeddingDimensionMismatch)
		require.Nil(t, chunks)
		require.Nil(t, embs)
	})
}
