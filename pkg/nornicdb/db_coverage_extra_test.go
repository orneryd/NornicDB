package nornicdb

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type unwrapOnlyEngine struct {
	storage.Engine
	inner storage.Engine
}

func (u unwrapOnlyEngine) GetInnerEngine() storage.Engine { return u.inner }

func TestDB_Coverage_ResolveSearchFlagsLazyFromConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.SearchBM25Warming = "lazy"
	cfg.Memory.SearchVectorWarming = "lazy"
	db := &DB{config: cfg}

	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags("tenant")
	require.True(t, bm25On)
	require.True(t, vectorOn)
	require.Equal(t, "lazy", bm25Warming)
	require.Equal(t, "lazy", vectorWarming)
}

func TestDB_Coverage_EmbeddingCountErrorPaths(t *testing.T) {
	t.Run("allDatabasesProvider ignores errored services", func(t *testing.T) {
		db := &DB{
			embeddingDims:       3,
			searchMinSimilarity: 0.1,
			searchServices:      map[string]*dbSearchService{},
			allDatabasesProvider: func() []DatabaseAndStorage {
				return []DatabaseAndStorage{
					{Name: "system"},
					{Name: "tenant"},
				}
			},
		}
		require.Zero(t, db.EmbeddingCount())
	})

	t.Run("default path returns zero when default db service cannot be created", func(t *testing.T) {
		base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { _ = base.Close() })
		db := &DB{
			storage:             storage.NewNamespacedEngine(base, "system"),
			embeddingDims:       3,
			searchMinSimilarity: 0.1,
			searchServices:      map[string]*dbSearchService{},
		}
		require.Zero(t, db.EmbeddingCount())
	})
}

func TestDB_Coverage_QueryChunkEmbeddingEdgeBranches(t *testing.T) {
	db := &DB{}

	chunks, embs, err := db.embedQueryChunksWithEmbedder(context.Background(), &coverageQueryEmbedder{}, "query")
	require.NoError(t, err)
	require.Nil(t, chunks)
	require.Nil(t, embs)

	embErr := errors.New("embed failed")
	chunks, embs, err = db.embedQueryChunksWithEmbedder(context.Background(), &coverageQueryEmbedder{
		chunks:   []string{"c1"},
		embedErr: embErr,
	}, "query")
	require.ErrorIs(t, err, embErr)
	require.Equal(t, []string{"c1"}, chunks)
	require.Nil(t, embs)

	chunks, embs, err = db.embedQueryChunksWithEmbedder(context.Background(), &coverageQueryEmbedder{
		chunks: []string{"c1"},
		embed:  []float32{},
	}, "query")
	require.ErrorContains(t, err, "empty vector", "an empty vector is a failed embedding")
	require.Equal(t, []string{"c1"}, chunks)
	require.Nil(t, embs)

	chunksOnly, err := db.chunkQueryWithEmbedder(context.Background(), nil, "query")
	require.NoError(t, err)
	require.Nil(t, chunksOnly)
}

func TestDB_Coverage_UnwrapToBadgerEngine_NonBadgerChain(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	wrapper := &unwrapOnlyEngine{Engine: base, inner: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
	t.Cleanup(func() { _ = wrapper.inner.Close() })
	require.Nil(t, unwrapToBadgerEngine(wrapper))
}

func TestDB_Coverage_MaybeEnableReplication_DefaultDataDirBranch(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })

	t.Setenv("NORNICDB_CLUSTER_MODE", "invalid-mode")
	t.Setenv("NORNICDB_CLUSTER_DATA_DIR", "")

	db := &DB{config: &Config{}}
	_, err := db.maybeEnableReplication(base)
	require.Error(t, err)
}
