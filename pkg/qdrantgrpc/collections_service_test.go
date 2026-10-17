package qdrantgrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type denyCollectionChecker struct{}

func (denyCollectionChecker) AllowDatabaseAccess(ctx context.Context, database string, write bool) error {
	_ = ctx
	_ = database
	_ = write
	return errors.New("denied")
}

func (denyCollectionChecker) VisibleDatabases(ctx context.Context, candidates []string) ([]string, error) {
	_ = ctx
	return []string{}, nil
}

func newTestCollectionStore(t *testing.T) CollectionStore {
	t.Helper()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	store, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	return store
}

func TestCollectionsService_Create(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	t.Run("create collection successfully", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "test_collection",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		resp, err := service.Create(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)
		assert.Greater(t, resp.Time, float64(0))
	})

	t.Run("error on duplicate collection", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "test_collection",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on missing vectors config", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "no_config",
			VectorsConfig:  nil,
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on zero dimensions", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "zero_dim",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     0,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_Get(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	// Create a test collection first
	require.NoError(t, collections.Create(ctx, "my_collection", 512, qpb.Distance_Dot))

	t.Run("get existing collection", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "my_collection",
		}

		resp, err := service.Get(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.Result)
		assert.Equal(t, qpb.CollectionStatus_Green, resp.Result.Status)
		require.NotNil(t, resp.Result.PointsCount)
		assert.Equal(t, uint64(0), *resp.Result.PointsCount)

		// Check config
		require.NotNil(t, resp.Result.Config)
		params := resp.Result.Config.GetParams()
		require.NotNil(t, params)
		require.NotNil(t, params.VectorsConfig)
		require.NotNil(t, params.VectorsConfig.GetParams())
		assert.Equal(t, uint64(512), params.VectorsConfig.GetParams().Size)
		assert.Equal(t, qpb.Distance_Dot, params.VectorsConfig.GetParams().Distance)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "not_found",
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "",
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_List(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	t.Run("list empty collections", func(t *testing.T) {
		req := &qpb.ListCollectionsRequest{}

		resp, err := service.List(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, resp.Collections)
	})

	t.Run("list multiple collections", func(t *testing.T) {
		// Create some collections
		require.NoError(t, collections.Create(ctx, "collection_a", 128, qpb.Distance_Cosine))
		require.NoError(t, collections.Create(ctx, "collection_b", 256, qpb.Distance_Euclid))
		require.NoError(t, collections.Create(ctx, "collection_c", 512, qpb.Distance_Dot))

		req := &qpb.ListCollectionsRequest{}

		resp, err := service.List(ctx, req)
		require.NoError(t, err)
		assert.Len(t, resp.Collections, 3)

		// Check all names are present
		names := make(map[string]bool)
		for _, c := range resp.Collections {
			names[c.Name] = true
		}
		assert.True(t, names["collection_a"])
		assert.True(t, names["collection_b"])
		assert.True(t, names["collection_c"])
	})
}

func TestCollectionsService_Delete(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	// Create a collection first
	require.NoError(t, collections.Create(ctx, "to_delete", 128, qpb.Distance_Cosine))

	t.Run("delete existing collection", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "to_delete",
		}

		resp, err := service.Delete(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)

		// Verify it's gone
		_, err = collections.GetMeta(ctx, "to_delete")
		require.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "not_found",
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "",
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_UpdateAndExists(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	require.NoError(t, collections.Create(ctx, "existing_collection", 128, qpb.Distance_Cosine))

	t.Run("update existing collection", func(t *testing.T) {
		resp, err := service.Update(ctx, &qpb.UpdateCollection{
			CollectionName: "existing_collection",
		})
		require.NoError(t, err)
		require.True(t, resp.Result)
	})

	t.Run("update missing collection", func(t *testing.T) {
		_, err := service.Update(ctx, &qpb.UpdateCollection{
			CollectionName: "missing_collection",
		})
		require.Error(t, err)
	})

	t.Run("update invalid request", func(t *testing.T) {
		_, err := service.Update(ctx, &qpb.UpdateCollection{
			CollectionName: "",
		})
		require.Error(t, err)
	})

	t.Run("collection exists true", func(t *testing.T) {
		resp, err := service.CollectionExists(ctx, &qpb.CollectionExistsRequest{
			CollectionName: "existing_collection",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Result)
		require.True(t, resp.Result.Exists)
	})

	t.Run("collection exists false", func(t *testing.T) {
		resp, err := service.CollectionExists(ctx, &qpb.CollectionExistsRequest{
			CollectionName: "missing_collection",
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Result)
		require.False(t, resp.Result.Exists)
	})

	t.Run("collection exists invalid request", func(t *testing.T) {
		_, err := service.CollectionExists(ctx, &qpb.CollectionExistsRequest{
			CollectionName: "",
		})
		require.Error(t, err)
	})
}

func TestCollectionStore_PointCountAndNameValidation(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)

	require.NoError(t, collections.Create(ctx, "count_collection", 4, qpb.Distance_Cosine))
	store, _, err := collections.Open(ctx, "count_collection")
	require.NoError(t, err)

	// Count only Qdrant point nodes.
	_, err = store.CreateNode(&storage.Node{
		ID:     "qdrant:point:1",
		Labels: []string{QdrantPointLabel},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{
		ID:     "qdrant:point:2",
		Labels: []string{"Other"},
	})
	require.NoError(t, err)

	count, err := collections.PointCount(ctx, "count_collection")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	_, err = collections.PointCount(ctx, "missing")
	require.Error(t, err)

	reserved := map[string]struct{}{
		"system": {},
	}
	require.Error(t, validateCollectionName("", reserved))
	require.Error(t, validateCollectionName("a:b", reserved))
	require.Error(t, validateCollectionName("_hidden", reserved))
	require.Error(t, validateCollectionName("system", reserved))
	require.NoError(t, validateCollectionName("valid_name", reserved))
}

func TestCollectionsService_AccessCheckerPaths(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	require.NoError(t, collections.Create(ctx, "visible_a", 4, qpb.Distance_Cosine))
	require.NoError(t, collections.Create(ctx, "visible_b", 4, qpb.Distance_Cosine))

	service := NewCollectionsService(collections, denyCollectionChecker{})

	_, err := service.Create(ctx, &qpb.CreateCollection{
		CollectionName: "deny_create",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.Error(t, err)

	_, err = service.Get(ctx, &qpb.GetCollectionInfoRequest{CollectionName: "visible_a"})
	require.Error(t, err)

	resp, err := service.List(ctx, &qpb.ListCollectionsRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Collections)
}

func TestCollectionStore_MetaAndOpenBranches(t *testing.T) {
	t.Parallel()

	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	storeI, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	store := storeI.(*databaseCollectionStore)
	ctx := context.Background()

	_, _, err = store.Open(ctx, "")
	require.ErrorIs(t, err, ErrCollectionNotFound)

	require.Error(t, store.Create(ctx, "system", 4, qpb.Distance_Cosine))
	require.Error(t, store.Create(ctx, "_bad", 4, qpb.Distance_Cosine))
	require.Error(t, store.Create(ctx, "bad:name", 4, qpb.Distance_Cosine))
	require.Error(t, store.Create(ctx, "bad_dim", 0, qpb.Distance_Cosine))

	require.NoError(t, dbm.CreateDatabase("plain_db"))
	_, _, err = store.Open(ctx, "plain_db")
	require.ErrorIs(t, err, ErrCollectionNotFound)

	require.NoError(t, store.Create(ctx, "meta_db", 4, qpb.Distance_Cosine))
	engine, meta, err := store.Open(ctx, "meta_db")
	require.NoError(t, err)
	require.Equal(t, "meta_db", meta.Name)
	require.NotNil(t, engine)

	// Corrupt metadata label to exercise invalid contract branch.
	node, err := engine.GetNode(collectionMetaNodeID)
	require.NoError(t, err)
	node.Labels = []string{"WrongLabel"}
	require.NoError(t, engine.UpdateNode(node))
	_, _, err = store.Open(ctx, "meta_db")
	require.ErrorIs(t, err, ErrCollectionNotFound)

	// readCollectionMeta direct branches.
	require.NoError(t, dbm.CreateDatabase("meta_direct"))
	mem, err := dbm.GetStorage("meta_direct")
	require.NoError(t, err)
	_, err = mem.CreateNode(&storage.Node{
		ID:     collectionMetaNodeID,
		Labels: []string{collectionMetaNodeLabel},
		Properties: map[string]any{
			"dimensions": 4,
			"distance":   "bad-type",
			"created_at": time.Now().Unix(),
		},
	})
	require.NoError(t, err)
	meta2, err := readCollectionMeta(mem)
	require.NoError(t, err)
	require.Equal(t, qpb.Distance_Cosine, meta2.Distance) // default fallback

	require.NoError(t, dbm.CreateDatabase("meta_empty"))
	emptyMetaDB, err := dbm.GetStorage("meta_empty")
	require.NoError(t, err)
	_, err = readCollectionMeta(emptyMetaDB)
	require.Error(t, err)

	require.NoError(t, dbm.CreateDatabase("meta_float_dist"))
	memFloatDist, err := dbm.GetStorage("meta_float_dist")
	require.NoError(t, err)
	_, err = memFloatDist.CreateNode(&storage.Node{
		ID:         collectionMetaNodeID,
		Labels:     []string{collectionMetaNodeLabel},
		Properties: map[string]any{"dimensions": int64(4), "distance": float64(qpb.Distance_Euclid)},
	})
	require.NoError(t, err)
	meta3, err := readCollectionMeta(memFloatDist)
	require.NoError(t, err)
	require.Equal(t, qpb.Distance_Euclid, meta3.Distance)

	require.NoError(t, dbm.CreateDatabase("meta_int_dist"))
	memIntDist, err := dbm.GetStorage("meta_int_dist")
	require.NoError(t, err)
	_, err = memIntDist.CreateNode(&storage.Node{
		ID:         collectionMetaNodeID,
		Labels:     []string{collectionMetaNodeLabel},
		Properties: map[string]any{"dimensions": int(4), "distance": int(qpb.Distance_Dot)},
	})
	require.NoError(t, err)
	meta4, err := readCollectionMeta(memIntDist)
	require.NoError(t, err)
	require.Equal(t, qpb.Distance_Dot, meta4.Distance)

	require.NoError(t, dbm.CreateDatabase("meta_bad_dims"))
	memBadDims, err := dbm.GetStorage("meta_bad_dims")
	require.NoError(t, err)
	_, err = memBadDims.CreateNode(&storage.Node{
		ID:         collectionMetaNodeID,
		Labels:     []string{collectionMetaNodeLabel},
		Properties: map[string]any{"dimensions": 0, "distance": int32(qpb.Distance_Dot)},
	})
	require.NoError(t, err)
	_, err = readCollectionMeta(memBadDims)
	require.ErrorIs(t, err, ErrInvalidCollection)
}

func TestCollectionsService_CreateAdditionalBranches(t *testing.T) {
	ctx := context.Background()
	collections := newTestCollectionStore(t)
	service := NewCollectionsService(collections, nil)

	// ParamsMap branch.
	_, err := service.Create(ctx, &qpb.CreateCollection{
		CollectionName: "params_map_collection",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_ParamsMap{
				ParamsMap: &qpb.VectorParamsMap{
					Map: map[string]*qpb.VectorParams{
						"v": {Size: 4, Distance: qpb.Distance_Dot},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Invalid vectors config branch.
	_, err = service.Create(ctx, &qpb.CreateCollection{
		CollectionName: "bad_cfg",
		VectorsConfig:  &qpb.VectorsConfig{},
	})
	require.Error(t, err)

	// Duplicate create path (CreateDatabase failure).
	_, err = service.Create(ctx, &qpb.CreateCollection{
		CollectionName: "params_map_collection",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.Error(t, err)

	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	storeNilVec, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	require.NoError(t, storeNilVec.Create(ctx, "nil_vec_store", 4, qpb.Distance_Cosine))
}

func TestCollectionStore_DropExistsPointCountBranches(t *testing.T) {
	ctx := context.Background()
	collectionsI := newTestCollectionStore(t)
	collections := collectionsI.(*databaseCollectionStore)

	require.Error(t, collections.Drop(ctx, ""))
	require.Error(t, collections.Drop(ctx, "missing"))

	require.NoError(t, collections.Create(ctx, "drop_invalid_meta", 4, qpb.Distance_Cosine))
	engine, _, err := collections.Open(ctx, "drop_invalid_meta")
	require.NoError(t, err)
	metaNode, err := engine.GetNode(collectionMetaNodeID)
	require.NoError(t, err)
	metaNode.Labels = []string{"bad"}
	require.NoError(t, engine.UpdateNode(metaNode))
	require.Error(t, collections.Drop(ctx, "drop_invalid_meta"))
	require.False(t, collections.Exists("drop_invalid_meta"))

	require.NoError(t, collections.Create(ctx, "count_empty", 4, qpb.Distance_Cosine))
	count, err := collections.PointCount(ctx, "count_empty")
	require.NoError(t, err)
	require.Equal(t, int64(0), count)
}
