package qdrantgrpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupSnapshotsTest(t *testing.T) (*SnapshotsService, *PointsService, *PersistentCollectionRegistry, string, func()) {
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)

	snapshotDir := t.TempDir()

	config := &Config{
		ListenAddr:           ":6334",
		AllowVectorMutations: true,
		MaxVectorDim:         4096,
		MaxBatchPoints:       1000,
		MaxTopK:              1000,
		SnapshotDir:          snapshotDir,
	}

	snapshotsService := NewSnapshotsService(config, store, registry, snapshotDir)
	pointsService := NewPointsService(config, store, registry, nil)

	// Create test collection
	ctx := context.Background()
	err = registry.CreateCollection(ctx, "test_collection", 4, pb.Distance_COSINE)
	require.NoError(t, err)

	// Add some test points
	_, err = pointsService.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: "test_collection",
		Points: []*pb.PointStruct{
			{
				Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}},
				Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}}}},
				Payload: map[string]*pb.Value{"name": {Kind: &pb.Value_StringValue{StringValue: "first"}}},
			},
			{
				Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}},
				Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{0, 1, 0, 0}}}},
				Payload: map[string]*pb.Value{"name": {Kind: &pb.Value_StringValue{StringValue: "second"}}},
			},
		},
	})
	require.NoError(t, err)

	cleanup := func() {
		store.Close()
	}

	return snapshotsService, pointsService, registry, snapshotDir, cleanup
}

func TestSnapshotsService_Create(t *testing.T) {
	service, _, _, snapshotDir, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("create snapshot successfully", func(t *testing.T) {
		resp, err := service.Create(ctx, &pb.CreateSnapshotRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Result.Name)
		assert.True(t, resp.Result.CreationTime > 0)
		assert.True(t, resp.Result.Size > 0)

		// Verify file exists
		snapshotPath := filepath.Join(snapshotDir, "collections", "test_collection", resp.Result.Name)
		_, err = os.Stat(snapshotPath)
		assert.NoError(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.Create(ctx, &pb.CreateSnapshotRequest{})
		assert.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		_, err := service.Create(ctx, &pb.CreateSnapshotRequest{
			CollectionName: "non_existent",
		})
		assert.Error(t, err)
	})
}

func TestSnapshotsService_List(t *testing.T) {
	service, _, _, _, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("list empty snapshots", func(t *testing.T) {
		resp, err := service.List(ctx, &pb.ListSnapshotsRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)
		assert.Empty(t, resp.Snapshots)
	})

	t.Run("list multiple snapshots", func(t *testing.T) {
		// Create a few snapshots
		_, err := service.Create(ctx, &pb.CreateSnapshotRequest{CollectionName: "test_collection"})
		require.NoError(t, err)
		_, err = service.Create(ctx, &pb.CreateSnapshotRequest{CollectionName: "test_collection"})
		require.NoError(t, err)

		resp, err := service.List(ctx, &pb.ListSnapshotsRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)
		assert.Len(t, resp.Snapshots, 2)

		// Should be sorted by creation time descending
		assert.True(t, resp.Snapshots[0].CreationTime >= resp.Snapshots[1].CreationTime)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.List(ctx, &pb.ListSnapshotsRequest{})
		assert.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		_, err := service.List(ctx, &pb.ListSnapshotsRequest{
			CollectionName: "non_existent",
		})
		assert.Error(t, err)
	})
}

func TestSnapshotsService_Delete(t *testing.T) {
	service, _, _, _, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("delete snapshot successfully", func(t *testing.T) {
		// Create a snapshot first
		createResp, err := service.Create(ctx, &pb.CreateSnapshotRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)

		// Delete it
		resp, err := service.Delete(ctx, &pb.DeleteSnapshotRequest{
			CollectionName: "test_collection",
			SnapshotName:   createResp.Result.Name,
		})
		require.NoError(t, err)
		assert.True(t, resp.Result)

		// Verify it's gone
		listResp, err := service.List(ctx, &pb.ListSnapshotsRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)
		assert.Empty(t, listResp.Snapshots)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.Delete(ctx, &pb.DeleteSnapshotRequest{
			SnapshotName: "some-snapshot",
		})
		assert.Error(t, err)
	})

	t.Run("error on empty snapshot name", func(t *testing.T) {
		_, err := service.Delete(ctx, &pb.DeleteSnapshotRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})

	t.Run("error on non-existent snapshot", func(t *testing.T) {
		_, err := service.Delete(ctx, &pb.DeleteSnapshotRequest{
			CollectionName: "test_collection",
			SnapshotName:   "non-existent.snapshot",
		})
		assert.Error(t, err)
	})
}

func TestSnapshotsService_CreateFull(t *testing.T) {
	service, _, _, snapshotDir, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("create full snapshot successfully", func(t *testing.T) {
		resp, err := service.CreateFull(ctx, &pb.CreateFullSnapshotRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Result.Name)
		assert.True(t, resp.Result.CreationTime > 0)
		assert.True(t, resp.Result.Size > 0)

		// Verify file exists
		snapshotPath := filepath.Join(snapshotDir, "full", resp.Result.Name)
		_, err = os.Stat(snapshotPath)
		assert.NoError(t, err)
	})
}

func TestSnapshotsService_ListFull(t *testing.T) {
	service, _, _, _, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("list empty full snapshots", func(t *testing.T) {
		resp, err := service.ListFull(ctx, &pb.ListFullSnapshotsRequest{})
		require.NoError(t, err)
		assert.Empty(t, resp.Snapshots)
	})

	t.Run("list multiple full snapshots", func(t *testing.T) {
		// Create a few full snapshots
		_, err := service.CreateFull(ctx, &pb.CreateFullSnapshotRequest{})
		require.NoError(t, err)
		_, err = service.CreateFull(ctx, &pb.CreateFullSnapshotRequest{})
		require.NoError(t, err)

		resp, err := service.ListFull(ctx, &pb.ListFullSnapshotsRequest{})
		require.NoError(t, err)
		assert.Len(t, resp.Snapshots, 2)
	})
}

func TestSnapshotsService_DeleteFull(t *testing.T) {
	service, _, _, _, cleanup := setupSnapshotsTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("delete full snapshot successfully", func(t *testing.T) {
		// Create a full snapshot first
		createResp, err := service.CreateFull(ctx, &pb.CreateFullSnapshotRequest{})
		require.NoError(t, err)

		// Delete it
		resp, err := service.DeleteFull(ctx, &pb.DeleteFullSnapshotRequest{
			SnapshotName: createResp.Result.Name,
		})
		require.NoError(t, err)
		assert.True(t, resp.Result)

		// Verify it's gone
		listResp, err := service.ListFull(ctx, &pb.ListFullSnapshotsRequest{})
		require.NoError(t, err)
		assert.Empty(t, listResp.Snapshots)
	})

	t.Run("error on empty snapshot name", func(t *testing.T) {
		_, err := service.DeleteFull(ctx, &pb.DeleteFullSnapshotRequest{})
		assert.Error(t, err)
	})

	t.Run("error on non-existent snapshot", func(t *testing.T) {
		_, err := service.DeleteFull(ctx, &pb.DeleteFullSnapshotRequest{
			SnapshotName: "non-existent.snapshot",
		})
		assert.Error(t, err)
	})
}
