package qdrantgrpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
)

// CollectionsService implements the Qdrant Collections gRPC service.
type CollectionsService struct {
	pb.UnimplementedCollectionsServer
	registry CollectionRegistry
}

// NewCollectionsService creates a new Collections service.
func NewCollectionsService(registry CollectionRegistry) *CollectionsService {
	return &CollectionsService{
		registry: registry,
	}
}

// CreateCollection creates a new vector collection.
func (s *CollectionsService) CreateCollection(ctx context.Context, req *pb.CreateCollectionRequest) (*pb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Extract vector config
	var dims int
	var distance pb.Distance

	if req.VectorsConfig == nil {
		return nil, status.Error(codes.InvalidArgument, "vectors_config is required")
	}

	switch cfg := req.VectorsConfig.Config.(type) {
	case *pb.VectorsConfig_Params:
		dims = int(cfg.Params.Size)
		distance = cfg.Params.Distance
	case *pb.VectorsConfig_ParamsMap:
		// Multi-vector: use first entry for now
		for _, params := range cfg.ParamsMap.Map {
			dims = int(params.Size)
			distance = params.Distance
			break
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid vectors_config")
	}

	if dims <= 0 {
		return nil, status.Error(codes.InvalidArgument, "vector size must be positive")
	}

	// Create collection
	if err := s.registry.CreateCollection(ctx, req.CollectionName, dims, distance); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "failed to create collection: %v", err)
	}

	return &pb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// GetCollectionInfo returns information about a collection.
func (s *CollectionsService) GetCollectionInfo(ctx context.Context, req *pb.GetCollectionInfoRequest) (*pb.GetCollectionInfoResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	meta, err := s.registry.GetCollection(ctx, req.CollectionName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "collection not found: %v", err)
	}

	// Get point count
	var pointsCount uint64
	if count, err := s.registry.GetPointCount(ctx, req.CollectionName); err == nil {
		pointsCount = uint64(count)
	}

	return &pb.GetCollectionInfoResponse{
		Result: &pb.CollectionInfo{
			Status:       meta.Status,
			VectorsCount: pointsCount,
			PointsCount:  pointsCount,
			Config: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     uint64(meta.Dimensions),
						Distance: meta.Distance,
					},
				},
			},
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// ListCollections lists all collections.
func (s *CollectionsService) ListCollections(ctx context.Context, req *pb.ListCollectionsRequest) (*pb.ListCollectionsResponse, error) {
	start := time.Now()

	names, err := s.registry.ListCollections(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list collections: %v", err)
	}

	collections := make([]*pb.CollectionDescription, len(names))
	for i, name := range names {
		collections[i] = &pb.CollectionDescription{Name: name}
	}

	return &pb.ListCollectionsResponse{
		Collections: collections,
		Time:        time.Since(start).Seconds(),
	}, nil
}

// DeleteCollection deletes a collection.
func (s *CollectionsService) DeleteCollection(ctx context.Context, req *pb.DeleteCollectionRequest) (*pb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if err := s.registry.DeleteCollection(ctx, req.CollectionName); err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to delete collection: %v", err)
	}

	return &pb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// UpdateCollection updates collection parameters.
// NornicDB manages HNSW parameters internally, so this is a no-op that validates the collection exists.
func (s *CollectionsService) UpdateCollection(ctx context.Context, req *pb.UpdateCollectionRequest) (*pb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// NornicDB manages HNSW/optimizer parameters automatically
	// This call succeeds if the collection exists

	return &pb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// CollectionExists checks if a collection exists.
func (s *CollectionsService) CollectionExists(ctx context.Context, req *pb.CollectionExistsRequest) (*pb.CollectionExistsResponse, error) {
	start := time.Now()

	if req.CollectionName == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	exists := s.registry.CollectionExists(req.CollectionName)

	return &pb.CollectionExistsResponse{
		Result: exists,
		Time:   time.Since(start).Seconds(),
	}, nil
}

