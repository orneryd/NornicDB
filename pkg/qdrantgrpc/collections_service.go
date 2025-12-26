package qdrantgrpc

import (
	"context"
	"time"

	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// CollectionsService implements the Qdrant Collections gRPC service.
type CollectionsService struct {
	qpb.UnimplementedCollectionsServer
	registry      CollectionRegistry
	storage       storage.Engine
	searchService *search.Service
	vecIndex      *vectorIndexCache
}

// NewCollectionsService creates a new Collections service.
func NewCollectionsService(registry CollectionRegistry, store storage.Engine, searchService *search.Service, vecIndex *vectorIndexCache) *CollectionsService {
	return &CollectionsService{
		registry:      registry,
		storage:       store,
		searchService: searchService,
		vecIndex:      vecIndex,
	}
}

func (s *CollectionsService) Create(ctx context.Context, req *qpb.CreateCollection) (*qpb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Extract vector config
	var dims int
	var distance qpb.Distance

	if req.VectorsConfig == nil {
		return nil, status.Error(codes.InvalidArgument, "vectors_config is required")
	}

	switch cfg := req.VectorsConfig.Config.(type) {
	case *qpb.VectorsConfig_Params:
		dims = int(cfg.Params.Size)
		distance = cfg.Params.Distance
	case *qpb.VectorsConfig_ParamsMap:
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

	return &qpb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *CollectionsService) Get(ctx context.Context, req *qpb.GetCollectionInfoRequest) (*qpb.GetCollectionInfoResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
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

	hnswM := uint64(16)
	hnswEfConstruct := uint64(100)
	hnswFullScanThreshold := uint64(10_000)

	optimizerDeletedThreshold := float64(0.2)
	optimizerVacuumMinVectorNumber := uint64(1000)
	optimizerDefaultSegmentNumber := uint64(1)
	optimizerFlushIntervalSec := uint64(5)

	walCapacityMb := uint64(32)
	walSegmentsAhead := uint64(0)
	walRetainClosed := uint64(0)

	return &qpb.GetCollectionInfoResponse{
		Result: &qpb.CollectionInfo{
			Status:              qpb.CollectionStatus_Green,
			PointsCount:         &pointsCount,
			IndexedVectorsCount: &pointsCount,
			Config: &qpb.CollectionConfig{
				Params: &qpb.CollectionParams{
					VectorsConfig: &qpb.VectorsConfig{
						Config: &qpb.VectorsConfig_Params{
							Params: &qpb.VectorParams{
								Size:     uint64(meta.Dimensions),
								Distance: meta.Distance,
							},
						},
					},
				},
				HnswConfig: &qpb.HnswConfigDiff{
					M:                 &hnswM,
					EfConstruct:       &hnswEfConstruct,
					FullScanThreshold: &hnswFullScanThreshold,
				},
				OptimizerConfig: &qpb.OptimizersConfigDiff{
					DeletedThreshold:      &optimizerDeletedThreshold,
					VacuumMinVectorNumber: &optimizerVacuumMinVectorNumber,
					DefaultSegmentNumber:  &optimizerDefaultSegmentNumber,
					FlushIntervalSec:      &optimizerFlushIntervalSec,
				},
				WalConfig: &qpb.WalConfigDiff{
					WalCapacityMb:    &walCapacityMb,
					WalSegmentsAhead: &walSegmentsAhead,
					WalRetainClosed:  &walRetainClosed,
				},
			},
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

func (s *CollectionsService) List(ctx context.Context, req *qpb.ListCollectionsRequest) (*qpb.ListCollectionsResponse, error) {
	start := time.Now()

	names, err := s.registry.ListCollections(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list collections: %v", err)
	}

	collections := make([]*qpb.CollectionDescription, len(names))
	for i, name := range names {
		collections[i] = &qpb.CollectionDescription{Name: name}
	}

	return &qpb.ListCollectionsResponse{
		Collections: collections,
		Time:        time.Since(start).Seconds(),
	}, nil
}

func (s *CollectionsService) Delete(ctx context.Context, req *qpb.DeleteCollection) (*qpb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if err := s.registry.DeleteCollection(ctx, req.CollectionName); err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to delete collection: %v", err)
	}

	// Best-effort: remove points and search index entries for the collection.
	// This is required for correctness/compatibility: in Qdrant, deleting a
	// collection removes its points.
	if s.storage != nil {
		_ = deleteCollectionPoints(ctx, s.storage, s.searchService, req.CollectionName)
	}
	if s.vecIndex != nil {
		s.vecIndex.deleteCollection(req.CollectionName)
	}

	return &qpb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

// Update acknowledges the update request if the collection exists.
// NornicDB manages tuning parameters internally.
func (s *CollectionsService) Update(ctx context.Context, req *qpb.UpdateCollection) (*qpb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// NornicDB manages HNSW/optimizer parameters automatically
	// This call succeeds if the collection exists

	return &qpb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *CollectionsService) CollectionExists(ctx context.Context, req *qpb.CollectionExistsRequest) (*qpb.CollectionExistsResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	exists := s.registry.CollectionExists(req.CollectionName)

	return &qpb.CollectionExistsResponse{
		Result: &qpb.CollectionExists{Exists: exists},
		Time:   time.Since(start).Seconds(),
	}, nil
}
