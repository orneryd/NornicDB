package qdrantgrpc

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
)

// CollectionsService implements the Qdrant Collections gRPC service.
type CollectionsService struct {
	qpb.UnimplementedCollectionsServer
	collections CollectionStore
	vecIndex    *vectorIndexCache
	checker     DatabaseAccessChecker // optional; when set, enforces per-database (per-collection) RBAC
	localizer   *localization.Manager
}

// NewCollectionsService creates a new Collections service.
func NewCollectionsService(collections CollectionStore, vecIndex *vectorIndexCache, checker DatabaseAccessChecker) *CollectionsService {
	return &CollectionsService{
		collections: collections,
		vecIndex:    vecIndex,
		checker:     checker,
	}
}

func (s *CollectionsService) allowAccess(ctx context.Context, collectionName string, write bool) error {
	if s.checker == nil {
		return nil
	}
	return s.checker.AllowDatabaseAccess(ctx, collectionName, write)
}

func (s *CollectionsService) Create(ctx context.Context, req *qpb.CreateCollection) (*qpb.CollectionOperationResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired())
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), true); err != nil {
		return nil, err
	}

	// Extract vector config
	var dims int
	var distance qpb.Distance

	if req.VectorsConfig == nil {
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantFieldRequired("vectors_config"))
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
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantInvalidVectorsConfig())
	}

	if dims <= 0 {
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantVectorSizeMustBePositive())
	}

	// Create collection
	if err := s.collections.Create(ctx, req.CollectionName, dims, distance); err != nil {
		return nil, localizedStatus(ctx, s.localizer, codes.AlreadyExists, localization.QdrantCreateCollectionFailed(err))
	}

	return &qpb.CollectionOperationResponse{
		Result: true,
		Time:   time.Since(start).Seconds(),
	}, nil
}

func (s *CollectionsService) Get(ctx context.Context, req *qpb.GetCollectionInfoRequest) (*qpb.GetCollectionInfoResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired())
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), false); err != nil {
		return nil, err
	}

	meta, err := s.collections.GetMeta(ctx, req.CollectionName)
	if err != nil {
		return nil, localizedStatus(ctx, s.localizer, codes.NotFound, localization.QdrantCollectionNotFoundWithCause(err))
	}

	// Get point count
	var pointsCount uint64
	if count, err := s.collections.PointCount(ctx, req.CollectionName); err == nil {
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

	names, err := s.collections.List(ctx)
	if err != nil {
		return nil, localizedStatus(ctx, s.localizer, codes.Internal, localization.QdrantListCollectionsFailed(err))
	}
	if s.checker != nil {
		names, err = s.checker.VisibleDatabases(ctx, names)
		if err != nil {
			return nil, err
		}
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
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired())
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), true); err != nil {
		return nil, err
	}

	if err := s.collections.Drop(ctx, req.CollectionName); err != nil {
		return nil, localizedStatus(ctx, s.localizer, codes.NotFound, localization.QdrantDeleteCollectionFailed(err))
	}

	// Drop per-collection in-memory index cache (persistent storage already
	// removed by collections.Drop.
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
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired())
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), true); err != nil {
		return nil, err
	}

	// Verify collection exists
	if !s.collections.Exists(req.CollectionName) {
		return nil, localizedStatus(ctx, s.localizer, codes.NotFound, localization.QdrantCollectionNotFound(req.CollectionName))
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
		return nil, localizedStatus(ctx, s.localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired())
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), false); err != nil {
		return nil, err
	}

	exists := s.collections.Exists(req.CollectionName)

	return &qpb.CollectionExistsResponse{
		Result: &qpb.CollectionExists{Exists: exists},
		Time:   time.Since(start).Seconds(),
	}, nil
}
