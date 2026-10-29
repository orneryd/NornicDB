package qdrantgrpc

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/grpcresponse"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// SnapshotsService implements the Qdrant Snapshots gRPC service.
// It maps to NornicDB's storage.Snapshot and BadgerEngine.Backup functionality.
type SnapshotsService struct {
	qpb.UnimplementedSnapshotsServer
	config      *Config
	collections CollectionStore
	baseStorage storage.Engine
	snapshotDir string
	checker     DatabaseAccessChecker // optional; when set, enforces per-database (per-collection) RBAC
}

// NewSnapshotsService creates a new Snapshots service.
// snapshotDir is the directory where snapshots will be stored.
func NewSnapshotsService(config *Config, collections CollectionStore, baseStorage storage.Engine, snapshotDir string, checker DatabaseAccessChecker) *SnapshotsService {
	if snapshotDir == "" {
		snapshotDir = "./data/qdrant-snapshots"
	}
	// Ensure directory exists
	os.MkdirAll(snapshotDir, 0755)

	return &SnapshotsService{
		config:      config,
		collections: collections,
		baseStorage: baseStorage,
		snapshotDir: snapshotDir,
		checker:     checker,
	}
}

func (s *SnapshotsService) allowAccess(ctx context.Context, collectionName string, write bool) error {
	if s.checker == nil {
		return nil
	}
	return s.checker.AllowDatabaseAccess(ctx, collectionName, write)
}

func (s *SnapshotsService) allowFullAccess(ctx context.Context) error {
	if s.checker == nil {
		return nil
	}
	if checker, ok := s.checker.(interface{ AllowAllDatabaseAccess(context.Context) error }); ok {
		return checker.AllowAllDatabaseAccess(ctx)
	}
	return grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.PermissionDenied, localization.QdrantPermissionDenied(), grpcresponse.QdrantSourceFallback)
}

// Create creates a new snapshot of a collection.
// Maps to: Export collection nodes as JSON snapshot
func (s *SnapshotsService) Create(ctx context.Context, req *qpb.CreateSnapshotRequest) (*qpb.CreateSnapshotResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired(), grpcresponse.QdrantSourceFallback)
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), false); err != nil {
		return nil, err
	}

	// Verify collection exists
	if s.collections == nil || !s.collections.Exists(req.CollectionName) {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantCollectionNotFound(req.CollectionName), grpcresponse.QdrantSourceFallback)
	}

	// Create snapshot directory for this collection
	collectionSnapshotDir := filepath.Join(s.snapshotDir, "collections", req.CollectionName)
	if err := os.MkdirAll(collectionSnapshotDir, 0755); err != nil {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotDirectoryCreateFailed(err), grpcresponse.QdrantSourceFallback)
	}

	// Generate snapshot name with timestamp
	timestamp := time.Now().UTC()
	snapshotName := fmt.Sprintf("%s-%d.snapshot", req.CollectionName, timestamp.UnixNano())
	snapshotPath := filepath.Join(collectionSnapshotDir, snapshotName)

	store, _, err := s.collections.Open(ctx, req.CollectionName)
	if err != nil {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantCollectionNotFound(req.CollectionName), grpcresponse.QdrantSourceFallback)
	}
	// A snapshot holds every stored point, those the visibility layer hides
	// from reads included.
	nodes, err := storage.GetStoredNodesByLabel(store, QdrantPointLabel)
	if err != nil && err != storage.ErrNotFound {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantGetCollectionPointsFailed(err), grpcresponse.QdrantSourceFallback)
	}

	var qdrantPoints []*storage.Node
	for _, node := range nodes {
		if isQdrantPointNode(node) {
			qdrantPoints = append(qdrantPoints, node)
		}
	}

	// Create snapshot using NornicDB's snapshot format
	snapshot := &storage.Snapshot{
		Sequence:  uint64(timestamp.UnixNano()),
		Timestamp: timestamp,
		Nodes:     qdrantPoints,
		Edges:     nil, // Qdrant collections don't have edges
		Version:   "qdrant-compat-1.0",
	}

	// Save snapshot
	if err := storage.SaveSnapshot(snapshot, snapshotPath); err != nil {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotSaveFailed(err), grpcresponse.QdrantSourceFallback)
	}

	// Get file size
	fileInfo, _ := os.Stat(snapshotPath)
	size := int64(0)
	if fileInfo != nil {
		size = fileInfo.Size()
	}

	return &qpb.CreateSnapshotResponse{
		SnapshotDescription: &qpb.SnapshotDescription{
			Name:         snapshotName,
			CreationTime: timestamppb.New(timestamp),
			Size:         size,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// List lists all snapshots for a collection.
// Maps to: List files in collection snapshot directory
func (s *SnapshotsService) List(ctx context.Context, req *qpb.ListSnapshotsRequest) (*qpb.ListSnapshotsResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired(), grpcresponse.QdrantSourceFallback)
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), false); err != nil {
		return nil, err
	}

	// Verify collection exists
	if s.collections == nil || !s.collections.Exists(req.CollectionName) {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantCollectionNotFound(req.CollectionName), grpcresponse.QdrantSourceFallback)
	}

	// List snapshots in collection directory
	collectionSnapshotDir := filepath.Join(s.snapshotDir, "collections", req.CollectionName)
	entries, err := os.ReadDir(collectionSnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshots yet
			return &qpb.ListSnapshotsResponse{
				SnapshotDescriptions: []*qpb.SnapshotDescription{},
				Time:                 time.Since(start).Seconds(),
			}, nil
		}
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotListFailed(err), grpcresponse.QdrantSourceFallback)
	}

	// Build snapshot list
	var snapshots []*qpb.SnapshotDescription
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snapshot") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		snapshots = append(snapshots, &qpb.SnapshotDescription{
			Name:         entry.Name(),
			CreationTime: timestamppb.New(info.ModTime().UTC()),
			Size:         info.Size(),
		})
	}

	// Sort by creation time descending (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		ai := snapshots[i].CreationTime
		aj := snapshots[j].CreationTime
		if ai == nil || aj == nil {
			return snapshots[i].Name > snapshots[j].Name
		}
		return ai.AsTime().After(aj.AsTime())
	})

	return &qpb.ListSnapshotsResponse{
		SnapshotDescriptions: snapshots,
		Time:                 time.Since(start).Seconds(),
	}, nil
}

// Delete removes a snapshot.
// Maps to: Delete snapshot file
func (s *SnapshotsService) Delete(ctx context.Context, req *qpb.DeleteSnapshotRequest) (*qpb.DeleteSnapshotResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.InvalidArgument, localization.QdrantCollectionNameRequired(), grpcresponse.QdrantSourceFallback)
	}
	if err := s.allowAccess(ctx, req.GetCollectionName(), true); err != nil {
		return nil, err
	}

	if req.GetSnapshotName() == "" {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.InvalidArgument, localization.QdrantFieldRequired("snapshot_name"), grpcresponse.QdrantSourceFallback)
	}

	// Verify collection exists
	if s.collections == nil || !s.collections.Exists(req.CollectionName) {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantCollectionNotFound(req.CollectionName), grpcresponse.QdrantSourceFallback)
	}

	// Delete snapshot file
	snapshotPath := filepath.Join(s.snapshotDir, "collections", req.CollectionName, req.SnapshotName)
	if err := os.Remove(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantSnapshotNotFound(req.SnapshotName), grpcresponse.QdrantSourceFallback)
		}
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotDeleteFailed(err), grpcresponse.QdrantSourceFallback)
	}

	return &qpb.DeleteSnapshotResponse{
		Time: time.Since(start).Seconds(),
	}, nil
}

// CreateFull creates a full storage snapshot (all collections).
// Maps to: BadgerEngine.Backup or WAL.CreateSnapshot for all data
func (s *SnapshotsService) CreateFull(ctx context.Context, req *qpb.CreateFullSnapshotRequest) (*qpb.CreateSnapshotResponse, error) {
	if err := s.allowFullAccess(ctx); err != nil {
		return nil, err
	}
	start := time.Now()

	if s.baseStorage == nil {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.FailedPrecondition, localization.QdrantFullSnapshotsRequireBaseStorage(), grpcresponse.QdrantSourceFallback)
	}

	// Create full snapshots directory
	fullSnapshotDir := filepath.Join(s.snapshotDir, "full")
	if err := os.MkdirAll(fullSnapshotDir, 0755); err != nil {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotDirectoryCreateFailed(err), grpcresponse.QdrantSourceFallback)
	}

	// Generate snapshot name with timestamp
	timestamp := time.Now().UTC()
	snapshotName := fmt.Sprintf("full-%d.snapshot", timestamp.UnixNano())
	snapshotPath := filepath.Join(fullSnapshotDir, snapshotName)

	// Try to use BadgerEngine.Backup if available.
	if badger, ok := storage.OwnedCapability[interface{ Backup(string) error }](s.baseStorage); ok {
		if err := badger.Backup(snapshotPath); err != nil {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantCreateBackupFailed(err), grpcresponse.QdrantSourceFallback)
		}
	} else {
		// Fallback: export all nodes and edges as a snapshot, those the
		// visibility layer hides from reads included.
		nodes, err := storage.CollectStoredNodes(ctx, s.baseStorage)
		if err != nil {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantGetNodesFailed(err), grpcresponse.QdrantSourceFallback)
		}

		edges, err := storage.CollectStoredEdges(ctx, s.baseStorage)
		if err != nil {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantGetEdgesFailed(err), grpcresponse.QdrantSourceFallback)
		}

		snapshot := &storage.Snapshot{
			Sequence:  uint64(timestamp.UnixNano()),
			Timestamp: timestamp,
			Nodes:     nodes,
			Edges:     edges,
			Version:   "qdrant-compat-full-1.0",
		}

		if err := storage.SaveSnapshot(snapshot, snapshotPath); err != nil {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotSaveFailed(err), grpcresponse.QdrantSourceFallback)
		}
	}

	// Get file size
	fileInfo, _ := os.Stat(snapshotPath)
	size := int64(0)
	if fileInfo != nil {
		size = fileInfo.Size()
	}

	return &qpb.CreateSnapshotResponse{
		SnapshotDescription: &qpb.SnapshotDescription{
			Name:         snapshotName,
			CreationTime: timestamppb.New(timestamp),
			Size:         size,
		},
		Time: time.Since(start).Seconds(),
	}, nil
}

// ListFull lists all full storage snapshots.
// Maps to: List files in full snapshot directory
func (s *SnapshotsService) ListFull(ctx context.Context, req *qpb.ListFullSnapshotsRequest) (*qpb.ListSnapshotsResponse, error) {
	if err := s.allowFullAccess(ctx); err != nil {
		return nil, err
	}
	start := time.Now()

	// List snapshots in full directory
	fullSnapshotDir := filepath.Join(s.snapshotDir, "full")
	entries, err := os.ReadDir(fullSnapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshots yet
			return &qpb.ListSnapshotsResponse{
				SnapshotDescriptions: []*qpb.SnapshotDescription{},
				Time:                 time.Since(start).Seconds(),
			}, nil
		}
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotListFailed(err), grpcresponse.QdrantSourceFallback)
	}

	// Build snapshot list
	var snapshots []*qpb.SnapshotDescription
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		snapshots = append(snapshots, &qpb.SnapshotDescription{
			Name:         entry.Name(),
			CreationTime: timestamppb.New(info.ModTime().UTC()),
			Size:         info.Size(),
		})
	}

	// Sort by creation time descending (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		ai := snapshots[i].CreationTime
		aj := snapshots[j].CreationTime
		if ai == nil || aj == nil {
			return snapshots[i].Name > snapshots[j].Name
		}
		return ai.AsTime().After(aj.AsTime())
	})

	return &qpb.ListSnapshotsResponse{
		SnapshotDescriptions: snapshots,
		Time:                 time.Since(start).Seconds(),
	}, nil
}

// DeleteFull removes a full storage snapshot.
// Maps to: Delete full snapshot file
func (s *SnapshotsService) DeleteFull(ctx context.Context, req *qpb.DeleteFullSnapshotRequest) (*qpb.DeleteSnapshotResponse, error) {
	if err := s.allowFullAccess(ctx); err != nil {
		return nil, err
	}
	start := time.Now()

	if req.GetSnapshotName() == "" {
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.InvalidArgument, localization.QdrantFieldRequired("snapshot_name"), grpcresponse.QdrantSourceFallback)
	}

	// Delete snapshot file
	snapshotPath := filepath.Join(s.snapshotDir, "full", req.SnapshotName)
	if err := os.Remove(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.NotFound, localization.QdrantSnapshotNotFound(req.SnapshotName), grpcresponse.QdrantSourceFallback)
		}
		return nil, grpcresponse.LocalizedStatus(ctx, s.config.Localizer, codes.Internal, localization.QdrantSnapshotDeleteFailed(err), grpcresponse.QdrantSourceFallback)
	}

	return &qpb.DeleteSnapshotResponse{
		Time: time.Since(start).Seconds(),
	}, nil
}
