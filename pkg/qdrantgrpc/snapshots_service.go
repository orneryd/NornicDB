package qdrantgrpc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// SnapshotsService implements the Qdrant Snapshots gRPC service.
// It maps to NornicDB's storage.Snapshot and BadgerEngine.Backup functionality.
type SnapshotsService struct {
	qpb.UnimplementedSnapshotsServer
	config      *Config
	storage     storage.Engine
	registry    CollectionRegistry
	snapshotDir string
}

// NewSnapshotsService creates a new Snapshots service.
// snapshotDir is the directory where snapshots will be stored.
func NewSnapshotsService(config *Config, store storage.Engine, registry CollectionRegistry, snapshotDir string) *SnapshotsService {
	if snapshotDir == "" {
		snapshotDir = "./data/qdrant-snapshots"
	}
	// Ensure directory exists
	os.MkdirAll(snapshotDir, 0755)

	return &SnapshotsService{
		config:      config,
		storage:     store,
		registry:    registry,
		snapshotDir: snapshotDir,
	}
}

// Create creates a new snapshot of a collection.
// Maps to: Export collection nodes as JSON snapshot
func (s *SnapshotsService) Create(ctx context.Context, req *qpb.CreateSnapshotRequest) (*qpb.CreateSnapshotResponse, error) {
	start := time.Now()

	if req.GetCollectionName() == "" {
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// Create snapshot directory for this collection
	collectionSnapshotDir := filepath.Join(s.snapshotDir, "collections", req.CollectionName)
	if err := os.MkdirAll(collectionSnapshotDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot directory: %v", err)
	}

	// Generate snapshot name with timestamp
	timestamp := time.Now().UTC()
	snapshotName := fmt.Sprintf("%s-%d.snapshot", req.CollectionName, timestamp.UnixNano())
	snapshotPath := filepath.Join(collectionSnapshotDir, snapshotName)

	// Get all points in the collection
	nodes, err := s.storage.GetNodesByLabel(req.CollectionName)
	if err != nil && err != storage.ErrNotFound {
		return nil, status.Errorf(codes.Internal, "failed to get collection points: %v", err)
	}

	// Filter to only Qdrant points
	var qdrantPoints []*storage.Node
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				qdrantPoints = append(qdrantPoints, node)
				break
			}
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
		return nil, status.Errorf(codes.Internal, "failed to save snapshot: %v", err)
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
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
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
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
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
		return nil, status.Error(codes.InvalidArgument, "collection_name is required")
	}

	if req.GetSnapshotName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_name is required")
	}

	// Verify collection exists
	if !s.registry.CollectionExists(req.CollectionName) {
		return nil, status.Errorf(codes.NotFound, "collection %q not found", req.CollectionName)
	}

	// Delete snapshot file
	snapshotPath := filepath.Join(s.snapshotDir, "collections", req.CollectionName, req.SnapshotName)
	if err := os.Remove(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", req.SnapshotName)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
	}

	return &qpb.DeleteSnapshotResponse{
		Time: time.Since(start).Seconds(),
	}, nil
}

// CreateFull creates a full storage snapshot (all collections).
// Maps to: BadgerEngine.Backup or WAL.CreateSnapshot for all data
func (s *SnapshotsService) CreateFull(ctx context.Context, req *qpb.CreateFullSnapshotRequest) (*qpb.CreateSnapshotResponse, error) {
	start := time.Now()

	// Create full snapshots directory
	fullSnapshotDir := filepath.Join(s.snapshotDir, "full")
	if err := os.MkdirAll(fullSnapshotDir, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot directory: %v", err)
	}

	// Generate snapshot name with timestamp
	timestamp := time.Now().UTC()
	snapshotName := fmt.Sprintf("full-%d.snapshot", timestamp.UnixNano())
	snapshotPath := filepath.Join(fullSnapshotDir, snapshotName)

	// Try to use BadgerEngine.Backup if available
	if badger, ok := s.storage.(interface{ Backup(string) error }); ok {
		if err := badger.Backup(snapshotPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to create backup: %v", err)
		}
	} else {
		// Fallback: export all nodes and edges as a snapshot
		nodes, err := s.storage.AllNodes()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get nodes: %v", err)
		}

		edges, err := s.storage.AllEdges()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get edges: %v", err)
		}

		snapshot := &storage.Snapshot{
			Sequence:  uint64(timestamp.UnixNano()),
			Timestamp: timestamp,
			Nodes:     nodes,
			Edges:     edges,
			Version:   "qdrant-compat-full-1.0",
		}

		if err := storage.SaveSnapshot(snapshot, snapshotPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to save snapshot: %v", err)
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
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
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
	start := time.Now()

	if req.GetSnapshotName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_name is required")
	}

	// Delete snapshot file
	snapshotPath := filepath.Join(s.snapshotDir, "full", req.SnapshotName)
	if err := os.Remove(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", req.SnapshotName)
		}
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot: %v", err)
	}

	return &qpb.DeleteSnapshotResponse{
		Time: time.Since(start).Seconds(),
	}, nil
}
