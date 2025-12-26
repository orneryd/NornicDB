// Package qdrantgrpc provides a Qdrant-compatible gRPC API for NornicDB.
package qdrantgrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
)

const (
	// CollectionMetaLabel is the label used to identify collection metadata nodes
	CollectionMetaLabel = "_QdrantCollection"

	// QdrantPointLabel is the label prefix for points in a collection
	QdrantPointLabel = "QdrantPoint"
)

// collectionMetaData is the persisted metadata for a collection.
type collectionMetaData struct {
	Name       string `json:"name"`
	Dimensions int    `json:"dimensions"`
	Distance   int32  `json:"distance"` // qpb.Distance enum value
	CreatedAt  int64  `json:"created_at"`
}

// CollectionMeta holds metadata about a collection.
type CollectionMeta struct {
	Name       string
	Dimensions int
	Distance   qpb.Distance
	Status     qpb.CollectionStatus
}

// CollectionRegistry manages vector collections (indexes).
// This interface is implemented by PersistentCollectionRegistry (production)
// and MemoryCollectionRegistry (testing).
type CollectionRegistry interface {
	// CreateCollection creates a new collection with the given parameters.
	CreateCollection(ctx context.Context, name string, dims int, distance qpb.Distance) error

	// GetCollection returns collection metadata.
	GetCollection(ctx context.Context, name string) (*CollectionMeta, error)

	// ListCollections returns all collection names.
	ListCollections(ctx context.Context) ([]string, error)

	// DeleteCollection removes a collection.
	DeleteCollection(ctx context.Context, name string) error

	// CollectionExists checks if a collection exists.
	CollectionExists(name string) bool

	// GetPointCount returns the number of points in a collection.
	GetPointCount(ctx context.Context, name string) (int, error)
}

// PersistentCollectionRegistry is a production-ready implementation of CollectionRegistry.
// It persists collection metadata to storage. Vector indexing is delegated to search.Service.
//
// This registry does NOT maintain its own HNSW indexes. Instead:
// - Collection metadata (name, dimensions, distance) is stored as nodes
// - Points are stored as nodes with embeddings in ChunkEmbeddings (supports named vectors)
// - Vector search is delegated to the unified search.Service
type PersistentCollectionRegistry struct {
	mu          sync.RWMutex
	storage     storage.Engine
	collections map[string]*CollectionMeta
}

// NewPersistentCollectionRegistry creates a new persistent registry.
// It loads existing collections from storage on initialization.
func NewPersistentCollectionRegistry(store storage.Engine) (*PersistentCollectionRegistry, error) {
	if store == nil {
		return nil, fmt.Errorf("storage engine required")
	}

	r := &PersistentCollectionRegistry{
		storage:     store,
		collections: make(map[string]*CollectionMeta),
	}

	// Load existing collections from storage
	if err := r.loadCollections(); err != nil {
		return nil, fmt.Errorf("failed to load collections: %w", err)
	}

	return r, nil
}

// loadCollections loads all existing collections from storage.
func (r *PersistentCollectionRegistry) loadCollections() error {
	// Find all collection metadata nodes
	metaNodes, err := r.storage.GetNodesByLabel(CollectionMetaLabel)
	if err != nil {
		// No collections yet is fine
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}

	for _, node := range metaNodes {
		// Parse metadata from properties
		meta, err := r.parseCollectionMeta(node)
		if err != nil {
			log.Printf("warning: failed to parse collection metadata for %s: %v", node.ID, err)
			continue
		}

		r.collections[meta.Name] = meta
		log.Printf("loaded collection %q: %d dimensions", meta.Name, meta.Dimensions)
	}

	return nil
}

// parseCollectionMeta parses collection metadata from a storage node.
func (r *PersistentCollectionRegistry) parseCollectionMeta(node *storage.Node) (*CollectionMeta, error) {
	// Get metadata from properties
	name, _ := node.Properties["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("missing collection name")
	}

	dims := 0
	switch v := node.Properties["dimensions"].(type) {
	case float64:
		dims = int(v)
	case int:
		dims = v
	case int64:
		dims = int(v)
	}
	if dims <= 0 {
		return nil, fmt.Errorf("invalid dimensions: %d", dims)
	}

	var distance qpb.Distance
	switch v := node.Properties["distance"].(type) {
	case float64:
		distance = qpb.Distance(int32(v))
	case int:
		distance = qpb.Distance(int32(v))
	case int64:
		distance = qpb.Distance(int32(v))
	case int32:
		distance = qpb.Distance(v)
	default:
		distance = qpb.Distance_Cosine
	}

	return &CollectionMeta{
		Name:       name,
		Dimensions: dims,
		Distance:   distance,
		Status:     qpb.CollectionStatus_Green,
	}, nil
}

// CreateCollection creates a new collection with the given parameters.
func (r *PersistentCollectionRegistry) CreateCollection(ctx context.Context, name string, dims int, distance qpb.Distance) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if already exists
	if _, exists := r.collections[name]; exists {
		return fmt.Errorf("collection %q already exists", name)
	}

	// Create metadata node in storage
	metaNode := &storage.Node{
		ID:     storage.NodeID(fmt.Sprintf("_qdrant_collection:%s", name)),
		Labels: []string{CollectionMetaLabel},
		Properties: map[string]any{
			"name":       name,
			"dimensions": dims,
			"distance":   int32(distance),
			"created_at": time.Now().Unix(),
		},
		CreatedAt: time.Now(),
	}

	if _, err := r.storage.CreateNode(metaNode); err != nil {
		return fmt.Errorf("failed to persist collection metadata: %w", err)
	}

	r.collections[name] = &CollectionMeta{
		Name:       name,
		Dimensions: dims,
		Distance:   distance,
		Status:     qpb.CollectionStatus_Green,
	}

	log.Printf("created collection %q: %d dimensions, distance=%s", name, dims, distance.String())
	return nil
}

// GetCollection returns collection metadata.
func (r *PersistentCollectionRegistry) GetCollection(ctx context.Context, name string) (*CollectionMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, exists := r.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %q not found", name)
	}

	// Return a copy
	result := *meta
	return &result, nil
}

// ListCollections returns all collection names.
func (r *PersistentCollectionRegistry) ListCollections(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.collections))
	for name := range r.collections {
		names = append(names, name)
	}
	return names, nil
}

// DeleteCollection removes a collection and all its points.
func (r *PersistentCollectionRegistry) DeleteCollection(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collections[name]; !exists {
		return fmt.Errorf("collection %q not found", name)
	}

	// Delete all points in the collection
	pointLabel := name
	nodes, err := r.storage.GetNodesByLabel(pointLabel)
	if err != nil && err != storage.ErrNotFound {
		return fmt.Errorf("failed to get collection points: %w", err)
	}

	// Collect point IDs for deletion
	pointIDs := make([]storage.NodeID, 0, len(nodes))
	for _, node := range nodes {
		// Verify it's a Qdrant point
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				pointIDs = append(pointIDs, node.ID)
				break
			}
		}
	}

	// Bulk delete points
	deletedCount := len(pointIDs)
	if deletedCount > 0 {
		if err := r.storage.BulkDeleteNodes(pointIDs); err != nil {
			log.Printf("warning: failed to delete some points in collection %s: %v", name, err)
		}
	}

	// Delete collection metadata node
	metaNodeID := storage.NodeID(fmt.Sprintf("_qdrant_collection:%s", name))
	if err := r.storage.DeleteNode(metaNodeID); err != nil {
		log.Printf("warning: failed to delete collection metadata: %v", err)
	}

	// Remove from in-memory cache
	delete(r.collections, name)

	log.Printf("deleted collection %q: removed %d points", name, deletedCount)
	return nil
}

// CollectionExists checks if a collection exists.
func (r *PersistentCollectionRegistry) CollectionExists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.collections[name]
	return exists
}

// GetPointCount returns the number of points in a collection by counting nodes.
func (r *PersistentCollectionRegistry) GetPointCount(ctx context.Context, name string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.collections[name]; !exists {
		return 0, fmt.Errorf("collection %q not found", name)
	}

	// Count nodes with the collection label
	nodes, err := r.storage.GetNodesByLabel(name)
	if err != nil {
		if err == storage.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	// Count only Qdrant points
	count := 0
	for _, node := range nodes {
		for _, label := range node.Labels {
			if label == QdrantPointLabel {
				count++
				break
			}
		}
	}

	return count, nil
}

// GetAllCollections returns metadata for all collections.
func (r *PersistentCollectionRegistry) GetAllCollections(ctx context.Context) ([]*CollectionMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*CollectionMeta, 0, len(r.collections))
	for _, meta := range r.collections {
		copy := *meta
		result = append(result, &copy)
	}
	return result, nil
}

// ExportCollectionMeta exports collection metadata as JSON bytes.
func (r *PersistentCollectionRegistry) ExportCollectionMeta(name string) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, exists := r.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %q not found", name)
	}

	data := collectionMetaData{
		Name:       meta.Name,
		Dimensions: meta.Dimensions,
		Distance:   int32(meta.Distance),
		CreatedAt:  time.Now().Unix(),
	}

	return json.Marshal(data)
}

// ImportCollectionMeta imports collection metadata from JSON bytes.
func (r *PersistentCollectionRegistry) ImportCollectionMeta(ctx context.Context, data []byte) error {
	var meta collectionMetaData
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("invalid collection metadata: %w", err)
	}

	return r.CreateCollection(ctx, meta.Name, meta.Dimensions, qpb.Distance(meta.Distance))
}

// Close releases any resources held by the registry.
func (r *PersistentCollectionRegistry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collections = make(map[string]*CollectionMeta)
	return nil
}

// =============================================================================
// MEMORY REGISTRY (for testing only)
// =============================================================================

// MemoryCollectionRegistry is an in-memory implementation for testing.
// Use PersistentCollectionRegistry for production.
type MemoryCollectionRegistry struct {
	mu          sync.RWMutex
	collections map[string]*CollectionMeta
	pointCounts map[string]int
}

// NewMemoryCollectionRegistry creates a new in-memory registry for testing.
func NewMemoryCollectionRegistry() *MemoryCollectionRegistry {
	return &MemoryCollectionRegistry{
		collections: make(map[string]*CollectionMeta),
		pointCounts: make(map[string]int),
	}
}

// CreateCollection creates a new collection.
func (r *MemoryCollectionRegistry) CreateCollection(ctx context.Context, name string, dims int, distance qpb.Distance) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collections[name]; exists {
		return fmt.Errorf("collection %q already exists", name)
	}

	r.collections[name] = &CollectionMeta{
		Name:       name,
		Dimensions: dims,
		Distance:   distance,
		Status:     qpb.CollectionStatus_Green,
	}
	r.pointCounts[name] = 0
	return nil
}

// GetCollection returns collection metadata.
func (r *MemoryCollectionRegistry) GetCollection(ctx context.Context, name string) (*CollectionMeta, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, exists := r.collections[name]
	if !exists {
		return nil, fmt.Errorf("collection %q not found", name)
	}
	return meta, nil
}

// ListCollections returns all collection names.
func (r *MemoryCollectionRegistry) ListCollections(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.collections))
	for name := range r.collections {
		names = append(names, name)
	}
	return names, nil
}

// DeleteCollection removes a collection.
func (r *MemoryCollectionRegistry) DeleteCollection(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.collections[name]; !exists {
		return fmt.Errorf("collection %q not found", name)
	}
	delete(r.collections, name)
	delete(r.pointCounts, name)
	return nil
}

// CollectionExists checks if a collection exists.
func (r *MemoryCollectionRegistry) CollectionExists(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.collections[name]
	return exists
}

// GetPointCount returns the number of points in a collection.
func (r *MemoryCollectionRegistry) GetPointCount(ctx context.Context, name string) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.collections[name]; !exists {
		return 0, fmt.Errorf("collection %q not found", name)
	}
	return r.pointCounts[name], nil
}

// IncrementPointCount increments the point count for a collection (testing helper).
func (r *MemoryCollectionRegistry) IncrementPointCount(name string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pointCounts[name] += delta
}
