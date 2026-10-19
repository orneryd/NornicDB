package qdrantgrpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
)

const (
	collectionMetaNodeID    storage.NodeID = "_collection_meta"
	collectionMetaNodeLabel                = "_CollectionMeta"
)

var (
	// ErrCollectionNotFound indicates the requested Qdrant collection does not exist.
	// This includes databases that exist but do not contain the required _collection_meta node.
	ErrCollectionNotFound = errors.New("collection not found")
	// ErrInvalidCollection indicates the collection database exists but does not satisfy the metadata contract.
	ErrInvalidCollection = errors.New("invalid collection metadata")
)

// CollectionStore maps Qdrant collections to NornicDB database namespaces.
//
// The implementation enforces the required _collection_meta node contract and does not
// support legacy layouts (shared namespace + label filtering).
type CollectionStore interface {
	Create(ctx context.Context, name string, dims int, distance qpb.Distance) error
	Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error)
	GetMeta(ctx context.Context, name string) (*CollectionMeta, error)
	List(ctx context.Context) ([]string, error)
	Drop(ctx context.Context, name string) error
	Exists(name string) bool
	PointCount(ctx context.Context, name string) (int64, error)
}

type databaseCollectionStore struct {
	dbManager *multidb.DatabaseManager
	vecIndex  *vectorIndexCache

	// reserved database names that must not be used for Qdrant collections.
	reserved map[string]struct{}
}

// NewDatabaseCollectionStore creates a CollectionStore backed by DatabaseManager namespaces.
func NewDatabaseCollectionStore(dbManager *multidb.DatabaseManager, vecIndex *vectorIndexCache) (CollectionStore, error) {
	if dbManager == nil {
		return nil, fmt.Errorf("db manager required")
	}

	reserved := map[string]struct{}{
		"system": {},
	}
	// Reserve the default database name as well; Qdrant collections are dedicated namespaces.
	reserved[strings.ToLower(dbManager.DefaultDatabaseName())] = struct{}{}

	return &databaseCollectionStore{
		dbManager: dbManager,
		vecIndex:  vecIndex,
		reserved:  reserved,
	}, nil
}

func (s *databaseCollectionStore) Create(ctx context.Context, name string, dims int, distance qpb.Distance) error {
	if err := ctx.Err(); err != nil { return err }
	if err := validateCollectionName(name, s.reserved); err != nil {
		return err
	}
	if dims <= 0 {
		return fmt.Errorf("invalid dimensions: %d", dims)
	}

	if err := s.dbManager.CreateDatabaseContext(ctx, name); err != nil {
		return err
	}

	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		_ = s.dbManager.DropDatabaseContext(context.WithoutCancel(ctx), name)
		return err
	}

	now := time.Now()
	_, err = engine.CreateNode(&storage.Node{
		ID:     collectionMetaNodeID,
		Labels: []string{collectionMetaNodeLabel},
		Properties: map[string]any{
			"dimensions":     dims,
			"distance":       int32(distance),
			"schema_version": 1,
			"created_at":     now.Unix(),
		},
		CreatedAt: now,
	})
	if err != nil {
		_ = s.dbManager.DropDatabaseContext(context.WithoutCancel(ctx), name)
		return err
	}

	if s.vecIndex != nil {
		// Ensure an empty per-collection index exists with the correct dimension/distance.
		_ = s.vecIndex.getOrCreate(name, "", dims, distance)
	}

	return nil
}

func (s *databaseCollectionStore) Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error) {
	_ = ctx
	if name == "" {
		return nil, nil, ErrCollectionNotFound
	}
	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		return nil, nil, ErrCollectionNotFound
	}
	meta, err := readCollectionMeta(engine)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, ErrInvalidCollection) {
			return nil, nil, ErrCollectionNotFound
		}
		return nil, nil, err
	}
	meta.Name = name
	return engine, meta, nil
}

func (s *databaseCollectionStore) GetMeta(ctx context.Context, name string) (*CollectionMeta, error) {
	_, meta, err := s.Open(ctx, name)
	return meta, err
}

func (s *databaseCollectionStore) List(ctx context.Context) ([]string, error) {
	_ = ctx
	infos := s.dbManager.ListDatabases()
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		if info == nil {
			continue
		}
		engine, err := s.dbManager.GetStorage(info.Name)
		if err != nil {
			continue
		}
		if _, err := readCollectionMeta(engine); err == nil {
			out = append(out, info.Name)
		}
	}
	return out, nil
}

func (s *databaseCollectionStore) Drop(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil { return err }
	_ = ctx
	if name == "" {
		return ErrCollectionNotFound
	}
	// Only allow dropping Qdrant collections (must have meta node).
	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		return ErrCollectionNotFound
	}
	if _, err := readCollectionMeta(engine); err != nil {
		return ErrCollectionNotFound
	}

	if err := s.dbManager.DropDatabaseContext(ctx, name); err != nil {
		return err
	}
	if s.vecIndex != nil {
		s.vecIndex.deleteCollection(name)
	}
	return nil
}

func (s *databaseCollectionStore) Exists(name string) bool {
	if name == "" {
		return false
	}
	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		return false
	}
	_, err = readCollectionMeta(engine)
	return err == nil
}

func (s *databaseCollectionStore) PointCount(ctx context.Context, name string) (int64, error) {
	engine, _, err := s.Open(ctx, name)
	if err != nil {
		return 0, err
	}
	nodes, err := engine.GetNodesByLabel(QdrantPointLabel)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	var count int64
	for _, node := range nodes {
		if isQdrantPointNode(node) {
			count++
		}
	}
	return count, nil
}

func validateCollectionName(name string, reserved map[string]struct{}) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("collection_name is required")
	}
	if strings.Contains(name, ":") {
		return fmt.Errorf("invalid collection_name: ':' is not allowed")
	}
	if strings.HasPrefix(name, "_") {
		return fmt.Errorf("invalid collection_name: '_' prefix is reserved")
	}
	if _, ok := reserved[strings.ToLower(name)]; ok {
		return fmt.Errorf("invalid collection_name: %q is reserved", name)
	}
	return nil
}

func readCollectionMeta(engine storage.Engine) (*CollectionMeta, error) {
	node, err := engine.GetNode(collectionMetaNodeID)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, ErrInvalidCollection
	}
	hasLabel := false
	for _, label := range node.Labels {
		if label == collectionMetaNodeLabel {
			hasLabel = true
			break
		}
	}
	if !hasLabel {
		return nil, ErrInvalidCollection
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
		return nil, ErrInvalidCollection
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
		Name:       "", // Name is the database name (known by caller); keep empty here.
		Dimensions: dims,
		Distance:   distance,
		Status:     qpb.CollectionStatus_Green,
	}, nil
}
