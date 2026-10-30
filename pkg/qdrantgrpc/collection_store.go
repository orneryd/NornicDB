package qdrantgrpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/encoding/protojson"
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
	// dropDatabase drops a collection's database and the state derived from
	// it (Config.DropDatabase); it defaults to dbManager.DropDatabase.
	dropDatabase func(ctx context.Context, name string) error
	defaultDrop  bool

	// reserved database names that must not be used for Qdrant collections.
	reserved map[string]struct{}
}

// NewDatabaseCollectionStore creates a CollectionStore backed by DatabaseManager
// namespaces. dropDatabase drops a collection's database together with its
// derived state (Config.DropDatabase); nil uses dbManager.DropDatabase.
func NewDatabaseCollectionStore(dbManager *multidb.DatabaseManager, dropDatabase func(ctx context.Context, name string) error) (CollectionStore, error) {
	if dbManager == nil {
		return nil, fmt.Errorf("db manager required")
	}

	reserved := map[string]struct{}{
		"system": {},
	}
	// Reserve the default database name as well; Qdrant collections are dedicated namespaces.
	reserved[strings.ToLower(dbManager.DefaultDatabaseName())] = struct{}{}

	defaultDrop := dropDatabase == nil
	if defaultDrop {
		dropDatabase = dbManager.DropDatabase
	}

	return &databaseCollectionStore{
		dbManager:    dbManager,
		dropDatabase: dropDatabase,
		defaultDrop:  defaultDrop,
		reserved:     reserved,
	}, nil
}

func (s *databaseCollectionStore) Create(ctx context.Context, name string, dims int, distance qpb.Distance) error {
	return s.CreateWithQuantization(ctx, name, dims, distance, nil)
}

func (s *databaseCollectionStore) CreateWithQuantization(ctx context.Context, name string, dims int, distance qpb.Distance, quantization *qpb.QuantizationConfig) error {
	var encoded []byte
	if quantization != nil {
		if _, err := searchQuantizationConfig(quantization); err != nil {
			return err
		}
		var err error
		encoded, err = protojson.Marshal(quantization)
		if err != nil {
			return err
		}
	}

	if err := validateCollectionName(name, s.reserved); err != nil {
		return err
	}
	if dims <= 0 {
		return fmt.Errorf("invalid dimensions: %d", dims)
	}

	if err := s.dbManager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: name}); err != nil {
		return err
	}

	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		_ = s.dropDatabase(ctx, name)
		return err
	}

	now := time.Now()
	properties := map[string]any{"dimensions": dims, "distance": int32(distance), "schema_version": 1, "created_at": now.Unix()}
	if len(encoded) > 0 {
		properties["quantization_config"] = string(encoded)
	}
	_, err = engine.CreateNode(&storage.Node{
		ID:         collectionMetaNodeID,
		Labels:     []string{collectionMetaNodeLabel},
		Properties: properties,
		CreatedAt:  now,
	})
	if err != nil {
		_ = s.dropDatabase(ctx, name)
		return err
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
	if name == "" {
		return ErrCollectionNotFound
	}
	// Only allow dropping Qdrant collections (must have meta node).
	engine, err := s.dbManager.GetStorage(name)
	if err != nil {
		// Author: NOR-288 lane (Claude), 2026-10-30. An unavailable name may
		// be a drop the DROP owner already accepted; the owner decides and
		// finishes only that, never a live database of another kind.
		if err := s.dbManager.FinishPendingDrop(ctx, name); !errors.Is(err, multidb.ErrDatabaseNotFound) {
			return err
		}
		return ErrCollectionNotFound
	}
	if _, err := readCollectionMeta(engine); err != nil {
		return ErrCollectionNotFound
	}

	return s.dropDatabase(ctx, name)
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

	// Stored numbers come back as whichever integer type fits them (int8 for 4
	// dimensions) once the node is decoded from disk, so read them as integers
	// of any type.
	dims, ok := convert.ToInt64(node.Properties["dimensions"])
	if !ok || dims <= 0 {
		return nil, ErrInvalidCollection
	}

	distance := qpb.Distance_Cosine
	if v, ok := convert.ToInt64(node.Properties["distance"]); ok {
		distance = qpb.Distance(int32(v))
	}

	var quantization *qpb.QuantizationConfig
	if raw, exists := node.Properties["quantization_config"]; exists {
		encoded, ok := raw.(string)
		if !ok {
			return nil, ErrInvalidCollection
		}
		quantization = new(qpb.QuantizationConfig)
		if err := protojson.Unmarshal([]byte(encoded), quantization); err != nil {
			return nil, ErrInvalidCollection
		}
		if _, err := searchQuantizationConfig(quantization); err != nil {
			return nil, ErrInvalidCollection
		}
	}
	return &CollectionMeta{
		Quantization: quantization,
		Name:         "", // Name is the database name (known by caller); keep empty here.
		Dimensions:   int(dims),
		Distance:     distance,
		Status:       qpb.CollectionStatus_Green,
	}, nil
}
