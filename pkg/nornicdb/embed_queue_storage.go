package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// SetEmbeddingStoreCatalog supplies the physical stores managed by the server.
// The worker owns its existing claims, provider selection, and retry scheduler.
func (db *DB) SetEmbeddingStoreCatalog(manager *multidb.DatabaseManager) {
	db.embedStoreCatalog.Store(manager)
}

func (db *DB) embeddingQueueStorage() storage.Engine {
	if root, ok := db.baseStorage.(*storage.WALEngine); ok {
		return &embeddingQueueStorage{WALEngine: root, catalog: &db.embedStoreCatalog}
	}
	return db.baseStorage
}

// An embedding queue uses fully qualified node IDs. Store-wide scans visit
// each physical store once. Point operations use the matching WAL and Badger.
type embeddingQueueStorage struct {
	*storage.WALEngine
	catalog *atomic.Pointer[multidb.DatabaseManager]
	next    atomic.Uint64
}

func (s *embeddingQueueStorage) storeForNode(id storage.NodeID) (storage.Engine, error) {
	name, _, _ := splitQualifiedID(string(id))
	return s.storeForNamespace(name)
}

func (s *embeddingQueueStorage) storeForNamespace(name string) (storage.Engine, error) {
	if manager := s.catalog.Load(); manager != nil && name != "" {
		return manager.BackgroundGraphStore(name)
	}
	return s.WALEngine, nil
}

func (s *embeddingQueueStorage) stores() []storage.Engine {
	if manager := s.catalog.Load(); manager != nil {
		return append(manager.ActiveDedicatedGraphStores(), s.WALEngine)
	}
	return []storage.Engine{s.WALEngine}
}

func (s *embeddingQueueStorage) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	engine, err := s.storeForNode(id)
	if err != nil {
		return nil, err
	}
	return engine.GetNode(ctx, id)
}

func (s *embeddingQueueStorage) GetNodeWithoutEmbeddings(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	engine, err := s.storeForNode(id)
	if err != nil {
		return nil, err
	}
	if reader, ok := engine.(storage.NodeWithoutEmbeddingsReader); ok {
		return reader.GetNodeWithoutEmbeddings(ctx, id)
	}
	return engine.GetNode(ctx, id)
}

func (s *embeddingQueueStorage) FindNodeNeedingEmbedding(ctx context.Context) (*storage.Node, error) {
	stores := s.stores()
	start := 0
	if len(stores) > 1 {
		start = int((s.next.Add(1) - 1) % uint64(len(stores)))
	}
	for offset := range stores {
		engine := stores[(start+offset)%len(stores)]
		var node *storage.Node
		var err error
		if finder, ok := engine.(EmbeddingFinder); ok {
			node, err = finder.FindNodeNeedingEmbedding(ctx)
		} else {
			node, err = storage.FindNodeNeedingEmbedding(ctx, engine)
		}
		if engine != s.WALEngine && errors.Is(err, storage.ErrStorageClosed) {
			continue
		}
		if node != nil || err != nil {
			return node, err
		}
	}
	return nil, nil
}

func (s *embeddingQueueStorage) CountPendingEmbeddingIDs(ctx context.Context, accept func([]byte) bool) int {
	count := 0
	for _, engine := range s.stores() {
		if ctx.Err() != nil {
			break
		}
		count += storage.CountPendingEmbeddingIDs(ctx, engine, accept)
	}
	return count
}

func (s *embeddingQueueStorage) MarkNodeEmbedded(id storage.NodeID) {
	engine, err := s.storeForNode(id)
	if err == nil {
		if index, ok := engine.(EmbeddingIndexManager); ok {
			index.MarkNodeEmbedded(id)
		}
	}
}

func (s *embeddingQueueStorage) AddToPendingEmbeddings(id storage.NodeID) {
	engine, err := s.storeForNode(id)
	if err == nil {
		if index, ok := engine.(interface{ AddToPendingEmbeddings(storage.NodeID) }); ok {
			index.AddToPendingEmbeddings(id)
		}
	}
}

func (s *embeddingQueueStorage) RefreshPendingEmbeddingsIndex() int {
	count := 0
	for _, engine := range s.stores() {
		if index, ok := engine.(EmbeddingIndexManager); ok {
			count += index.RefreshPendingEmbeddingsIndex()
		}
	}
	return count
}

func (s *embeddingQueueStorage) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	if name, _, qualified := strings.Cut(opts.Prefix, ":"); qualified && name != "" {
		engine, err := s.storeForNamespace(name)
		if err != nil {
			return err
		}
		return engine.StreamNodesWithOptions(ctx, opts, visit)
	}
	for _, engine := range s.stores() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var visitErr error
		err := engine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
			visitErr = visit(node)
			return visitErr
		})
		if errors.Is(visitErr, storage.ErrIterationStopped) {
			return nil
		}
		if visitErr == nil && engine != s.WALEngine && errors.Is(err, storage.ErrStorageClosed) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *embeddingQueueStorage) StreamParkedEmbeddingFailures(ctx context.Context, visit func(storage.NodeID, map[string]any) error) (int, error) {
	count := 0
	for _, engine := range s.stores() {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		streamer, ok := engine.(storage.EmbeddingFailureStreamer)
		if !ok {
			return count, fmt.Errorf("storage engine %T does not support embedding failure scans", engine)
		}
		var visitErr error
		visited, err := streamer.StreamParkedEmbeddingFailures(ctx, func(id storage.NodeID, meta map[string]any) error {
			visitErr = visit(id, meta)
			return visitErr
		})
		count += visited
		if errors.Is(visitErr, storage.ErrIterationStopped) {
			return count, nil
		}
		if visitErr == nil && engine != s.WALEngine && errors.Is(err, storage.ErrStorageClosed) {
			continue
		}
		if err != nil {
			return count, err
		}
	}
	return count, ctx.Err()
}
