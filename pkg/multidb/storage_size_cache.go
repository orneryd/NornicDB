package multidb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ensureStorageSizeInitialized performs one-time exact size calculation from storage
// and caches the result for O(1) future reads.
func (m *DatabaseManager) ensureStorageSizeInitialized(databaseName string, engine storage.Engine) error {
	m.mu.RLock()
	info, exists := m.databases[databaseName]
	m.mu.RUnlock()
	if !exists {
		return ErrDatabaseNotFound
	}

	info.sizeMu.RLock()
	if info.sizeInitialized {
		info.sizeMu.RUnlock()
		return nil
	}
	info.sizeMu.RUnlock()

	nodeSize, edgeSize, err := m.calculateStorageSizeFromEngine(engine)
	if err != nil {
		return err
	}

	info.sizeMu.Lock()
	if !info.sizeInitialized {
		info.nodeSize = nodeSize
		info.edgeSize = edgeSize
		info.totalSize = nodeSize + edgeSize
		info.sizeInitialized = true
	}
	info.sizeMu.Unlock()
	return nil
}

// markStorageSizeDirty marks cached size as stale. The next read/operation will
// recalculate from storage.
func (m *DatabaseManager) markStorageSizeDirty(databaseName string) {
	m.mu.RLock()
	info, exists := m.databases[databaseName]
	m.mu.RUnlock()
	if !exists {
		return
	}
	info.sizeMu.Lock()
	info.sizeInitialized = false
	info.sizeMu.Unlock()
}

// applyStorageSizeDelta updates cached size counters in O(1). If a previous operation
// marked the cache dirty, deltas are ignored until re-initialization.
func (m *DatabaseManager) applyStorageSizeDelta(databaseName string, nodeDelta, edgeDelta int64) {
	m.mu.RLock()
	info, exists := m.databases[databaseName]
	m.mu.RUnlock()
	if !exists {
		return
	}
	info.sizeMu.Lock()
	defer info.sizeMu.Unlock()
	if !info.sizeInitialized {
		return
	}
	info.nodeSize += nodeDelta
	info.edgeSize += edgeDelta
	info.totalSize += nodeDelta + edgeDelta
	if info.nodeSize < 0 {
		info.nodeSize = 0
	}
	if info.edgeSize < 0 {
		info.edgeSize = 0
	}
	if info.totalSize < 0 {
		info.totalSize = 0
	}
}

func (m *DatabaseManager) calculateStorageSizeFromEngine(engine storage.Engine) (int64, int64, error) {
	var nodeSize int64
	var edgeSize int64

	err := storage.StreamNodesWithFallback(context.Background(), engine, 1000, func(node *storage.Node) error {
		size, sizeErr := calculateNodeSize(node)
		if sizeErr != nil {
			return localizedError(localization.MultidbStorageCalculateNodeSizeFailed(sizeErr), sizeErr)
		}
		nodeSize += size
		return nil
	})
	if err != nil {
		return 0, 0, localizedError(localization.MultidbStorageGetAllNodesForSizeCalculationFailed(err), err)
	}
	err = storage.StreamEdgesWithFallback(context.Background(), engine, 1000, func(edge *storage.Edge) error {
		size, sizeErr := calculateEdgeSize(edge)
		if sizeErr != nil {
			return localizedError(localization.MultidbStorageCalculateEdgeSizeFailed(sizeErr), sizeErr)
		}
		edgeSize += size
		return nil
	})
	if err != nil {
		return 0, 0, localizedError(localization.MultidbStorageGetAllEdgesForSizeCalculationFailed(err), err)
	}

	return nodeSize, edgeSize, nil
}
