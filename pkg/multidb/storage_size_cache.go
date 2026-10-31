package multidb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// storageSizeInitialized reports whether incremental byte accounting is
// active. Ordinary writes must not force the deferred full-corpus scan; a
// MaxBytes check or an explicit GetStorageSize call initializes it when exact
// accounting is actually needed.
func (m *DatabaseManager) storageSizeInitialized(databaseName string) (bool, error) {
	m.mu.RLock()
	info, exists := m.databases[databaseName]
	m.mu.RUnlock()
	if !exists || info == nil {
		return false, ErrDatabaseNotFound
	}
	info = info.accountingOwner()
	info.sizeMu.RLock()
	initialized := info.sizeInitialized
	info.sizeMu.RUnlock()
	return initialized, nil
}

// ensureStorageSizeInitialized performs one-time exact size calculation from storage
// and caches the result for O(1) future reads.
func (m *DatabaseManager) ensureStorageSizeInitialized(ctx context.Context, databaseName string, engine storage.Engine) error {
	m.mu.RLock()
	info := m.databases[databaseName]
	var limits *Limits
	if info != nil {
		limits = info.limitsSnapshot()
	}
	m.mu.RUnlock()
	if info == nil {
		return ErrDatabaseNotFound
	}
	runtime := info.managed.Load()
	if runtime == nil {
		var err error
		runtime, err = bindManagedRuntime(info, limits, databaseName, managedEntityOwnerFor(engine))
		if err != nil {
			return err
		}
	}
	if runtime.observer != nil {
		return runtime.initializeStorageUsage(true, managedEntityOwnerFor(engine).boundary)
	}
	// A read-only opaque Engine retains its existing initial size read. Writes
	// requiring atomic managed accounting validate capabilities before mutation.
	accounting := runtime.accounting
	accounting.sizeMu.RLock()
	ready := accounting.sizeInitialized
	accounting.sizeMu.RUnlock()
	if ready {
		return nil
	}
	nodes, edges, err := m.calculateStorageSizeFromEngine(ctx, engine)
	if err != nil {
		return err
	}
	accounting.sizeMu.Lock()
	defer accounting.sizeMu.Unlock()
	if !accounting.sizeInitialized {
		accounting.nodeSize, accounting.edgeSize = nodes, edges
		accounting.totalSize = nodes + edges
		accounting.sizeInitialized = true
	}
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
	info = info.accountingOwner()
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
	info = info.accountingOwner()
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

func (m *DatabaseManager) calculateStorageSizeFromEngine(ctx context.Context, engine storage.Engine) (int64, int64, error) {
	var nodeSize int64
	var edgeSize int64

	err := storage.StreamStoredNodes(ctx, engine, storage.StreamNodesOptions{WithEmbeddings: true}, func(node *storage.Node) error {
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
	err = storage.StreamStoredEdgesByType(ctx, engine, "", func(edge *storage.Edge) error {
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
