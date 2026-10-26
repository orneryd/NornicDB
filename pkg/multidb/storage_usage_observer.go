package multidb

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type storedEntityObserverInitializer interface {
	InitializeStoredEntityObserver(string, func(storage.StoredEntitySnapshot) (storage.StoredEntityObserver, error), storage.StoredEntityReadBoundary) error
}

func (m *DatabaseManager) initializeStorageUsage(database string, owner storedEntityObserverInitializer, trackBytes bool, boundary storage.StoredEntityReadBoundary) error {
	m.mu.RLock()
	info := m.databases[database]
	var limits *Limits
	if info != nil {
		limits = info.limitsSnapshot()
	}
	m.mu.RUnlock()
	if info == nil {
		return ErrDatabaseNotFound
	}
	registry, _ := storage.OwnedCapability[storage.ManagedEntityRuntimeRegistry](owner)
	runtime, err := bindManagedRuntime(info, limits, database, managedEntityOwner{observer: owner, registry: registry, boundary: boundary})
	if err != nil {
		return err
	}
	return runtime.initializeStorageUsage(trackBytes, boundary)
}

// initializeStorageUsage captures no manager metadata lock. Registered native
// initialization may run from an internal writer whose caller owns that lock.
func (runtime *managedDatabaseRuntime) initializeStorageUsage(trackBytes bool, boundaries ...storage.StoredEntityReadBoundary) error {
	return runtime.initializeStorageUsageWithContext(context.Background(), trackBytes, boundaries...)
}

func (runtime *managedDatabaseRuntime) initializeStorageUsageWithContext(ctx context.Context, trackBytes bool, boundaries ...storage.StoredEntityReadBoundary) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The first actual initialization captures its decorated reader. Later manager
	// views reuse it rather than replacing an installed namespace fault/policy seam.
	runtime.boundaryOnce.Do(func() {
		if len(boundaries) > 0 && boundaries[0] != nil {
			runtime.boundary = boundaries[0]
		}
	})
	info := runtime.accounting
	info.sizeMu.RLock()
	ready := info.usageInitialized && (!trackBytes || info.sizeInitialized)
	info.sizeMu.RUnlock()
	if ready {
		return nil
	}
	return runtime.observer.InitializeStoredEntityObserver(runtime.namespace, func(snapshot storage.StoredEntitySnapshot) (storage.StoredEntityObserver, error) {
		info.sizeMu.Lock()
		defer info.sizeMu.Unlock()
		trackBytes := trackBytes || info.sizeInitialized
		if !info.usageInitialized || (trackBytes && !info.sizeInitialized) {
			var actual storageUsage
			err := snapshot.ForEachWithContext(ctx, func(entry storage.StoredEntitySnapshotEntry) error {
				// Initial population is unsigned presence; commit deltas remain
				// signed before/after images in the existing fold below.
				switch entry.Kind {
				case storage.StoredEntityNodeKind:
					actual.nodes++
				case storage.StoredEntityEdgeKind:
					actual.edges++
				}
				if !trackBytes {
					return nil
				}
				change, err := entry.Read()
				if err != nil {
					return err
				}
				delta, err := storageChangesUsageWithContext(ctx, runtime.namespace, []storage.StoredEntityChange{change}, true)
				if err != nil {
					return err
				}
				actual.nodeBytes += delta.nodeBytes
				actual.edgeBytes += delta.edgeBytes
				return nil
			})
			if err != nil {
				return nil, err
			}
			info.usageNodes, info.usageEdges = actual.nodes, actual.edges
			info.usageInitialized = true
			if trackBytes {
				info.nodeSize, info.edgeSize = actual.nodeBytes, actual.edgeBytes
				info.totalSize = actual.nodeBytes + actual.edgeBytes
				info.sizeInitialized = true
			}
		}
		return info.storageUsageObserver(runtime.namespace, trackBytes), nil
	}, runtime.boundary)
}

// storageUsageObserver performs fallible sizing before physical commit. The
// returned closure only applies immutable scalar usage, with no latest reread.
func (info *DatabaseInfo) storageUsageObserver(database string, trackBytes bool) storage.StoredEntityObserver {
	info = info.accountingOwner()
	return func(publication storage.StoredEntityPublication) (func(), error) {
		delta, err := storageChangesUsage(database, publication.Changes, trackBytes)
		if err != nil {
			return nil, err
		}
		return func() { info.publishStorageUsage(publication.LogicalID, delta) }, nil
	}
}
