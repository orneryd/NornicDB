package multidb

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// IsolatedStoreConfig owns physically separate databases on a standalone
// server. Its presence makes isolated storage the default for new catalogs.
type IsolatedStoreConfig struct {
	Create func(context.Context, string, map[string]string) (string, storage.Engine, error)
	Open   func(context.Context, string, string, map[string]string, bool) (storage.Engine, error)
	Drop   func(string) error
}

var ErrPreparedImportDisabled = errors.New("publishing prepared databases is disabled")
var errPreparedAlreadyPublished = errors.New("prepared database already published")

type isolatedStorePublication struct {
	id      string
	engine  storage.Engine
	discard func(string) error
}

// PublishPreparedDatabase verifies a closed store before the existing catalog
// receipt makes its binding visible. Only that receipt can transfer ownership.
func (m *DatabaseManager) PublishPreparedDatabase(ctx context.Context, name, id string) error {
	config := m.config.IsolatedStores
	if config == nil || config.Open == nil {
		return ErrPreparedImportDisabled
	}
	publication := &isolatedStorePublication{id: id}
	err := m.transitionCatalog(ctx, catalogTransitionPolicy{
		prepared: publication,
	}, func(candidate *DatabaseManager) error {
		if current := candidate.databases[name]; current != nil && current.IsolatedStoreID == id && id != "" && !candidate.pendingDrops[name] {
			return errPreparedAlreadyPublished
		}
		if err := candidate.addStandardDatabase(DatabaseCreateRequest{Name: name}); err != nil {
			return err
		}
		if err := m.checkSharedNamespace(ctx, name); err != nil {
			return err
		}
		engine, err := config.Open(ctx, name, id, nil, true)
		if err != nil {
			return err
		}
		publication.engine = engine
		candidate.databases[name].IsolatedStoreID = id
		return nil
	})
	if errors.Is(err, errPreparedAlreadyPublished) {
		return m.syncCatalogMetadata()
	}
	return err
}

// Legacy offline imports can write a namespace before it has a catalog entry.
func (m *DatabaseManager) checkSharedNamespace(ctx context.Context, name string) error {
	legacy := storage.NewNamespacedEngine(m.inner, name)
	nodes, err := legacy.NodeCount(ctx)
	if err != nil {
		return err
	}
	if nodes != 0 {
		return ErrDatabaseExists
	}
	edges, err := legacy.EdgeCount(ctx)
	if err != nil {
		return err
	}
	if edges != 0 {
		return ErrDatabaseExists
	}
	return nil
}

func (m *DatabaseManager) installIsolatedStore(p *isolatedStorePublication) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isolatedStoresClosed {
		return storage.ErrStorageClosed
	}
	m.isolatedStores[p.id] = p.engine
	p.engine = nil
	return nil
}

func (m *DatabaseManager) loadIsolatedStores(ctx context.Context) error {
	for name, info := range m.databases {
		if info == nil || info.IsolatedStoreID == "" {
			continue
		}
		config := m.config.IsolatedStores
		if config == nil || config.Open == nil || config.Drop == nil {
			return fmt.Errorf("prepared store for %q is unavailable", name)
		}
		if m.pendingDrops[name] {
			continue
		}
		engine, err := config.Open(ctx, name, info.IsolatedStoreID, info.Options, false)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("Isolated database %q is offline: %v", name, err)
			continue
		}
		m.isolatedStores[info.IsolatedStoreID] = engine
	}
	return nil
}

// graphStoreLocked resolves the catalog's physical owner. A missing isolated
// store must never resolve to the shared engine.
func (m *DatabaseManager) graphStoreLocked(name string) (storage.Engine, error) {
	return m.storeForDatabaseInfoLocked(m.databases[name])
}

func (m *DatabaseManager) storeForDatabaseInfoLocked(info *DatabaseInfo) (storage.Engine, error) {
	if info != nil && info.IsolatedStoreID != "" {
		if store := m.isolatedStores[info.IsolatedStoreID]; store != nil {
			return store, nil
		}
		return nil, ErrDatabaseOffline
	}
	return m.inner, nil
}

func (m *DatabaseManager) dropPreparedOrSharedStore(name string) error {
	m.mu.RLock()
	var id string
	if info := m.databases[name]; info != nil {
		id = info.IsolatedStoreID
	}
	store := m.isolatedStores[id]
	m.mu.RUnlock()
	if id == "" {
		_, _, err := m.inner.DeleteByPrefix(name + ":")
		return err
	}
	if store != nil {
		if err := store.Close(); err != nil {
			return err
		}
	}
	if err := m.config.IsolatedStores.Drop(id); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.isolatedStores, id)
	m.mu.Unlock()
	return nil
}

// CloseIsolatedStores releases only dedicated database engines. The serving
// DB's lifetime retains ownership of the shared engine.
func (m *DatabaseManager) CloseIsolatedStores() error {
	m.mu.Lock()
	stores := m.isolatedStores
	m.isolatedStores = nil
	m.isolatedStoresClosed = true
	m.mu.Unlock()
	var result error
	for _, store := range stores {
		result = errors.Join(result, store.Close())
	}
	return result
}
