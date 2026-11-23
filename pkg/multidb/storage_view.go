package multidb

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GetStorageWithAuth returns a storage engine for the specified database and forwards
// authToken to remote constituent factories when composite databases include remotes.
//
// Resolution and construction are separate owners so that a configured remote engine
// factory never runs under the metadata lock. The factory is foreign code: it may
// reenter this manager's public catalog (for example ListDatabaseNames), and holding
// m.mu across it blocks that reentry permanently (NOR-183).
func (m *DatabaseManager) GetStorageWithAuth(name string, authToken string) (storage.Engine, error) {
	m.mu.RLock()

	// Check cache first
	if engine, exists := m.engines[name]; exists {
		m.mu.RUnlock()
		return engine, nil
	}
	m.mu.RUnlock()

	selection, err := m.resolveStorageSelection(name)
	if err != nil {
		return nil, err
	}
	switch {
	case selection.composite != nil:
		return m.buildCompositeStorageSelection(selection.composite, authToken)
	case selection.remote != nil:
		// The dotted remote constituent identity is precomputed, so this stays
		// allocation-free until the configured factory creates its engine.
		return m.getRemoteStorageResolvedInternal(selection.remote, authToken)
	default:
		return selection.engine, nil
	}
}

// storageSelection is the immutable result of resolving one storage-view
// selection under the metadata lock. Local namespace views are created during
// resolution because they are manager state; each remote constituent is
// captured as its current resolution snapshot so that its engine can be built
// after the lock is released.
type storageSelection struct {
	// engine is complete when the selection needs no remote factory: a cached
	// view, a local constituent, or a standard database namespace.
	engine storage.Engine
	// remote is set for a dotted remote constituent selection ("composite.alias").
	remote *runtimeConstituent
	// composite is set when a composite engine must be assembled.
	composite *compositeSelection
}

// compositeSelection holds one composite's local constituent views and the
// remote constituent snapshots whose engines remain to be built.
type compositeSelection struct {
	name             string
	constituents     map[string]storage.Engine
	constituentNames map[string]string
	accessModes      map[string]string
	remotes          []remoteConstituentSelection
}

// remoteConstituentSelection is one remote constituent's catalog identity,
// captured under the metadata lock so that exactly one engine is built per
// alias once the lock is released.
type remoteConstituentSelection struct {
	alias        string
	databaseName string
	ref          *runtimeConstituent
}

// resolveStorageSelection is the single resolution owner for database storage
// views. It holds the metadata lock only while it reads catalog state and
// creates local namespace views; it never invokes a remote engine factory.
func (m *DatabaseManager) resolveStorageSelection(name string) (storageSelection, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if engine, exists := m.engines[name]; exists {
		return storageSelection{engine: engine}, nil
	}

	// Resolve dotted composite constituent references first (e.g. "cmp.tr").
	// This is required for protocol-level USE routing where the effective database
	// name is a constituent graph reference, not a top-level database metadata key.
	if constituent, resolvedName, ok := m.resolveCompositeConstituentInternal(name); ok {
		if strings.EqualFold(strings.TrimSpace(constituent.ref.Type), "remote") {
			return storageSelection{remote: constituent}, nil
		}
		engine, _, err := m.databaseViewLocked(resolvedName, true)
		if err != nil {
			return storageSelection{}, err
		}
		return storageSelection{engine: engine}, nil
	}

	// Validate database exists
	info, exists := m.databases[name]
	if !exists {
		return storageSelection{}, ErrDatabaseNotFound
	}

	if m.pendingDrops[name] || info.Status != "online" {
		return storageSelection{}, ErrDatabaseOffline
	}

	if info.Type != "composite" {
		engine, _, err := m.databaseViewLocked(name, true)
		if err != nil {
			return storageSelection{}, err
		}
		return storageSelection{engine: engine}, nil
	}

	composite := &compositeSelection{
		name:             name,
		constituents:     make(map[string]storage.Engine),
		constituentNames: make(map[string]string),
		accessModes:      make(map[string]string),
	}
	for _, ref := range canonicalConstituents(info.Constituents, "") {
		composite.accessModes[ref.Alias] = ref.AccessMode

		resolved := info.runtimeConstituentsByAlias[ref.Alias]
		if resolved == nil {
			m.rebuildRuntimeConstituentsFor(info)
			resolved = info.runtimeConstituentsByAlias[ref.Alias]
		}

		if ref.Type == "remote" {
			if resolved == nil {
				return storageSelection{}, ErrDatabaseNotFound
			}
			composite.remotes = append(composite.remotes, remoteConstituentSelection{
				alias:        ref.Alias,
				databaseName: ref.DatabaseName,
				ref:          resolved,
			})
			continue
		}

		var err error
		if resolved == nil || resolved.err != nil {
			if resolved != nil {
				err = resolved.err
			}
			return storageSelection{}, localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)
		}
		actualName := resolved.resolvedDatabaseName

		// Get storage for constituent
		constituentStorage, _, err := m.databaseViewLocked(actualName, true)
		if err != nil {
			return storageSelection{}, localizedError(localization.MultidbManagerGetConstituentStorageFailed(ref.DatabaseName, err), err)
		}
		composite.constituents[ref.Alias] = constituentStorage
		composite.constituentNames[ref.Alias] = actualName
	}
	return storageSelection{composite: composite}, nil
}

// buildCompositeStorageSelection completes a resolved composite selection. The
// metadata lock is not held here: every remote factory call runs foreign code,
// which may reenter the manager's public catalog. Each remote alias is built
// exactly once from the snapshot captured during resolution.
func (m *DatabaseManager) buildCompositeStorageSelection(composite *compositeSelection, authToken string) (storage.Engine, error) {
	for _, remote := range composite.remotes {
		constituentStorage, err := m.getRemoteStorageResolvedInternal(remote.ref, authToken)
		if err != nil {
			return nil, localizedError(localization.MultidbManagerGetRemoteStorageFailed(remote.alias, err), err)
		}
		composite.constituents[remote.alias] = constituentStorage
		composite.constituentNames[remote.alias] = remote.databaseName
	}

	// Create composite engine with intelligent default routing
	// Routing will be auto-configured based on constituent aliases and access modes
	compositeEngine := storage.NewCompositeEngine(composite.constituents, composite.constituentNames, composite.accessModes)
	compositeEngine.SetCompositeName(composite.name)
	// An explicit transaction on the composite ends at the same configured
	// transaction timeout as one on any of the manager's databases.
	compositeEngine.SetServerTransactionTimeout(storage.ServerTransactionTimeoutOf(m.inner))
	// Note: We don't cache composite engines the same way as they're lightweight wrappers
	return compositeEngine, nil
}
