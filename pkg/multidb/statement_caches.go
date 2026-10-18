package multidb

import (
	"strings"
	"sync"
	"sync/atomic"
)

// StatementCacheResource belongs to a canonical database. Selection removal
// invalidates dependent entries; only removal of that database retires the owner.
// Implementations must not call back into the manager while holding cache locks.
type StatementCacheResource interface {
	Dispose()
	InvalidateScopes()
}

// StatementCacheScope is one catalog selection generation. It contains names
// and lifetime state only, never storage, credentials or execution state.
type StatementCacheScope struct {
	generation uint64
	database   string
	references []string
	retired    atomic.Bool
}

func (s *StatementCacheScope) Valid() bool { return s == nil || !s.retired.Load() }

// Generation identifies this selection's lifetime, including same-name recreation.
func (s *StatementCacheScope) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

// StatementCacheIndex registers one retained-view index for its host lifetime.
// Bind and Close may race; Close never retires resources the index only borrows.
type StatementCacheIndex struct {
	once       sync.Once
	mu         sync.Mutex
	closed     bool
	unregister func()
}

func (i *StatementCacheIndex) Bind(manager *DatabaseManager, invalidate func()) {
	if manager == nil {
		return
	}
	i.once.Do(func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		if !i.closed {
			i.unregister = manager.RegisterStatementCacheIndex(invalidate)
		}
	})
}

func (i *StatementCacheIndex) Close() {
	i.mu.Lock()
	i.closed = true
	unregister := i.unregister
	i.unregister = nil
	i.mu.Unlock()
	if unregister != nil {
		unregister()
	}
}

// RegisterStatementCacheIndex publishes invalidation only after releasing the
// metadata lock. A callback may hold its index lock and reenter the manager.
func (m *DatabaseManager) RegisterStatementCacheIndex(invalidate func()) func() {
	m.mu.Lock()
	if m.statementCachesClosed {
		m.mu.Unlock()
		return func() {}
	}
	m.statementCacheIndexID++
	id := m.statementCacheIndexID
	if m.statementCacheIndexes == nil {
		m.statementCacheIndexes = make(map[uint64]func())
	}
	m.statementCacheIndexes[id] = invalidate
	m.mu.Unlock()
	return func() { m.mu.Lock(); delete(m.statementCacheIndexes, id); m.mu.Unlock() }
}

// Called with m.mu held by a catalog removal. Tokens are already retired, so
// insertion and cleanup races cannot publish an old generation after the sweep.
func (m *DatabaseManager) unlockStatementCacheIndexes(changed, closing bool) {
	var indexes []func()
	if changed {
		for _, invalidate := range m.statementCacheIndexes {
			indexes = append(indexes, invalidate)
		}
	}
	if closing {
		m.statementCacheIndexes = nil
	}
	m.mu.Unlock()
	for _, invalidate := range indexes {
		invalidate()
	}
}

// DatabaseStatementCaches atomically acquires an owner and selection generation.
// Aliases share their canonical database's owner; constituents keep the local
// target's owner (or the composite owner's for remote targets).
func (m *DatabaseManager) DatabaseStatementCaches(name string, create func(*StatementCacheScope) StatementCacheResource) (StatementCacheResource, *StatementCacheScope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	database, scope, err := m.statementCacheScopeLocked(name)
	if err != nil {
		return nil, nil, err
	}
	if resource := m.statementCaches[database]; resource != nil {
		return resource, scope, nil
	}
	_, canonicalScope, err := m.statementCacheScopeLocked(database)
	if err != nil {
		return nil, nil, err
	}
	if m.statementCaches == nil {
		m.statementCaches = make(map[string]StatementCacheResource)
	}
	resource := create(canonicalScope)
	m.statementCaches[database] = resource
	return resource, scope, nil
}

// StatementCacheScope resolves a selection under the metadata lock. A retained
// executor keeps its old token; removing/recreating the selection never revives it.
func (m *DatabaseManager) StatementCacheScope(name string) (*StatementCacheScope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, scope, err := m.statementCacheScopeLocked(name)
	return scope, err
}

func (m *DatabaseManager) statementCacheScopeLocked(name string) (string, *StatementCacheScope, error) {
	if m.statementCachesClosed {
		return "", nil, ErrDatabaseNotFound
	}
	name = strings.TrimSpace(name)
	if scope := m.statementCacheScopes[name]; scope != nil {
		return scope.database, scope, nil
	}
	database, err := m.resolveDatabaseInternal(name)
	key := name
	var references []string
	if err == nil {
		references = []string{name, database}
	} else {
		ref, resolved, ok := m.resolveCompositeConstituentInternal(name)
		if !ok {
			return "", nil, ErrDatabaseNotFound
		}
		composite := name[:strings.IndexByte(name, '.')]
		key = composite + "." + ref.Alias
		references = []string{key, composite}
		if strings.EqualFold(ref.Type, "remote") {
			database = composite
		} else {
			database = resolved
			references = append(references, ref.DatabaseName, resolved)
		}
	}
	if scope := m.statementCacheScopes[key]; scope != nil {
		return database, scope, nil
	}
	if m.statementCacheScopes == nil {
		m.statementCacheScopes = make(map[string]*StatementCacheScope)
	}
	m.statementCacheGeneration++
	scope := &StatementCacheScope{database: database, references: references, generation: m.statementCacheGeneration}
	m.statementCacheScopes[key] = scope
	return database, scope, nil
}

// invalidateStatementCacheReferenceLocked retires only selections resolving
// through the removed name, then purges their entries from surviving owners.
func (m *DatabaseManager) invalidateStatementCacheReferenceLocked(name string) {
	for selection, scope := range m.statementCacheScopes {
		for _, reference := range scope.references {
			if reference == name {
				scope.retired.Store(true)
				delete(m.statementCacheScopes, selection)
				break
			}
		}
	}
	for _, resource := range m.statementCaches {
		resource.InvalidateScopes()
	}
}

func (m *DatabaseManager) dropDatabaseStatementCachesLocked(name string) {
	if resource := m.statementCaches[name]; resource != nil {
		resource.Dispose()
		delete(m.statementCaches, name)
	}
	m.invalidateStatementCacheReferenceLocked(name)
}

// disposeStatementCachesLocked is the manager-wide shutdown boundary only.
func (m *DatabaseManager) disposeStatementCachesLocked() {
	for selection, scope := range m.statementCacheScopes {
		scope.retired.Store(true)
		delete(m.statementCacheScopes, selection)
	}
	for name, resource := range m.statementCaches {
		resource.Dispose()
		delete(m.statementCaches, name)
	}
}
