package multidb

import (
	"context"
	"errors"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A legacy synchronous delete cannot deliver the physical owner's callbacks.
// Once it succeeds, finish the same catalog delta and cache retirement locally;
// caller cancellation cannot undo deletion. Forwarded engines with registered
// notifications retain their existing wait for local committed replay.
func (m *DatabaseManager) completeLegacyNamespaceDrop(ctx context.Context, name string, mutation storage.NamespaceCatalogMutation, op *storage.NamespaceDrop, err error) error {
	if !op.Committed() {
		// Physical deletion already succeeded. Its catalog completion must not
		// revalidate the old lifetime that the legacy deleter has retired.
		op.MarkCommittedEntry()
		err = storage.PublishNamespaceCatalogContext(storage.WithNamespaceDrop(context.WithoutCancel(ctx), op), m.inner, mutation)
	}
	m.retireDatabaseCatalog(name)
	err = errors.Join(err, m.completeDatabaseRetirement(name))
	completionErr := op.Complete(nil)
	return errors.Join(op.Result(err), completionErr)
}

// Lifecycle serialization belongs to the target, never the catalog read lock.
// Entries remain stable across recreation, so waiters cannot acquire a retired
// lock object while a fresh operation uses a different one for the same name.
func (m *DatabaseManager) admitDatabaseLifecycle(ctx context.Context, name string) (func(), error) {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	value, _ := m.namespaceLifecycles.LoadOrStore(name, gate)
	gate = value.(chan struct{})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate:
	}
	var once sync.Once
	finish := func() { once.Do(func() { gate <- struct{}{} }) }
	if err := ctx.Err(); err != nil {
		finish()
		return nil, err
	}
	return finish, nil
}

func (m *DatabaseManager) removeNamespaceRetirementWaiter(name string, waiter chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	waiters := m.namespaceRetirements[name]
	for i, w := range waiters {
		if w == waiter {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(m.namespaceRetirements, name)
	} else {
		m.namespaceRetirements[name] = waiters
	}
}

// Called at the physical owner's commit point. Only logical state changes here;
// persistence and external callbacks belong to post-commit completion.
func (m *DatabaseManager) retireDatabaseCatalog(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.databases, name)
	delete(m.committedCatalog, name)
	m.applyChangeCapture(name, nil)
	delete(m.engines, name)
	delete(m.limitCheckers, name)
	if m.pendingCatalogRetirements == nil {
		m.pendingCatalogRetirements = make(map[string]bool)
	}
	m.pendingCatalogRetirements[name] = true
}

func (m *DatabaseManager) completeDatabaseRetirement(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.pendingCatalogRetirements[name] {
		return nil
	}
	delete(m.pendingCatalogRetirements, name)
	for _, waiter := range m.namespaceRetirements[name] {
		close(waiter)
	}
	delete(m.namespaceRetirements, name)
	return nil
}
