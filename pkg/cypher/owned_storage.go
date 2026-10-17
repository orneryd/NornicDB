package cypher

import (
	"log/slog"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type ownedStorageCleanup struct {
	once  sync.Once
	close func() error
}

func (owner *ownedStorageCleanup) release(report func(error)) {
	owner.once.Do(func() {
		if err := owner.close(); err != nil {
			report(err)
		}
	})
}

// SetOwnedStorageCleanup transfers a request/session storage resource to the
// executor's transport lifecycle. Shared database engines are never transferred.
// Call during construction; clones do not inherit ownership.
func (e *StorageExecutor) SetOwnedStorageCleanup(close func() error) {
	e.ownedStorageCleanup = &ownedStorageCleanup{close: close}
}

// ReleaseOwnedStorage runs the single resource owner once. Cleanup errors are
// recorded without changing an already committed transaction's wire outcome.
func (e *StorageExecutor) ReleaseOwnedStorage() {
	if e == nil || e.ownedStorageCleanup == nil {
		return
	}
	owner := e.ownedStorageCleanup
	owner.release(func(err error) {
		e.logEvent(slog.LevelError, localization.OperationStorageCleanupFailedEvent(e.currentDatabaseName(), err))
	})
}
