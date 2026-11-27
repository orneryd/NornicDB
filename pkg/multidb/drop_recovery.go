package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// transitionPendingDrop is the only writer of the durable catalog DROP intent.
// The caller holds m.mu, or is constructing an unpublished manager.
func (m *DatabaseManager) transitionPendingDrop(name string, pending bool) {
	if pending {
		if m.pendingDrops == nil {
			m.pendingDrops = make(map[string]bool)
		}
		m.pendingDrops[name] = true
	} else {
		delete(m.pendingDrops, name)
	}
}

// syncCatalogMetadata establishes the durable catalog boundary for changes
// that add or remove a physical database store.
func (m *DatabaseManager) syncCatalogMetadata() error {
	if durable, ok := storage.UnwrapEngine(m.inner).(interface{ Sync() error }); ok {
		return durable.Sync()
	}
	return nil
}

// recoverPendingDrops uses the same DROP owner before writable startup admits
// traffic. A read-only replica retains the intent and refuses that database.
func (m *DatabaseManager) recoverPendingDrops(ctx context.Context) error {
	for name := range m.pendingDrops {
		if err := m.DropDatabase(ctx, name); err != nil {
			return err
		}
	}
	return nil
}
