package multidb

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BackupCatalog is a stable physical inventory for a full backup. It is valid
// only during the callback passed to WithBackupCatalog.
type BackupCatalog struct {
	stores map[string]storage.Engine
}

// HasDedicatedStores reports whether the catalog includes multiple stores.
func (b BackupCatalog) HasDedicatedStores() bool {
	return len(b.stores) > 1
}

// CanStream reports whether the catalog can provide a native full backup.
func (b BackupCatalog) CanStream() bool {
	_, ok := b.stores[""].(storage.NativeBackupStreamer)
	return b.HasDedicatedStores() || ok
}

// Write preserves the native single-store format or writes all stores together
// in one archive. The shared writer preserves each caller's legacy fallback.
func (b BackupCatalog) Write(ctx context.Context, path string, shared func() error) error {
	if b.HasDedicatedStores() {
		return storage.WriteDatabaseBackupArchive(ctx, path, b.stores)
	}
	if store, ok := b.stores[""].(storage.NativeBackupStreamer); ok {
		return storage.WriteNativeDatabaseBackup(ctx, path, store)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return shared()
}

// WithBackupCatalog holds the catalog stable while the caller authorizes and
// captures a backup. It also waits for DROP's physical cleanup, which can run
// outside the catalog gate. Ordinary graph transactions keep serving.
func (m *DatabaseManager) WithBackupCatalog(ctx context.Context, operation func(BackupCatalog) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.catalogGate == nil {
		return operation(BackupCatalog{})
	}
	releaseDrop, err := m.dropAdmission.acquire(ctx, false, false)
	if err != nil {
		return err
	}
	defer releaseDrop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.catalogGate:
	}
	defer func() { m.catalogGate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	stores := map[string]storage.Engine{"": m.inner}
	for name, info := range m.databases {
		if info == nil || info.IsolatedStoreID == "" || m.pendingDrops[name] {
			continue
		}
		store := m.isolatedStores[info.IsolatedStoreID]
		if store == nil {
			m.mu.RUnlock()
			return fmt.Errorf("backup store for %q: %w", name, ErrDatabaseOffline)
		}
		stores[info.IsolatedStoreID] = store
	}
	m.mu.RUnlock()
	return operation(BackupCatalog{stores: stores})
}
