package multidb

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ReloadAfterRestore publishes the restored catalog into the running manager.
// The caller must hold the catalog gate through WithBackupCatalog so another
// catalog transition cannot publish the pre-restore image afterwards.
func (m *DatabaseManager) ReloadAfterRestore(ctx context.Context) error {
	candidate := &DatabaseManager{
		inner:                  m.inner,
		config:                 m.config,
		databases:              make(map[string]*DatabaseInfo),
		pendingDrops:           make(map[string]bool),
		remoteCredentialCipher: m.remoteCredentialCipher,
	}
	if _, err := candidate.loadMetadata(ctx); err != nil {
		return fmt.Errorf("load restored catalog: %w", err)
	}
	if len(candidate.databases) == 0 {
		return fmt.Errorf("load restored catalog: %w", storage.ErrInvalidData)
	}
	return m.publishCatalog(candidate)
}
