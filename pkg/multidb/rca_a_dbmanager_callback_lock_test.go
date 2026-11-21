package multidb

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-dbmanager-callback-lock).
// Test-side only, no deadlock: the factory probes the manager's metadata lock
// with TryRLock instead of taking it. Decisive value: the lock is held for
// writing at the moment GetStorageWithAuth (manager.go:655-666) calls the
// remote-engine factory (manager.go:847), on the dotted-constituent path
// (manager.go:678) and on the composite-root path (manager.go:716). The
// variant calls the same factory with the lock released, where the callback
// into ListDatabaseNames (database_names.go:7-8) completes.

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCARemoteFactoryRunsUnderMetadataWriteLock(t *testing.T) {
	manager, _ := setupTestManager(t)
	if err := manager.CreateCompositeDatabase(t.Context(), "rca_cmp", []ConstituentRef{{Alias: "r", DatabaseName: "rca_remote", Type: "remote", AccessMode: "read_write", URI: "http://rca.invalid"}}); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("rca: factory probe stops here")
	var heldDuringFactory []bool
	manager.remoteEngineFactory = func(ConstituentRef, string) (storage.Engine, error) {
		acquired := manager.mu.TryRLock()
		if acquired {
			manager.mu.RUnlock()
		}
		heldDuringFactory = append(heldDuringFactory, !acquired)
		return nil, stop
	}
	for _, selection := range []string{"rca_cmp.r", "rca_cmp"} {
		heldDuringFactory = nil
		_, err := manager.GetStorageWithAuth(selection, "")
		t.Logf("RCA_DBM selection=%s factoryCalls=%d metadataLockHeldDuringFactory=%v err=%v", selection, len(heldDuringFactory), heldDuringFactory, err)
	}

	// Variant: the same callback with the metadata lock released.
	done := make(chan []string, 1)
	go func() { done <- manager.ListDatabaseNames() }()
	names := <-done
	t.Logf("RCA_DBM variant: ListDatabaseNames outside the lock returned %d names", len(names))
}
