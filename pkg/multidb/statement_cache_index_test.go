package multidb

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync"
	"testing"
	"time"
)

func TestStatementCacheIndexCleanupOutsideMetadataLock(t *testing.T) {
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	if err := manager.CreateAlias(t.Context(), "index_alias", "nornic"); err != nil {
		t.Fatal(err)
	}
	var indexMu sync.Mutex
	started := make(chan struct{}, 1)
	cancel := manager.RegisterStatementCacheIndex(func() { started <- struct{}{}; indexMu.Lock(); defer indexMu.Unlock() })
	defer cancel()
	indexMu.Lock()
	dropped := make(chan error, 1)
	go func() { dropped <- manager.DropAlias(t.Context(), "index_alias") }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		indexMu.Unlock()
		t.Fatal("cleanup was not published")
	}
	// Model the factory's index-lock -> metadata-lock order while cleanup is
	// waiting on that index. A callback under metadata lock deadlocks this lookup.
	acquired := make(chan struct{})
	go func() { manager.StatementCacheScope("nornic"); close(acquired) }()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		indexMu.Unlock()
		t.Fatal("cleanup held metadata lock while waiting on index")
	}
	indexMu.Unlock()
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup could not reenter manager")
	}
}

func TestStatementCacheIndexBindCloseRace(t *testing.T) {
	manager, err := NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	for range 32 {
		var index StatementCacheIndex
		var workers sync.WaitGroup
		for range 8 {
			workers.Go(func() { index.Bind(manager, func() {}) })
		}
		workers.Go(index.Close)
		workers.Wait()
		index.Close()
		manager.mu.Lock()
		count := len(manager.statementCacheIndexes)
		manager.mu.Unlock()
		if count != 0 {
			t.Fatalf("closed host retained %d registrations", count)
		}
	}
}
