package bolt

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"sync"
	"testing"
)

func TestRule73CachedBoltViewsDropCycles64(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	server := &Server{dbManager: manager, executors: make(map[string]QueryExecutor)}
	session := &Session{server: server}
	for i := range 64 {
		name := fmt.Sprintf("gate_cached_%d", i)
		if err := manager.CreateDatabase(name); err != nil {
			t.Fatal(err)
		}
		executor, err := session.getExecutorForDatabase(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.Execute(context.Background(), "RETURN 67 AS gate_value", nil); err != nil {
			t.Fatal(err)
		}
		if err := manager.DropDatabase(name); err != nil {
			t.Fatal(err)
		}
	}
	if len(server.executors) != 0 {
		t.Fatalf("catalog registry retired owners, but server retained %d dropped execution views", len(server.executors))
	}
	if count := reflect.ValueOf(manager).Elem().FieldByName("statementCacheIndexes").Len(); count != 1 {
		t.Fatalf("index hosts=%d, want 1", count)
	}
	server.Close()
	if count := reflect.ValueOf(manager).Elem().FieldByName("statementCacheIndexes").Len(); count != 0 {
		t.Fatalf("closed host retained %d registrations", count)
	}
}

func TestStatementCacheFamilyCachedIndexConcurrentDrop(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	server := &Server{dbManager: manager, executors: make(map[string]QueryExecutor)}
	defer server.Close()
	if err := manager.CreateDatabase("index_concurrent"); err != nil {
		t.Fatal(err)
	}
	session := &Session{server: server}
	stop := make(chan struct{})
	var workers sync.WaitGroup
	var stopOnce sync.Once
	stopWorkers := func() { stopOnce.Do(func() { close(stop) }); workers.Wait() }
	defer stopWorkers()
	for range 8 {
		workers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				executor, err := session.getExecutorForDatabase("index_concurrent")
				if err == nil {
					if _, err := executor.Execute(context.Background(), "RETURN 67 AS value", nil); err != nil {
						t.Error(err)
						return
					}
				}
			}
		})
	}
	for range 16 {
		if err := manager.DropDatabase("index_concurrent"); err != nil {
			t.Fatal(err)
		}
		if err := manager.CreateDatabase("index_concurrent"); err != nil {
			t.Fatal(err)
		}
	}
	stopWorkers()
	if err := manager.DropDatabase("index_concurrent"); err != nil {
		t.Fatal(err)
	}
	server.executorsMu.RLock()
	count := len(server.executors)
	server.executorsMu.RUnlock()
	if count != 0 {
		t.Fatalf("concurrent publication retained %d retired views", count)
	}
}
