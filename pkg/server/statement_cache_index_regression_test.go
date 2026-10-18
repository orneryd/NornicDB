package server

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestRule73CachedHeimdallViewsDropCycles64(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	router := newHeimdallDBRouter(nil, manager, nil)
	for i := range 64 {
		name := fmt.Sprintf("gate_cached_%d", i)
		if err := manager.CreateDatabase(name); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := router.executorForDatabase(name); err != nil {
			t.Fatal(err)
		}
		if err := manager.DropDatabase(name); err != nil {
			t.Fatal(err)
		}
	}
	if len(router.executors) != 0 {
		t.Fatalf("catalog registry retired owners, but Heimdall retained %d dropped execution views", len(router.executors))
	}
}
