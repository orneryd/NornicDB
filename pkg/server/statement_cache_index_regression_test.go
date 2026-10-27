package server

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestRule73CachedHeimdallViewsDropCycles64(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	router := newHeimdallDBRouter(nil, manager, nil)
	for i := range 64 {
		name := fmt.Sprintf("gate_cached_%d", i)
		if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := router.executorForDatabase(name); err != nil {
			t.Fatal(err)
		}
		if err := manager.DropDatabase(t.Context(), name); err != nil {
			t.Fatal(err)
		}
	}
	if len(router.executors) != 0 {
		t.Fatalf("catalog registry retired owners, but Heimdall retained %d dropped execution views", len(router.executors))
	}
}
