package server

import (
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
	fixtureowner.Own(t, manager)
	router := newHeimdallDBRouter(nil, manager, nil)
	fixtureowner.RunCatalogDropCycles(t, 64, "gate_cached_",
		func(name string) error {
			return manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name})
		},
		func(name string) error {
			_, _, _, err := router.executorForDatabase(name)
			return err
		},
		func(name string) error {
			return manager.DropDatabase(t.Context(), name)
		},
	)
	if len(router.executors) != 0 {
		t.Fatalf("catalog registry retired owners, but Heimdall retained %d dropped execution views", len(router.executors))
	}
}
