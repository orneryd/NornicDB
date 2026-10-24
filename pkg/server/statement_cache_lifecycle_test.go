package server
import "github.com/orneryd/nornicdb/pkg/cypher"


import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestStatementCacheFamilyHTTPMetadataRemoval(t *testing.T) {
	for _, removal := range []string{"database", "alias", "constituent", "composite"} {
		t.Run(removal, func(t *testing.T) {
			server, _ := setupTestServer(t)
			manager := server.dbManager
			if err := manager.CreateDatabase("cache_local"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateAlias("cache_alias", "cache_local"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("cache_comp", []multidb.ConstituentRef{
				{Alias: "local", DatabaseName: "cache_local", Type: "local", AccessMode: "read_write"},
				{Alias: "remote", DatabaseName: "remote_db", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
			}); err != nil {
				t.Fatal(err)
			}
			first, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("cache_comp", "Bearer lifecycle-test"))
			if err != nil {
				t.Fatal(err)
			}
			query := "CALL { USE cache_comp.local RETURN 1 AS value } RETURN value"
			if _, err := first.Execute(context.Background(), query, nil); err != nil {
				t.Fatal(err)
			}
			regular, err := server.executorCache.Get("cache_local")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := regular.Execute(context.Background(), "RETURN 2 AS value", nil); err != nil {
				t.Fatal(err)
			}
			switch removal {
			case "database":
				err = manager.DropDatabase("cache_local")
			case "alias":
				err = manager.DropAlias("cache_alias")
			case "constituent":
				err = manager.RemoveConstituent("cache_comp", "local")
			case "composite":
				err = manager.DropCompositeDatabase("cache_comp")
			}
			if err != nil {
				t.Fatal(err)
			}
			// Removal goes through the manager, bypassing HTTP invalidation. Only
			// owners whose canonical databases were dropped may be retired.
			if first.StatementCaches().Disposed() != (removal == "composite") || regular.StatementCaches().Disposed() != (removal == "database") {
				t.Fatal("removal retired a surviving canonical owner")
			}
			registry := reflect.ValueOf(manager).Elem().FieldByName("statementCaches")
			expectedOwners := 2
			if removal == "database" || removal == "composite" {
				expectedOwners = 1
			}
			if registry.Len() != expectedOwners {
				t.Fatalf("owners=%d, want%d", registry.Len(), expectedOwners)
			}
			if removal == "database" {
				if _, err := server.executorCache.Get("cache_local"); err == nil {
					t.Fatal("cached executor survived database drop")
				}
				if err := manager.CreateDatabase("cache_local"); err != nil {
					t.Fatal(err)
				}
			}
			next, err := server.executorCache.Get("cache_local")
			if err != nil {
				t.Fatal(err)
			}
			if removal == "database" && (next == regular || next.StatementCaches() == regular.StatementCaches()) {
				t.Fatal("request reused retired executor/owner")
			}
			if _, err := next.Execute(context.Background(), "RETURN 3 AS value", nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStatementCacheFamilyHeimdallLifecycle(t *testing.T) {
	server, _ := setupTestServer(t)
	manager := server.dbManager
	if err := manager.CreateDatabase("cache_heimdall"); err != nil {
		t.Fatal(err)
	}
	router := newHeimdallDBRouter(server.db, manager, nil)
	_, first, _, err := router.executorForDatabase("cache_heimdall")
	if err != nil {
		t.Fatal(err)
	}
	regular, err := server.executorCache.Get("cache_heimdall")
	if err != nil {
		t.Fatal(err)
	}
	if first.StatementCaches() != regular.StatementCaches() {
		t.Fatal("protocols have distinct catalog owners")
	}
	if _, err := first.Execute(context.Background(), "RETURN 1 AS value", nil); err != nil {
		t.Fatal(err)
	}
	if err := manager.DropDatabase("cache_heimdall"); err != nil {
		t.Fatal(err)
	}
	if !first.StatementCaches().Disposed() {
		t.Fatal("drop retained Heimdall cache owner")
	}
	if err := manager.CreateDatabase("cache_heimdall"); err != nil {
		t.Fatal(err)
	}
	_, next, _, err := router.executorForDatabase("cache_heimdall")
	if err != nil {
		t.Fatal(err)
	}
	if next == first || next.StatementCaches() == first.StatementCaches() {
		t.Fatal("Heimdall reused dropped database generation")
	}
}

func TestStatementCacheFamilyHeimdallAliasGeneration(t *testing.T) {
	server, _ := setupTestServer(t)
	manager := server.dbManager
	if err := manager.CreateAlias("cache_heimdall_alias", "nornic"); err != nil {
		t.Fatal(err)
	}
	router := newHeimdallDBRouter(server.db, manager, nil)
	_, canonical, _, err := router.executorForDatabase("nornic")
	if err != nil {
		t.Fatal(err)
	}
	_, selected, _, err := router.executorForDatabase("cache_heimdall_alias")
	if err != nil {
		t.Fatal(err)
	}
	if selected.StatementCaches() != canonical.StatementCaches() {
		t.Fatal("alias has a separate owner")
	}
	if err := manager.DropAlias("cache_heimdall_alias"); err != nil {
		t.Fatal(err)
	}
	if canonical.StatementCaches().Disposed() || !canonical.StatementCacheSelectionValid() || selected.StatementCacheSelectionValid() {
		t.Fatal("alias removal retired canonical owner or retained Heimdall selection")
	}
	if _, _, _, err := router.executorForDatabase("cache_heimdall_alias"); err == nil {
		t.Fatal("Heimdall cached a removed alias")
	}
	if err := manager.CreateAlias("cache_heimdall_alias", "nornic"); err != nil {
		t.Fatal(err)
	}
	_, next, _, err := router.executorForDatabase("cache_heimdall_alias")
	if err != nil {
		t.Fatal(err)
	}
	if next == selected || next.StatementCaches() != canonical.StatementCaches() || !next.StatementCacheSelectionValid() {
		t.Fatal("recreated alias did not acquire a new Heimdall selection")
	}
}
