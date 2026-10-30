package bolt

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Adopted from the #1382 review: bearer executors must not leave cache roots
// after repeated create/query/drop cycles, including drops through another adapter.
func TestStatementCacheFamilyDroppedDatabaseCaches(t *testing.T) {
	inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	manager, err := multidb.NewDatabaseManager(t.Context(), inner, nil)
	if err != nil {
		t.Fatal(err)
	}
	fixtureowner.Own(t, manager)
	server := &Server{dbManager: manager, executors: make(map[string]QueryExecutor)}
	adapter := &boltDatabaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(manager)}
	session := &Session{server: server, forwardedAuthHeader: "Bearer lifecycle-test"}
	var executor QueryExecutor
	fixtureowner.RunCatalogDropCycles(t, 64, "cache_tenant_",
		func(name string) error {
			return adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name})
		},
		func(name string) error {
			var err error
			executor, err = session.getExecutorForDatabase(name)
			if err != nil {
				return err
			}
			if _, err := executor.Execute(context.Background(), "RETURN 1 AS value", nil); err != nil {
				return err
			}
			return nil
		},
		func(name string) error {
			if err := adapter.DropDatabase(t.Context(), name); err != nil {
				return err
			}
			retired := executor.(*boltQueryExecutorAdapter).executor
			// Inspect existing cache fields so this regression also runs before the owner API.
			assertDroppedBoltTextCacheEmpty(t, retired)
			return nil
		},
	)
	if len(server.executors) != 0 {
		t.Fatal("bearer path cached execution state")
	}
	registry := reflect.ValueOf(manager).Elem().FieldByName("statementCaches")
	if registry.IsValid() && registry.Len() != 0 {
		t.Fatalf("retained %d dropped database owners", registry.Len())
	}
}

func assertDroppedBoltTextCacheEmpty(t *testing.T, executor *cypher.StorageExecutor) {
	t.Helper()
	value := reflect.ValueOf(executor).Elem()
	upper := value.FieldByName("upperQueryCache").Elem().FieldByName("cache").Len()
	syntax := value.FieldByName("syntaxValidationCache").Elem().FieldByName("shards")
	entries := upper
	for i := range syntax.Len() {
		entries += syntax.Index(i).FieldByName("cache").Len()
	}
	if entries != 0 {
		t.Fatalf("dropped database retained %d statement-text entries", entries)
	}
}
