package resolvers

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"reflect"
	"testing"
)

func TestStatementCacheFamilyGraphQLDropCycles(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, manager)
	var executor *cypher.StorageExecutor
	fixtureowner.RunCatalogDropCycles(t, 64, "cache_graphql_",
		func(name string) error {
			return manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name})
		},
		func(name string) error {
			var err error
			executor, err = resolver.getCypherExecutor(context.Background(), name)
			if err != nil {
				return err
			}
			_, err = executor.Execute(context.Background(), "RETURN 1 AS value", nil)
			return err
		},
		func(name string) error {
			if err := manager.DropDatabase(t.Context(), name); err != nil {
				return err
			}
			if !executor.StatementCaches().Disposed() {
				t.Fatal("dropped GraphQL database retained an owner")
			}
			registry := reflect.ValueOf(manager).Elem().FieldByName("statementCaches")
			if registry.Len() != 0 {
				t.Fatalf("retained %d dropped database cache owners", registry.Len())
			}
			return nil
		},
	)
}
