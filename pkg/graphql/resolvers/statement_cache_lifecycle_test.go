package resolvers

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testutil"
	"reflect"
	"testing"
)

func TestStatementCacheFamilyGraphQLDropCycles(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	resolver := NewResolver(db, manager)
	for i := range 64 {
		name := fmt.Sprintf("cache_graphql_%d", i)
		if err := manager.CreateDatabase(name); err != nil {
			t.Fatal(err)
		}
		executor, err := resolver.getCypherExecutor(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := executor.Execute(context.Background(), "RETURN 1 AS value", nil); err != nil {
			t.Fatal(err)
		}
		if err := manager.DropDatabase(name); err != nil {
			t.Fatal(err)
		}
		if !executor.StatementCaches().Disposed() {
			t.Fatal("dropped GraphQL database retained an owner")
		}
		registry := reflect.ValueOf(manager).Elem().FieldByName("statementCaches")
		if registry.Len() != 0 {
			t.Fatalf("retained %d dropped database cache owners", registry.Len())
		}
	}
}
