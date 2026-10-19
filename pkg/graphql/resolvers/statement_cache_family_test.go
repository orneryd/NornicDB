package resolvers

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testutil"
	"reflect"
	"testing"
)

func TestStatementCacheFamilyGraphQLRequests(t *testing.T) {
	for _, withDB := range []bool{true, false} {
		name := "resolver owner"
		if withDB {
			name = "database owner"
		}
		t.Run(name, func(t *testing.T) {
			db := testDB(t)
			manager := testutil.DatabaseManager(t, db)
			resolver := NewResolver(db, manager)
			if !withDB {
				resolver.DB = nil
			}
			first, err := resolver.getCypherExecutor(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Execute(context.Background(), "RETURN 1 AS value", nil); err != nil {
				t.Fatal(err)
			}
			second, err := resolver.getCypherExecutor(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			if first == second {
				t.Fatal("GraphQL requests shared an execution-state object")
			}
			for _, field := range []string{"syntaxValidationCache", "upperQueryCache"} {
				left := reflect.ValueOf(first).Elem().FieldByName(field).Pointer()
				right := reflect.ValueOf(second).Elem().FieldByName(field).Pointer()
				if left == 0 || left != right {
					t.Errorf("%s warmed by the first request was lost: %x != %x", field, left, right)
				}
			}
		})
	}
}
