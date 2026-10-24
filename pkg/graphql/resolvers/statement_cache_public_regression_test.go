package resolvers

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/graphql/models"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"testing"
)

func TestRule73GraphQLPublicLifetime(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	resolver := NewResolver(nil, manager)
	owner, err := cypher.DatabaseStatementCaches(manager, "nornic")
	if err != nil {
		t.Fatal(err)
	}
	database := "nornic"
	run := func(statement string) {
		t.Helper()
		if _, err := resolver.Query().Cypher(context.Background(), models.CypherInput{Statement: statement, Database: &database}); err != nil {
			t.Fatal(err)
		}
	}
	run("RETURN 89 AS live")
	for i := range 64 {
		name := fmt.Sprintf("gate_graphql_%d", i)
		if err := manager.CreateDatabase(name); err != nil {
			t.Fatal(err)
		}
		alias := name + "_alias"
		if err := manager.CreateAlias(alias, name); err != nil {
			t.Fatal(err)
		}
		body := fmt.Sprintf("MATCH (n:GraphQLGate%d) RETURN count(n) AS c", i)
		run("USE " + alias + " " + body)
		if err := manager.DropAlias(alias); err != nil {
			t.Fatal(err)
		}
		shards := reflect.ValueOf(owner).Elem().FieldByName("syntax").Elem().FieldByName("shards")
		for j := range shards.Len() {
			if shards.Index(j).FieldByName("cache").MapIndex(reflect.ValueOf(body)).IsValid() {
				t.Fatal("GraphQL retained removed alias verdict")
			}
		}
		if err := manager.DropDatabase(name); err != nil {
			t.Fatal(err)
		}
		if owner.Disposed() {
			t.Fatal("GraphQL removal retired default owner")
		}
	}
	run("RETURN 89 AS live")
	if count := reflect.ValueOf(manager).Elem().FieldByName("statementCaches").Len(); count != 1 {
		t.Fatalf("retained %d owners, want default only", count)
	}
}
