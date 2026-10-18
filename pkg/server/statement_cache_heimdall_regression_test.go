package server

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"testing"
)

func TestRule73HeimdallPublicColdAdmission(t *testing.T) {
	for _, form := range []string{"direct", "use", "fabric"} {
		t.Run(form, func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Close()
			if err := manager.CreateDatabase("gate_heimdall_leaf"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateAlias("gate_heimdall_alias", "gate_heimdall_leaf"); err != nil {
				t.Fatal(err)
			}
			if err := manager.CreateCompositeDatabase("gate_heimdall_comp", []multidb.ConstituentRef{{Alias: "leaf", DatabaseName: "gate_heimdall_leaf", Type: "local", AccessMode: "read_write"}}); err != nil {
				t.Fatal(err)
			}
			owners := []*cypher.StatementCaches{}
			for _, name := range []string{"nornic", "gate_heimdall_leaf"} {
				owner, err := cypher.DatabaseStatementCaches(manager, name)
				if err != nil {
					t.Fatal(err)
				}
				owners = append(owners, owner)
			}
			router := newHeimdallDBRouter(nil, manager, nil)
			const body = "MATCH (n:HeimdallGate) RETURN count(n) AS c"
			query := body
			if form == "use" {
				query = "USE gate_heimdall_alias " + body
			}
			if form == "fabric" {
				query = "CALL { USE gate_heimdall_comp.leaf " + body + " } RETURN c"
			}
			ctx := context.Background()
			if _, err := router.Query(ctx, "nornic", query, nil); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, owner := range owners {
				shards := reflect.ValueOf(owner).Elem().FieldByName("syntax").Elem().FieldByName("shards")
				for i := range shards.Len() {
					if shards.Index(i).FieldByName("cache").MapIndex(reflect.ValueOf(body)).IsValid() {
						found = true
					}
				}
			}
			if !found {
				t.Fatal("public cold Heimdall request populated no catalog-owned verdict")
			}
		})
	}
}
