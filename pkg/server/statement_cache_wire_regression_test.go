package server

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestReview1477R2PublicCrossProtocolDrop(t *testing.T) {
	server, authenticator := setupTestServer(t)
	manager := server.GetDatabaseManager()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(database, statement string) {
		t.Helper()
		response := makeRequest(t, server, "POST", "/db/"+database+"/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": statement}},
		}, token)
		assertTxResponseHasNoErrors(t, response)
	}
	post("system", "CREATE DATABASE review_wire")
	post("review_wire", "MATCH (n:CrossProtocolReview) RETURN count(n) AS c")
	first, err := cypher.DatabaseStatementCaches(manager, "review_wire")
	if err != nil {
		t.Fatal(err)
	}
	review1477AssertPublicVerdict(t, first, "MATCH (n:CrossProtocolReview) RETURN count(n) AS c")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	boltServer := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, manager)
	finished := make(chan error, 1)
	go func() { finished <- boltServer.Serve(listener) }()
	t.Cleanup(func() { _ = boltServer.Close(); <-finished })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	run := func(database, statement string) {
		t.Helper()
		session := driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: database})
		defer session.Close(context.Background())
		result, err := session.Run(context.Background(), statement, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result.Consume(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	run("review_wire", "RETURN 18 AS value")
	run("system", "DROP DATABASE review_wire")
	server.executorsMu.RLock()
	_, retained := server.executors["review_wire"]
	server.executorsMu.RUnlock()
	if retained {
		t.Fatal("cross-protocol drop retained HTTP execution view")
	}
	if !first.Disposed() {
		t.Fatal("Bolt DROP did not retire HTTP's owner")
	}
	post("system", "CREATE DATABASE review_wire")
	post("review_wire", "RETURN 19 AS value")
	second, err := cypher.DatabaseStatementCaches(manager, "review_wire")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || second.Disposed() {
		t.Fatal("recreation reused the retired owner")
	}
	post("system", "DROP DATABASE review_wire")
	if !second.Disposed() {
		t.Fatal("HTTP DROP did not retire the shared owner")
	}
	for i := range 8 {
		post("system", "CREATE DATABASE review_wire")
		run("review_wire", fmt.Sprintf("RETURN %d AS value", i))
		post("system", "DROP DATABASE review_wire")
	}
}

func TestReview1477R2PublicAliasConstituentRecreation(t *testing.T) {
	server, authenticator := setupTestServer(t)
	manager := server.GetDatabaseManager()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	if err := manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "review_local"}); err != nil {
		t.Fatal(err)
	}
	local := multidb.ConstituentRef{Alias: "leaf", DatabaseName: "review_local", Type: "local", AccessMode: "read_write"}
	if err := manager.CreateCompositeDatabase(t.Context(), "review_comp", []multidb.ConstituentRef{
		local,
		{Alias: "remote", DatabaseName: "remote_db", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
	}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := manager.CreateAlias(t.Context(), "review_alias", "review_local"); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"review_local", "review_comp"} {
			query := "USE review_alias MATCH (n:PublicRecreationReview) RETURN count(n) AS c"
			if path == "review_comp" {
				query = "CALL { USE review_comp.leaf MATCH (n:PublicRecreationReview) RETURN count(n) AS c } RETURN c"
			}
			response := makeRequest(t, server, "POST", "/db/"+path+"/tx/commit", map[string]interface{}{
				"statements": []map[string]interface{}{{"statement": query}},
			}, token)
			assertTxResponseHasNoErrors(t, response)
			owner, err := cypher.DatabaseStatementCaches(manager, path)
			if err != nil {
				t.Fatal(err)
			}
			review1477AssertPublicVerdict(t, owner, "MATCH (n:PublicRecreationReview) RETURN count(n) AS c")
		}
		aliasOwner, err := cypher.DatabaseStatementCaches(manager, "review_alias")
		if err != nil {
			t.Fatal(err)
		}
		compOwner, err := cypher.DatabaseStatementCaches(manager, "review_comp")
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.DropAlias(t.Context(), "review_alias"); err != nil {
			t.Fatal(err)
		}
		if aliasOwner.Disposed() {
			t.Fatal("alias removal retired canonical owner")
		}
		review1477AssertNoPublicVerdict(t, aliasOwner, "MATCH (n:PublicRecreationReview) RETURN count(n) AS c")
		if err := manager.RemoveConstituent(t.Context(), "review_comp", "leaf"); err != nil {
			t.Fatal(err)
		}
		if compOwner.Disposed() {
			t.Fatal("constituent removal retired composite owner")
		}
		review1477AssertNoPublicVerdict(t, compOwner, "MATCH (n:PublicRecreationReview) RETURN count(n) AS c")
		if err := manager.AddConstituent(t.Context(), "review_comp", local); err != nil {
			t.Fatal(err)
		}
	}
}

func review1477AssertPublicVerdict(t *testing.T, owner *cypher.StatementCaches, query string) {
	t.Helper()
	shards := reflect.ValueOf(owner).Elem().FieldByName("syntax").Elem().FieldByName("shards")
	for i := range shards.Len() {
		if shards.Index(i).FieldByName("cache").MapIndex(reflect.ValueOf(query)).IsValid() {
			return
		}
	}
	t.Fatalf("public request did not populate its canonical owner's syntax verdict for %q", query)
}

func review1477AssertNoPublicVerdict(t *testing.T, owner *cypher.StatementCaches, query string) {
	shards := reflect.ValueOf(owner).Elem().FieldByName("syntax").Elem().FieldByName("shards")
	for i := range shards.Len() {
		if shards.Index(i).FieldByName("cache").MapIndex(reflect.ValueOf(query)).IsValid() {
			t.Fatal("removed selection retained public verdict")
		}
	}
}
