package bolt

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type review1477BearerAuthenticator struct{}

func (review1477BearerAuthenticator) Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error) {
	if scheme != "bearer" || credentials != "review-token" {
		return nil, fmt.Errorf("unexpected credentials")
	}
	return &BoltAuthResult{Authenticated: true, Username: "review", PrincipalID: "review", Roles: []string{"admin"}}, nil
}

func TestReview1477R2PublicBearerTransactions(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	config := DefaultConfig()
	config.Authenticator = review1477BearerAuthenticator{}
	server := NewWithDatabaseManager(config, nil, manager)
	port := startBoltTestServer(t, server)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.BearerAuth("review-token"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	ctx := context.Background()
	var owner *cypher.StatementCaches
	for range 4 {
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		tx, err := session.BeginTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.Run(ctx, "MATCH (n:WireBearerReview) RETURN count(n) AS c", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result.Consume(ctx); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := session.Close(ctx); err != nil {
			t.Fatal(err)
		}
		next, err := cypher.DatabaseStatementCaches(manager, "nornic")
		if err != nil {
			t.Fatal(err)
		}
		if owner != nil && next != owner {
			t.Fatal("successive bearer transactions changed owners")
		}
		shards := reflect.ValueOf(next).Elem().FieldByName("syntax").Elem().FieldByName("shards")
		found := false
		for i := range shards.Len() {
			if shards.Index(i).FieldByName("cache").MapIndex(reflect.ValueOf("MATCH (n:WireBearerReview) RETURN count(n) AS c")).IsValid() {
				found = true
			}
		}
		if !found {
			t.Fatal("public bearer transaction did not populate the canonical owner's syntax verdict")
		}
		owner = next
	}
	server.executorsMu.RLock()
	defer server.executorsMu.RUnlock()
	if len(server.executors) != 0 {
		t.Fatal("bearer path cached execution/auth state")
	}
}
