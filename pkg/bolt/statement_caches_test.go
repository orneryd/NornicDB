package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func transactionalCypherExecutor(t *testing.T, session *Session, dbName string) *boltQueryExecutorAdapter {
	t.Helper()
	executor, err := session.getTransactionalExecutorForDatabase(dbName)
	if err != nil {
		t.Fatalf("getTransactionalExecutorForDatabase: %v", err)
	}
	adapter, ok := executor.(*transactionalBoltQueryExecutorAdapter)
	if !ok {
		t.Fatalf("unexpected transactional executor %T", executor)
	}
	return &adapter.boltQueryExecutorAdapter
}

// Every Bolt executor of a database uses the server's statement caches for
// it, so explicit transactions of bearer-authenticated sessions, which build
// a new executor each time and bypass the executor cache, reuse the syntax
// verdicts of earlier ones instead of validating every statement again.
func TestBearerExplicitTransactionsShareStatementCaches(t *testing.T) {
	store := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = store.Close() })
	manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
	server := &Server{dbManager: manager, executors: make(map[string]QueryExecutor)}
	bearerA := &Session{server: server, forwardedAuthHeader: "Bearer token-a"}
	bearerB := &Session{server: server, forwardedAuthHeader: "Bearer token-b"}
	plain := &Session{server: server}
	shared := server.statementCachesFor("nornic")

	first := transactionalCypherExecutor(t, bearerA, "nornic")
	if _, err := first.executor.Execute(context.Background(), "MATCH (n:BearerShare) RETURN count(n) AS c", nil); err != nil {
		t.Fatal(err)
	}
	for name, session := range map[string]*Session{"same bearer session": bearerA, "other bearer session": bearerB, "session without bearer": plain} {
		next := transactionalCypherExecutor(t, session, "nornic")
		if next.executor == first.executor {
			t.Fatalf("%s: expected a new executor per transaction", name)
		}
		if next.executor.StatementCaches() != shared {
			t.Fatalf("%s: transaction executor does not use the database's statement caches", name)
		}
	}
	if first.executor.StatementCaches() != shared {
		t.Fatalf("the first transaction executor does not use the database's statement caches")
	}
	base, err := plain.getExecutorForDatabase("nornic")
	if err != nil {
		t.Fatal(err)
	}
	if base.(*boltQueryExecutorAdapter).executor.StatementCaches() != shared {
		t.Fatalf("the cached database executor does not use the database's statement caches")
	}
	if server.statementCachesFor("other") == shared {
		t.Fatalf("another database shares the statement caches")
	}
}
