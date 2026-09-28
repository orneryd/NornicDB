package nornicdb

// The base DB executor is shared by every embedded caller of Cypher /
// ExecuteCypher. A bare transaction command must not open a transaction on
// it: a concurrent caller's statements would then run inside another
// caller's transaction. Embedded callers that want explicit transactions
// create their own session executor.

import (
	"context"
	"sync"
	"testing"
)

func TestDBCypherRejectsBareTransactionCommandsOnSharedExecutor(t *testing.T) {
	db := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	for _, query := range []string{"BEGIN", "COMMIT", "ROLLBACK", "BEGIN TRANSACTION"} {
		if _, err := db.Cypher(ctx, query, nil); err == nil {
			t.Fatalf("db.Cypher(%q) succeeded; want rejection on the shared executor", query)
		}
		if _, err := db.ExecuteCypher(ctx, query, nil); err == nil {
			t.Fatalf("db.ExecuteCypher(%q) succeeded; want rejection on the shared executor", query)
		}
	}

	// The shared executor is not inside a transaction afterwards and keeps
	// serving auto-commit statements.
	if _, err := db.Cypher(ctx, "CREATE (:SharedProbe {v: 1})", nil); err != nil {
		t.Fatalf("auto-commit CREATE after rejection failed: %v", err)
	}
	rows, err := db.Cypher(ctx, "MATCH (n:SharedProbe) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("auto-commit MATCH after rejection failed: %v", err)
	}
	if got := rows[0]["c"]; got != int64(1) {
		t.Fatalf("count = %v; want 1", got)
	}

	// The one-statement script form keeps working (it runs on a private
	// clone).
	if _, err := db.Cypher(ctx, "BEGIN CREATE (:ScriptProbe) COMMIT", nil); err != nil {
		t.Fatalf("one-statement script failed: %v", err)
	}
	rows, err = db.Cypher(ctx, "MATCH (n:ScriptProbe) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("script probe read failed: %v", err)
	}
	if got := rows[0]["c"]; got != int64(1) {
		t.Fatalf("script probe count = %v; want 1", got)
	}
}

func TestDBCypherConcurrentCallersStayIsolated(t *testing.T) {
	db := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	const writers = 8
	const rounds = 20
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				if _, err := db.Cypher(ctx, "BEGIN CREATE (:ConcurrentProbe) COMMIT", nil); err != nil {
					errs <- err
					return
				}
				if _, err := db.Cypher(ctx, "MATCH (n:ConcurrentProbe) RETURN count(n)", nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent caller failed: %v", err)
	}
	rows, err := db.Cypher(ctx, "MATCH (n:ConcurrentProbe) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("final count failed: %v", err)
	}
	if got := rows[0]["c"]; got != int64(writers*rounds) {
		t.Fatalf("final count = %v; want %d (no acknowledged write lost)", got, writers*rounds)
	}
}
