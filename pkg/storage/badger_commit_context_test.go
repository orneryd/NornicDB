package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Commit-time constraint validation must stop when the commit context is
// cancelled (TERMINATE TRANSACTIONS, a disconnected client), and nothing of the
// transaction may be stored. Before #1115 a statement whose commit validated
// through the label-scan fallback kept running for minutes after TERMINATE.
func TestBadgerTransactionCommitContextCancelsValidation(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()

	schema := engine.GetSchemaForNamespace("test")
	// A UNIQUE constraint whose value cache was never rebuilt validates through
	// the label scan, the slow path the hung statements were in.
	if err := schema.AddUniqueConstraint("unique_k", "Item", "k"); err != nil {
		t.Fatalf("AddUniqueConstraint failed: %v", err)
	}

	tx1, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	for i := 0; i < 300; i++ {
		if _, err := tx1.CreateNode(&Node{ID: NodeID(prefixTestID(fmt.Sprintf("item-%d", i))), Labels: []string{"Item"}, Properties: map[string]interface{}{"k": int64(i)}}); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}
	if err := tx1.CommitContext(context.Background()); err != nil {
		t.Fatalf("CommitContext with a live context failed: %v", err)
	}

	tx2, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	for i := 1000; i < 2000; i++ {
		if _, err := tx2.CreateNode(&Node{ID: NodeID(prefixTestID(fmt.Sprintf("item-%d", i))), Labels: []string{"Item"}, Properties: map[string]interface{}{"k": int64(i)}}); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scans := 0
	restore := setUniqueConstraintScanHook(func() {
		scans++
		cancel() // the statement is terminated while its commit validates
	})
	defer restore()

	err = tx2.CommitContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitContext error = %v, want context.Canceled", err)
	}
	if scans == 0 {
		t.Fatalf("test setup: commit validation did not take the label-scan path")
	}
	if scans > commitCancelPollEvery+1 {
		t.Fatalf("validation ran %d label scans after cancellation, want it to stop within one poll interval", scans)
	}
	if _, err := engine.GetNode(NodeID(prefixTestID("item-1500"))); err == nil {
		t.Fatalf("a node of the cancelled commit was stored")
	}

	// An already-cancelled context fails before any work.
	tx3, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if _, err := tx3.CreateNode(&Node{ID: NodeID(prefixTestID("item-x")), Labels: []string{"Item"}, Properties: map[string]interface{}{"k": int64(99999)}}); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}
	if err := tx3.CommitContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitContext(cancelled) error = %v, want context.Canceled", err)
	}
	_ = tx3.Rollback()
}
