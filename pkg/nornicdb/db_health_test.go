package nornicdb

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// openTestDB returns a fresh temp-dir-backed DB for HealthCheck tests.
//
// The package's existing tests (db_test.go) all use the pattern
// `Open(t.TempDir(), nil)` — DefaultConfig() is applied internally when the
// passed config is nil. We mirror that pattern here so HealthCheck tests
// stay aligned with the rest of the package's test conventions.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func TestDB_HealthCheck_NilWhenOpen(t *testing.T) {
	db := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	if err := db.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck on open DB returned error: %v", err)
	}
}

func TestDB_HealthCheck_FailsWhenClosed(t *testing.T) {
	db := openTestDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err := db.HealthCheck(context.Background())
	if err == nil {
		t.Fatalf("HealthCheck on closed DB returned nil; expected error wrapping storage.ErrStorageClosed")
	}
	if !errors.Is(err, storage.ErrStorageClosed) {
		t.Fatalf("HealthCheck on closed DB: got %v, want errors.Is(err, storage.ErrStorageClosed)", err)
	}
}

func TestDB_HealthCheck_RespectsContextCancellation(t *testing.T) {
	db := openTestDB(t)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := db.HealthCheck(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("HealthCheck with cancelled ctx: got %v, want errors.Is(err, context.Canceled)", err)
	}
}
