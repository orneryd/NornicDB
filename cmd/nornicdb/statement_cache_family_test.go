package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestStatementCacheFamilyCLIBoltTransactionCaches(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := db.GetCypherExecutor()
	if _, err := base.Execute(context.Background(), "MATCH (n:StatementCacheFamily) RETURN count(n) AS c", nil); err != nil {
		t.Fatal(err)
	}
	tx, err := newTxScopedExecutor(db, "nornic")
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"syntaxValidationCache", "upperQueryCache"} {
		left := reflect.ValueOf(base).Elem().FieldByName(field).Pointer()
		right := reflect.ValueOf(tx).Elem().FieldByName(field).Pointer()
		if left == 0 || left != right {
			t.Errorf("%s warmed in database lost in CLI transaction factory: %x != %x", field, left, right)
		}
	}
}

func TestStatementCacheFamilyCLIClosesOwner(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := newTxScopedExecutor(db, "nornic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Execute(context.Background(), "RETURN 1 AS value", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !tx.StatementCaches().Disposed() {
		t.Fatal("CLI database Close retained shared statement caches")
	}
}
