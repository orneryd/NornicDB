package testfixture

import (
	"context"
	"testing"
)

// NamespaceTransaction opens a test transaction with the begin request its
// caller owns, binds its namespace when provided, and owns rollback even when an
// assertion terminates its caller. A test that needs an autocommit schema
// statement passes storage.AutocommitSchemaBegin here only through
// SchemaManager.BeginSchemaTransaction; this helper opens the transaction it is
// given.
// For example: tx := NamespaceTransaction(t, engine.BeginTransaction, storage.OrdinaryBegin, "tenant").
func NamespaceTransaction[T interface {
	SetNamespace(string) error
	Rollback() error
}, R comparable](t testing.TB, begin func(context.Context, R) (T, error), request R, namespace string) T {
	t.Helper()
	transaction, err := begin(t.Context(), request)
	if err != nil {
		t.Fatalf("begin fixture transaction: %v", err)
	}
	t.Cleanup(func() { _ = transaction.Rollback() })
	if namespace != "" {
		if err := transaction.SetNamespace(namespace); err != nil {
			t.Fatalf("bind fixture namespace: %v", err)
		}
	}
	return transaction
}
