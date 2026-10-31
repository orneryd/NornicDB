package testfixture

import (
	"errors"
	"testing"
)

// NamespaceTransaction opens a test transaction, binds its namespace when
// provided, and owns rollback even when an assertion terminates its caller.
// A rollback failure is reported; closedError is the error a transaction the
// test already ended returns. For example:
// tx := NamespaceTransaction(t, engine.BeginTransaction, "tenant", storage.ErrTransactionClosed).
func NamespaceTransaction[T interface {
	SetNamespace(string) error
	Rollback() error
}](t testing.TB, begin func() (T, error), namespace string, closedError error) T {
	t.Helper()
	transaction, err := begin()
	if err != nil {
		t.Fatalf("begin fixture transaction: %v", err)
	}
	Own(t, CloseFunc(func() error {
		if err := transaction.Rollback(); err != nil && !errors.Is(err, closedError) {
			return err
		}
		return nil
	}))
	if namespace != "" {
		if err := transaction.SetNamespace(namespace); err != nil {
			t.Fatalf("bind fixture namespace: %v", err)
		}
	}
	return transaction
}
