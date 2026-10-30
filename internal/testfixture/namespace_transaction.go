package testfixture

import "testing"

// NamespaceTransaction opens a test transaction, binds its namespace when
// provided, and owns rollback even when an assertion terminates its caller.
// For example: tx := NamespaceTransaction(t, engine.BeginTransaction, "tenant").
func NamespaceTransaction[T interface {
	SetNamespace(string) error
	Rollback() error
}](t testing.TB, begin func() (T, error), namespace string) T {
	t.Helper()
	transaction, err := begin()
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
