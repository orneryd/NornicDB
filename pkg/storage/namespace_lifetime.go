package storage

import "fmt"

// namespaceLifetimes is immutable after publication. Retirements survive a
// same-name recreation for the lifetime of this engine; transactions cannot
// survive closing their engine, so no durable format or restart state is needed.
type namespaceLifetimes struct {
	revision uint64
	retired  map[string]uint64
}

func (b *BadgerEngine) namespaceLifetimeRevision() uint64 {
	if lifetimes := b.namespaceLifetimes.Load(); lifetimes != nil {
		return lifetimes.revision
	}
	return 0
}

// retireNamespace is the only writer, called while deletion holds the exclusive
// publication barrier. Retire at return from physical deletion, including a
// partial failure, so transactions opened during deletion also become stale.
func (b *BadgerEngine) retireNamespace(namespace string) {
	next := &namespaceLifetimes{revision: 1, retired: make(map[string]uint64)}
	if previous := b.namespaceLifetimes.Load(); previous != nil {
		next.revision = previous.revision + 1
		for name, revision := range previous.retired {
			next.retired[name] = revision
		}
	}
	next.retired[namespace] = next.revision
	b.namespaceLifetimes.Store(next)
}

// Commit holds the publication barrier, so retirement cannot change between
// this check and synchronous publication or the buffered WAL acknowledgement.
func (tx *BadgerTransaction) checkNamespaceLifetimeLocked() error {
	if lifetimes := tx.engine.namespaceLifetimes.Load(); lifetimes != nil &&
		lifetimes.retired[tx.namespace] > tx.beginNamespaceRevision {
		return fmt.Errorf("%w: namespace %q was dropped after transaction began", ErrConflict, tx.namespace)
	}
	return nil
}
