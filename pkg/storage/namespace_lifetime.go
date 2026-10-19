package storage

import "fmt"

// namespaceLifetimes is immutable after publication. Retirements survive a
// same-name recreation. Catalog retirement is also persisted, so replaying an
// older catalog snapshot cannot resurrect a dropped lifetime after restart.
type namespaceLifetimes struct {
	revision       uint64
	retired        map[string]uint64
	catalogRetired map[string]uint64
	active         map[string]*NamespaceDrop
}

func (b *BadgerEngine) namespaceLifetimeRevision() uint64 {
	if lifetimes := b.namespaceLifetimes.Load(); lifetimes != nil {
		return lifetimes.revision
	}
	return 0
}

// Writers publish copied state under the exclusive publication barrier.
// Retirement advances at the DROP commit point, before physical deletion.
func (b *BadgerEngine) copyNamespaceLifetimes() *namespaceLifetimes {
	next := &namespaceLifetimes{retired: make(map[string]uint64), catalogRetired: make(map[string]uint64), active: make(map[string]*NamespaceDrop)}
	if previous := b.namespaceLifetimes.Load(); previous != nil {
		next.revision = previous.revision
		for name, revision := range previous.retired {
			next.retired[name] = revision
		}
		for name, revision := range previous.catalogRetired {
			next.catalogRetired[name] = revision
		}
		for name, op := range previous.active {
			next.active[name] = op
		}
	}
	return next
}

func (b *BadgerEngine) retireNamespace(namespace string, catalogRevision uint64) error {
	next := b.copyNamespaceLifetimes()
	next.revision++
	next.retired[namespace] = next.revision
	next.catalogRetired[namespace] = catalogRevision
	b.observeCatalogRevision(catalogRevision)
	err := b.persistNamespaceLifetime(namespace, catalogRevision)
	b.namespaceLifetimes.Store(next)
	return err
}

// Commit holds the publication barrier, so retirement cannot change between
// this check and synchronous publication or the buffered WAL acknowledgement.
func (tx *BadgerTransaction) checkNamespaceLifetimeLocked() error {
	if tx.logicalNamespace != "" && tx.logicalNamespace != tx.namespace {
		if err := tx.engine.checkNamespaceLifetimeRevision(tx.logicalNamespace, tx.beginNamespaceRevision); err != nil {
			return err
		}
	}
	return tx.engine.checkNamespaceLifetimeRevision(tx.namespace, tx.beginNamespaceRevision)
}

func (b *BadgerEngine) checkNamespaceLifetimeRevision(namespace string, revision uint64) error {
	if lifetimes := b.namespaceLifetimes.Load(); lifetimes != nil {
		if lifetimes.active[namespace] != nil || lifetimes.retired[namespace] > revision {
			return fmt.Errorf("%w: namespace %q was dropped or is retiring after operation began", ErrConflict, namespace)
		}
	}
	return nil
}
