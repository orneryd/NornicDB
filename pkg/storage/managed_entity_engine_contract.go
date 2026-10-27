package storage

// ManagedEntityEngine is the atomic publication capability required by managed
// database writes that enforce storage quotas, track exact bytes, or cross a
// journal/replication boundary. Writable database catalogs require native entity
// preparation and authoritative EntityCommitSubmitter receipts, even when graph
// quotas are unlimited. Managed graph writes additionally require this complete
// accounting/observation contract; a catalog-capable engine must not be treated
// as implicitly providing those graph capabilities.
//
// A transparent EngineUnwrapper may expose these capabilities through its own
// backing engine. A wrapper with per-operation write policy additionally uses
// EntityWriteBoundary so staged writes retain that policy on the same store.
type ManagedEntityEngine interface {
	Engine
	BeginTransaction() (*BadgerTransaction, error)
	InitializeStoredEntityObserver(string, func(StoredEntitySnapshot) (StoredEntityObserver, error), StoredEntityReadBoundary) error
	EntityCommitSubmitter
	ManagedEntityRuntimeRegistry
}

// EntityWriteBoundary preserves a decorator's per-operation admission policy
// while the supplied mutation stages in an atomic native transaction. Return a
// policy error without calling apply, or invoke apply exactly once and return
// its result. The callback must not issue an independent storage write.
//
// For example, a tenant policy can reject an Operation's node ID and otherwise
// return apply(). Nested policy wrappers are invoked from outermost to innermost.
type EntityWriteBoundary interface {
	WithEntityChange(Operation, func() error) error
}

// ManagedEntityRuntimeRegistry keeps one usage/admission owner and its claimed
// preparation lifetime on the physical engine, shared by namespace views.
type ManagedEntityRuntimeRegistry interface {
	GetOrAttachManagedEntityRuntime(string, any) any
	RegisterManagedEntityPreparer(string, ManagedEntityPrepare, ManagedEntityInitialize)
	RetireManagedEntityRuntime(string)
}
