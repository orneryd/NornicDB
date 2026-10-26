package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync"
)

// This descriptor is replaced at storage issuance/rebinding. It retains actual
// providers; mutable transaction, lease, limit and mode values are never cached.
type storageExecutionBinding struct {
	issued                       storage.Engine
	membership                   storage.MembershipHandle
	committer                    storage.TransactionCommitter
	native                       *transactionStorageWrapper
	pending, schema, lease, mode *transactionStorageWrapper
	identity                     storage.EntityIdentityLookup
	queryMu                      sync.Mutex
	queryReads                   map[storage.Engine]boundQueryRead
}

type boundQueryRead struct {
	read      func(storage.NodeID) (*storage.Node, error)
	headers   func(storage.NodeID, bool) ([]*storage.Edge, bool, error)
	identity  storage.EntityIdentityLookup
	native    *transactionStorageWrapper
	supported bool
	member    bool
}

func (binding *storageExecutionBinding) queryRead(engine storage.Engine) boundQueryRead {
	binding.queryMu.Lock()
	defer binding.queryMu.Unlock()
	if read, ok := binding.queryReads[engine]; ok {
		return read
	}
	member := storage.MembershipForEngine(engine)
	native, _ := member.Adapter.(*transactionStorageWrapper)
	read := boundQueryRead{read: storage.BindQueryNodeRead(engine), headers: storage.BindEdgeHeaderReads(engine), identity: storage.BindEntityIdentityLookup(engine), native: native, supported: storage.HasReadCapability[storage.QueryNodeReader](engine), member: member.Status != storage.MembershipUnowned}
	if binding.queryReads == nil {
		binding.queryReads = make(map[storage.Engine]boundQueryRead)
	}
	binding.queryReads[engine] = read
	return read
}

func bindStorageExecution(engine storage.Engine) *storageExecutionBinding {
	storage.RebindReadView(engine)
	committer, _ := storage.CommitOwnerForEngine(engine)
	binding := &storageExecutionBinding{issued: engine, membership: storage.BindMembership(engine), committer: committer, identity: storage.BindEntityIdentityLookup(engine)}
	refs := binding.membership.Current()
	binding.native, _ = refs.Adapter.(*transactionStorageWrapper)
	binding.pending, _ = refs.PendingActions.(*transactionStorageWrapper)
	binding.schema, _ = refs.SchemaPublication.(*transactionStorageWrapper)
	binding.lease, _ = refs.ReadLease.(*transactionStorageWrapper)
	binding.mode, _ = refs.StatementMode.(*transactionStorageWrapper)
	return binding
}

func bindExecutionEnforcement(engine storage.Engine, binding *storageExecutionBinding) executorQueryLimitProvider {
	if provider, ok := storage.MetadataCapabilityForEngine[executorQueryLimitProvider](engine); ok {
		return provider
	}
	source := binding.membership.Current().Enforcement
	if provider, ok := storage.OwnedCapability[executorQueryLimitProvider](source); ok {
		return provider
	}
	if engine, ok := source.(storage.Engine); ok {
		provider, _ := storage.MetadataCapabilityForEngine[executorQueryLimitProvider](engine)
		return provider
	}
	return nil
}

// Filtered and identity views carry namespace identity independently of data
// reads. Bind its provider once; namespace values still come from that owner.
type carriedNamespace struct {
	once sync.Once
	read func() string
}

func (n *carriedNamespace) For(engine storage.Engine) string {
	n.once.Do(func() {
		if provider, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](engine); ok {
			n.read = provider.Namespace
		} else {
			n.read = func() string { return "" }
		}
	})
	return n.read()
}
func (r *algorithmRelationshipReader) Namespace() string { return r.namespace.For(r.Engine) }
func (r *graphIdentityStorage) Namespace() string        { return r.namespace.For(r.Engine) }

// Deferred visibility publication is an exact operation of this adapter's
// original dependency. Namespace adapters outside it apply their own prefix.

func (e *StorageExecutor) storageMembership() storage.MembershipBinding {
	if e.storageExecution != nil {
		return e.storageExecution.membership.Current()
	}
	return storage.MembershipForEngine(e.storage)
}

func (w *transactionStorageWrapper) StorageMembership() storage.MembershipBinding {
	status := storage.MembershipUnbound
	if w.tx != nil {
		status = storage.MembershipBound
		if !w.tx.IsActive() {
			status = storage.MembershipEnded
		}
	}
	owner := w.lifecycleOwner
	if owner == nil {
		owner = w
	}
	w.membershipOnce.Do(func() {
		if lifecycle, ok := owner.(*TransactionContext); ok {
			w.membershipKind = func() storage.MembershipKind {
				if lifecycle.coordinatorOwned {
					return storage.MembershipCoordinator
				}
				return storage.MembershipNativeExplicit
			}
		} else {
			w.membershipKind = func() storage.MembershipKind { return storage.MembershipImplicit }
		}
	})
	kind := w.membershipKind()
	return storage.MembershipBinding{Status: status, Kind: kind, Boundary: w, Owner: owner, Adapter: w, Controller: w.controller, PendingActions: w, SchemaPublication: w, ReadLease: w, StatementMode: w, Enforcement: w.underlying}
}

func (owner *TransactionContext) StorageMembership() storage.MembershipBinding {
	status := storage.MembershipEnded
	if owner.active {
		status = storage.MembershipBound
	}
	kind := storage.MembershipNativeExplicit
	if owner.coordinatorOwned {
		kind = storage.MembershipCoordinator
	}
	if _, ok := owner.tx.(*fabric.FabricTransaction); ok {
		kind = storage.MembershipCoordinator
	}
	if owner.storageWrapper != nil {
		binding := owner.storageWrapper.StorageMembership()
		binding.Kind = kind
		binding.Status = status
		return binding
	}
	return storage.MembershipBinding{Status: status, Kind: kind, Boundary: owner, Owner: owner, Controller: owner.controller, Enforcement: owner.engine}
}

// A composite fragment carries its actual coordinator, including when a new
// executor is reconstructed without the original context or controller.
type fabricExecutionMembership struct{ transaction *fabric.FabricTransaction }

func (m *fabricExecutionMembership) StorageMembership() storage.MembershipBinding {
	status := storage.MembershipUnbound
	if m.transaction != nil {
		status = storage.MembershipBound
		if m.transaction.State() != "open" {
			status = storage.MembershipEnded
		}
	}
	return storage.MembershipBinding{Status: status, Kind: storage.MembershipCoordinator, Boundary: m, Owner: m.transaction}
}
func (w *transactionStorageWrapper) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	if policy == storage.WrapperMetadataPolicy {
		return w.underlying, true
	}
	return nil, false
}
func (w *transactionStorageWrapper) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	if tx == nil || tx != w.tx {
		return storage.ErrInvalidData
	}
	if !tx.IsActive() {
		return storage.ErrTransactionClosed
	}
	w.ownerOnce.Do(func() { w.commitOwner, _ = storage.CommitOwnerForEngine(w.underlying) })
	if w.commitOwner != nil {
		return w.commitOwner.CommitTransaction(ctx, tx)
	}
	return storage.ErrNotImplemented
}

// Filtered/identity/system views deliberately carry the membership and original
// database enforcement dependency when issuing a new data view. They do not
// expose an underlying read or grant lifecycle controller authority.
func (r *algorithmRelationshipReader) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return r.Engine, policy != storage.CommitOwnerPolicy
}
func (r *graphIdentityStorage) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return r.Engine, policy != storage.CommitOwnerPolicy
}
func (r systemGraphView) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return r.Engine, policy != storage.CommitOwnerPolicy
}

func (w *transactionStorageWrapper) borrowReadLease(ctx context.Context) context.Context {
	w.leaseMu.Lock()
	lease := w.leaseContext
	w.leaseMu.Unlock()
	return storage.InheritQueryRevealScopes(ctx, lease)
}
func (w *transactionStorageWrapper) borrowStatementMode(ctx context.Context) context.Context {
	w.leaseMu.Lock()
	mode := w.statementMode
	w.leaseMu.Unlock()
	if mode != nil && mode.active.Load() && ctx.Value(resolvedStatementModeKey{}) == nil {
		return context.WithValue(ctx, resolvedStatementModeKey{}, mode)
	}
	return ctx
}
func (w *transactionStorageWrapper) bindReadLease(ctx context.Context, release func()) func() {
	w.leaseMu.Lock()
	previous := w.leaseContext
	w.leaseContext = ctx
	w.leaseMu.Unlock()
	return func() { w.leaseMu.Lock(); w.leaseContext = previous; w.leaseMu.Unlock(); release() }
}

func (e *StorageExecutor) boundCommitOwner() storage.TransactionCommitter {
	if e.storageExecution != nil {
		return e.storageExecution.committer
	}
	owner, _ := storage.CommitOwnerForEngine(e.storage)
	return owner
}

func (e *StorageExecutor) boundExecutionContext(ctx context.Context) (context.Context, error) {
	binding := e.storageMembership()
	if binding.Status == storage.MembershipEnded {
		return ctx, storage.ErrTransactionClosed
	}
	if binding.Status == storage.MembershipUnbound {
		return ctx, storage.ErrNotImplemented
	}
	if e.storageExecution != nil {
		if owner := e.storageExecution.lease; owner != nil {
			ctx = owner.borrowReadLease(ctx)
		}
		if owner := e.storageExecution.mode; owner != nil {
			ctx = owner.borrowStatementMode(ctx)
		}
	} else {
		if owner, ok := binding.ReadLease.(*transactionStorageWrapper); ok {
			ctx = owner.borrowReadLease(ctx)
		}
		if owner, ok := binding.StatementMode.(*transactionStorageWrapper); ok {
			ctx = owner.borrowStatementMode(ctx)
		}
	}
	return ctx, nil
}

func (e *StorageExecutor) identityLookup(engine storage.Engine) storage.EntityIdentityLookup {
	if e.storageExecution == nil {
		return storage.BindEntityIdentityLookup(engine)
	}
	if e.storageExecution.issued == engine {
		return e.storageExecution.identity
	}
	return e.storageExecution.queryRead(engine).identity
}
func (e *StorageExecutor) nativeMemberFor(engine storage.Engine) (*transactionStorageWrapper, bool) {
	var member *transactionStorageWrapper
	if e.storageExecution != nil {
		if e.storageExecution.issued == engine {
			member = e.storageExecution.native
		} else {
			member = e.storageExecution.queryRead(engine).native
		}
	} else {
		member, _ = storage.MembershipForEngine(engine).Adapter.(*transactionStorageWrapper)
	}
	return member, member != nil
}
func transactionMemberForEngine(engine storage.Engine) (*transactionStorageWrapper, bool) {
	member, ok := storage.MembershipForEngine(engine).Adapter.(*transactionStorageWrapper)
	return member, ok
}

type carriedIdentityLookup struct {
	once   sync.Once
	lookup storage.EntityIdentityLookup
}

func (l *carriedIdentityLookup) For(engine storage.Engine) storage.EntityIdentityLookup {
	l.once.Do(func() { l.lookup = storage.BindEntityIdentityLookup(engine) })
	return l.lookup
}

func (e *StorageExecutor) schemaMember() (*transactionStorageWrapper, bool) {
	if e.storageExecution != nil {
		return e.storageExecution.schema, e.storageExecution.schema != nil
	}
	member, ok := e.storageMembership().SchemaPublication.(*transactionStorageWrapper)
	return member, ok
}
func (e *StorageExecutor) pendingMember() (*transactionStorageWrapper, bool) {
	if e.storageExecution != nil {
		return e.storageExecution.pending, e.storageExecution.pending != nil
	}
	member, ok := e.storageMembership().PendingActions.(*transactionStorageWrapper)
	return member, ok
}

func (w *transactionStorageWrapper) SetManualSuppression(kind storage.StoredEntityKind, id string, suppressed bool) error {
	if owner, ok := storage.OwnedCapability[storage.ManualSuppressionEngine](w.underlying); ok {
		return owner.SetManualSuppression(kind, id, suppressed)
	}
	return storage.ErrNotImplemented
}
