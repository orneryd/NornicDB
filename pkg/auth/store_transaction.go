package auth

import (
	"context"
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// WithStoreTransaction runs system-node mutations in the same BadgerTransaction
// used by Cypher GRANT/REVOKE. An explicit transaction's engine supplies its
// existing view; otherwise this function owns begin, commit and rollback.
// Callers publish their in-memory state only after this function succeeds.
func WithStoreTransaction(ctx context.Context, engine storage.Engine, mutate func(storage.Engine) error) error {
	if runner, ok := storage.OwnedCapability[interface {
		WithStoreTransaction(context.Context, func(storage.Engine) error) error
	}](engine); ok {
		return runner.WithStoreTransaction(ctx, mutate)
	}
	if member := storage.MembershipForEngine(engine); member.Status != storage.MembershipUnowned {
		if member.Status == storage.MembershipEnded {
			return storage.ErrTransactionClosed
		}
		if runner, ok := storage.OwnedCapability[interface {
			WithStoreTransaction(context.Context, func(storage.Engine) error) error
		}](member.Adapter); ok {
			return runner.WithStoreTransaction(ctx, func(storage.Engine) error { return mutate(engine) })
		}
		return storage.ErrNotImplemented
	}
	namespace := ""
	if scoped, ok := storage.ViewMetadataForEngine[interface{ Namespace() string }](engine); ok {
		namespace = scoped.Namespace()
	}
	beginner, ok := storage.TransactionOwnerForEngine[interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}](engine)
	if !ok {
		return storage.ErrNotImplemented
	}
	inner := engine
	if drainer, ok := storage.OwnedCapability[interface{ FlushWriteBehind() error }](inner); ok {
		if err := drainer.FlushWriteBehind(); err != nil {
			return err
		}
	}
	tx, err := beginner.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if namespace != "" {
		if err := tx.SetNamespace(namespace); err != nil {
			return err
		}
	}
	view := &storeTransaction{Engine: inner, tx: tx}
	var scoped storage.Engine = view
	if namespace != "" {
		scoped = storage.NewNamespacedEngine(view, namespace)
	}
	if err := mutate(scoped); err != nil {
		return err
	}
	if committer, ok := storage.CommitOwnerForEngine(engine); ok {
		return committer.CommitTransaction(ctx, tx)
	}
	return storage.ErrNotImplemented
}

// storeTransaction adapts only system-node operations; it owns no separate
// transaction implementation. All reads and writes go to BadgerTransaction.
type storeTransaction struct {
	storage.Engine
	tx *storage.BadgerTransaction
}

// storeTransaction declares every optional method; admission is derived from this policy.
var storeTransactionReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "BatchGetNodesWithContext", Disposition: storage.CapabilityOwned, Scope: "Delegates node hydration to the same BadgerTransaction as auth GetNode/CreateNode, keeping pending state and native lifetime instead of embedded committed Engine."},
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityOwned, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "WithStoreTransaction",
		Disposition: storage.CapabilityRefused, Scope: "No scoped optional read contract on this view; configuration, mutation and maintenance require their exact operation owner."},
}

func (*storeTransaction) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return storeTransactionReadPolicy.Copy()
}

// ReadCapabilitySource keeps reads on this transaction's pending-write view.
func (s *storeTransaction) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }

func (s *storeTransaction) ReadCapabilityScoped(request reflect.Type) bool {
	return storeTransactionReadPolicy.Scoped(request)
}

func (s *storeTransaction) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return mutate(s)
}

func (s *storeTransaction) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return s.tx.CreateNode(node)
}

func (s *storeTransaction) UpdateNode(node *storage.Node) error {
	return s.tx.UpdateGraphNode(node, storage.ClientNodeEdits)
}
func (s *storeTransaction) DeleteNode(id storage.NodeID) error {
	return s.tx.DeleteGraphNode(id, storage.CascadeEdges)
}
func (s *storeTransaction) GetNode(id storage.NodeID) (*storage.Node, error) {
	return s.tx.GetNode(id)
}
func (s *storeTransaction) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return s.tx.GetNodesByLabel(label)
}
func (s *storeTransaction) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return s.BatchGetNodesWithContext(context.Background(), ids)
}

// BatchGetNodesWithContext retains the same native pending node view as GetNode.
func (s *storeTransaction) BatchGetNodesWithContext(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return s.tx.BatchGetNodesWithContext(ctx, ids)
}
func (s *storeTransaction) AllNodes() ([]*storage.Node, error) { return s.tx.AllNodes() }
//
// Deprecated: Use AllNodes to receive errors. Failed reads are omitted and may appear as an empty collection.
func (s *storeTransaction) GetAllNodes() []*storage.Node       { return s.tx.GetAllNodes() }

func (s *storeTransaction) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return s.tx.StreamNodesWithOptions(ctx, opts, visit)
}

func (s *storeTransaction) StorageMembership() storage.MembershipBinding {
	status := storage.MembershipUnbound
	if s.tx != nil {
		status = storage.MembershipBound
		if !s.tx.IsActive() {
			status = storage.MembershipEnded
		}
	}
	return storage.MembershipBinding{Status: status, Kind: storage.MembershipStoreTransaction, Boundary: s, Owner: s.tx, Adapter: s, Enforcement: s.Engine}
}
func (s *storeTransaction) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return s.Engine, policy == storage.WrapperMetadataPolicy
}

func (*storeTransaction) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingOwned, Membership: storage.BindingOwned, Controller: storage.BindingRefused, PendingActions: storage.BindingRefused, SchemaPublication: storage.BindingRefused, ReadLease: storage.BindingRefused, StatementMode: storage.BindingUnowned, Identity: storage.BindingOwned, Enforcement: storage.BindingDelegated, CompoundDependencies: storage.BindingOwned, SameVersionFallback: storage.BindingOwned, Delivery: storage.BindingOwned, ExactEffects: storage.BindingOwned, FullRead: storage.BindingOwned, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

// Atomic auth exposes its real pending node owner; edge integrity is outside
// this node-only view and cannot fall through to the embedded live Engine.
func (*storeTransaction) StoredReadPolicy() storage.StoredReadPolicy {
	p := storage.NativeStoredReadPolicy()
	p.Edges = storage.BindingRefused
	p.Adjacency = storage.BindingRefused
	return p
}
func (s *storeTransaction) ReadStored(ctx context.Context, r storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if !s.StoredReadPolicy().Admits(r) {
		return nil, storage.ErrNotImplemented
	}
	if s.tx == nil {
		return nil, storage.ErrTransactionClosed
	}
	return storage.ReadStored(ctx, s.tx, r)
}
