package auth

import (
	"context"

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
	inner := engine
	namespace := ""
	seen := make(map[storage.Engine]bool)
	for inner != nil && !seen[inner] {
		seen[inner] = true
		if runner, ok := storage.OwnedCapability[interface {
			WithStoreTransaction(context.Context, func(storage.Engine) error) error
		}](inner); ok {
			return runner.WithStoreTransaction(ctx, func(storage.Engine) error { return mutate(engine) })
		}
		if scoped, ok := storage.ViewMetadataForEngine[*storage.NamespacedEngine](inner); ok && namespace == "" {
			namespace = scoped.Namespace()
		}
		if wrapper, ok := storage.OwnedCapability[storage.EngineUnwrapper](inner); ok {
			inner = wrapper.GetInnerEngine()
			continue
		}
		break
	}
	beginner, ok := storage.OwnedCapability[interface {
		BeginTransaction() (*storage.BadgerTransaction, error)
	}](inner)
	if !ok {
		return storage.ErrNotImplemented
	}
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
	return tx.CommitContext(ctx)
}

// storeTransaction adapts only system-node operations; it owns no separate
// transaction implementation. All reads and writes go to BadgerTransaction.
type storeTransaction struct {
	storage.Engine
	tx *storage.BadgerTransaction
}

// ReadCapabilitySource keeps reads on this transaction's pending-write view.
func (s *storeTransaction) ReadCapabilitySource() (storage.Engine, bool) { return nil, false }

func (s *storeTransaction) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return mutate(s)
}

func (s *storeTransaction) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return s.tx.CreateNode(node)
}

func (s *storeTransaction) UpdateNode(node *storage.Node) error { return s.tx.UpdateNode(node) }
func (s *storeTransaction) DeleteNode(id storage.NodeID) error  { return s.tx.DeleteNode(id) }
func (s *storeTransaction) GetNode(id storage.NodeID) (*storage.Node, error) {
	return s.tx.GetNode(id)
}
func (s *storeTransaction) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return s.tx.GetNodesByLabel(label)
}
func (s *storeTransaction) AllNodes() ([]*storage.Node, error) { return s.tx.AllNodes() }
func (s *storeTransaction) GetAllNodes() []*storage.Node       { return s.tx.GetAllNodes() }

func (s *storeTransaction) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return s.tx.StreamNodesWithOptions(ctx, opts, visit)
}
