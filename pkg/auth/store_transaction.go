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
	if runner, ok := engine.(interface {
		WithStoreTransaction(context.Context, func(storage.Engine) error) error
	}); ok {
		return runner.WithStoreTransaction(ctx, mutate)
	}
	inner := engine
	namespace := ""
	seen := make(map[storage.Engine]bool)
	for inner != nil && !seen[inner] {
		seen[inner] = true
		if runner, ok := inner.(interface {
			WithStoreTransaction(context.Context, func(storage.Engine) error) error
		}); ok {
			return runner.WithStoreTransaction(ctx, func(storage.Engine) error { return mutate(engine) })
		}
		if scoped, ok := inner.(*storage.NamespacedEngine); ok && namespace == "" {
			namespace = scoped.Namespace()
		}
		if wrapper, ok := inner.(storage.EngineUnwrapper); ok {
			inner = wrapper.GetInnerEngine()
			continue
		}
		break
	}
	beginner, ok := inner.(interface {
		BeginTransaction(context.Context) (*storage.BadgerTransaction, error)
	})
	if !ok {
		return storage.ErrNotImplemented
	}
	if drainer, ok := inner.(interface{ FlushWriteBehind(context.Context) error }); ok {
		if err := drainer.FlushWriteBehind(ctx); err != nil {
			return err
		}
	}
	tx, err := beginner.BeginTransaction(ctx)
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

func (s *storeTransaction) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return mutate(s)
}

func (s *storeTransaction) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	return s.tx.CreateNode(ctx, node)
}

func (s *storeTransaction) UpdateNode(ctx context.Context, updateRequest storage.NodeUpdateRequest) error {
	return s.tx.UpdateGraphNode(ctx, updateRequest)
}
func (s *storeTransaction) DeleteNode(ctx context.Context, id storage.NodeID) error {
	return s.tx.DeleteGraphNode(ctx, id, storage.CascadeEdges)
}
func (s *storeTransaction) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	return s.tx.GetNode(ctx, id)
}
func (s *storeTransaction) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	return s.tx.GetNodesByLabel(ctx, label)
}
func (s *storeTransaction) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	return s.tx.AllNodes(ctx)
}

func (s *storeTransaction) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return s.tx.StreamNodesWithOptions(ctx, opts, visit)
}
