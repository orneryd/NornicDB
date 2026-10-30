package auth

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// WithStoreTransaction runs system-node mutations in the same BadgerTransaction
// used by Cypher GRANT/REVOKE. An explicit transaction's engine supplies its
// existing view; otherwise this function owns begin, commit and rollback.
// Callers publish their in-memory state only after this function succeeds.
// Every write runs under the system-store admission (storage.SystemStoreEngine).
func WithStoreTransaction(ctx context.Context, engine storage.Engine, mutate func(storage.Engine) error) error {
	// The handle is unwrapped below: an enclosing transaction's view still
	// receives the system-store handle, never the raw view.
	engine = storage.SystemStoreEngine(engine)
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
		BeginTransaction() (*storage.BadgerTransaction, error)
	})
	if !ok {
		return storage.ErrNotImplemented
	}
	if drainer, ok := inner.(interface{ FlushWriteBehind() error }); ok {
		if err := drainer.FlushWriteBehind(); err != nil {
			return err
		}
	}
	tx, err := storage.BeginSystemStoreTransaction(beginner)
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
	if err := mutate(storage.SystemStoreEngine(scoped)); err != nil {
		return err
	}
	return tx.CommitContext(ctx)
}

// systemStoreEngine is storage.SystemStoreEngine for constructors whose
// parameter shadows the storage package.
func systemStoreEngine(engine storage.Engine) storage.Engine { return storage.SystemStoreEngine(engine) }

// storeTransaction adapts only system-node operations; it owns no separate
// transaction implementation. All reads and writes go to BadgerTransaction.
type storeTransaction struct {
	storage.Engine
	tx *storage.BadgerTransaction
}

func (s *storeTransaction) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return mutate(storage.SystemStoreEngine(s))
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
func (s *storeTransaction) AllNodes() ([]*storage.Node, error) { return s.tx.AllNodes() }
func (s *storeTransaction) GetAllNodes() []*storage.Node       { return s.tx.GetAllNodes() }

func (s *storeTransaction) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return s.tx.StreamNodesWithOptions(ctx, opts, visit)
}
