package storage

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/util"
)

// DeletePrefixContext preserves context-aware deletion through engine views.
// Legacy engines retain their existing deletion contract.
func DeletePrefixContext(ctx context.Context, engine Engine, prefix string) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	if deleter, ok := engine.(interface {
		DeleteByPrefixContext(context.Context, string) (int64, int64, error)
	}); ok {
		return deleter.DeleteByPrefixContext(ctx, prefix)
	}
	return engine.DeleteByPrefix(prefix)
}

// DeleteByPrefixContext preserves cancellation at the WAL mutation boundary.
func (w *WALEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	if NamespaceDropFromContext(ctx) == nil {
		ctx = WithNamespaceDrop(ctx, NewNamespaceDrop(nil))
	}
	release, err := util.LockContext(ctx, w.mutationMu.RLock, w.mutationMu.RUnlock)
	if err != nil {
		return 0, 0, err
	}
	nodes, edges, err := DeletePrefixContext(context.WithValue(ctx, deferredDropCompletionKey{}, true), w.engine, prefix)
	release()
	if op := NamespaceDropFromContext(ctx); op != nil && op.physicalDone && !op.consensus && op.phase.Load() != dropCompleted {
		err = op.finish(err)
	}
	return nodes, edges, err
}

// DeleteByPrefixContext translates the prefix into this view's namespace.
func (n *NamespacedEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	return DeletePrefixContext(ctx, n.inner, n.namespace+n.separator+prefix)
}

// DeleteByPrefixContext preserves cancellation through tracing.
func (t *TracedEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	return DeletePrefixContext(ctx, t.Engine, prefix)
}
