package replication

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// PublishNamespaceCatalogContext retains CREATE/ALTER journal admission and
// caller cancellation in every replication mode, at the same lifecycle owner
// as DROP. No full catalog snapshot crosses the replication journal.
func (e *ReplicatedEngine) PublishNamespaceCatalogContext(ctx context.Context, mutation storage.NamespaceCatalogMutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	op := storage.NamespaceDropFromContext(ctx)
	if op == nil {
		op = storage.NewNamespaceDrop(nil)
	}
	op.SetCatalogMutation(mutation)
	payload, err := encodeGob(namespaceDropCommand{Prefix: mutation.Namespace + ":", Catalog: &mutation})
	if err != nil {
		return err
	}
	cmd := &Command{Type: CmdNamespaceCatalog, Data: payload, Timestamp: time.Now(), namespaceDrop: op}
	ctx = storage.WithNamespaceDrop(ctx, op)
	if r, ok := e.replicator.(ContextReplicator); ok {
		err = r.ApplyContext(ctx, cmd, e.timeout)
	} else if ctx.Done() == nil {
		err = e.replicator.Apply(cmd, e.timeout)
	} else {
		err = storage.ErrNotImplemented
	}
	return op.Result(err)
}
