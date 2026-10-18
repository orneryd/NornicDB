package replication

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ContextReplicator preserves cancellation before command admission and while
// waiting for acknowledgement. An entry already committed by consensus must
// still apply on every replica, independently of the requesting connection.
type ContextReplicator interface {
	ApplyContext(context.Context, *Command, time.Duration) error
}

// ContextStorage supports cancellable preparation of a local command. Once a
// destructive operation begins, its storage owner completes its cleanup.
type ContextStorage interface {
	ApplyCommandContext(context.Context, *Command) error
}

func applyStorageContext(ctx context.Context, target Storage, cmd *Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if target, ok := target.(ContextStorage); ok {
		return target.ApplyCommandContext(ctx, cmd)
	}
	if ctx.Done() != nil {
		return storage.ErrNotImplemented
	}
	return target.ApplyCommand(cmd)
}

var (
	_ ContextReplicator = (*StandaloneReplicator)(nil)
	_ ContextReplicator = (*HAStandbyReplicator)(nil)
	_ ContextReplicator = (*RaftReplicator)(nil)
	_ ContextReplicator = (*MultiRegionReplicator)(nil)
	_ ContextStorage    = (*StorageAdapter)(nil)
)
