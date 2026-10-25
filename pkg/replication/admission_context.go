package replication

import (
	"context"
	"time"
)

// ContextualReplicator carries the inbound context to the retained admission
// owner. The context-free Replicator API remains a compatibility entrance.
type ContextualReplicator interface {
	ApplyWithContext(context.Context, *Command, time.Duration) error
}

func applyReplicatorWithContext(ctx context.Context, replicator Replicator, cmd *Command, timeout time.Duration) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if owner, ok := replicator.(ContextualReplicator); ok {
			return owner.ApplyWithContext(ctx, cmd, timeout)
		}
	}
	return replicator.Apply(cmd, timeout)
}
