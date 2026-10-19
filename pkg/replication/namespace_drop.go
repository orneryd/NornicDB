package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type namespaceDropCommand struct {
	Prefix     string
	Retirement uint64
}

func stampNamespaceDrop(cmd *Command, op *storage.NamespaceDrop) error {
	var request namespaceDropCommand
	if err := decodeGob(cmd.Data, &request); err != nil {
		return err
	}
	request.Retirement = op.CatalogRevision()
	data, err := encodeGob(request)
	if err == nil {
		cmd.Data = data
	}
	return err
}

type committedCommandKey struct{}

// Consensus and streamed WAL entries already crossed their journal commit
// point. A replica's secondary journal error cannot turn them into preparation
// failures or undo their deterministic physical/catalog completion.
func applyCommittedStorageCommand(target Storage, cmd *Command) error {
	return applyStorageContext(context.WithValue(context.Background(), committedCommandKey{}, true), target, cmd)
}

func (r *RaftReplicator) applyCommittedCommand(cmd *Command) error {
	if cmd.Type == CmdDeleteByPrefix {
		if cmd.namespaceDrop == nil {
			cmd.namespaceDrop = storage.NewNamespaceDrop(nil)
		}
		cmd.namespaceDrop.DeferConsensusCompletion()
	}
	return applyCommittedStorageCommand(r.storage, cmd)
}

func (r *RaftReplicator) awaitNamespacePreparationFrontier(ctx context.Context) error {
	r.logMu.RLock()
	frontier := r.log[len(r.log)-1].Index
	applied := r.lastApplied
	r.logMu.RUnlock()
	if applied >= frontier {
		return ctx.Err()
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(r.config.Raft.CommitTimeout)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stopCh:
			return ErrClosed
		case <-timeout.C:
			return fmt.Errorf("DROP preparation waiting for prior consensus entries: deadline exceeded")
		case <-ticker.C:
			r.logMu.RLock()
			applied = r.lastApplied
			r.logMu.RUnlock()
			if applied >= frontier {
				return nil
			}
		}
	}
}

func commandWaitOutcome(cmd *Command, err error) error {
	if err == nil || cmd.Type != CmdDeleteByPrefix {
		return err
	}
	return &storage.NamespaceDropError{Committed: cmd.namespaceDrop != nil && cmd.namespaceDrop.Committed(), Cause: err}
}

// Consensus admission prepares the SAME operation later applied by the storage
// adapter. No snapshot, frontier or lock wait is first discovered after append.
func (a *StorageAdapter) PrepareCommandContext(ctx context.Context, cmd *Command) error {
	var req struct{ Prefix string }
	if err := decodeGob(cmd.Data, &req); err != nil {
		return err
	}
	op := cmd.namespaceDrop
	if op == nil {
		op = storage.NamespaceDropFromContext(ctx)
	}
	if op == nil {
		op = storage.NewNamespaceDrop(nil)
	}
	cmd.namespaceDrop = op
	release, err := a.admitCommand(ctx, cmd, true)
	if err != nil {
		return err
	}
	op.SetPublicationRelease(release)
	if err := storage.PrepareNamespaceDrop(ctx, a.engine, req.Prefix, op); err != nil {
		release()
		op.Abort()
		return err
	}
	return nil
}
