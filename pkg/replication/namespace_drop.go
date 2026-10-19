package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type namespaceDropCommand struct {
	Prefix           string
	Retirement       uint64
	RevisionAssigned bool
	Catalog          *storage.NamespaceCatalogMutation
}

func isNamespaceLifecycle(cmd *Command) bool {
	return cmd.Type == CmdDeleteByPrefix || cmd.Type == CmdNamespaceCatalog
}

func configureNamespaceOperation(cmd *Command, op *storage.NamespaceDrop) (namespaceDropCommand, error) {
	var request namespaceDropCommand
	if err := decodeGob(cmd.Data, &request); err != nil {
		return request, err
	}
	if request.Catalog != nil {
		op.SetCatalogMutation(*request.Catalog)
	}
	if request.RevisionAssigned || request.Retirement != 0 {
		op.SetCatalogRevision(request.Retirement)
	}
	return request, nil
}

func stampNamespaceDrop(cmd *Command, op *storage.NamespaceDrop) error {
	var request namespaceDropCommand
	if err := decodeGob(cmd.Data, &request); err != nil {
		return err
	}
	// A consensus/WAL entry is immutable after admission. Application uses its
	// assigned revision and catalog delta even while peers serialize the log.
	if request.RevisionAssigned {
		return nil
	}
	request.Retirement = op.CatalogRevision()
	request.RevisionAssigned = true
	request.Catalog = op.CatalogMutation()
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
	if isNamespaceLifecycle(cmd) {
		if cmd.namespaceDrop == nil {
			cmd.namespaceDrop = storage.NewNamespaceDrop(nil)
		}
		cmd.namespaceDrop.DeferConsensusCompletion()
	}
	return applyCommittedStorageCommand(r.storage, cmd)
}

func (r *RaftReplicator) awaitNamespacePreparationFrontier(ctx context.Context, cmd *Command) error {
	namespaces, err := commandNamespaces(cmd)
	if err != nil {
		return err
	}
	r.logMu.RLock()
	var frontier uint64
	for _, entry := range r.log {
		if entry.Command == nil || entry.Index <= r.lastApplied {
			continue
		}
		prior, err := commandNamespaces(entry.Command)
		if err != nil {
			r.logMu.RUnlock()
			return err
		}
		matches := len(prior) == 0 || len(namespaces) == 0
		for _, name := range namespaces {
			for _, other := range prior {
				matches = matches || name == other
			}
		}
		if matches {
			frontier = entry.Index
		}
	}
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
			return fmt.Errorf("namespace preparation waiting for prior consensus entries: deadline exceeded")
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
	if err == nil || !isNamespaceLifecycle(cmd) {
		return err
	}
	return &storage.NamespaceDropError{Committed: cmd.namespaceDrop != nil && cmd.namespaceDrop.Committed(), Cause: err}
}

// Consensus admission prepares the SAME operation later applied by the storage
// adapter. No snapshot, frontier or lock wait is first discovered after append.
func (a *StorageAdapter) PrepareCommandContext(ctx context.Context, cmd *Command) error {
	op := cmd.namespaceDrop
	if op == nil {
		op = storage.NamespaceDropFromContext(ctx)
	}
	if op == nil {
		op = storage.NewNamespaceDrop(nil)
	}
	cmd.namespaceDrop = op
	req, err := configureNamespaceOperation(cmd, op)
	if err != nil {
		return err
	}
	if err := storage.PrepareNamespaceJournal(ctx, a.engine, req.Prefix, op); err != nil {
		op.Abort()
		return err
	}
	release, err := a.admitCommand(ctx, cmd, true)
	if err != nil {
		op.Abort()
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
