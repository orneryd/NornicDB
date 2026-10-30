package storage

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/config"
)

// tryContextEntityOperations consumes the existing native preparation and WAL
// accepted-command owner. No journal entry is created until preparation succeeds.
// Legacy records retain their established non-transactional/default-ID protocol.
func (w *WALEngine) tryContextEntityOperations(ctx context.Context, operations []Operation, mode EntityCommitMode) (bool, error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if ctx.Done() == nil || !config.IsWALEnabled() || len(operations) == 0 {
		return false, nil
	}
	namespace := ""
	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		id := string(op.NodeID)
		if op.Node != nil {
			id = string(op.Node.ID)
		}
		if op.Edge != nil {
			id = string(op.Edge.ID)
		}
		if op.EdgeID != "" {
			id = string(op.EdgeID)
		}
		ns, _, qualified := ParseDatabasePrefix(id)
		if !qualified {
			return false, nil
		}
		if namespace != "" && namespace != ns {
			return true, ErrCrossNamespaceTransaction
		}
		namespace = ns
	}
	// The common staging owner applies client edits once; semantic replay keeps
	// using its stored-image policy. The same prepared frame reaches the same WAL
	// SubmitEntityCommit/receipt owner, which survives cancellation after acceptance.
	receipt, err := submitDecodedEntityTransactionWithPolicy(ctx, w, EntityTransactionEnvelope{
		Version: EntityTransactionVersion, Namespace: namespace, LogicalTxID: generateTxID(), Mode: mode,
	}, operations, nil, ClientNodeEdits)
	if errors.Is(err, errEntityTransactionUnavailable) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, receipt.Wait(context.Background())
}

// journalApplyContext retains caller values but not cancellation after a legacy
// record is durable. Returning Canceled there would leave an unapplied record
// which could create a supposedly rejected mutation at recovery.
func journalApplyContext(ctx context.Context) context.Context {
	if config.IsWALEnabled() {
		return context.WithoutCancel(ctx)
	}
	return ctx
}
