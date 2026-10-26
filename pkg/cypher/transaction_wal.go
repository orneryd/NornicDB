// Author: Codex, NOR-180 implementation lane, 2026-10-26.
package cypher

import (
	"context"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// transactionWAL owns logical terminal bookkeeping for both explicit and
// implicit transactions. Native storage alone decides whether graph writes
// committed; a failed commit marker must never trigger an abort of that commit.
type transactionWAL struct {
	wal      *storage.WAL
	start    uint64
	txID     string
	database string
}

type transactionWALFailure = nornicerrors.TerminalFailure

func (w transactionWAL) finish(ctx context.Context, committed bool, opCount int, reason string, primary error, operation nornicerrors.TerminalOperation) (*storage.Receipt, error) {
	if w.wal == nil || w.start == 0 {
		return nil, nil
	}
	if !committed {
		if _, err := w.wal.AppendWithDatabaseReturningSeq(storage.OpTxAbort, storage.WALTxData{TxID: w.txID, Reason: reason}, w.database); err != nil {
			message := localization.CypherTransactionsWALAbortFailed(w.txID, err)
			if operation == nornicerrors.TerminalCommit && primary != nil {
				primary = localizedError(localization.CypherTransactionsCommitFailed(primary), primary)
			}
			return nil, newTransactionWALFailure(ctx, operation, false, primary, localizedError(message, err), message)
		}
		return nil, nil
	}
	end, err := w.wal.AppendWithDatabaseReturningSeq(storage.OpTxCommit, storage.WALTxData{TxID: w.txID, OpCount: opCount}, w.database)
	if err != nil {
		message := localization.CypherTransactionsWALCommitFailed(w.txID, err)
		return nil, newTransactionWALFailure(ctx, operation, true, primary, localizedError(message, err), message)
	}
	return storage.NewReceipt(w.txID, w.start, end, w.database, time.Now().UTC())
}

// finishCommitted is the one completion of a natively committed transaction,
// explicit or implicit: its post-commit actions run, then its logical COMMIT
// record is always written. A failed record keeps a failed action as its
// primary; otherwise the action's failure is returned with committed truth.
func (w transactionWAL) finishCommitted(ctx context.Context, opCount int, actions func() error) (receipt *storage.Receipt, completionErr, bookkeepingErr error) {
	if actions != nil {
		// A failed post-commit action is this commit's failure: it gets the
		// committed outcome's status here, so every protocol and route reports
		// it alike and no client retries the committed work.
		completionErr = nornicerrors.CommittedFailure(actions())
	}
	receipt, bookkeepingErr = w.finish(ctx, true, opCount, "", completionErr, nornicerrors.TerminalCommit)
	return receipt, completionErr, bookkeepingErr
}

func (w transactionWAL) abort(ctx context.Context, primary error, reason string) error {
	operation := nornicerrors.TerminalAbort
	if primary == nil {
		operation = nornicerrors.TerminalEmptyCompletion
	}
	return w.abortOperation(ctx, primary, reason, operation)
}

func (w transactionWAL) rejectCommit(ctx context.Context, primary error, reason string) error {
	return w.abortOperation(ctx, primary, reason, nornicerrors.TerminalCommit)
}

func (w transactionWAL) abortOperation(ctx context.Context, primary error, reason string, operation nornicerrors.TerminalOperation) error {
	_, err := w.finish(ctx, false, 0, reason, primary, operation)
	if err == nil {
		return primary
	}
	return err
}

func newTransactionWALFailure(ctx context.Context, operation nornicerrors.TerminalOperation, committed bool, primary, terminal error, message localization.Message) *transactionWALFailure {
	primary = nornicerrors.MarkStatementTimeout(primary, context.Cause(ctx))
	failure := nornicerrors.NewTerminalFailure(operation, committed, primary, terminal, message)
	// Record the whole outcome before returning to any callback, aggregation or
	// error mapper. Native child executions retain independent commit truth;
	// their enclosing statements must still report each failed terminal append.
	recordTerminalOutcome(ctx, failure)
	return failure
}

func recordTerminalOutcome(ctx context.Context, outcome error) {
	if nativeTerminalOutcomeScope(ctx) {
		for {
			statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext)
			if !ok {
				return
			}
			statement.outcome.record(false, outcome)
			ctx = statement.Context
		}
	}
}

// Fabric coordinator outcome aggregation belongs to NOR-174. These are the
// existing scope markers; this journal does not change coordinator policy.
func nativeTerminalOutcomeScope(ctx context.Context) bool {
	if ctx == nil || storage.CoordinatorOwnsTransaction(ctx) {
		return false
	}
	_, distributed := fabric.FabricTransactionFromContext(ctx)
	return !distributed
}

// Qualifiers such as commit classification, timeout ownership and source
// projection belong to their existing return boundaries. Retain their complete
// returned object before a callback can discard it, without reimplementing them.
func completeStatementTerminalOutcome(ctx context.Context, result **ExecuteResult, retErr *error, project func()) {
	var owner *statementExecutionContext
	if nativeTerminalOutcomeScope(ctx) {
		if statement, ok := ctx.Value(queryEmbeddingStatementKey{}).(*statementExecutionContext); ok {
			if statement.outcome.hasTerminalFailures() {
				owner = statement
				*retErr = statement.outcome.retainTerminalFailures(*retErr)
				*result = nil
			}
		}
	}
	// Compose first, then let the existing display owner project every child.
	// Only the final display-safe value is promoted to enclosing statements.
	if project != nil {
		project()
	}
	if owner != nil {
		recordTerminalOutcome(ctx, *retErr)
	}
	if *retErr != nil && nativeTerminalOutcomeScope(ctx) {
		nornicerrors.RecordTerminalOutcome(ctx, *retErr)
	}
}
