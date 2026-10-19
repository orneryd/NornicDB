package multidb

import (
	"context"
	"errors"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// CommitTransaction admits and accounts for a raw transaction. Managed direct
// writes enter the same owner with a storage-owned direct-write transaction.
func (t *sizeTrackingEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	return t.commitTransaction(ctx, tx)
}

type accountingTransaction interface {
	PendingChanges(bool) storage.TransactionChanges
	AccountingChanges() storage.TransactionChanges
	StoredNodeImage(*storage.Node) (*storage.Node, error)
	StoredEdgeImage(*storage.Edge) (*storage.Edge, error)
	CommitContextWithPublication(context.Context, func()) error
	UseManagedAccounting(func())
	Rollback() error
}

// commitTransaction is the only managed-write admission and accounting owner.
// The storage adapter owns staging/codec/WAL differences; this owner derives
// counts and bytes together from each final ID's stored prior/after pair.
func (t *sizeTrackingEngine) commitTransaction(ctx context.Context, tx accountingTransaction) (commitErr error) {
	changes := tx.PendingChanges(false)
	preparer, needsStaging := tx.(interface{ PrepareChanges() error })
	if ctx == nil {
		ctx = context.Background()
	}
	committed, commitAttempted := false, false
	var release func()
	defer func() {
		if release != nil {
			release()
		}
		if !committed {
			_ = tx.Rollback()
			// Every admission failure precedes a physical commit. Keep its
			// cleanup and known outcome together, including future read seams.
			if !commitAttempted {
				commitErr = nornicerrors.MarkCommitRolledBack(commitErr)
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	commit := func(afterPublication, accounting func()) error {
		if preparer, ok := tx.(interface {
			PrepareCommit(context.Context) (func(), error)
		}); ok {
			releaseJournal, err := preparer.PrepareCommit(ctx)
			if err != nil {
				return err
			}
			defer releaseJournal()
		}
		tx.UseManagedAccounting(accounting)
		commitAttempted = true
		err := tx.CommitContextWithPublication(ctx, afterPublication)
		if !committed && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			pending, routed := tx.(interface{ CommitMayBePending() bool })
			if !routed || !pending.CommitMayBePending() {
				return nornicerrors.MarkCommitRolledBack(err)
			}
		}
		return err
	}
	if changes.Operations == 0 {
		// Read-only commits bypass write admission but still own cancellation
		// cleanup, including the snapshot and any acquired MERGE key.
		return commit(func() { committed = true }, nil)
	}
	if changes.Namespace != t.dbName {
		return nornicerrors.MarkCommitRolledBack(storage.ErrCrossNamespaceTransaction)
	}
	checker := t.checker
	if checker == nil {
		checker = &databaseLimitChecker{manager: t.manager, databaseName: t.dbName}
	}
	limits := checker.limitsSnapshot()
	initialized, err := t.manager.storageSizeInitialized(t.dbName)
	if err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	needsExclusive := func(l *Limits, initialized bool) bool {
		return initialized || (l != nil && (l.Storage.MaxNodes > 0 || l.Storage.MaxEdges > 0 || l.Storage.MaxBytes > 0 || l.Rate.MaxWritesPerSecond > 0))
	}
	exclusive := needsStaging || needsExclusive(limits, initialized)
	for {
		release, err = t.writeMu.acquire(ctx, exclusive, changes.HoldsMergeKey)
		if err != nil {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		// Initialization can win before our shared lease. Recheck inside the gate
		// so a first size read cannot race a write that skips incremental accounting.
		limits = checker.limitsSnapshot()
		initialized, err = t.manager.storageSizeInitialized(t.dbName)
		if err != nil {
			return nornicerrors.MarkCommitRolledBack(err)
		}
		if exclusive || !needsExclusive(limits, initialized) {
			break
		}
		release()
		release = nil
		exclusive = true
	}
	if needsStaging {
		if err := preparer.PrepareChanges(); err != nil {
			return err
		}
		changes = tx.PendingChanges(false)
	}
	publish := func() {
		committed = true
		release()
		release = nil
	}
	if !exclusive {
		return commit(publish, func() {})
	}
	trackBytes := initialized || (limits != nil && limits.Storage.MaxBytes > 0)
	if trackBytes || (limits != nil && (limits.Storage.MaxNodes > 0 || limits.Storage.MaxEdges > 0)) {
		changes = tx.AccountingChanges()
	}
	var currentBytes int64
	if trackBytes {
		currentBytes, err = checker.getCurrentStorageSize(t.Engine)
		if err != nil {
			return err
		}
	}
	var batch accountingBatch
	if trackBytes || (limits != nil && (limits.Storage.MaxNodes > 0 || limits.Storage.MaxEdges > 0)) {
		batch, err = t.prepareAccountingBatch(tx, changes, trackBytes)
		if err != nil {
			return err
		}
	}
	if err := batch.admit(t, limits, currentBytes); err != nil {
		return err
	}
	// Match ordinary writes: one token per buffered storage operation, including
	// deletes. Rejected attempts retain consumed rate tokens, not storage usage.
	if limits != nil && limits.Rate.MaxWritesPerSecond > 0 {
		for i := 0; i < changes.Operations; i++ {
			if err := checker.CheckWriteRate(); err != nil {
				return nornicerrors.MarkCommitRolledBack(err)
			}
		}
	}
	account := func() {}
	if trackBytes {
		manager, database, admitted := t.manager, t.dbName, batch
		account = func() { manager.publishAccountingBatch(database, admitted, nil) }
	}
	return commit(publish, account)
}
