package cypher

import (
	"context"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// beginTransactionSnapshot opens a transaction snapshot directly on the
// transaction-capable engine. Writes are committed synchronously by their
// statements, so no cache visibility barrier is needed.
func beginTransactionSnapshot(txEngine TransactionCapableEngine) (*storage.BadgerTransaction, error) {
	return txEngine.BeginTransaction()
}

// transactionStart describes how openTransaction opens one transaction.
// implicit marks an auto-commit statement's transaction: it applies the
// statement's metadata, defers constraint validation to commit and skips the
// per-commit sync; an explicit transaction checks constraints per statement
// (#700). drain, when set, flushes acknowledged write-behind writes first so
// an explicit snapshot observes every acknowledged autocommit statement.
type transactionStart struct {
	implicit  bool
	namespace string
	drain     interface{ FlushWriteBehind(context.Context) error }
	// drainContext is the caller's context for drain.
	drainContext context.Context
}

// openTransaction is the one way a Cypher transaction is opened, for explicit
// BEGIN and for implicit (auto-commit) statements alike: namespace priming,
// the native snapshot, namespace pin, configuration and the WAL begin record.
//
// Author: Claude, NOR-180. Its failure is a BEGIN failure and is classified
// here, where it is produced, so every route reports the same status. A
// snapshot opened before the failure is rolled back; BadgerTransaction.Rollback
// only reports an already-closed transaction, so no cleanup failure is lost.
func (e *StorageExecutor) openTransaction(ctx context.Context, txEngine TransactionCapableEngine, start transactionStart) (tx *storage.BadgerTransaction, bookkeeping transactionWAL, retErr error) {
	defer func() {
		if retErr != nil && tx != nil {
			_ = tx.Rollback()
			tx = nil
		}
		retErr = nornicerrors.TransactionControlFailure(nornicerrors.TerminalBegin, retErr)
	}()
	prime, begin, pin, configure, walBegin := localization.CypherTransactionsPrimeNamespaceFailed, localization.CypherTransactionsStartFailed,
		localization.CypherTransactionsPinNamespaceFailed, localization.CypherTransactionsConfigureFailed, localization.CypherTransactionsWALBeginFailed
	if start.implicit {
		prime, begin, pin, configure, walBegin = localization.CypherCoreImplicitTransactionPrimeFailed, localization.CypherCoreImplicitTransactionStartFailed,
			localization.CypherCoreImplicitTransactionPinFailed, localization.CypherCoreImplicitTransactionConfigureFailed, localization.CypherCoreImplicitTransactionWALBeginFailed
	}
	if start.namespace != "" {
		if primer, ok := txEngine.(interface{ EnsureNamespaceMVCC(string) error }); ok {
			if err := primer.EnsureNamespaceMVCC(start.namespace); err != nil {
				return nil, bookkeeping, localizedError(prime(err), err)
			}
		}
	}
	if start.drain != nil {
		if err := start.drain.FlushWriteBehind(start.drainContext); err != nil {
			return nil, bookkeeping, localizedError(begin(err), err)
		}
	}
	tx, err := beginTransactionSnapshot(txEngine)
	if err != nil {
		return nil, bookkeeping, localizedError(begin(err), err)
	}
	if start.namespace != "" {
		if err := tx.SetNamespace(start.namespace); err != nil {
			return tx, bookkeeping, localizedError(pin(err), err)
		}
	}
	if start.implicit {
		if err := applyTransactionMetadata(ctx, tx); err != nil {
			return tx, bookkeeping, err
		}
		// Defer constraint validation to commit and skip the per-commit
		// engine.Sync(). The Bolt session's end-of-session Flush and the
		// async engine's ticker-driven flush coalesce durability for implicit
		// writes; forcing an Msync per UNWIND batch turned every batch into a
		// 300µs syscall for no user-visible benefit.
		for _, set := range []func() error{
			func() error { return tx.SetValidationSetting(storage.DeferredConstraintValidation, true) },
			func() error { return tx.SetValidationSetting(storage.SkipCreateCommitHeadReload, true) },
			func() error { return tx.SetImplicit(true) },
		} {
			if err := set(); err != nil {
				return tx, bookkeeping, localizedError(configure(err), err)
			}
		}
	} else if err := tx.SetValidationSetting(storage.DeferredConstraintValidation, false); err != nil {
		return tx, bookkeeping, localizedError(configure(err), err)
	}
	bookkeeping.txID = tx.ID
	if tx.ID == "" {
		return tx, bookkeeping, nil
	}
	if wal, database := e.resolveWALAndDatabase(); wal != nil {
		seq, err := wal.AppendWithDatabaseReturningSeq(storage.OpTxBegin, storage.WALTxData{TxID: tx.ID, Metadata: nil}, database)
		if err != nil {
			return tx, bookkeeping, localizedError(walBegin(err), err)
		}
		bookkeeping = transactionWAL{wal, seq, tx.ID, database}
	}
	return tx, bookkeeping, nil
}
