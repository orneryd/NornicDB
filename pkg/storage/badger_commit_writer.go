package storage

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// commitWriter writes one atomic commit of any size. The engine's own writes
// (withUpdate, withUpdateUnits) and a transaction's commit
// (BadgerTransaction.Commit) all go through it.
//
// Writes go in units through the embedded batchWriter. While they fit, they
// stay in the commit's first Badger transaction and the commit is an ordinary
// one-batch commit. When a unit does not fit, the commit becomes a large
// commit (see managedBadgerDB): the full batch is written as its first batch,
// the unit is repeated in the next, and so on; finish writes the last batch
// and publishes everything at once. The first batch is the transaction that
// carries the commit's conflict-tracked reads, so Badger's conflict check of
// everything the commit read runs before anything else is written.
//
// Bookkeeping that is keyed by the Badger transaction moves with the
// batches: numeric-ID counter high-water marks follow the open batch, and
// property-key tokens are made durable before the first batch that can
// reference them (ordinary commit: one separate commit before it; large
// commit: a batch of their own ahead of each batch).
type commitWriter struct {
	batchWriter
	engine *BadgerEngine
	db     *managedBadgerDB
	large  *largeCommit
	// prev is the batch most recently written, whose counters the next
	// batch takes over.
	prev *badger.Txn
	// beforeLarge, when set, runs once before the commit becomes large,
	// i.e. before the exclusive commit gate is taken.
	beforeLarge func()
	// onBatch, when set, is told about every batch the writer opens.
	onBatch func(next *badger.Txn)
}

func (b *BadgerEngine) newCommitWriter(db *managedBadgerDB, txn *badger.Txn) *commitWriter {
	cw := &commitWriter{engine: b, db: db}
	cw.batchWriter = batchWriter{txn: txn, to: cw}
	return cw
}

// commitBatch writes a full batch as the next batch of the large commit,
// turning the commit into one first.
func (cw *commitWriter) commitBatch(txn *badger.Txn) error {
	if cw.large == nil {
		if cw.beforeLarge != nil {
			cw.beforeLarge()
		}
		lc, err := cw.db.beginLargeCommit()
		if err != nil {
			return err
		}
		cw.large = lc
	}
	if err := cw.commitTokens(txn); err != nil {
		return err
	}
	cw.prev = txn
	return cw.large.commitBatch(txn)
}

// openBatch opens the next batch of the large commit.
func (cw *commitWriter) openBatch() *badger.Txn {
	next := cw.large.openBatch()
	cw.engine.idDict.moveTxnCounters(cw.prev, next)
	if cw.onBatch != nil {
		cw.onBatch(next)
	}
	return next
}

// commitTokens writes the property-key tokens staged against txn as
// batches of the large commit, ahead of txn. They are never rolled back
// (rollbackSince).
func (cw *commitWriter) commitTokens(txn *badger.Txn) error {
	dict := cw.engine.propKeyDict
	drain := dict.flushTxnCounters(txn)
	if drain.empty() {
		return nil
	}
	tokens := &batchWriter{to: cw.large}
	defer tokens.discard()
	err := drain.writeTo(tokens)
	if err == nil {
		err = tokens.flush()
	}
	if err != nil {
		return fmt.Errorf("%w: %w", errPersistingTokens, err)
	}
	dict.markPersisted(drain)
	return nil
}

// finish commits: an ordinary commit after persisting its property-key
// tokens, or a large commit's last batch, publishing it. On error the
// caller must abort.
func (cw *commitWriter) finish() error {
	if cw.large == nil {
		dict := cw.engine.propKeyDict
		if err := dict.persistTxnCounters(cw.db, dict.flushTxnCounters(cw.txn)); err != nil {
			return fmt.Errorf("%w: %w", errPersistingTokens, err)
		}
		return cw.db.commit(cw.txn)
	}
	err := cw.commitTokens(cw.txn)
	if err == nil {
		err = cw.large.finish(&cw.batchWriter)
	}
	return err
}

// abort rolls back the batches a large commit already wrote. The open batch
// belongs to the caller, which discards it. An error means the rollback
// failed and the engine refuses writes until restart (commitOracle.fail).
func (cw *commitWriter) abort() error {
	if cw.large == nil {
		return nil
	}
	lc := cw.large
	cw.large = nil
	if err := lc.abort(); err != nil {
		return cw.db.oracle.failure()
	}
	return nil
}

// errPersistingTokens marks a commit that failed while making its
// property-key tokens durable. Commit reports it as is, as before the
// commit writer existed.
var errPersistingTokens = errors.New("persisting property key dictionary")
