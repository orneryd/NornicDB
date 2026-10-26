package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
)

// ensureOpen rejects a closed engine; write admission also rejects Close in progress.
func (b *BadgerEngine) ensureOpen(writing bool) error {
	b.mu.RLock()
	closed := b.closed || (writing && b.closing)
	b.mu.RUnlock()
	if closed {
		return ErrStorageClosed
	}
	return nil
}

// beginBarrierWrite marks a durable write in flight so Close waits for it before
// releasing engine state. It returns a stack lease the caller must release,
// or ErrStorageClosed when the engine has already closed (the caller must
// not touch Badger in that case). The closed check runs under the barrier so
// a Close that has completed is always observed and a Close that has not
// started yet cannot slip in between the check and the write.
func (b *BadgerEngine) beginBarrierWrite(exclusive bool, publication bool) (writeBarrierLease, error) {
	lease := writeBarrierLease{engine: b, exclusive: exclusive}
	if exclusive {
		b.writeBarrier.Lock()
	} else {
		b.writeBarrier.RLock()
	}
	if err := b.ensureOpen(true); err != nil {
		lease.release()
		return writeBarrierLease{}, err
	}
	if publication {
		lease.held = b.holdPublication()
	}
	return lease, nil
}

// writeBarrierLease retains admission on the writer's stack. A function value
// is needed only when a transaction stores its release for publication.
type writeBarrierLease struct {
	engine    *BadgerEngine
	exclusive bool
	held      *commitNotification
}

func (lease writeBarrierLease) release() {
	if lease.exclusive {
		lease.engine.writeBarrier.Unlock()
	} else {
		lease.engine.writeBarrier.RUnlock()
	}
}

// releaseCallback returns the mutex's existing bound method when a transaction
// must keep admission through publication. Capturing the whole lease would
// retain its unused notification state in every transaction's callback.
func (lease writeBarrierLease) releaseCallback() func() {
	if lease.exclusive {
		return lease.engine.writeBarrier.Unlock
	}
	return lease.engine.writeBarrier.RUnlock
}

// orderedWrite is a write ordered after the write-behind buffer
// (orderAfterBufferedCommits), from its write barrier to its publication
// point (beginOrderedWrite), kept on the writer's stack.
type orderedWrite struct {
	b    *BadgerEngine
	held *commitNotification
}

// beginOrderedWrite acquires shared admission for a write ordered after the write-behind
// buffer. finish, deferred before every other lock the write takes, delivers
// the notifications of the write, and of the commits its drain replays, after
// the write released every other lock (holdPublication).
func (b *BadgerEngine) beginOrderedWrite() (orderedWrite, error) {
	lease, err := b.beginBarrierWrite(false, true)
	return orderedWrite{b: lease.engine, held: lease.held}, err
}

func (w orderedWrite) finish() {
	w.b.writeBarrier.RUnlock()
	w.b.completeCommitNotification(w.held)
}

func (b *BadgerEngine) withView(fn func(txn *badger.Txn) error) error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	return recoverBadgerClosedPanic(func() error {
		return db.View(fn)
	})
}

func (b *BadgerEngine) beginHelperTxn() (*managedBadgerDB, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrStorageClosed
	}
	b.txnWG.Add(1)
	return b.db, nil
}

// withUpdate runs fn as one atomic engine write, committed through the same
// commit writer as a transaction. fn reads and writes in one Badger
// transaction and cannot be repeated, so its writes must fit one Badger
// batch (badger.ErrTxnTooBig otherwise); writes of any size go through
// commitEngineWrite repeatable units. Optional entities acquire their write-behind order first;
// their notifications are delivered only after that order is released.
func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error, entities ...writtenEntities) error {
	if len(entities) > 0 && b.writeBehind != nil {
		var order orderHold
		if _, err := b.orderAfterBufferedCommits(&order, entities[0]); err != nil {
			return err
		}
		defer b.drainCommitNotifications()
		defer order.release()
	}
	return b.commitEngineWrite(func(cw *commitWriter) error { return cw.writeOnce(fn) })
}

// withPublishingUpdate binds a memory-only delta to the existing atomic writer.
// keys reads the prior/written images captured by fn, after fn succeeds.
func (b *BadgerEngine) withPublishingUpdate(fn func(*badger.Txn) error, publish func(), keys func() []any, notification *commitNotification, stages ...func(*commitWriter) error) error {
	return b.commitEngineWrite(func(cw *commitWriter) error {
		cw.notification = notification
		if err := cw.writeOnce(fn); err != nil {
			return err
		}
		cw.publish = publish
		if keys != nil {
			cw.publishKeys = keys()
		}
		for _, stage := range stages {
			if err := stage(cw); err != nil {
				return err
			}
		}
		return nil
	})
}

type engineWriteOptions struct {
	units     []func(*badger.Txn) error
	lifecycle *lifecycleWrite
}

// commitEngineWrite is the one commit of the engine's own writes (withUpdate callbacks or repeatable units): write fills the commit writer, which then commits. A
// failure rolls back any batches a large commit already wrote.
//
// Property-key tokens are made durable before the entity bytes that
// reference them (a later failure leaves only harmless orphaned tokens,
// matching Neo4j's token-before-entity invariant), and the numeric-ID
// counter high-water marks are persisted after the commit, in a separate
// transaction, so concurrent writers never conflict on the shared keys.
func (b *BadgerEngine) commitEngineWrite(write func(cw *commitWriter) error, options ...engineWriteOptions) error {
	var option engineWriteOptions
	if len(options) > 0 {
		option = options[0]
	}
	if option.lifecycle != nil && (option.lifecycle.b != b || !option.lifecycle.suppressions) {
		return ErrInvalidData
	}
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	var nodeMax, edgeMax uint64
	err = recoverBadgerClosedPanic(func() error {
		var txn *badger.Txn
		var readTs uint64
		if option.lifecycle != nil {
			readTs = db.oracle.beginReadHeld()
			txn = db.newTxnAt(readTs, true)
		} else {
			txn, readTs, err = db.beginTxn(true)
		}
		if err != nil {
			return err
		}
		defer db.endRead(readTs)
		cw := b.newCommitWriter(db, txn)
		defer cw.discard()
		fail := func(err error) error {
			b.idDict.discardTxnCounters(cw.txn)
			b.propKeyDict.discardTxnCounters(cw.txn)
			if abortErr := cw.abort(); abortErr != nil {
				err = compose.Outcomes(err, abortErr)
			}
			return err
		}
		var writeErr error
		if option.units != nil {
			for _, unit := range option.units {
				if writeErr = cw.write(unit); writeErr != nil {
					break
				}
			}
		} else {
			writeErr = write(cw)
		}
		if writeErr != nil {
			return fail(writeErr)
		}
		nodeMax, edgeMax = b.idDict.flushTxnCounters(cw.txn)
		committing := cw.txn
		if err := cw.finish(); err != nil {
			return fail(err)
		}
		b.idDict.markTxnCommitted(committing)
		return nil
	})
	if err == nil {
		runCommitTailHook()
		b.idDict.persistCounters(db, nodeMax, edgeMax)
	}
	return err
}

func recoverBadgerClosedPanic(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if isBadgerClosedPanic(recovered) {
				err = ErrStorageClosed
				return
			}
			panic(recovered)
		}
	}()

	return fn()
}

func isBadgerClosedPanic(recovered interface{}) bool {
	message := fmt.Sprint(recovered)
	return strings.Contains(message, "DB Closed")
}

// IsTransactionTooBig reports whether err is Badger's "Txn is too big" (the
// writes exceed one Badger batch), wrapped or only kept as text by a wrapper
// that dropped the chain.
func IsTransactionTooBig(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, badger.ErrTxnTooBig) || strings.Contains(err.Error(), badger.ErrTxnTooBig.Error())
}
