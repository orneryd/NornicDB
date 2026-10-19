package storage

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// readViewOptions is private to the storage owner. cached must have no externally
// visible effects: a concurrent publication can require a retry. current is only
// for materializers with no user callbacks; it permits current body-cache reads,
// validated as one group, with a strict snapshot retry if a cache changes.
type readViewOptions struct {
	longLived bool
	scope     string
	cached    func(*WriteBehindBuffer) (bool, error)
	current   *bool
}

type badgerReadView struct {
	txn       *badger.Txn
	db        *managedBadgerDB
	engine    *BadgerEngine
	readTs    uint64
	longLived bool
	mu        sync.Mutex
	closed    bool
}

// captureReadView owns every committed/overlay boundary. A nil successful view
// means cached completed the read. The idle case uses two epoch loads, no
// transaction, overlay copy or view allocation. Busy cache reads and transaction
// capture share the retirement lock. Default callers always get a strict view.
func (b *BadgerEngine) captureReadView(capture func(*WriteBehindBuffer), options ...readViewOptions) (*badgerReadView, error) {
	var opts readViewOptions
	if len(options) != 0 {
		opts = options[0]
	}
	w := b.writeBehind
	if opts.cached != nil {
		epoch, idle := w.idleReadEpoch(opts.scope)
		if idle {
			done, err := opts.cached(nil)
			if after, stillIdle := w.idleReadEpoch(opts.scope); stillIdle && after == epoch && (done || err != nil) {
				return nil, err
			}
		}
	}
	if w != nil {
		w.mu.RLock()
		defer w.mu.RUnlock()
		if opts.cached != nil {
			if done, err := opts.cached(w); done || err != nil {
				return nil, err
			}
		}
	}
	db, err := b.beginHelperTxn()
	if err != nil {
		return nil, err
	}
	var txn *badger.Txn
	var readTs uint64
	if opts.longLived {
		readTs, err = db.oracle.beginRead()
	} else {
		txn, readTs, err = db.beginTxn(false)
	}
	if err != nil {
		b.txnWG.Done()
		return nil, err
	}
	view := &badgerReadView{txn: txn, db: db, engine: b, readTs: readTs, longLived: opts.longLived}
	if w != nil && capture != nil {
		if _, idle := w.idleReadEpoch(opts.scope); !idle {
			capture(w)
		}
	}
	if opts.longLived {
		b.txnWG.Done()
	}
	return view, nil
}

func (v *badgerReadView) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	v.closed = true
	if v.txn != nil {
		v.txn.Discard()
		v.txn = nil
	}
	v.db.endRead(v.readTs)
	if !v.longLived {
		v.engine.txnWG.Done()
	}
}

// Long-lived views hold only the oracle timestamp between scans. Engine Close
// waits for an active scan, never for the entire build lifetime.
func (v *badgerReadView) withView(read func(*badger.Txn) error) error {
	if !v.longLived {
		return read(v.txn)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return ErrStorageClosed
	}
	db, err := v.engine.beginHelperTxn()
	if err != nil {
		return err
	}
	defer v.engine.txnWG.Done()
	if db != v.db {
		return ErrStorageClosed
	}
	return recoverBadgerClosedPanic(func() error {
		txn := db.newTxnAt(v.readTs, false)
		defer txn.Discard()
		return read(txn)
	})
}

func (b *BadgerEngine) withReadView(capture func(*WriteBehindBuffer), read func(*badger.Txn) error, options ...readViewOptions) error {
	var opts readViewOptions
	if len(options) != 0 {
		opts = options[0]
	}
	nodeGen, edgeGen := b.nodeCacheGen.current(), b.edgeCacheGen.current()
	if opts.current != nil {
		*opts.current = true
	}
	view, err := b.captureReadView(capture, opts)
	if err != nil || view == nil {
		return err
	}
	defer view.Close()
	err = recoverBadgerClosedPanic(func() error { return view.withView(read) })
	if opts.current != nil && (nodeGen != b.nodeCacheGen.current() || edgeGen != b.edgeCacheGen.current()) {
		// Re-evaluate only internal materialization, never a user callback.
		*opts.current = false
		return recoverBadgerClosedPanic(func() error { return view.withView(read) })
	}
	return err
}
