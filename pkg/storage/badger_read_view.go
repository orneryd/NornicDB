package storage

import "github.com/dgraph-io/badger/v4"

type readViewOptions struct {
	scope  string
	cached func(*WriteBehindBuffer) (bool, error)
}

type badgerReadView struct {
	txn    *badger.Txn
	db     *managedBadgerDB
	engine *BadgerEngine
	readTs uint64
}

// captureReadView owns the committed/overlay boundary for node snapshots and
// embedding revision reads. Open the durable view before releasing the overlay
// retirement lock; a captured generation can then retire without creating a gap.
// Release's adapter additionally supports its scoped idle/cache fast paths.
func (b *BadgerEngine) captureReadView(capture func(*WriteBehindBuffer), options ...readViewOptions) (*badgerReadView, error) {
	w := b.writeBehind
	if w != nil {
		w.mu.RLock()
		defer w.mu.RUnlock()
	}
	if len(options) != 0 && options[0].cached != nil {
		if done, err := options[0].cached(w); done || err != nil {
			return nil, err
		}
	}
	db, err := b.beginHelperTxn()
	if err != nil {
		return nil, err
	}
	txn, readTs, err := db.beginTxn(false)
	if err != nil {
		b.txnWG.Done()
		return nil, err
	}
	view := &badgerReadView{txn: txn, db: db, engine: b, readTs: readTs}
	if capture != nil {
		capture(w)
	}
	return view, nil
}

func (v *badgerReadView) Close() {
	if v.txn == nil {
		return
	}
	v.txn.Discard()
	v.txn = nil
	v.db.endRead(v.readTs)
	v.engine.txnWG.Done()
}

func (v *badgerReadView) readAndClose(read func(*badger.Txn) error) error {
	defer v.Close()
	return recoverBadgerClosedPanic(func() error { return read(v.txn) })
}

func (b *BadgerEngine) withReadView(capture func(*WriteBehindBuffer), read func(*badger.Txn) error, options ...readViewOptions) error {
	view, err := b.captureReadView(capture, options...)
	if err != nil || view == nil {
		return err
	}
	return view.readAndClose(read)
}
