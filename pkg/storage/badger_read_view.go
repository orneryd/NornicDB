package storage

import "github.com/dgraph-io/badger/v4"

// badgerReadView owns one physical read point and the lifetime of its managed
// timestamp. Overlay selectors run at that same point in captureReadView.
type badgerReadView struct {
	txn *badger.Txn
	db *managedBadgerDB
	engine *BadgerEngine
	readTs uint64
}

// captureReadView is the single snapshot/overlay capture boundary. capture must
// only copy buffer state using locked selectors; it must not acquire w.mu again
// or call a reader. Entity versions remain immutable after AppendCommit.
// ReadSnapshotter uses this same primitive to select both full overlays.
func (b *BadgerEngine) captureReadView(capture func(*WriteBehindBuffer)) (*badgerReadView, error) {
	db, err := b.beginHelperTxn()
	if err != nil { return nil, err }
	if w := b.writeBehind; w != nil {
		w.mu.RLock()
		defer w.mu.RUnlock()
	}
	txn, readTs, err := db.beginTxn(false)
	if err != nil { b.txnWG.Done(); return nil, err }
	view := &badgerReadView{txn: txn, db: db, engine: b, readTs: readTs}
	if b.writeBehind != nil && capture != nil { capture(b.writeBehind) }
	return view, nil
}

func (v *badgerReadView) Close() {
	if v.txn == nil { return }
	v.txn.Discard()
	v.txn = nil
	v.db.endRead(v.readTs)
	v.engine.txnWG.Done()
}

func (v *badgerReadView) withView(read func(*badger.Txn) error) error { return read(v.txn) }

func (b *BadgerEngine) withReadView(capture func(*WriteBehindBuffer), read func(*badger.Txn) error) error {
	view, err := b.captureReadView(capture)
	if err != nil { return err }
	defer view.Close()
	return recoverBadgerClosedPanic(func() error { return view.withView(read) })
}
