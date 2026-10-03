package storage

import "github.com/dgraph-io/badger/v4"

// singleBatchWriter writes into txn only; a write that does not fit returns
// badger.ErrTxnTooBig. Tests use it to drive commit-phase writers against a
// transaction they inspect afterwards.
func singleBatchWriter(txn *badger.Txn) *batchWriter {
	return &batchWriter{txn: txn}
}

// testTxn opens a Badger transaction at the published timestamp without
// registering it as a read; tests use it for short-lived direct reads.
func (m *managedBadgerDB) testTxn(update bool) *badger.Txn {
	return m.DB.NewTransactionAt(m.oracle.published.Load(), update)
}
