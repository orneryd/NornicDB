package storage

// Port of the earlier probe seam ei1_probe_seam.go (L484) as a test-only file:
// it is compiled only into this package's test binary, where the external test
// v152p_ei1_corrupt_fulltext_test.go (package storage_test) uses it. It plants
// an undecodable body under a stored relationship's key through the engine's
// own Badger handle.

import "github.com/dgraph-io/badger/v4"

func EI1CorruptEdgeRowForProbe(b *BadgerEngine, id EdgeID, value []byte) error {
	return b.db.Update(func(txn *badger.Txn) error { return txn.Set(edgeKey(id), value) })
}
