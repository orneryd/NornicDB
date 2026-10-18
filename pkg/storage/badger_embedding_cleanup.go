package storage

import "github.com/dgraph-io/badger/v4"

// deleteEmbeddingKeysInTxn removes the vectors, shard parts and sidecar metadata
// in a node's released embedding key range. Badger retains deletion keys until
// commit, so the iterator's reusable key buffer must never escape an iteration.
func deleteEmbeddingKeysInTxn(txn *badger.Txn, id NodeID) error {
	return visitEmbeddingKeys(txn, id, txn.Delete)
}

// visitEmbeddingKeys gives the callback an owned copy of each physical key.
// Cleanup needs only keys; prefetching potentially large vectors wastes work.
func visitEmbeddingKeys(txn *badger.Txn, id NodeID, visit func([]byte) error) error {
	prefix := embeddingPrefix(id)
	opts := badgerPrefixIteratorOptions(prefix)
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := visit(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return nil
}
