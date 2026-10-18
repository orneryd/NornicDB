package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// countQueryReadEmbeddingKeys counts release's existing 0x08 embedding key family.
func countQueryReadEmbeddingKeys(t testing.TB, engine *BadgerEngine, id NodeID) int {
	t.Helper()
	count := 0
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = embeddingPrefix(id)
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	}))
	return count
}
