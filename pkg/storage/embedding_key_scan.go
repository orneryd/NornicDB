package storage

import (
	"bytes"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type embeddingRangeRetention struct {
	contains func([]byte) bool
}

// Badger's Seek copies its input but its escape contract retains it. Reuse
// only scratch keys here; callback keys remain independently owned.
var embeddingCursorPool = sync.Pool{New: func() any { return new([256]byte) }}

// visitEmbeddingPrefixKeys visits the newest visible, live item for each key.
// The callback owns its key, including when it stages a later write. after is
// an inclusive resume position; the prefix still bounds the entire scan.
func visitEmbeddingPrefixKeys(txn *badger.Txn, prefix, after []byte, visit func([]byte) error, retained ...embeddingRangeRetention) error {
	opts := badgerPrefixIteratorOptions(prefix)
	// Badger prefetches two items even without values. Its normal iterator
	// walks all retained versions while seeking the second live key. Expose
	// versions so prefetch stays bounded, then seek past each logical key.
	opts.AllVersions = true
	it := txn.NewIterator(opts)
	defer it.Close()
	start := prefix
	if bytes.Compare(after, start) > 0 {
		start = after
	}
	cursor := embeddingCursorPool.Get().(*[256]byte)
	defer embeddingCursorPool.Put(cursor)
	next := cursor[:0]
	var keep embeddingRangeRetention
	if len(retained) > 0 {
		keep = retained[0]
	}
	for it.Seek(start); it.ValidForPrefix(prefix); it.Seek(next) {
		item := it.Item()
		// Seek uses the transaction's read timestamp, so this is the newest
		// visible version. Appending zero skips its history but retains keys
		// nested beneath it, including embedding shard parts.
		next = append(item.KeyCopy(next[:0]), 0)
		if item.IsDeletedOrExpired() || keep.contains != nil && keep.contains(item.Key()) {
			continue
		}
		if err := visit(item.KeyCopy(nil)); err != nil {
			return err
		}
	}
	return nil
}
