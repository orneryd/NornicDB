package storage

import "github.com/dgraph-io/badger/v4"

func badgerIterOptsKeyOnly(prefix []byte) badger.IteratorOptions {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	return opts
}

func badgerIterOptsPrefetchValues(prefix []byte, prefetchSize int) badger.IteratorOptions {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	if prefetchSize > 0 {
		opts.PrefetchSize = prefetchSize
	}
	opts.Prefix = prefix
	return opts
}
