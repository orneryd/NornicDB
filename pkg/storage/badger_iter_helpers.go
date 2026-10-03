package storage

import "github.com/dgraph-io/badger/v4"

// badgerIteratorOptions returns the options of every storage iterator:
// Badger's defaults without value prefetching. A prefetching iterator starts
// a goroutine per item to copy its value ahead of the read. NornicDB keeps
// values up to the value threshold (64 KB by default) inline in the LSM
// tree, so an Item.Value call reads them in place and the prefetch copies
// nothing worth having; the goroutine starts cost more than the scans they
// serve, most of all in short lookups when the scheduler has to wake a
// thread for each.
func badgerIteratorOptions() badger.IteratorOptions {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	return opts
}

// badgerPrefixIteratorOptions is badgerIteratorOptions bounded to prefix.
func badgerPrefixIteratorOptions(prefix []byte) badger.IteratorOptions {
	opts := badgerIteratorOptions()
	opts.Prefix = prefix
	return opts
}
