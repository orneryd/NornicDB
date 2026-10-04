package storage

import (
	"bytes"

	"github.com/dgraph-io/badger/v4"
)

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
//
// Every range scan is a forward scan with this bound, or descendingPrefixKeys
// for one that wants the greatest keys first. A forward iterator
// without IteratorOptions.Prefix keeps prefetching past the range, and a
// reverse iterator ignores Prefix altogether: either skips every deleted key
// beyond the range until it finds live ones, on each Seek and Next. Deleted
// keys pile up next to a range (DROP DATABASE deletes the dropped database's
// numeric-ID keys; MVCC pruning deletes old versions), so an unbounded scan of
// a small range cost as much as the deleted run beside it: one relationship
// read per node walked a dropped database's adjacency keys. ValidForPrefix only
// stops the caller's loop; it does not bound what the iterator reads.
func badgerPrefixIteratorOptions(prefix []byte) badger.IteratorOptions {
	opts := badgerIteratorOptions()
	opts.Prefix = prefix
	return opts
}

// descendingPrefixKeys visits the keys of prefix that sort at or below
// upper, greatest first, until visit returns false or an error. The item is
// valid only during the call. A reverse Badger iterator reads ahead for a
// further live key after each one it returns and ignores the prefix bound
// (see badgerPrefixIteratorOptions), so it is only advanced while two live
// keys of the prefix remain below its position: the two smallest keys are
// found by a forward scan and visited by forward seeks. No read leaves the
// prefix, and the cost follows the keys visited, not the prefix's size.
func descendingPrefixKeys(txn *badger.Txn, prefix, upper []byte, visit func(*badger.Item) (bool, error)) error {
	forward := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer forward.Close()
	var lowest [][]byte // the (up to) two smallest keys at or below upper
	for forward.Seek(prefix); forward.ValidForPrefix(prefix) && len(lowest) < 2; forward.Next() {
		if bytes.Compare(forward.Item().Key(), upper) > 0 {
			break
		}
		lowest = append(lowest, forward.Item().KeyCopy(nil))
	}
	if len(lowest) == 2 {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		opts.Reverse = true
		reverse := txn.NewIterator(opts)
		defer reverse.Close()
		for reverse.Seek(upper); !bytes.Equal(reverse.Item().Key(), lowest[1]); reverse.Next() {
			more, err := visit(reverse.Item())
			if err != nil || !more {
				return err
			}
		}
	}
	for i := len(lowest) - 1; i >= 0; i-- {
		forward.Seek(lowest[i])
		more, err := visit(forward.Item())
		if err != nil || !more {
			return err
		}
	}
	return nil
}
