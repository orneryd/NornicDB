package storage

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// nodeRef carries a node ID and the numeric identity used by its stored indexes
// and MVCC records. Selecting a live identity or admitting a label generation
// remains the responsibility of the writer or reader that consumes this value.
type nodeRef struct {
	id  NodeID
	num uint64
}

// snapshotNodeRef resolves the persisted forward dictionary in the caller's
// already captured transaction. It neither opens a view nor consults the live
// dictionary, so a later insertion, deletion or renumbering cannot change this
// reference. The caller retains ownership of txn and its lifetime.
//
// A missing entry is (ref, false, nil); a malformed entry is ErrInvalidData.
// Reader-specific missing-ID diagnostics remain with that reader. Generation
// admission and lifecycle test hooks belong above this shared primitive.
func (*BadgerEngine) snapshotNodeRef(txn *badger.Txn, id NodeID) (ref nodeRef, found bool, err error) {
	ref.id = id
	item, err := txn.Get(nodeIDForwardKey(id))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ref, false, nil
	}
	if err != nil {
		return ref, false, err
	}
	err = item.Value(func(value []byte) error {
		if len(value) != 8 {
			return ErrInvalidData
		}
		ref.num = binary.BigEndian.Uint64(value)
		return nil
	})
	return ref, err == nil, err
}
