package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

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
		var ok bool
		if ref.num, ok = decodeNodeRefNum(value); !ok {
			return ErrInvalidData
		}
		return nil
	})
	return ref, err == nil, err
}

// decodeNodeRefNum is the one decoder of a forward dictionary value: the node's
// 8-byte big-endian numeric identity. Each reader keeps its own policy for a
// malformed value; ok reports whether the value had that shape.
func decodeNodeRefNum(value []byte) (num uint64, ok bool) {
	if len(value) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(value), true
}
