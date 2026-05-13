package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// detectedSerializer is what the in-place migration tool finds in an
// existing data directory. "gob" is only ever returned for legacy data
// that pre-dates the msgpack header; current engines never emit it.
type detectedSerializer byte

const (
	detectedNone    detectedSerializer = 0
	detectedGob     detectedSerializer = detectedSerializer(serializerIDGob)
	detectedMsgpack detectedSerializer = detectedSerializer(serializerIDMsgpack)
)

// detectStoredSerializer scans the first non-empty body it finds under
// the node/edge/embedding prefixes and returns which encoding it uses.
// Returns (detectedNone, false, nil) for an empty database. Used solely
// by the migration tool — the engine's read path dispatches on the body
// header itself, not on a global "active serializer".
func detectStoredSerializer(db *badger.DB) (detectedSerializer, bool, error) {
	if db == nil {
		return detectedNone, false, fmt.Errorf("nil badger db")
	}

	var detected detectedSerializer
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true

		prefixes := [][]byte{
			{prefixNode},
			{prefixEdge},
			{prefixEmbedding},
		}

		for _, prefix := range prefixes {
			opts.Prefix = prefix
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				val, err := item.ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				if len(val) == 0 {
					continue
				}
				headerID, _, ok, err := splitSerializationHeader(val)
				if err != nil {
					it.Close()
					return err
				}
				if ok {
					detected = detectedSerializer(headerID)
				} else {
					detected = detectedGob
				}
				it.Close()
				return ErrIterationStopped
			}
			it.Close()
		}
		return nil
	})
	if err == ErrIterationStopped {
		err = nil
	}
	if err != nil {
		return detectedNone, false, err
	}
	if detected == detectedNone {
		return detectedNone, false, nil
	}
	return detected, true, nil
}
