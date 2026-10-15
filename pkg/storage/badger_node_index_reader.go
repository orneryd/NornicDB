package storage

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// nodeIndexReader walks a numeric node index in its existing key order. A
// scoped read derives candidates from the forward dictionary's physical
// namespace range, then probes index membership in the same transaction.
// The global label and MVCC-head indexes contain no namespace in their keys;
// walking them and filtering afterwards makes a small database pay for all
// other databases. No node bodies, embeddings or unrelated dictionary ranges
// are read while selecting candidates.
type nodeIndexReader struct {
	engine     *BadgerEngine
	txn        *badger.Txn
	index      *badger.Iterator
	prefix     []byte
	key        []byte
	candidates []nodeIndexCandidate
	position   int
	item       *badger.Item
	id         NodeID
	err        error
	started    bool
}

type nodeIndexCandidate struct {
	id  NodeID
	num uint64
}

func (b *BadgerEngine) newNodeIndexReader(txn *badger.Txn, scope string, prefix []byte, after NodeID) *nodeIndexReader {
	r := &nodeIndexReader{engine: b, txn: txn, prefix: prefix}
	if scope == "" {
		r.index = txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		if after != "" {
			num, ok := b.idDict.lookupNodeNumID(after)
			if !ok {
				r.err = ErrNotFound
				return r
			}
			r.key = append(append([]byte(nil), prefix...), encodeNumID(num)...)
		}
		return r
	}

	var afterNum uint64
	if after != "" {
		item, err := txn.Get(nodeIDForwardKey(after))
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = ErrNotFound
		}
		if err == nil {
			err = item.Value(func(value []byte) error {
				if len(value) != 8 {
					return ErrInvalidData
				}
				afterNum = binary.BigEndian.Uint64(value)
				return nil
			})
		}
		if err != nil {
			r.err = err
			return r
		}
	}
	// The dictionary is already persisted by every numeric-index writer and
	// retains entries needed by MVCC history. Use its snapshot, not the live
	// dictionary cache: a later deletion must not hide a BEGIN-visible node.
	entries := txn.NewIterator(badgerPrefixIteratorOptions(nodeIDForwardKey(NodeID(scope))))
	defer entries.Close()
	for entries.Rewind(); entries.Valid(); entries.Next() {
		item := entries.Item()
		var num uint64
		if err := item.Value(func(value []byte) error {
			if len(value) == 8 {
				num = binary.BigEndian.Uint64(value)
			}
			return nil
		}); err != nil {
			r.err = err
			return r
		}
		if num != 0 && (after == "" || num > afterNum) {
			r.candidates = append(r.candidates, nodeIndexCandidate{id: NodeID(item.Key()[1:]), num: num})
		}
	}
	sort.Slice(r.candidates, func(i, j int) bool { return r.candidates[i].num < r.candidates[j].num })
	r.key = make([]byte, len(prefix)+8)
	copy(r.key, prefix)
	return r
}

func (r *nodeIndexReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.index != nil {
		if !r.started {
			r.started = true
			if r.key == nil {
				r.index.Rewind()
			} else {
				r.index.Seek(r.key)
				if r.index.Valid() && string(r.index.Item().Key()) == string(r.key) {
					r.index.Next()
				}
			}
		} else {
			r.index.Next()
		}
		for ; r.index.Valid(); r.index.Next() {
			item := r.index.Item()
			key := item.Key()
			if len(key) != len(r.prefix)+8 {
				continue
			}
			num := binary.BigEndian.Uint64(key[len(r.prefix):])
			id, ok := r.engine.idDict.lookupNodeIDByNum(num)
			if ok && id != "" {
				r.item, r.id = item, id
				return true
			}
		}
		return false
	}
	for r.position < len(r.candidates) {
		candidate := r.candidates[r.position]
		r.position++
		binary.BigEndian.PutUint64(r.key[len(r.prefix):], candidate.num)
		item, err := r.txn.Get(r.key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			r.err = err
			return false
		}
		r.item, r.id = item, candidate.id
		return true
	}
	return false
}

func (r *nodeIndexReader) Close() {
	if r.index != nil {
		r.index.Close()
	}
}

func (r *nodeIndexReader) Item() *badger.Item { return r.item }
func (r *nodeIndexReader) NodeID() NodeID     { return r.id }
func (r *nodeIndexReader) Err() error         { return r.err }
