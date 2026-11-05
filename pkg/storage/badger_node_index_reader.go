package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"

	"github.com/orneryd/nornicdb/internal/ctxsort"

	"github.com/dgraph-io/badger/v4"
)

// nodeIndexReader walks a numeric node index in its existing key order. A
// scoped read walks the index only until its first foreign or unresolved
// entry, then derives remaining candidates from the forward dictionary's
// physical namespace range and probes membership in the same transaction.
// The global label and MVCC-head indexes contain no namespace in their keys;
// walking them and filtering afterwards makes a small database pay for all
// other databases. The first boundary entry stops that walk: no foreign run
// is enumerated. A sparse label or LIMIT satisfied before the boundary keeps
// its original streaming cost. No node bodies, embeddings or unrelated
// dictionary ranges are read while selecting candidates.
type nodeIndexReader struct {
	ctx        context.Context
	cancel     bulkReadCancellation
	engine     *BadgerEngine
	txn        *badger.Txn
	index      *badger.Iterator
	prefix     []byte
	scope      string
	afterNum   uint64
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

func (b *BadgerEngine) newNodeIndexReader(ctx context.Context, txn *badger.Txn, scope string, prefix []byte, after NodeID) *nodeIndexReader {
	r := &nodeIndexReader{ctx: ctx, engine: b, txn: txn, prefix: prefix, scope: scope}
	if r.err = ctx.Err(); r.err != nil {
		return r
	}
	r.index = txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	if after != "" {
		// The cursor resolves through the stream's own snapshot, in either
		// scope, and keeps snapshotNodeRef's malformed-value diagnostic.
		ref, found, err := b.snapshotNodeRef(txn, after)
		if err == nil && !found {
			err = ErrNotFound
		}
		if err != nil {
			r.err = err
			return r
		}
		r.afterNum = ref.num
		r.key = append(append([]byte(nil), prefix...), encodeNumID(r.afterNum)...)
	}
	return r
}

// useScopedCandidates switches once, before stepping past the first boundary
// entry. Already emitted numeric IDs are excluded, preserving index order and
// cursor behavior without reading or buffering another database's entries.
func (r *nodeIndexReader) useScopedCandidates() {
	r.index.Close()
	r.index = nil
	// The dictionary is already persisted by every numeric-index writer and
	// retains entries needed by MVCC history. Use its snapshot, not the live
	// dictionary cache: a later deletion must not hide a BEGIN-visible node.
	entries := r.txn.NewIterator(badgerPrefixIteratorOptions(nodeIDForwardKey(NodeID(r.scope))))
	defer entries.Close()
	for entries.Rewind(); entries.Valid(); entries.Next() {
		if r.err = r.cancel.check(r.ctx); r.err != nil {
			return
		}
		item := entries.Item()
		var num uint64
		// A malformed forward entry is skipped here (num stays 0), as before.
		if err := item.Value(func(value []byte) error {
			num, _ = decodeNodeRefNum(value)
			return nil
		}); err != nil {
			r.err = err
			return
		}
		if num > r.afterNum {
			r.candidates = append(r.candidates, nodeIndexCandidate{id: NodeID(item.Key()[1:]), num: num})
		}
	}
	if r.err = ctxsort.Sort(r.ctx, r.candidates, func(a, b nodeIndexCandidate) int { return cmp.Compare(a.num, b.num) }, false); r.err != nil {
		return
	}
	r.key = make([]byte, len(r.prefix)+8)
	copy(r.key, r.prefix)
}

func (r *nodeIndexReader) Next() bool {
	if r.err == nil {
		r.err = r.cancel.check(r.ctx)
	}
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
			if r.err = r.cancel.check(r.ctx); r.err != nil {
				return false
			}
			item := r.index.Item()
			key := item.Key()
			if len(key) != len(r.prefix)+8 {
				if r.scope != "" {
					r.useScopedCandidates()
					return r.Next()
				}
				continue
			}
			num := binary.BigEndian.Uint64(key[len(r.prefix):])
			// The index entry stores a numeric identity; the name it stood
			// for belongs to this snapshot, not to the current dictionary a
			// later DROP may have rebuilt.
			name, ok, err := r.engine.readStoredBinding(r.txn, storedBinding{kind: nodeIdentityBinding, number: num})
			if err != nil {
				r.err = err
				return false
			}
			id := NodeID(name)
			if r.scope != "" && (!ok || id == "" || !nodeIDInScope(id, r.scope)) {
				r.useScopedCandidates()
				return r.Next()
			}
			if ok && id != "" {
				r.item, r.id = item, id
				r.afterNum = num
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
func (r *nodeIndexReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.ctx.Err()
}

func (r *nodeIndexReader) IndexKey() []byte {
	if r.item == nil {
		return nil
	}
	return r.item.Key()
}
