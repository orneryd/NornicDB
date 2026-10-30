package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// nodeIndexReader walks a numeric node index in its existing key order. Label
// reads use the index after its ready marker is committed, and scan stored
// node bodies while that index is being rebuilt. In ready mode, a scoped read
// walks the index only until its first foreign or unresolved entry, then
// derives remaining candidates from the forward dictionary's physical
// namespace range and probes membership in the same transaction. This avoids
// reading node bodies, embeddings or unrelated dictionary ranges on the fast
// path.
type nodeIndexReader struct {
	ctx        context.Context
	engine     *BadgerEngine
	txn        *badger.Txn
	index      *badger.Iterator
	prefixLen  int
	scope      string
	afterNum   uint64
	key        []byte
	candidates []nodeIndexCandidate
	position   int
	item       *badger.Item
	id         NodeID
	num        uint64
	err        error
}

type nodeIndexCandidate struct {
	id  NodeID
	num uint64
}

func (b *BadgerEngine) newNodeIndexReader(ctx context.Context, txn *badger.Txn, scope string, prefix []byte, after NodeID) *nodeIndexReader {
	// key borrows the prefix until a resume key or scoped probe buffer is
	// needed. Keeping a second slice header made every ready reader larger.
	r := &nodeIndexReader{ctx: ctx, engine: b, txn: txn, key: prefix, prefixLen: len(prefix), scope: scope, position: -1}
	if err := ctx.Err(); err != nil {
		r.err = err
		return r
	}
	r.index = txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	if scope == "" {
		if after != "" {
			num, ok, err := b.labelIndexNodeNumInTxn(txn, after)
			if err == nil && !ok {
				err = ErrNotFound
			}
			if err != nil {
				r.err = err
				return r
			}
			r.key = append(append([]byte(nil), prefix...), encodeNumID(num)...)
		}
		return r
	}

	if after != "" {
		num, found, err := b.labelIndexNodeNumInTxn(txn, after)
		if err == nil && !found {
			err = ErrNotFound
		}
		if err != nil {
			r.err = err
			return r
		}
		r.afterNum = num
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
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return
	}
	// The dictionary is already persisted by every numeric-index writer and
	// retains entries needed by MVCC history. Use its snapshot, not the live
	// dictionary cache: a later deletion must not hide a BEGIN-visible node.
	entries := r.txn.NewIterator(badgerPrefixIteratorOptions(nodeIDForwardKey(NodeID(r.scope))))
	defer entries.Close()
	for entries.Rewind(); entries.Valid(); entries.Next() {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return
		}
		item := entries.Item()
		var num uint64
		if err := item.Value(func(value []byte) error {
			if len(value) == 8 {
				num = binary.BigEndian.Uint64(value)
			}
			return nil
		}); err != nil {
			r.err = err
			return
		}
		if num > r.afterNum {
			r.candidates = append(r.candidates, nodeIndexCandidate{id: NodeID(item.Key()[1:]), num: num})
		}
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return
	}
	if err := sortIndexCandidatesWithContext(r.ctx, r.candidates, func(a, b nodeIndexCandidate) bool { return a.num < b.num }); err != nil {
		r.err = err
		return
	}
	key := make([]byte, r.prefixLen+8)
	copy(key, r.key[:r.prefixLen])
	r.key = key
}

func (r *nodeIndexReader) Next() bool {
	if r.err != nil {
		return false
	}
	if err := r.ctx.Err(); err != nil {
		r.err = err
		return false
	}
	if r.index != nil {
		if r.position < 0 {
			r.position = 0
			if len(r.key) == r.prefixLen {
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
			if err := r.ctx.Err(); err != nil {
				r.err = err
				return false
			}
			item := r.index.Item()
			key := item.Key()
			if len(key) != r.prefixLen+8 {
				if r.scope != "" {
					r.useScopedCandidates()
					return r.Next()
				}
				continue
			}
			num := binary.BigEndian.Uint64(key[r.prefixLen:])
			id, ok := r.engine.labelIndexNodeID(num)
			if err := r.ctx.Err(); err != nil {
				r.err = err
				return false
			}
			if r.scope != "" && (!ok || id == "" || !nodeIDInScope(id, r.scope)) {
				r.useScopedCandidates()
				return r.Next()
			}
			if ok && id != "" {
				r.item, r.id, r.num = item, id, num
				r.afterNum = num
				return true
			}
		}
		return false
	}
	for r.position < len(r.candidates) {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			return false
		}
		candidate := r.candidates[r.position]
		r.position++
		binary.BigEndian.PutUint64(r.key[r.prefixLen:], candidate.num)
		item, err := r.txn.Get(r.key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			r.err = err
			return false
		}
		r.item, r.id, r.num = item, candidate.id, candidate.num
		return true
	}
	return false
}

func (r *labelFallbackReader) nextLabelBody() bool {
	for {
		if err := r.labelContext.Err(); err != nil {
			r.err = err
			return false
		}
		if !r.bodyScanStarted {
			r.bodyScanStarted = true
			if r.after == "" {
				r.bodyScan.Rewind()
			} else {
				start := nodeKey(r.after)
				r.bodyScan.Seek(start)
				if r.bodyScan.Valid() && string(r.bodyScan.Item().Key()) == string(start) {
					r.bodyScan.Next()
				}
			}
		} else {
			r.bodyScan.Next()
		}
		if !r.bodyScan.ValidForPrefix(r.bodyPrefix) {
			return false
		}

		item := r.bodyScan.Item()
		key := item.Key()
		if len(key) <= len(r.bodyPrefix) {
			continue
		}
		id := NodeID(key[1:])

		var labels []string
		hidden := false
		if err := item.Value(func(value []byte) error {
			// Visible decay scoring needs stored properties and timestamps. Stored
			// discovery and non-decay label checks use the payload-free mode instead.
			if r.visibility == visibleRead && r.engine.decayEnabled && !r.engine.revealAll.Load() {
				node, err := r.engine.decodeNodeProjected(namespaceForNodeID(id), value, nil)
				if err != nil {
					return err
				}
				labels = node.Labels
				hidden = r.engine.filterEntityByDecay(r.visibility, node, r.nowNanos)
				return nil
			}
			metadata, err := decodeNodeMetadata(value)
			if err != nil {
				return err
			}
			labels = metadata.Labels
			hidden = r.engine.filterEntityByDecay(r.visibility, &Node{ID: metadata.ID, Labels: labels, VisibilitySuppressed: metadata.VisibilitySuppressed}, r.nowNanos)
			return nil
		}); err != nil {
			r.err = err
			return false
		}
		if !labelsContain(labels, r.label) || hidden {
			continue
		}

		r.item = item
		r.id = id
		return true
	}
}

func (r *labelFallbackReader) nextLabelID() bool {
	if !r.idScanStarted {
		r.idScanStarted = true
		r.forwardScan = r.txn.NewIterator(badgerPrefixIteratorOptions(r.forwardPrefix))
		r.bodyScan = r.txn.NewIterator(badgerPrefixIteratorOptions(r.bodyPrefix))
		seekAfter := func(it *badger.Iterator, prefix, start []byte) {
			if r.after == "" {
				it.Rewind()
				return
			}
			it.Seek(start)
			if it.ValidForPrefix(prefix) && string(it.Item().Key()) == string(start) {
				it.Next()
			}
		}
		seekAfter(r.forwardScan, r.forwardPrefix, nodeIDForwardKey(r.after))
		seekAfter(r.bodyScan, r.bodyPrefix, nodeKey(r.after))
	}

	for {
		if err := r.labelContext.Err(); err != nil {
			r.err = err
			return false
		}
		forwardValid := r.forwardScan.ValidForPrefix(r.forwardPrefix)
		bodyValid := r.bodyScan.ValidForPrefix(r.bodyPrefix)
		if !forwardValid && !bodyValid {
			return false
		}

		var id NodeID
		switch {
		case !bodyValid:
			key := r.forwardScan.Item().Key()
			if len(key) > 1 {
				id = NodeID(key[1:])
			}
			r.forwardScan.Next()
		case !forwardValid:
			key := r.bodyScan.Item().Key()
			if len(key) > 1 {
				id = NodeID(key[1:])
			}
			r.bodyScan.Next()
		default:
			forwardKey := r.forwardScan.Item().Key()
			bodyKey := r.bodyScan.Item().Key()
			forwardID := forwardKey[1:]
			bodyID := bodyKey[1:]
			switch bytes.Compare(forwardID, bodyID) {
			case -1:
				id = NodeID(forwardID)
				r.forwardScan.Next()
			case 1:
				id = NodeID(bodyID)
				r.bodyScan.Next()
			default:
				id = NodeID(forwardID)
				r.forwardScan.Next()
				r.bodyScan.Next()
			}
		}
		if id == "" {
			continue
		}
		r.id = id
		r.item = nil
		return true
	}
}

func (r *nodeIndexReader) Close() {
	if r.index != nil {
		r.index.Close()
	}
}

func (r *nodeIndexReader) Item() *badger.Item      { return r.item }
func (r *nodeIndexReader) NodeID() NodeID          { return r.id }
func (r *nodeIndexReader) Err() error              { return r.err }
func (r *nodeIndexReader) UsesLabelFallback() bool { return false }

// NodeRef returns the number the current entry carries in the snapshot's
// index key, or in its forward dictionary for a scoped candidate.
func (r *nodeIndexReader) NodeRef() (nodeRef, bool, error) {
	return nodeRef{id: r.id, num: r.num}, r.id != "", nil
}

// IndexKey returns the actual key when candidates came from the label index.
// A body-scan fallback has no index key to check for tombstones.
func (r *nodeIndexReader) IndexKey() []byte {
	if r.item == nil {
		return nil
	}
	return r.item.Key()
}
