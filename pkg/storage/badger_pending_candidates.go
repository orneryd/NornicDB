package storage

import (
	"container/heap"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// pendingEmbeddingCandidates is a derived key catalog, not a second durable
// queue. The existing pending markers remain authoritative: every candidate
// is checked in the finder's transaction before its node is returned. Loading
// a page of the catalog avoids repeatedly walking old marker versions/tombstones
// when a fresh Badger iterator rewinds (and prefetches a second live item).
// Pages continue from a cursor, so a claim's latency and memory do not grow
// with the pending backlog.
type pendingEmbeddingCandidates struct {
	mu         sync.Mutex
	loaded     bool
	generation uint64
	ids        pendingEmbeddingHeap
	members    map[NodeID]struct{}
	// next is the first durable marker key no page has read yet; nil once a
	// page reached the end of the index.
	next []byte
	// A verified empty snapshot is reusable only while the authoritative
	// commit timestamp and invalidation generation are unchanged. Hint loss
	// cannot hide a new durable marker: its commit advances the timestamp.
	emptyVerified bool
	emptyReadTs   uint64
}

const pendingCandidatePageSize = 1024

type pendingEmbeddingHeap []NodeID

func (h pendingEmbeddingHeap) Len() int           { return len(h) }
func (h pendingEmbeddingHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h pendingEmbeddingHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pendingEmbeddingHeap) Push(x any)        { *h = append(*h, x.(NodeID)) }
func (h *pendingEmbeddingHeap) Pop() any {
	old := *h
	last := len(old) - 1
	id := old[last]
	old[last] = ""
	*h = old[:last]
	return id
}

func (p *pendingEmbeddingCandidates) add(id NodeID) {
	if _, exists := p.members[id]; exists {
		return
	}
	if p.members == nil {
		p.members = make(map[NodeID]struct{})
	}
	p.members[id] = struct{}{}
	p.emptyVerified = false
	heap.Push(&p.ids, id)
}

func (p *pendingEmbeddingCandidates) removeFirst() {
	id := heap.Pop(&p.ids).(NodeID)
	delete(p.members, id)
	if len(p.ids) == 0 {
		p.ids = nil
		p.members = nil
		p.emptyVerified = false
	}
}

// load runs under mu and reads at most one page: the next one after the
// cursor, or the first one when the catalog is invalid or a page reached the
// end. A concurrent commit publishes its additions under the same lock, so an
// addition missed by this snapshot cannot be lost. Failed loads leave the
// catalog as it was and can be retried.
func (p *pendingEmbeddingCandidates) load(b *BadgerEngine) error {
	generation := b.pendingCandidatesGeneration.Load()
	if p.loaded && p.generation == generation && len(p.ids) > 0 {
		return nil
	}
	restart := !p.loaded || p.generation != generation || p.next == nil
	start := p.next
	if restart {
		start = []byte{prefixPendingEmbed}
	}
	var ids pendingEmbeddingHeap
	var members map[NodeID]struct{}
	var next []byte
	var readTs uint64
	err := b.withView(func(txn *badger.Txn) error {
		readTs = txn.ReadTs()
		if restart && p.loaded && p.generation == generation && p.emptyVerified && p.emptyReadTs == readTs {
			return nil
		}
		prefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Seek(start); it.Valid() && len(ids) < pendingCandidatePageSize; it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			id := NodeID(key[1:])
			if members == nil {
				members = make(map[NodeID]struct{})
			}
			ids = append(ids, id)
			members[id] = struct{}{}
		}
		if it.Valid() {
			next = it.Item().KeyCopy(nil)
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.ids, p.members, p.loaded, p.next = ids, members, true, next
	p.generation = generation
	p.emptyVerified = restart && len(ids) == 0
	p.emptyReadTs = readTs
	heap.Init(&p.ids)
	return nil
}

// behindCursor reports whether a page already passed id's marker. A later
// marker is read by its page, so only these additions need a hint.
func (p *pendingEmbeddingCandidates) behindCursor(id NodeID) bool {
	return p.next == nil || string(p.next[1:]) > string(id)
}

// setPendingEmbeddingInTxn is the single writer of pending-marker additions.
// Track only hints until commit: rollbacks never publish candidates. Deletes
// need no journal: point validation discards a stale candidate once, without
// another range scan. Keeping hints on out-of-order commit tails is safe for
// the same reason (a later delete always wins in the durable index).
func (b *BadgerEngine) setPendingEmbeddingInTxn(txn *badger.Txn, id NodeID) error {
	return b.setKeyWithPendingHint(txn, pendingEmbedKey(id), []byte{})
}

// Both synchronous buffered keys and write-behind/replay keys pass here, so
// there is one reader of the pending-marker key shape and one hint journal.
func (b *BadgerEngine) setKeyWithPendingHint(txn *badger.Txn, key, value []byte) error {
	if err := txn.Set(key, value); err != nil {
		return err
	}
	if len(key) <= 1 || key[0] != prefixPendingEmbed {
		return nil
	}
	id := NodeID(key[1:])
	journal, ok := b.pendingEmbeddingAdds.Load(txn)
	if !ok {
		journal = make(map[NodeID]struct{})
		b.pendingEmbeddingAdds.Store(txn, journal)
	}
	journal.(map[NodeID]struct{})[id] = struct{}{}
	return nil
}

func (cw *commitWriter) collectPendingEmbeddingAdds(txn *badger.Txn) {
	value, ok := cw.engine.pendingEmbeddingAdds.LoadAndDelete(txn)
	if !ok {
		return
	}
	if cw.pendingEmbeddingIDs == nil {
		cw.pendingEmbeddingIDs = make(map[NodeID]struct{})
	}
	for id := range value.(map[NodeID]struct{}) {
		cw.pendingEmbeddingIDs[id] = struct{}{}
	}
}

func (cw *commitWriter) publishPendingEmbeddingAdds() {
	if len(cw.pendingEmbeddingIDs) == 0 {
		return
	}
	p := &cw.engine.pendingCandidates
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loaded {
		for id := range cw.pendingEmbeddingIDs {
			if p.behindCursor(id) {
				p.add(id)
			}
		}
	}
	cw.pendingEmbeddingIDs = nil
}

// discard also releases unpublished hints when an engine write fails before
// finish. Explicit transactions release theirs through abort below.
func (cw *commitWriter) discard() {
	cw.engine.pendingEmbeddingAdds.Delete(cw.txn)
	cw.batchWriter.discard()
}

// Close has drained helper transactions before releasing the cache. Take this
// lock outside b.mu: a finder already holding it may still check closed state.
func (b *BadgerEngine) releasePendingEmbeddingCandidates() {
	p := &b.pendingCandidates
	p.mu.Lock()
	p.ids = nil
	p.members = nil
	p.next = nil
	p.loaded = false
	p.mu.Unlock()
	b.pendingEmbeddingAdds.Clear()
}
