package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// The ID dictionary assigns a compact 8-byte monotonic numeric ID to each
// string node and edge ID. Secondary indexes (edge-between, outgoing/
// incoming, edge-type, label, MVCC heads) can then encode relationships
// between entities using these numeric IDs instead of repeating full
// 40–50-byte UUIDs, cutting key-space for a 100k-node graph by tens of
// MiB.
//
// Layout on disk:
//
//	[prefixNodeIDForward]  + stringID     -> 8-byte numID
//	[prefixEdgeIDForward]  + stringID     -> 8-byte numID
//	[prefixIDCounter]      + "node" | "edge" -> 8-byte next value
//
// There is intentionally no reverse map. Every call site that resolves a
// numeric ID back to a string already has the string — lookups from
// numeric IDs alone are not required for the caller patterns we support.
//
// The in-memory cache is rebuilt on engine open by scanning the forward
// prefix once. From then on, resolveNodeNumID / resolveEdgeNumID operate
// at memory speed in the hot path.
const (
	prefixIDDictNodeForward = byte(0x1A)
	prefixIDDictEdgeForward = byte(0x1B)
	prefixIDDictCounter     = byte(0x1C)
	// Node reverse map: numID (8B) -> string nodeID. Used by readers
	// that need to recover the full string ID from a numeric ID carried
	// in a value payload (e.g. the compact edge body that stores
	// endpoint numIDs in place of string IDs).
	prefixIDDictNodeReverse = byte(0x1D)
	// Edge reverse map: numID (8B) -> string edgeID. Used by scanners
	// on numeric-keyed edge indexes (outgoing, incoming, edge-type,
	// edge MVCC head) to recover the original string edge ID.
	prefixIDDictEdgeReverse = byte(0x1E)
	// Freelist prefix layout:
	//   [prefixIDFreelist][kind='n'|'e'][numID 8B] -> [freedAtNanos 8B]
	// Sort order puts node and edge freelists next to each other under
	// this single prefix, minimizing iterator bookkeeping and allowing
	// batch promotion with a single scan.
	prefixIDFreelist = byte(0x1F)

	freelistKindNode byte = 'n'
	freelistKindEdge byte = 'e'
)

// numID recycling (debounced freelist).
//
// When a node/edge is deleted, its numID becomes reusable after a
// safety window long enough that no in-flight snapshot reader could
// still be referencing it. Freed numIDs are staged on a wall-clock TTL;
// once the TTL elapses, the allocation path pops them ahead of bumping
// the counter.
//
// Default TTL is 30 seconds: real snapshot readers are query-scoped
// (milliseconds to seconds), so 30s is comfortably larger than any
// normal read window. Operators running long analytics queries tune it
// via NORNICDB_ID_FREELIST_TTL / Database.IDFreelistTTL.
//
// This design fully replaces any need for offline compaction: deleted
// IDs cycle back into use automatically, the uint64 counter effectively
// tracks "peak live count" instead of "cumulative creates", and the
// hard limit (2^64) becomes purely theoretical rather than a real
// failure mode.
const (
	defaultIDFreelistTTL = 30 * time.Second
)

var (
	idCounterNodeKey = []byte{prefixIDDictCounter, 'n'}
	idCounterEdgeKey = []byte{prefixIDDictCounter, 'e'}
)

type idDictionary struct {
	// The maps are RCU-style: readers take the RLock, mutators take the
	// Lock. Hot path (lookup) rarely contends.
	mu          sync.RWMutex
	nodeForward map[NodeID]uint64
	nodeReverse map[uint64]NodeID
	edgeForward map[EdgeID]uint64
	edgeReverse map[uint64]EdgeID

	// Counters for next available numID; also persisted so we don't
	// reuse IDs across restarts.
	nextNode atomic.Uint64
	nextEdge atomic.Uint64

	// Freelist TTL. Recycled numIDs must age at least this long after
	// being freed before allocation can pop them. Protects in-flight
	// snapshot readers from observing numID aliasing across recycles.
	freelistTTL time.Duration

	// Freelist pending counters. Incremented on push, decremented on
	// successful pop. Zero means "freelist is empty for this kind" —
	// allocation can skip the Badger Seek entirely. This matters
	// because bulk seed workloads allocate hundreds of thousands of
	// numIDs and an empty-freelist check per alloc would be dominant.
	// Conservatively incremented by pushes from any txn that may not
	// have committed yet; rolling back a txn leaves the counter
	// slightly inflated but never low (safe direction — we'd Seek once,
	// find nothing, and decrement back to accuracy on next reload).
	nodeFreelistPending atomic.Int64
	edgeFreelistPending atomic.Int64

	// Per-badger.Txn counter-persistence state. Each txn tracks the
	// highest node/edge numID it has allocated; the counter key is
	// written ONCE at commit time instead of once per allocation.
	// Seed workloads commonly allocate thousands of numIDs per txn —
	// batching the counter write collapses N Badger Set() calls to 2.
	// Entries are created lazily on first alloc and removed at
	// flush/discard. Keyed by *badger.Txn pointer (unique per lifetime).
	txnMu       sync.Mutex
	txnCounters map[*badger.Txn]*txnCounterState
}

// txnCounterState tracks the high-water numID allocated during a single
// badger.Txn. Written as a single counter update at txn-flush time.
type txnCounterState struct {
	nodeMax uint64 // 0 = no node allocs on this txn
	edgeMax uint64
}

func newIDDictionary() *idDictionary {
	return &idDictionary{
		nodeForward: make(map[NodeID]uint64),
		nodeReverse: make(map[uint64]NodeID),
		edgeForward: make(map[EdgeID]uint64),
		edgeReverse: make(map[uint64]EdgeID),
		freelistTTL: defaultIDFreelistTTL,
		txnCounters: make(map[*badger.Txn]*txnCounterState),
	}
}

// recordTxnCounterUse stages a numID high-water mark against the txn.
// Called inside resolveOrAllocate* after a fresh allocation (non-recycled).
// Thread-safe for concurrent callers on distinct txns (common: AsyncEngine
// flushes in a single txn, but other paths open their own); distinct txns
// never contend beyond the brief map-insert under txnMu.
func (d *idDictionary) recordTxnCounterUse(txn *badger.Txn, kind byte, num uint64) {
	d.txnMu.Lock()
	st, ok := d.txnCounters[txn]
	if !ok {
		st = &txnCounterState{}
		d.txnCounters[txn] = st
	}
	switch kind {
	case freelistKindNode:
		if num > st.nodeMax {
			st.nodeMax = num
		}
	case freelistKindEdge:
		if num > st.edgeMax {
			st.edgeMax = num
		}
	}
	d.txnMu.Unlock()
}

// flushTxnCounters writes the single counter update(s) for this txn
// (at most one per kind) and detaches the state. Caller MUST invoke
// this from the commit path BEFORE badgerTx.Commit(). Safe to call on
// a txn that never allocated — does nothing.
func (d *idDictionary) flushTxnCounters(txn *badger.Txn) error {
	d.txnMu.Lock()
	st, ok := d.txnCounters[txn]
	if ok {
		delete(d.txnCounters, txn)
	}
	d.txnMu.Unlock()
	if !ok || st == nil {
		return nil
	}
	if st.nodeMax > 0 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], st.nodeMax)
		if err := txn.Set(idCounterNodeKey, append([]byte(nil), buf[:]...)); err != nil {
			return err
		}
	}
	if st.edgeMax > 0 {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], st.edgeMax)
		if err := txn.Set(idCounterEdgeKey, append([]byte(nil), buf[:]...)); err != nil {
			return err
		}
	}
	return nil
}

// discardTxnCounters drops any staged counter state for a txn that is
// about to be discarded (rollback or internal abort). The authoritative
// counter is the atomic one in memory — the persisted Badger key only
// matters on restart, and a rolled-back txn's allocs leave no data for
// those numIDs anyway (they'll be orphaned and eventually freelist-
// reclaimed once history retention clears). Safe to call multiple times.
func (d *idDictionary) discardTxnCounters(txn *badger.Txn) {
	d.txnMu.Lock()
	delete(d.txnCounters, txn)
	d.txnMu.Unlock()
}

// setFreelistTTL configures the debounce window before freed numIDs
// become re-allocatable. Callers (engine open) pass this through from
// config. Zero is treated as "use default".
func (d *idDictionary) setFreelistTTL(ttl time.Duration) {
	if ttl <= 0 {
		ttl = defaultIDFreelistTTL
	}
	d.mu.Lock()
	d.freelistTTL = ttl
	d.mu.Unlock()
}

func (d *idDictionary) currentFreelistTTL() time.Duration {
	d.mu.RLock()
	ttl := d.freelistTTL
	d.mu.RUnlock()
	if ttl <= 0 {
		return defaultIDFreelistTTL
	}
	return ttl
}

// freelistKey builds the per-entry staging key. Entries sort by kind,
// then freedAtNanos, then numID — so the first entry under the prefix
// is always the oldest-freed candidate. Allocation just peeks the head:
// if it's older than the TTL, claim it; otherwise bump the counter.
// No scanning required.
func freelistKey(kind byte, freedAtNanos uint64, numID uint64) []byte {
	out := make([]byte, 1+1+8+8)
	out[0] = prefixIDFreelist
	out[1] = kind
	binary.BigEndian.PutUint64(out[2:10], freedAtNanos)
	binary.BigEndian.PutUint64(out[10:18], numID)
	return out
}

// pushFreeNodeInTxn records a freed node numID with the current
// wall-clock so it can be re-issued after the TTL window elapses.
// Called by the prune pipeline when the last archived version for a
// tombstoned entity is deleted.
func (d *idDictionary) pushFreeNodeInTxn(txn *badger.Txn, numID uint64) error {
	now := uint64(time.Now().UnixNano())
	if err := txn.Set(freelistKey(freelistKindNode, now, numID), nil); err != nil {
		return err
	}
	d.nodeFreelistPending.Add(1)
	return nil
}

// pushFreeEdgeInTxn is the edge analogue of pushFreeNodeInTxn.
func (d *idDictionary) pushFreeEdgeInTxn(txn *badger.Txn, numID uint64) error {
	now := uint64(time.Now().UnixNano())
	if err := txn.Set(freelistKey(freelistKindEdge, now, numID), nil); err != nil {
		return err
	}
	d.edgeFreelistPending.Add(1)
	return nil
}

// popAgedFreelistEntryInTxn claims the oldest-freed numID for the given
// kind if its freedAt is past the TTL window; otherwise it returns
// (0, false, nil).
//
// Fast path: when the in-memory pending counter is zero, we skip the
// Badger iterator entirely. Seed workloads on a fresh database never
// touch the freelist iterator and pay zero overhead per allocation.
func (d *idDictionary) popAgedFreelistEntryInTxn(txn *badger.Txn, kind byte, ttl time.Duration) (uint64, bool, error) {
	// Empty-freelist fast path. No Seek, no allocation.
	switch kind {
	case freelistKindNode:
		if d.nodeFreelistPending.Load() == 0 {
			return 0, false, nil
		}
	case freelistKindEdge:
		if d.edgeFreelistPending.Load() == 0 {
			return 0, false, nil
		}
	}
	prefix := []byte{prefixIDFreelist, kind}
	cutoffNanos := uint64(time.Now().Add(-ttl).UnixNano())
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Rewind()
	if !it.ValidForPrefix(prefix) {
		return 0, false, nil
	}
	key := it.Item().KeyCopy(nil)
	if len(key) != 1+1+8+8 {
		return 0, false, fmt.Errorf("freelist key wrong size: %d", len(key))
	}
	freedAt := binary.BigEndian.Uint64(key[2:10])
	if freedAt > cutoffNanos {
		// Head entry is still too fresh. Because entries are sorted by
		// freedAt, every later entry is equally or more fresh — no point
		// scanning. Fall back to counter bump.
		return 0, false, nil
	}
	numID := binary.BigEndian.Uint64(key[10:18])
	if err := txn.Delete(key); err != nil {
		return 0, false, err
	}
	if kind == freelistKindNode {
		d.nodeFreelistPending.Add(-1)
	} else {
		d.edgeFreelistPending.Add(-1)
	}
	return numID, true, nil
}

// freelistStagedCount returns the persisted freelist entry count for
// the given kind. Used by metrics + tests.
func (d *idDictionary) freelistStagedCount(db *badger.DB, kind byte) (int, error) {
	prefix := []byte{prefixIDFreelist, kind}
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// loadFromBadger scans the persisted forward maps and populates the
// in-memory cache. Called once on engine open.
func (d *idDictionary) loadFromBadger(db *badger.DB) error {
	return db.View(func(txn *badger.Txn) error {
		// Node forward map.
		{
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte{prefixIDDictNodeForward}
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) <= 1 {
					continue
				}
				id := NodeID(string(key[1:]))
				var num uint64
				if err := item.Value(func(val []byte) error {
					if len(val) != 8 {
						return fmt.Errorf("node dict value wrong size: %d", len(val))
					}
					num = binary.BigEndian.Uint64(val)
					return nil
				}); err != nil {
					it.Close()
					return err
				}
				d.nodeForward[id] = num
				d.nodeReverse[num] = id
			}
			it.Close()
		}
		// Edge forward map.
		{
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte{prefixIDDictEdgeForward}
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) <= 1 {
					continue
				}
				id := EdgeID(string(key[1:]))
				var num uint64
				if err := item.Value(func(val []byte) error {
					if len(val) != 8 {
						return fmt.Errorf("edge dict value wrong size: %d", len(val))
					}
					num = binary.BigEndian.Uint64(val)
					return nil
				}); err != nil {
					it.Close()
					return err
				}
				d.edgeForward[id] = num
				d.edgeReverse[num] = id
			}
			it.Close()
		}
		// Counters.
		if item, err := txn.Get(idCounterNodeKey); err == nil {
			if err := item.Value(func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("node counter wrong size: %d", len(val))
				}
				d.nextNode.Store(binary.BigEndian.Uint64(val))
				return nil
			}); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if item, err := txn.Get(idCounterEdgeKey); err == nil {
			if err := item.Value(func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("edge counter wrong size: %d", len(val))
				}
				d.nextEdge.Store(binary.BigEndian.Uint64(val))
				return nil
			}); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		// Seed freelist-pending counters from persisted state so the
		// fast-path "is the freelist empty?" check is accurate after
		// reopen.
		var nodePending, edgePending int64
		for _, kind := range [2]byte{freelistKindNode, freelistKindEdge} {
			prefix := []byte{prefixIDFreelist, kind}
			flOpts := badger.DefaultIteratorOptions
			flOpts.Prefix = prefix
			flOpts.PrefetchValues = false
			flIt := txn.NewIterator(flOpts)
			count := int64(0)
			for flIt.Rewind(); flIt.ValidForPrefix(prefix); flIt.Next() {
				count++
			}
			flIt.Close()
			if kind == freelistKindNode {
				nodePending = count
			} else {
				edgePending = count
			}
		}
		d.nodeFreelistPending.Store(nodePending)
		d.edgeFreelistPending.Store(edgePending)
		return nil
	})
}

// nodeForwardKey / edgeForwardKey encode the forward-map lookup key.
func nodeIDForwardKey(id NodeID) []byte {
	out := make([]byte, 1+len(id))
	out[0] = prefixIDDictNodeForward
	copy(out[1:], id)
	return out
}

func edgeIDForwardKey(id EdgeID) []byte {
	out := make([]byte, 1+len(id))
	out[0] = prefixIDDictEdgeForward
	copy(out[1:], id)
	return out
}

// resolveOrAllocateNodeNumIDInTxn returns the 8-byte numeric ID for the
// given node string ID, allocating a fresh one (and persisting it) on
// first sight. Must be called inside an update txn.
//
// Lock discipline: the shared dict mutex is held ONLY for in-memory map
// lookups and the final map-update commit. Badger writes happen against
// the caller's private *badger.Txn (which is per-goroutine) outside the
// lock so we don't serialize seed-heavy workloads through one global
// mutex. Races where two goroutines allocate concurrently for the same
// string ID are resolved via a map re-check after the Badger writes —
// the loser's numID becomes orphaned and the pruner + freelist reclaim
// it on the next cycle. The counter itself is atomic (never reused
// across a race).
func (d *idDictionary) resolveOrAllocateNodeNumIDInTxn(txn *badger.Txn, id NodeID) (uint64, error) {
	d.mu.RLock()
	if num, ok := d.nodeForward[id]; ok {
		d.mu.RUnlock()
		return num, nil
	}
	ttl := d.freelistTTL
	d.mu.RUnlock()
	if ttl <= 0 {
		ttl = defaultIDFreelistTTL
	}

	// Try the debounced freelist (Badger seek, no dict lock held).
	recycled, ok, err := d.popAgedFreelistEntryInTxn(txn, freelistKindNode, ttl)
	if err != nil {
		return 0, err
	}
	var num uint64
	if ok {
		num = recycled
	} else {
		num = d.nextNode.Add(1)
	}

	// Persist forward + reverse via the per-txn Badger handle. No
	// shared lock required here. The monotonic counter key is NOT
	// written per-alloc; it's staged on the txn and flushed ONCE at
	// commit time. Seed workloads that allocate thousands of numIDs
	// per commit thereby pay two counter-key writes total (one for
	// nodes, one for edges) instead of one per allocation.
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Set(nodeIDForwardKey(id), append([]byte(nil), numBytes[:]...)); err != nil {
		return 0, err
	}
	reverseKey := append([]byte{prefixIDDictNodeReverse}, numBytes[:]...)
	if err := txn.Set(reverseKey, []byte(id)); err != nil {
		return 0, err
	}
	if !ok {
		d.recordTxnCounterUse(txn, freelistKindNode, num)
	}

	// Update the in-memory map under a narrow write lock. Resolve
	// concurrent-create races by adopting whichever numID another
	// goroutine already stored — we abandon `num` (it becomes orphaned
	// in Badger; the pruner + freelist reclaim it).
	d.mu.Lock()
	if existing, already := d.nodeForward[id]; already {
		d.mu.Unlock()
		return existing, nil
	}
	d.nodeForward[id] = num
	d.nodeReverse[num] = id
	d.mu.Unlock()
	return num, nil
}

// lookupNodeIDByNum returns the string node ID for a numeric ID, or
// ("", false) if the reverse map has no entry.
func (d *idDictionary) lookupNodeIDByNum(num uint64) (NodeID, bool) {
	d.mu.RLock()
	id, ok := d.nodeReverse[num]
	d.mu.RUnlock()
	return id, ok
}

// resolveOrAllocateEdgeNumIDInTxn is the edge analogue of
// resolveOrAllocateNodeNumIDInTxn.
func (d *idDictionary) resolveOrAllocateEdgeNumIDInTxn(txn *badger.Txn, id EdgeID) (uint64, error) {
	d.mu.RLock()
	if num, ok := d.edgeForward[id]; ok {
		d.mu.RUnlock()
		return num, nil
	}
	ttl := d.freelistTTL
	d.mu.RUnlock()
	if ttl <= 0 {
		ttl = defaultIDFreelistTTL
	}

	recycled, ok, err := d.popAgedFreelistEntryInTxn(txn, freelistKindEdge, ttl)
	if err != nil {
		return 0, err
	}
	var num uint64
	if ok {
		num = recycled
	} else {
		num = d.nextEdge.Add(1)
	}
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Set(edgeIDForwardKey(id), append([]byte(nil), numBytes[:]...)); err != nil {
		return 0, err
	}
	reverseKey := append([]byte{prefixIDDictEdgeReverse}, numBytes[:]...)
	if err := txn.Set(reverseKey, []byte(id)); err != nil {
		return 0, err
	}
	if !ok {
		d.recordTxnCounterUse(txn, freelistKindEdge, num)
	}

	d.mu.Lock()
	if existing, already := d.edgeForward[id]; already {
		d.mu.Unlock()
		return existing, nil
	}
	d.edgeForward[id] = num
	d.edgeReverse[num] = id
	d.mu.Unlock()
	return num, nil
}

// CounterUsage returns the current node/edge counters. Metrics consumers
// and tests use this to observe how far the monotonic counter has
// advanced. Values here don't decrease even as numIDs cycle via the
// freelist — "peak count" rather than "live count".
func (d *idDictionary) CounterUsage() (nodeCount, edgeCount uint64) {
	return d.nextNode.Load(), d.nextEdge.Load()
}

// FreelistPending returns the in-memory freelist pending counts for
// node and edge kinds. Zero means "no parked numIDs" — allocation
// skips the freelist Seek entirely on the fast path.
func (d *idDictionary) FreelistPending() (nodes, edges int64) {
	return d.nodeFreelistPending.Load(), d.edgeFreelistPending.Load()
}

// IDDictCounters exposes the dict's counter state for observability
// probes. Package-level method on BadgerEngine routes through here.
func (b *BadgerEngine) IDDictCounters() (nodes, edges uint64) {
	if b.idDict == nil {
		return 0, 0
	}
	return b.idDict.CounterUsage()
}

// IDDictFreelistPending exposes the freelist pending counters for
// observability probes.
func (b *BadgerEngine) IDDictFreelistPending() (nodes, edges int64) {
	if b.idDict == nil {
		return 0, 0
	}
	return b.idDict.FreelistPending()
}

// lookupEdgeIDByNum returns the string edge ID for a numeric ID, or
// ("", false) if not present.
func (d *idDictionary) lookupEdgeIDByNum(num uint64) (EdgeID, bool) {
	d.mu.RLock()
	id, ok := d.edgeReverse[num]
	d.mu.RUnlock()
	return id, ok
}

// deleteNodeEntryInTxn removes the forward and reverse dict entries for
// a node. Called by the delete path so dict entries don't accumulate
// after the node is gone. The counter is intentionally NOT decremented
// — numIDs are never reused to avoid aliasing on any index scanners or
// snapshot readers that still hold a reference.
//
// IMPORTANT: callers must not invoke this while any archived MVCC
// version record or in-flight snapshot reader could still need to
// resolve the numID back to a string. Practically, the BadgerEngine
// delete path gates this on `retentionRetainsHistory() == false &&
// activeMVCCSnapshotReaders == 0`. The prune pipeline (which knows when
// historical records go away) is the right place to flush dict entries
// for retention > 0 configurations — a future hook.
func (d *idDictionary) deleteNodeEntryInTxn(txn *badger.Txn, id NodeID) error {
	d.mu.Lock()
	num, ok := d.nodeForward[id]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	delete(d.nodeForward, id)
	delete(d.nodeReverse, num)
	d.mu.Unlock()
	if err := txn.Delete(nodeIDForwardKey(id)); err != nil {
		return err
	}
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	reverseKey := append([]byte{prefixIDDictNodeReverse}, numBytes[:]...)
	return txn.Delete(reverseKey)
}

// forgetNodeCache drops the in-memory cache entry for a node without
// touching persisted storage. Used post-commit by the buffered delete
// path (which persisted the delete via bufferDelete but hasn't yet
// updated the in-memory side).
func (d *idDictionary) forgetNodeCache(id NodeID) {
	d.mu.Lock()
	num, ok := d.nodeForward[id]
	if ok {
		delete(d.nodeForward, id)
		delete(d.nodeReverse, num)
	}
	d.mu.Unlock()
}

// forgetEdgeCache is the edge analogue of forgetNodeCache.
func (d *idDictionary) forgetEdgeCache(id EdgeID) {
	d.mu.Lock()
	num, ok := d.edgeForward[id]
	if ok {
		delete(d.edgeForward, id)
		delete(d.edgeReverse, num)
	}
	d.mu.Unlock()
}

// deleteNodeEntryByNumInTxn removes both dict keys for a node addressed
// by numID. Used by the prune pipeline which has the numID (from the
// version-record scan) but may not know the string ID.
func (d *idDictionary) deleteNodeEntryByNumInTxn(txn *badger.Txn, num uint64) error {
	d.mu.Lock()
	id, ok := d.nodeReverse[num]
	if ok {
		delete(d.nodeForward, id)
		delete(d.nodeReverse, num)
	}
	d.mu.Unlock()
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Delete(append([]byte{prefixIDDictNodeReverse}, numBytes[:]...)); err != nil {
		return err
	}
	if ok {
		if err := txn.Delete(nodeIDForwardKey(id)); err != nil {
			return err
		}
	}
	return nil
}

// deleteEdgeEntryByNumInTxn is the edge analogue.
func (d *idDictionary) deleteEdgeEntryByNumInTxn(txn *badger.Txn, num uint64) error {
	d.mu.Lock()
	id, ok := d.edgeReverse[num]
	if ok {
		delete(d.edgeForward, id)
		delete(d.edgeReverse, num)
	}
	d.mu.Unlock()
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Delete(append([]byte{prefixIDDictEdgeReverse}, numBytes[:]...)); err != nil {
		return err
	}
	if ok {
		if err := txn.Delete(edgeIDForwardKey(id)); err != nil {
			return err
		}
	}
	return nil
}

// deleteEdgeEntryInTxn is the edge analogue of deleteNodeEntryInTxn.
func (d *idDictionary) deleteEdgeEntryInTxn(txn *badger.Txn, id EdgeID) error {
	d.mu.Lock()
	num, ok := d.edgeForward[id]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	delete(d.edgeForward, id)
	delete(d.edgeReverse, num)
	d.mu.Unlock()
	if err := txn.Delete(edgeIDForwardKey(id)); err != nil {
		return err
	}
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	reverseKey := append([]byte{prefixIDDictEdgeReverse}, numBytes[:]...)
	return txn.Delete(reverseKey)
}

// lookupNodeNumID returns the numeric ID if present, or (0,false). Does
// not allocate. Use this on read paths that want to fast-path the
// edge-between index.
func (d *idDictionary) lookupNodeNumID(id NodeID) (uint64, bool) {
	d.mu.RLock()
	num, ok := d.nodeForward[id]
	d.mu.RUnlock()
	return num, ok
}

// lookupEdgeNumID is the edge analogue of lookupNodeNumID.
func (d *idDictionary) lookupEdgeNumID(id EdgeID) (uint64, bool) {
	d.mu.RLock()
	num, ok := d.edgeForward[id]
	d.mu.RUnlock()
	return num, ok
}

// encodeNumID writes a numeric ID as 8 big-endian bytes (for key
// components where we need sort-stable fixed-width encoding).
func encodeNumID(num uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, num)
	return out
}
