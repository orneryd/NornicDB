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

	// nodeDictShards is the number of independently locked slices of the
	// node ID dictionary. Every created node writes its forward/reverse
	// pair through this dictionary; under a single engine-wide lock the
	// bulk-create path serialized here at 87% of all lock wait (PR #1043
	// write-path measurements). Sharding roughly doubles batched node
	// creation at 16 writers (69k -> 148k nodes/s in-process).
	nodeDictShards = 64
)

var (
	idCounterNodeKey = []byte{prefixIDDictCounter, 'n'}
	idCounterEdgeKey = []byte{prefixIDDictCounter, 'e'}
)

// nodeDictShard is one slice of the node ID dictionary.
type nodeDictShard struct {
	mu      sync.RWMutex
	forward map[NodeID]uint64
	reverse map[uint64]NodeID
	// pending marks, by forward shard, the entries whose forward and
	// reverse keys no committed transaction has written yet: the
	// transaction that allocated one may still roll back, leaving the entry
	// in memory only (#907, I29). A transaction that resolves a pending
	// entry writes its keys too, and the first commit that persists them
	// clears the mark. Each holds the state of the transaction that last
	// wrote the keys, so that transaction resolving the entry again doesn't
	// write them again.
	pending map[NodeID]*txnCounterState
}

// nodeForwardShard picks the shard of a node's forward entry. FNV-1a over
// the string ID, no allocation.
func nodeForwardShard(id NodeID) int {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return int(h % nodeDictShards)
}

// nodeReverseShard picks the shard of a node's reverse entry.
func nodeReverseShard(num uint64) int {
	return int(num % nodeDictShards)
}

// edgeDictShard is one slice of the edge ID dictionary, sharded the same
// way as nodes: every created relationship writes its forward/reverse pair
// through this dictionary, so the single shared lock serialized bulk edge
// creation under concurrency.
type edgeDictShard struct {
	mu      sync.RWMutex
	forward map[EdgeID]uint64
	reverse map[uint64]EdgeID
	pending map[EdgeID]*txnCounterState // as nodeDictShard.pending
}

// edgeForwardShard picks the shard of an edge's forward entry.
func edgeForwardShard(id EdgeID) int {
	var h uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return int(h % nodeDictShards)
}

// edgeReverseShard picks the shard of an edge's reverse entry.
func edgeReverseShard(num uint64) int {
	return int(num % nodeDictShards)
}

type idDictionary struct {
	// nodeForward / nodeReverse and edgeForward / edgeReverse are sharded
	// into nodes[] / edges[]: each shard has its own RWMutex, so
	// concurrent creates of distinct entities no longer serialize on one
	// dictionary-wide lock. mu guards freelistTTL only.
	mu    sync.RWMutex
	nodes [nodeDictShards]nodeDictShard
	edges [nodeDictShards]edgeDictShard

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
	// lastFreedStamp is the monotonic freed-at stamp stamped on freelist
	// keys. Wall-clock time is NOT used directly: under NTP steps (and with
	// per-thread cached time.Now reads, which repeat under very fast
	// execution) two pushes can stamp out of order, breaking the
	// "head entry is oldest" invariant the pop fast path relies on. The
	// stamp is max(wallNow, last+1), so it never decreases, and it is
	// floored at open from the stamps already on disk.
	lastFreedStamp atomic.Uint64

	// Per-badger.Txn counter-persistence state. Each txn tracks the
	// highest node/edge numID it has allocated; the counter key is
	// written ONCE at commit time instead of once per allocation.
	// Seed workloads commonly allocate thousands of numIDs per txn —
	// batching the counter write collapses N Badger Set() calls to 2.
	// Entries are created lazily on first alloc and removed at
	// flush/discard. Keyed by *badger.Txn pointer (unique per lifetime).
	txnMu       sync.Mutex
	txnCounters map[*badger.Txn]*txnCounterState
	// txnCommitting holds the pending entries a txn persists, from
	// flushTxnCounters until its commit succeeds (markTxnCommitted) or it
	// is discarded.
	txnCommitting map[*badger.Txn]*txnCounterState
}

// txnCounterState tracks the high-water numID allocated during a single
// badger.Txn. Written as a single counter update at txn-flush time.
type txnCounterState struct {
	nodeMax uint64 // 0 = no node allocs on this txn
	edgeMax uint64
	// The pending entries whose keys this txn writes.
	nodes []NodeID
	edges []EdgeID
}

func newIDDictionary() *idDictionary {
	d := &idDictionary{
		freelistTTL:   defaultIDFreelistTTL,
		txnCounters:   make(map[*badger.Txn]*txnCounterState),
		txnCommitting: make(map[*badger.Txn]*txnCounterState),
	}
	for i := range d.nodes {
		d.nodes[i].forward = make(map[NodeID]uint64)
		d.nodes[i].reverse = make(map[uint64]NodeID)
		d.nodes[i].pending = make(map[NodeID]*txnCounterState)
	}
	for i := range d.edges {
		d.edges[i].forward = make(map[EdgeID]uint64)
		d.edges[i].reverse = make(map[uint64]EdgeID)
		d.edges[i].pending = make(map[EdgeID]*txnCounterState)
	}
	return d
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

// flushTxnCounters detaches the staged counter state for this txn and
// returns the high-water (node, edge) pair the caller should persist
// AFTER the user transaction commits. The persistence write must NOT go
// through the user txn: every node-creating commit would otherwise touch
// the same counter key and Badger's optimistic concurrency would surface
// a "Transaction Conflict" on concurrent writers, hiding genuine
// constraint violations from the per-namespace commit-time UNIQUE check
// (see docs/plans/consumer-pinned-error-contract-plan.md §2.1).
//
// Returns (0, 0) if the txn allocated no fresh numIDs.
func (d *idDictionary) flushTxnCounters(txn *badger.Txn) (uint64, uint64) {
	d.txnMu.Lock()
	st, ok := d.txnCounters[txn]
	if ok {
		delete(d.txnCounters, txn)
		if len(st.nodes) > 0 || len(st.edges) > 0 {
			d.txnCommitting[txn] = st
		}
	}
	d.txnMu.Unlock()
	if !ok || st == nil {
		return 0, 0
	}
	return st.nodeMax, st.edgeMax
}

// stageTxnEntry records that txn writes the keys of a pending entry (a node
// ID, or an edge ID when edge is set), so its commit can clear the mark, and
// returns txn's state.
func (d *idDictionary) stageTxnEntry(txn *badger.Txn, node NodeID, edge EdgeID) *txnCounterState {
	d.txnMu.Lock()
	st, ok := d.txnCounters[txn]
	if !ok {
		st = &txnCounterState{}
		d.txnCounters[txn] = st
	}
	if edge != "" {
		st.edges = append(st.edges, edge)
	} else {
		st.nodes = append(st.nodes, node)
	}
	d.txnMu.Unlock()
	return st
}

// txnWrote reports whether writer, a pending entry's last writer, is txn's
// state: txn has written the entry's keys already.
func (d *idDictionary) txnWrote(txn *badger.Txn, writer *txnCounterState) bool {
	if txn == nil || writer == nil {
		return false
	}
	d.txnMu.Lock()
	st := d.txnCounters[txn]
	d.txnMu.Unlock()
	return st == writer
}

// markTxnCommitted clears the pending mark of the entries txn persisted.
// Called once its commit succeeded; flushTxnCounters ran before it.
func (d *idDictionary) markTxnCommitted(txn *badger.Txn) {
	d.clearPending(d.takeTxnCommitting(txn))
}

// takeTxnCommitting detaches the pending entries txn's commit persists
// (staged by flushTxnCounters), for a commit whose keys land later:
// write-behind's flush persists them and then clears them (clearPending).
// Nil when txn wrote none.
func (d *idDictionary) takeTxnCommitting(txn *badger.Txn) *txnCounterState {
	d.txnMu.Lock()
	defer d.txnMu.Unlock()
	st := d.txnCommitting[txn]
	delete(d.txnCommitting, txn)
	return st
}

// clearPending clears the pending mark of the entries st wrote, once a
// commit has persisted their keys. An entry another transaction wrote since
// (rewritten, or deleted and allocated again) keeps the mark: its last
// writer's commit clears it. Nil does nothing.
func (d *idDictionary) clearPending(st *txnCounterState) {
	if st == nil {
		return
	}
	for _, id := range st.nodes {
		sh := &d.nodes[nodeForwardShard(id)]
		sh.mu.Lock()
		if sh.pending[id] == st {
			delete(sh.pending, id)
		}
		sh.mu.Unlock()
	}
	for _, id := range st.edges {
		sh := &d.edges[edgeForwardShard(id)]
		sh.mu.Lock()
		if sh.pending[id] == st {
			delete(sh.pending, id)
		}
		sh.mu.Unlock()
	}
}

// nodePendingWriter is a pending node entry's last writer, if it is
// pending.
func (d *idDictionary) nodePendingWriter(id NodeID) (*txnCounterState, bool) {
	sh := &d.nodes[nodeForwardShard(id)]
	sh.mu.RLock()
	writer, pending := sh.pending[id]
	sh.mu.RUnlock()
	return writer, pending
}

// edgePendingWriter is nodePendingWriter for an edge entry.
func (d *idDictionary) edgePendingWriter(id EdgeID) (*txnCounterState, bool) {
	sh := &d.edges[edgeForwardShard(id)]
	sh.mu.RLock()
	writer, pending := sh.pending[id]
	sh.mu.RUnlock()
	return writer, pending
}

// persistCounters writes the node and edge counter high-water marks in a
// fresh badger transaction so they do not participate in the user
// transaction's conflict-detection set. Best effort: the in-memory
// nextNode/nextEdge atomics are authoritative for the engine's lifetime;
// a crash between commit and persistence loses at most the unflushed
// window of allocated numIDs, reconciled at next engine open.
func (d *idDictionary) persistCounters(db badgerKV, nodeMax, edgeMax uint64) {
	if db == nil || (nodeMax == 0 && edgeMax == 0) {
		return
	}
	_ = updateBadgerMetadata(db, func(txn *badger.Txn) error {
		if nodeMax > 0 {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], nodeMax)
			if err := txn.Set(idCounterNodeKey, append([]byte(nil), buf[:]...)); err != nil {
				return err
			}
		}
		if edgeMax > 0 {
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], edgeMax)
			if err := txn.Set(idCounterEdgeKey, append([]byte(nil), buf[:]...)); err != nil {
				return err
			}
		}
		return nil
	})
}

// moveTxnCounters re-stages from's counter high-water marks against to, a
// Badger transaction just opened (nothing is staged against it yet). A
// commit larger than one Badger batch continues in a new Badger transaction
// (see commitWriter.openBatch); its staged counters follow it.
func (d *idDictionary) moveTxnCounters(from, to *badger.Txn) {
	d.txnMu.Lock()
	if st, ok := d.txnCounters[from]; ok {
		delete(d.txnCounters, from)
		d.txnCounters[to] = st
	}
	d.txnMu.Unlock()
}

// discardTxnCounters drops any staged counter state for a txn that is
// about to be discarded (rollback or internal abort). The authoritative
// counter is the atomic one in memory — the persisted Badger key only
// matters on restart, and a rolled-back txn's allocs leave no data for
// those numIDs anyway (they'll be orphaned and eventually freelist-
// reclaimed once history retention clears). Safe to call multiple times.
func (d *idDictionary) discardTxnCounters(txn *badger.Txn) {
	d.txnMu.Lock()
	for _, states := range []map[*badger.Txn]*txnCounterState{d.txnCounters, d.txnCommitting} {
		if st, ok := states[txn]; ok {
			// Pending entries still name it as their writer; drop the list.
			st.nodes, st.edges = nil, nil
			delete(states, txn)
		}
	}
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

// nextFreedStamp mints the next freelist freed-at stamp: strictly
// increasing, and never behind the wall clock. Since a stamp is always
// >= the wall time at push, comparing it against a wall-clock cutoff can
// only DELAY a pop, never claim an entry before its TTL elapsed.
func (d *idDictionary) nextFreedStamp() uint64 {
	wall := uint64(time.Now().UnixNano())
	for {
		last := d.lastFreedStamp.Load()
		next := wall
		if last >= next {
			next = last + 1
		}
		if d.lastFreedStamp.CompareAndSwap(last, next) {
			return next
		}
	}
}

// freelistKey builds the per-entry staging key. Entries sort by kind,
// then freedStamp, then numID — so the first entry under the prefix
// is always the oldest-freed candidate. Allocation just peeks the head:
// if it's older than the TTL, claim it; otherwise bump the counter.
// No scanning required.
func freelistKey(kind byte, freedStamp uint64, numID uint64) []byte {
	out := make([]byte, 1+1+8+8)
	out[0] = prefixIDFreelist
	out[1] = kind
	binary.BigEndian.PutUint64(out[2:10], freedStamp)
	binary.BigEndian.PutUint64(out[10:18], numID)
	return out
}

// pushFreeNodeInTxn records a freed node numID with a monotonic stamp so
// it can be re-issued after the TTL window elapses. Called by the prune
// pipeline when the last archived version for a tombstoned entity is
// deleted.
func (d *idDictionary) pushFreeNodeInTxn(txn *badger.Txn, numID uint64) error {
	now := d.nextFreedStamp()
	if err := txn.Set(freelistKey(freelistKindNode, now, numID), nil); err != nil {
		return err
	}
	d.nodeFreelistPending.Add(1)
	return nil
}

// pushFreeEdgeInTxn is the edge analogue of pushFreeNodeInTxn.
func (d *idDictionary) pushFreeEdgeInTxn(txn *badger.Txn, numID uint64) error {
	now := d.nextFreedStamp()
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
func (d *idDictionary) popAgedFreelistEntryInTxn(txn kvWriter, kind byte, ttl time.Duration) (uint64, bool, error) {
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
	opts := badgerIteratorOptions()
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	// Skip entries this transaction already popped: a transaction stages
	// its pops (stagedKV), and the iterator does not see staged deletions.
	it.Rewind()
	for it.ValidForPrefix(prefix) && kvStagedDeleted(txn, it.Item().Key()) {
		it.Next()
	}
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
func (d *idDictionary) freelistStagedCount(db badgerKV, kind byte) (int, error) {
	prefix := []byte{prefixIDFreelist, kind}
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
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
//
// The counter keys are advisory, not authoritative: persistCounters runs in
// its own Badger transaction AFTER the user transaction commits, so a crash,
// a kill, or an engine Close racing the commit tail leaves committed forward
// entries whose numIDs sit above the persisted counter. The forward map is
// the durable record of every numID actually in use, so the counters are
// floored at the highest numID it holds. Without that floor the next
// allocation reissues a live numID and two string IDs share one compact key
// in every numID-keyed index (adjacency, label, edge-between, MVCC heads),
// silently merging the two entities.
func (d *idDictionary) loadFromBadger(db badgerViewer) error {
	var maxNodeNum, maxEdgeNum uint64
	return db.View(func(txn *badger.Txn) error {
		// Node forward map.
		{
			opts := badgerIteratorOptions()
			opts.Prefix = []byte{prefixIDDictNodeForward}
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
				// Populate the sharded maps directly: loadFromBadger runs at
				// engine open before the dictionary is reachable by readers.
				d.nodes[nodeForwardShard(id)].forward[id] = num
				d.nodes[nodeReverseShard(num)].reverse[num] = id
				if num > maxNodeNum {
					maxNodeNum = num
				}
			}
			it.Close()
		}
		// Edge forward map.
		{
			opts := badgerIteratorOptions()
			opts.Prefix = []byte{prefixIDDictEdgeForward}
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
				d.edges[edgeForwardShard(id)].forward[id] = num
				d.edges[edgeReverseShard(num)].reverse[num] = id
				if num > maxEdgeNum {
					maxEdgeNum = num
				}
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
		// Floor both counters at the highest numID the durable forward maps
		// hold, so a counter write lost after a commit can never lead to a
		// live numID being reissued (see the function doc).
		if maxNodeNum > d.nextNode.Load() {
			d.nextNode.Store(maxNodeNum)
		}
		if maxEdgeNum > d.nextEdge.Load() {
			d.nextEdge.Store(maxEdgeNum)
		}
		// Seed freelist-pending counters from persisted state so the
		// fast-path "is the freelist empty?" check is accurate after
		// reopen, and floor the monotonic freed-stamp at the highest
		// stamp already on disk so new pushes keep the sort invariant
		// across restarts.
		var nodePending, edgePending int64
		var maxStamp uint64
		for _, kind := range [2]byte{freelistKindNode, freelistKindEdge} {
			prefix := []byte{prefixIDFreelist, kind}
			flOpts := badgerIteratorOptions()
			flOpts.Prefix = prefix
			flIt := txn.NewIterator(flOpts)
			count := int64(0)
			for flIt.Rewind(); flIt.ValidForPrefix(prefix); flIt.Next() {
				count++
				if key := flIt.Item().Key(); len(key) == 1+1+8+8 {
					if stamp := binary.BigEndian.Uint64(key[2:10]); stamp > maxStamp {
						maxStamp = stamp
					}
				}
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
		d.lastFreedStamp.Store(maxStamp)
		return nil
	})
}

// nodeForwardKey / edgeForwardKey encode the forward-map lookup key.
func nodeIDForwardKey(id NodeID) []byte {
	out := make([]byte, 1)
	out[0] = prefixIDDictNodeForward
	out = append(out, id...)
	return out
}

func edgeIDForwardKey(id EdgeID) []byte {
	out := make([]byte, 1)
	out[0] = prefixIDDictEdgeForward
	out = append(out, id...)
	return out
}

// nodeForwardLookup returns the numeric ID for a node string ID.
func (d *idDictionary) nodeForwardLookup(id NodeID) (uint64, bool) {
	sh := &d.nodes[nodeForwardShard(id)]
	sh.mu.RLock()
	num, ok := sh.forward[id]
	sh.mu.RUnlock()
	return num, ok
}

// nodeReverseLookup returns the string node ID for a numeric ID.
func (d *idDictionary) nodeReverseLookup(num uint64) (NodeID, bool) {
	sh := &d.nodes[nodeReverseShard(num)]
	sh.mu.RLock()
	id, ok := sh.reverse[num]
	sh.mu.RUnlock()
	return id, ok
}

// nodePairStore installs forward[id] = num and reverse[num] = id. The
// forward entry is published before the reverse one: a reverse reader can
// never observe a num whose forward entry isn't visible yet. When another
// goroutine already stored a num for id it returns that num with ok=false
// (the caller adopts it and must not publish reverse itself).
func (d *idDictionary) nodePairStore(id NodeID, num uint64, writer *txnCounterState) (uint64, bool) {
	fshard := &d.nodes[nodeForwardShard(id)]
	fshard.mu.Lock()
	if existing, already := fshard.forward[id]; already {
		fshard.mu.Unlock()
		return existing, false
	}
	fshard.forward[id] = num
	if writer != nil {
		// Pending until a commit persists its keys (pending).
		fshard.pending[id] = writer
	}
	fshard.mu.Unlock()
	rshard := &d.nodes[nodeReverseShard(num)]
	rshard.mu.Lock()
	// Sole writer of this num at creation time (numIDs are unique per
	// allocation; reissue requires a full freelist TTL cycle that cannot
	// overlap this pair write).
	rshard.reverse[num] = id
	rshard.mu.Unlock()
	return num, true
}

// nodePairRemoveByID removes forward[id] and the matching reverse entry.
// The forward entry goes first so a reverse reader can never observe a num
// whose forward entry is already gone. Returns the removed num.
func (d *idDictionary) nodePairRemoveByID(id NodeID) (uint64, bool) {
	fshard := &d.nodes[nodeForwardShard(id)]
	fshard.mu.Lock()
	num, ok := fshard.forward[id]
	if ok {
		delete(fshard.forward, id)
		delete(fshard.pending, id)
	}
	fshard.mu.Unlock()
	if !ok {
		return 0, false
	}
	rshard := &d.nodes[nodeReverseShard(num)]
	rshard.mu.Lock()
	if rshard.reverse[num] == id {
		delete(rshard.reverse, num)
	}
	rshard.mu.Unlock()
	return num, true
}

// nodePairRemoveByNum removes reverse[num] and the matching forward entry.
// The forward delete is guarded by its value so a reissued num can never
// strip a newer pair.
func (d *idDictionary) nodePairRemoveByNum(num uint64) (NodeID, bool) {
	rshard := &d.nodes[nodeReverseShard(num)]
	rshard.mu.Lock()
	id, ok := rshard.reverse[num]
	if ok {
		delete(rshard.reverse, num)
	}
	rshard.mu.Unlock()
	if !ok {
		return "", false
	}
	fshard := &d.nodes[nodeForwardShard(id)]
	fshard.mu.Lock()
	if fshard.forward[id] == num {
		delete(fshard.forward, id)
		delete(fshard.pending, id)
	}
	fshard.mu.Unlock()
	return id, true
}

// edgeForwardLookup returns the numeric ID for an edge string ID.
func (d *idDictionary) edgeForwardLookup(id EdgeID) (uint64, bool) {
	sh := &d.edges[edgeForwardShard(id)]
	sh.mu.RLock()
	num, ok := sh.forward[id]
	sh.mu.RUnlock()
	return num, ok
}

// edgeReverseLookup returns the string edge ID for a numeric ID.
func (d *idDictionary) edgeReverseLookup(num uint64) (EdgeID, bool) {
	sh := &d.edges[edgeReverseShard(num)]
	sh.mu.RLock()
	id, ok := sh.reverse[num]
	sh.mu.RUnlock()
	return id, ok
}

// edgePairStore installs forward[id] = num and reverse[num] = id, forward
// first so a reverse reader never sees a num before its forward entry.
// When another goroutine already stored a num for id it returns that num
// with ok=false (the caller adopts it).
func (d *idDictionary) edgePairStore(id EdgeID, num uint64, writer *txnCounterState) (uint64, bool) {
	fshard := &d.edges[edgeForwardShard(id)]
	fshard.mu.Lock()
	if existing, already := fshard.forward[id]; already {
		fshard.mu.Unlock()
		return existing, false
	}
	fshard.forward[id] = num
	if writer != nil {
		// Pending until a commit persists its keys (pending).
		fshard.pending[id] = writer
	}
	fshard.mu.Unlock()
	rshard := &d.edges[edgeReverseShard(num)]
	rshard.mu.Lock()
	rshard.reverse[num] = id
	rshard.mu.Unlock()
	return num, true
}

// edgePairRemoveByID removes forward[id] and the matching reverse entry.
func (d *idDictionary) edgePairRemoveByID(id EdgeID) (uint64, bool) {
	fshard := &d.edges[edgeForwardShard(id)]
	fshard.mu.Lock()
	num, ok := fshard.forward[id]
	if ok {
		delete(fshard.forward, id)
		delete(fshard.pending, id)
	}
	fshard.mu.Unlock()
	if !ok {
		return 0, false
	}
	rshard := &d.edges[edgeReverseShard(num)]
	rshard.mu.Lock()
	if rshard.reverse[num] == id {
		delete(rshard.reverse, num)
	}
	rshard.mu.Unlock()
	return num, true
}

// edgePairRemoveByNum removes reverse[num] and the matching forward entry;
// the forward delete is value-guarded against a reissued num.
func (d *idDictionary) edgePairRemoveByNum(num uint64) (EdgeID, bool) {
	rshard := &d.edges[edgeReverseShard(num)]
	rshard.mu.Lock()
	id, ok := rshard.reverse[num]
	if ok {
		delete(rshard.reverse, num)
	}
	rshard.mu.Unlock()
	if !ok {
		return "", false
	}
	fshard := &d.edges[edgeForwardShard(id)]
	fshard.mu.Lock()
	if fshard.forward[id] == num {
		delete(fshard.forward, id)
		delete(fshard.pending, id)
	}
	fshard.mu.Unlock()
	return id, true
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
func (d *idDictionary) resolveOrAllocateNodeNumIDInTxn(txn kvWriter, id NodeID) (uint64, error) {
	if num, ok := d.nodeForwardLookup(id); ok {
		if writer, pending := d.nodePendingWriter(id); pending && !d.txnWrote(kvWriterTxn(txn), writer) {
			return num, d.persistPendingNodeInTxn(txn, id, num)
		}
		return num, nil
	}
	ttl := d.currentFreelistTTL()
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
	if err := setNodeEntryInTxn(txn, id, num); err != nil {
		return 0, err
	}
	if !ok {
		d.recordTxnCounterUse(kvWriterTxn(txn), freelistKindNode, num)
	}

	// Update the sharded maps under narrow shard locks. Resolve
	// concurrent-create races by adopting whichever numID another
	// goroutine already stored — we abandon `num` (it becomes orphaned
	// in Badger; the pruner + freelist reclaim it).
	runIDAllocationRecheckHook()
	writer := d.stageTxnEntry(kvWriterTxn(txn), id, "")
	if adopted, ok := d.nodePairStore(id, num, writer); !ok {
		// Another allocation stored it first: its keys may not be
		// committed, so this txn writes the adopted entry's keys over ours.
		return adopted, d.persistPendingNodeInTxn(txn, id, adopted)
	}
	return num, nil
}

// idAllocationRecheckHook lets storage tests interpose between an
// allocation's Badger writes and its re-check of the in-memory map, where a
// concurrent allocation of the same ID is found.
var idAllocationRecheckHook atomic.Pointer[func()]

func runIDAllocationRecheckHook() {
	if hook := idAllocationRecheckHook.Load(); hook != nil {
		(*hook)()
	}
}

// setNodeEntryInTxn writes a node entry's forward and reverse keys.
func setNodeEntryInTxn(txn kvWriter, id NodeID, num uint64) error {
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Set(nodeIDForwardKey(id), append([]byte(nil), numBytes[:]...)); err != nil {
		return err
	}
	return txn.Set(append([]byte{prefixIDDictNodeReverse}, numBytes[:]...), []byte(id))
}

// persistPendingNodeInTxn writes the keys of an entry txn resolved but did
// not allocate, and stages it on txn: the transaction that allocated it
// may not have committed.
func (d *idDictionary) persistPendingNodeInTxn(txn kvWriter, id NodeID, num uint64) error {
	if err := setNodeEntryInTxn(txn, id, num); err != nil {
		return err
	}
	writer := d.stageTxnEntry(kvWriterTxn(txn), id, "")
	sh := &d.nodes[nodeForwardShard(id)]
	sh.mu.Lock()
	if _, pending := sh.pending[id]; pending {
		sh.pending[id] = writer
	}
	sh.mu.Unlock()
	return nil
}

// lookupNodeIDByNum returns the string node ID for a numeric ID, or
// ("", false) if the reverse map has no entry.
func (d *idDictionary) lookupNodeIDByNum(num uint64) (NodeID, bool) {
	return d.nodeReverseLookup(num)
}

// resolveOrAllocateEdgeNumIDInTxn is the edge analogue of
// resolveOrAllocateNodeNumIDInTxn.
func (d *idDictionary) resolveOrAllocateEdgeNumIDInTxn(txn kvWriter, id EdgeID) (uint64, error) {
	if num, ok := d.edgeForwardLookup(id); ok {
		if writer, pending := d.edgePendingWriter(id); pending && !d.txnWrote(kvWriterTxn(txn), writer) {
			return num, d.persistPendingEdgeInTxn(txn, id, num)
		}
		return num, nil
	}
	ttl := d.currentFreelistTTL()
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
	if err := setEdgeEntryInTxn(txn, id, num); err != nil {
		return 0, err
	}
	if !ok {
		d.recordTxnCounterUse(kvWriterTxn(txn), freelistKindEdge, num)
	}

	runIDAllocationRecheckHook()
	writer := d.stageTxnEntry(kvWriterTxn(txn), "", id)
	if adopted, stored := d.edgePairStore(id, num, writer); !stored {
		return adopted, d.persistPendingEdgeInTxn(txn, id, adopted)
	}
	return num, nil
}

// setEdgeEntryInTxn writes an edge entry's forward and reverse keys.
func setEdgeEntryInTxn(txn kvWriter, id EdgeID, num uint64) error {
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Set(edgeIDForwardKey(id), append([]byte(nil), numBytes[:]...)); err != nil {
		return err
	}
	return txn.Set(append([]byte{prefixIDDictEdgeReverse}, numBytes[:]...), []byte(id))
}

// persistPendingEdgeInTxn is the edge analogue of persistPendingNodeInTxn.
func (d *idDictionary) persistPendingEdgeInTxn(txn kvWriter, id EdgeID, num uint64) error {
	if err := setEdgeEntryInTxn(txn, id, num); err != nil {
		return err
	}
	writer := d.stageTxnEntry(kvWriterTxn(txn), "", id)
	sh := &d.edges[edgeForwardShard(id)]
	sh.mu.Lock()
	if _, pending := sh.pending[id]; pending {
		sh.pending[id] = writer
	}
	sh.mu.Unlock()
	return nil
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
	return d.edgeReverseLookup(num)
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
	num, ok := d.nodePairRemoveByID(id)
	if !ok {
		return nil
	}
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
	d.nodePairRemoveByID(id)
}

// forgetEdgeCache is the edge analogue of forgetNodeCache.
func (d *idDictionary) forgetEdgeCache(id EdgeID) {
	d.edgePairRemoveByID(id)
}

// deleteNodeEntryByNumInTxn removes both dict keys for a node addressed
// by numID. Used by the prune pipeline which has the numID (from the
// version-record scan) but may not know the string ID.
func (d *idDictionary) deleteNodeEntryByNumInTxn(txn *badger.Txn, num uint64) error {
	id, ok := d.nodePairRemoveByNum(num)
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
	id, ok := d.edgePairRemoveByNum(num)
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
	num, ok := d.edgePairRemoveByID(id)
	if !ok {
		return nil
	}
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
	return d.nodeForwardLookup(id)
}

// lookupEdgeNumID is the edge analogue of lookupNodeNumID.
func (d *idDictionary) lookupEdgeNumID(id EdgeID) (uint64, bool) {
	return d.edgeForwardLookup(id)
}

// encodeNumID writes a numeric ID as 8 big-endian bytes (for key
// components where we need sort-stable fixed-width encoding).
func encodeNumID(num uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, num)
	return out
}

// replaceWith makes d hold fresh's entries, counters and freelist counts, for
// Restore: d stays the engine's dictionary, and the swap is made under d's
// locks, so nothing reads a dictionary that is being built. Staged
// per-transaction counters are dropped with the store they belonged to.
func (d *idDictionary) replaceWith(fresh *idDictionary) {
	for i := range d.nodes {
		sh := &d.nodes[i]
		sh.mu.Lock()
		sh.forward, sh.reverse, sh.pending = fresh.nodes[i].forward, fresh.nodes[i].reverse, fresh.nodes[i].pending
		sh.mu.Unlock()
	}
	for i := range d.edges {
		sh := &d.edges[i]
		sh.mu.Lock()
		sh.forward, sh.reverse, sh.pending = fresh.edges[i].forward, fresh.edges[i].reverse, fresh.edges[i].pending
		sh.mu.Unlock()
	}
	d.mu.Lock()
	d.nextNode.Store(fresh.nextNode.Load())
	d.nextEdge.Store(fresh.nextEdge.Load())
	d.nodeFreelistPending.Store(fresh.nodeFreelistPending.Load())
	d.edgeFreelistPending.Store(fresh.edgeFreelistPending.Load())
	d.mu.Unlock()
	d.txnMu.Lock()
	d.txnCounters = make(map[*badger.Txn]*txnCounterState)
	d.txnCommitting = make(map[*badger.Txn]*txnCounterState)
	d.txnMu.Unlock()
}
