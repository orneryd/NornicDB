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
// Reverse entries map a numeric node or edge ID back to its string ID for
// index and MVCC readers that start from the numeric form.
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

// idDictShard is one independently locked slice of a numeric-ID dictionary.
// Pending entries and adopters belong to the forward shard for their ID.
type idDictShard[ID ~string] struct {
	mu       sync.RWMutex
	forward  map[ID]uint64
	reverse  map[uint64]ID
	pending  map[ID]*txnCounterState
	adopters map[ID]map[*txnCounterState]struct{}
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
	nodes [nodeDictShards]idDictShard[NodeID]
	edges [nodeDictShards]idDictShard[EdgeID]

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
	// A popped key remains visible to other transactions until the claiming
	// transaction commits. Reserve it in memory across aborts and transfer
	// its deletion when another transaction persists the same pending name.
	freelistClaimMu    sync.Mutex
	freelistClaims     map[freelistClaimID]*freelistClaim
	freelistClaimCount atomic.Int64
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
	nodes     []NodeID
	edges     []EdgeID
	claims    []*freelistClaim
	abandoned atomic.Bool
}

type freelistClaimID struct {
	kind byte
	id   uint64
}

type freelistClaim struct {
	id  freelistClaimID
	key []byte
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

// adoptPendingWriter records overlapping writers only for a pending name
// that another transaction has already staged. Its caller holds the forward
// shard lock. The ordinary one-writer path creates no additional map.
func adoptPendingWriter[K comparable](pending map[K]*txnCounterState, adopters *map[K]map[*txnCounterState]struct{}, key K, writer *txnCounterState) {
	previous, exists := pending[key]
	if !exists || previous == writer {
		return
	}
	owners := (*adopters)[key]
	if owners == nil && previous != nil && !previous.abandoned.Load() {
		if *adopters == nil {
			*adopters = make(map[K]map[*txnCounterState]struct{})
		}
		owners = make(map[*txnCounterState]struct{}, 2)
		owners[previous] = struct{}{}
		(*adopters)[key] = owners
	}
	if owners != nil {
		owners[writer] = struct{}{}
	}
	pending[key] = writer
}

// A successful writer makes this generation durable, even if an adopter was
// staged later. Old writers are removed from adopters when a name is removed,
// so they cannot clear a newer generation's pending mark.
func clearCommittedPending[K comparable](pending map[K]*txnCounterState, adopters map[K]map[*txnCounterState]struct{}, key K, writer *txnCounterState) {
	_, shared := adopters[key][writer]
	if pending[key] == writer || shared {
		delete(pending, key)
		delete(adopters, key)
	}
}

// abandonPendingWriter returns true only when no other live writer owns the
// name. A discarded writer can remain as a pending placeholder until another
// transaction adopts it, but cannot keep a rejected generation alive.
func abandonPendingWriter[K comparable](pending map[K]*txnCounterState, adopters map[K]map[*txnCounterState]struct{}, key K, writer *txnCounterState) bool {
	current, exists := pending[key]
	if !exists {
		return false
	}
	owners := adopters[key]
	if owners == nil {
		if current != writer {
			return false
		}
		delete(pending, key)
		return true
	}
	if _, tracked := owners[writer]; !tracked {
		return false
	}
	delete(owners, writer)
	var survivor *txnCounterState
	for owner := range owners {
		if owner.abandoned.Load() {
			delete(owners, owner)
		} else {
			survivor = owner
		}
	}
	if survivor != nil {
		pending[key] = survivor
		if len(owners) == 1 {
			delete(adopters, key)
		}
		return false
	}
	delete(pending, key)
	delete(adopters, key)
	return true
}

// clearPending clears the pending mark of the entries st wrote, once a
// commit has persisted their keys. An entry another transaction wrote since
// (rewritten, or deleted and allocated again) keeps the mark: its last
// writer's commit clears it. Nil does nothing.
func (d *idDictionary) clearPending(st *txnCounterState) {
	if st == nil {
		return
	}
	if len(st.claims) != 0 {
		d.freelistClaimMu.Lock()
		for _, claim := range st.claims {
			if d.freelistClaims[claim.id] == claim {
				delete(d.freelistClaims, claim.id)
				d.freelistClaimCount.Add(-1)
			}
		}
		d.freelistClaimMu.Unlock()
	}
	for _, id := range st.nodes {
		sh := &d.nodes[nodeForwardShard(id)]
		sh.mu.Lock()
		clearCommittedPending(sh.pending, sh.adopters, id, st)
		sh.mu.Unlock()
	}
	for _, id := range st.edges {
		sh := &d.edges[edgeForwardShard(id)]
		sh.mu.Lock()
		clearCommittedPending(sh.pending, sh.adopters, id, st)
		sh.mu.Unlock()
	}
}

// abandonPending releases entries belonging only to a rejected buffered
// commit. An adopter changes the pending writer under the forward shard lock,
// so its entries and recycled-ID claims survive the rejected commit.
func (d *idDictionary) abandonPending(st *txnCounterState) {
	if st == nil {
		return
	}
	st.abandoned.Store(true)
	for _, id := range st.nodes {
		sh := &d.nodes[nodeForwardShard(id)]
		sh.mu.Lock()
		num, found := sh.forward[id]
		remove := abandonPendingWriter(sh.pending, sh.adopters, id, st)
		if remove {
			delete(sh.forward, id)
		}
		sh.mu.Unlock()
		if remove && found {
			reverse := &d.nodes[nodeReverseShard(num)]
			reverse.mu.Lock()
			if reverse.reverse[num] == id {
				delete(reverse.reverse, num)
			}
			reverse.mu.Unlock()
		}
	}
	for _, id := range st.edges {
		sh := &d.edges[edgeForwardShard(id)]
		sh.mu.Lock()
		num, found := sh.forward[id]
		remove := abandonPendingWriter(sh.pending, sh.adopters, id, st)
		if remove {
			delete(sh.forward, id)
		}
		sh.mu.Unlock()
		if remove && found {
			reverse := &d.edges[edgeReverseShard(num)]
			reverse.mu.Lock()
			if reverse.reverse[num] == id {
				delete(reverse.reverse, num)
			}
			reverse.mu.Unlock()
		}
	}
	for _, claim := range st.claims {
		var used bool
		if claim.id.kind == freelistKindNode {
			_, used = d.lookupNodeIDByNum(claim.id.id)
		} else {
			_, used = d.lookupEdgeIDByNum(claim.id.id)
		}
		if used {
			continue
		}
		d.freelistClaimMu.Lock()
		if d.freelistClaims[claim.id] == claim {
			delete(d.freelistClaims, claim.id)
			d.freelistClaimCount.Add(-1)
			if claim.id.kind == freelistKindNode {
				d.nodeFreelistPending.Add(1)
			} else {
				d.edgeFreelistPending.Add(1)
			}
		}
		d.freelistClaimMu.Unlock()
	}
}

// addTxnFreelistClaim records a staged deletion for the transaction's
// commit/abort bookkeeping. The claim itself is retained after abort because
// its pending dictionary name may be adopted by another transaction.
func (d *idDictionary) addTxnFreelistClaim(txn *badger.Txn, claim *freelistClaim) {
	d.txnMu.Lock()
	st := d.txnCounters[txn]
	if st == nil {
		st = &txnCounterState{}
		d.txnCounters[txn] = st
	}
	st.claims = append(st.claims, claim)
	d.txnMu.Unlock()
}

// A second writer of a pending name must carry the same freelist deletion;
// either writer may be the first to commit its dictionary entries.
func (d *idDictionary) stagePendingFreelistClaim(txn kvWriter, kind byte, num uint64) error {
	if d.freelistClaimCount.Load() == 0 {
		return nil
	}
	d.freelistClaimMu.Lock()
	claim := d.freelistClaims[freelistClaimID{kind, num}]
	d.freelistClaimMu.Unlock()
	if claim == nil {
		return nil
	}
	if err := txn.Delete(claim.key); err != nil {
		return err
	}
	d.addTxnFreelistClaim(kvWriterTxn(txn), claim)
	return nil
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
	update := db.Update
	if managed, ok := db.(*managedBadgerDB); ok {
		update = managed.updateMetadata
	}
	_ = update(func(txn *badger.Txn) error {
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
			st.abandoned.Store(true)
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
	for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		if kvStagedDeleted(txn, key) {
			continue
		}
		if len(key) != 1+1+8+8 {
			return 0, false, fmt.Errorf("freelist key wrong size: %d", len(key))
		}
		if binary.BigEndian.Uint64(key[2:10]) > cutoffNanos {
			return 0, false, nil
		}
		numID := binary.BigEndian.Uint64(key[10:18])
		claimID := freelistClaimID{kind, numID}
		// Reserve before checking the reverse map so the claim lock is never
		// held while taking a dictionary shard lock.
		d.freelistClaimMu.Lock()
		if d.freelistClaims[claimID] != nil {
			d.freelistClaimMu.Unlock()
			continue
		}
		claim := &freelistClaim{id: claimID, key: key}
		if d.freelistClaims == nil {
			d.freelistClaims = make(map[freelistClaimID]*freelistClaim)
		}
		d.freelistClaims[claimID] = claim
		d.freelistClaimCount.Add(1)
		d.freelistClaimMu.Unlock()
		release := func() {
			d.freelistClaimMu.Lock()
			if d.freelistClaims[claimID] == claim {
				delete(d.freelistClaims, claimID)
				d.freelistClaimCount.Add(-1)
			}
			d.freelistClaimMu.Unlock()
		}
		// A transaction with an older read timestamp may still see a key
		// already removed by a committed claimant.
		if kind == freelistKindNode {
			if _, exists := d.lookupNodeIDByNum(numID); exists {
				release()
				continue
			}
		} else if _, exists := d.lookupEdgeIDByNum(numID); exists {
			release()
			continue
		}
		if err := txn.Delete(key); err != nil {
			release()
			return 0, false, err
		}
		d.addTxnFreelistClaim(kvWriterTxn(txn), claim)
		if kind == freelistKindNode {
			d.nodeFreelistPending.Add(-1)
		} else {
			d.edgeFreelistPending.Add(-1)
		}
		return numID, true, nil
	}
	return 0, false, nil
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

// storeIDPair installs forward[id] = num and reverse[num] = id. The
// forward entry is published before the reverse one: a reverse reader can
// never observe a num whose forward entry isn't visible yet. When another
// goroutine already stored a num for id it returns that num with ok=false
// (the caller adopts it and must not publish reverse itself).
func storeIDPair[ID ~string](shards *[nodeDictShards]idDictShard[ID], forwardShard func(ID) int, reverseShard func(uint64) int, id ID, num uint64, writer *txnCounterState) (uint64, bool) {
	fshard := &shards[forwardShard(id)]
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
	rshard := &shards[reverseShard(num)]
	rshard.mu.Lock()
	// Sole writer of this num at creation time (numIDs are unique per
	// allocation; reissue requires a full freelist TTL cycle that cannot
	// overlap this pair write).
	rshard.reverse[num] = id
	rshard.mu.Unlock()
	return num, true
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

type idPairRemoval uint8

const (
	idPairByString idPairRemoval = iota
	idPairByNumber
)

// removeIDPair removes a matching forward/reverse pair for either dictionary.
// ID removal locks the forward shard first; numeric removal locks the reverse
// shard first. The second delete is value-guarded so a new generation remains.
func removeIDPair[ID ~string](shards *[nodeDictShards]idDictShard[ID], forwardShard func(ID) int, reverseShard func(uint64) int, id ID, num uint64, mode idPairRemoval) (ID, uint64, bool) {
	if mode == idPairByNumber {
		rshard := &shards[reverseShard(num)]
		rshard.mu.Lock()
		id, ok := rshard.reverse[num]
		if ok {
			delete(rshard.reverse, num)
		}
		rshard.mu.Unlock()
		if !ok {
			return "", 0, false
		}
		fshard := &shards[forwardShard(id)]
		fshard.mu.Lock()
		if fshard.forward[id] == num {
			delete(fshard.forward, id)
			delete(fshard.pending, id)
			delete(fshard.adopters, id)
		}
		fshard.mu.Unlock()
		return id, num, true
	}
	fshard := &shards[forwardShard(id)]
	fshard.mu.Lock()
	num, ok := fshard.forward[id]
	if ok {
		delete(fshard.forward, id)
		delete(fshard.pending, id)
		delete(fshard.adopters, id)
	}
	fshard.mu.Unlock()
	if !ok {
		return "", 0, false
	}
	rshard := &shards[reverseShard(num)]
	rshard.mu.Lock()
	if rshard.reverse[num] == id {
		delete(rshard.reverse, num)
	}
	rshard.mu.Unlock()
	return id, num, true
}

// resolveOrAllocateNumericIDInTxn resolves or allocates a node or edge ID.
// The concrete ID type selects its dictionary and keeps their numeric spaces
// and persisted key families separate.
func resolveOrAllocateNumericIDInTxn[ID NodeID | EdgeID](d *idDictionary, txn kvWriter, id ID) (uint64, error) {
	switch value := any(id).(type) {
	case NodeID:
		sh := &d.nodes[nodeForwardShard(value)]
		if num, ok, err := resolveExistingIDInTxn(d, txn, sh, value, freelistKindNode); ok || err != nil {
			return num, err
		}
		return allocateIDInTxn(d, txn, value, sh, &d.nodes, nodeForwardShard, nodeReverseShard, &d.nextNode, freelistKindNode)
	case EdgeID:
		sh := &d.edges[edgeForwardShard(value)]
		if num, ok, err := resolveExistingIDInTxn(d, txn, sh, value, freelistKindEdge); ok || err != nil {
			return num, err
		}
		return allocateIDInTxn(d, txn, value, sh, &d.edges, edgeForwardShard, edgeReverseShard, &d.nextEdge, freelistKindEdge)
	default:
		panic("unsupported numeric ID type")
	}
}

// allocateIDInTxn owns the shared allocation and concurrent-adoption policy.
// It publishes the forward mapping before the reverse mapping and preserves a
// rejected winner's ability to disappear before an adopter writes its keys.
func allocateIDInTxn[ID ~string](d *idDictionary, txn kvWriter, id ID, sh *idDictShard[ID], shards *[nodeDictShards]idDictShard[ID], forwardShard func(ID) int, reverseShard func(uint64) int, counter *atomic.Uint64, kind byte) (uint64, error) {
	ttl := d.currentFreelistTTL()
	if ttl <= 0 {
		ttl = defaultIDFreelistTTL
	}
	recycled, ok, err := d.popAgedFreelistEntryInTxn(txn, kind, ttl)
	if err != nil {
		return 0, err
	}
	var num uint64
	if ok {
		num = recycled
	} else {
		num = counter.Add(1)
	}
	var writer *txnCounterState
	switch kind {
	case freelistKindNode:
		if err := setNodeEntryInTxn(txn, NodeID(id), num); err != nil {
			return 0, err
		}
	case freelistKindEdge:
		if err := setEdgeEntryInTxn(txn, EdgeID(id), num); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unknown numeric ID kind %q", kind)
	}
	if !ok {
		d.recordTxnCounterUse(kvWriterTxn(txn), kind, num)
	}
	runIDAllocationRecheckHook()
	if kind == freelistKindNode {
		writer = d.stageTxnEntry(kvWriterTxn(txn), NodeID(id), "")
	} else {
		writer = d.stageTxnEntry(kvWriterTxn(txn), "", EdgeID(id))
	}
	for {
		if _, stored := storeIDPair(shards, forwardShard, reverseShard, id, num, writer); stored {
			return num, nil
		}
		runIDAllocationAdoptionHook()
		adopted, found, err := persistPendingIDInTxn(d, txn, sh, id, kind)
		if found || err != nil {
			return adopted, err
		}
	}
}

// resolveExistingIDInTxn resolves the forward entry and pending owner under
// the same shard lock, rechecking after upgrading from a read to a write lock.
func resolveExistingIDInTxn[ID ~string](d *idDictionary, txn kvWriter, sh *idDictShard[ID], id ID, kind byte) (uint64, bool, error) {
	sh.mu.RLock()
	num, found := sh.forward[id]
	writer, pending := sh.pending[id]
	if !found || !pending || d.txnWrote(kvWriterTxn(txn), writer) {
		sh.mu.RUnlock()
		return num, found, nil
	}
	sh.mu.RUnlock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	num, found = sh.forward[id]
	writer, pending = sh.pending[id]
	if !found || !pending || d.txnWrote(kvWriterTxn(txn), writer) {
		return num, found, nil
	}
	if err := writePendingIDLocked(d, txn, sh, id, num, kind); err != nil {
		return 0, true, err
	}
	return num, true, nil
}

// idAllocationRecheckHook lets storage tests interpose between an
// allocation's Badger writes and its re-check of the in-memory map, where a
// concurrent allocation of the same ID is found.
var idAllocationRecheckHook atomic.Pointer[func()]

// idAllocationAdoptionHook permits a deterministic rejection between a
// losing allocation's recheck and the adoption of the winning binding.
var idAllocationAdoptionHook atomic.Pointer[func()]

func runIDAllocationRecheckHook() {
	if hook := idAllocationRecheckHook.Load(); hook != nil {
		(*hook)()
	}
}

func runIDAllocationAdoptionHook() {
	if hook := idAllocationAdoptionHook.Load(); hook != nil {
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

// persistPendingIDInTxn rechecks and stages the winning entry's keys while
// holding its shard lock. The winner may have been rejected after the caller
// observed it; found=false lets the caller publish its own allocation.
func persistPendingIDInTxn[ID ~string](d *idDictionary, txn kvWriter, sh *idDictShard[ID], id ID, kind byte) (uint64, bool, error) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	num, found := sh.forward[id]
	if !found {
		return 0, false, nil
	}
	return num, true, writePendingIDLocked(d, txn, sh, id, num, kind)
}

func writePendingIDLocked[ID ~string](d *idDictionary, txn kvWriter, sh *idDictShard[ID], id ID, num uint64, kind byte) error {
	if err := d.stagePendingFreelistClaim(txn, kind, num); err != nil {
		return err
	}
	var writer *txnCounterState
	switch kind {
	case freelistKindNode:
		node := NodeID(id)
		if err := setNodeEntryInTxn(txn, node, num); err != nil {
			return err
		}
		writer = d.stageTxnEntry(kvWriterTxn(txn), node, "")
	case freelistKindEdge:
		edge := EdgeID(id)
		if err := setEdgeEntryInTxn(txn, edge, num); err != nil {
			return err
		}
		writer = d.stageTxnEntry(kvWriterTxn(txn), "", edge)
	default:
		return fmt.Errorf("unknown numeric ID kind %q", kind)
	}
	adoptPendingWriter(sh.pending, &sh.adopters, id, writer)
	return nil
}

// lookupNodeIDByNum returns the string node ID for a numeric ID, or
// ("", false) if the reverse map has no entry.
func (d *idDictionary) lookupNodeIDByNum(num uint64) (NodeID, bool) {
	return d.nodeReverseLookup(num)
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

// deleteIDEntryInTxn removes a cached pair and its persistent dictionary keys.
// Callers must retain entries needed by archived MVCC versions or snapshots.
// A numeric deletion must remove the reverse key even after cache eviction.
func deleteIDEntryInTxn[ID ~string](txn *badger.Txn, shards *[nodeDictShards]idDictShard[ID], forwardShard func(ID) int, reverseShard func(uint64) int, forwardKey func(ID) []byte, reversePrefix byte, id ID, num uint64, mode idPairRemoval) error {
	removedID, removedNum, ok := removeIDPair(shards, forwardShard, reverseShard, id, num, mode)
	if mode == idPairByString {
		if !ok {
			return nil
		}
		if err := txn.Delete(forwardKey(removedID)); err != nil {
			return err
		}
		num = removedNum
	}
	var numBytes [8]byte
	binary.BigEndian.PutUint64(numBytes[:], num)
	if err := txn.Delete(append([]byte{reversePrefix}, numBytes[:]...)); err != nil {
		return err
	}
	if mode == idPairByNumber && ok {
		return txn.Delete(forwardKey(removedID))
	}
	return nil
}

// forgetNodeCache drops the in-memory cache entry for a node without
// touching persisted storage. Used post-commit by the buffered delete
// path (which persisted the delete via bufferDelete but hasn't yet
// updated the in-memory side).
func (d *idDictionary) forgetNodeCache(id NodeID) {
	removeIDPair(&d.nodes, nodeForwardShard, nodeReverseShard, id, 0, idPairByString)
}

// forgetEdgeCache is the edge analogue of forgetNodeCache.
func (d *idDictionary) forgetEdgeCache(id EdgeID) {
	removeIDPair(&d.edges, edgeForwardShard, edgeReverseShard, id, 0, idPairByString)
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
		sh.forward, sh.reverse, sh.pending, sh.adopters = fresh.nodes[i].forward, fresh.nodes[i].reverse, fresh.nodes[i].pending, fresh.nodes[i].adopters
		sh.mu.Unlock()
	}
	for i := range d.edges {
		sh := &d.edges[i]
		sh.mu.Lock()
		sh.forward, sh.reverse, sh.pending, sh.adopters = fresh.edges[i].forward, fresh.edges[i].reverse, fresh.edges[i].pending, fresh.edges[i].adopters
		sh.mu.Unlock()
	}
	d.mu.Lock()
	d.nextNode.Store(fresh.nextNode.Load())
	d.nextEdge.Store(fresh.nextEdge.Load())
	d.nodeFreelistPending.Store(fresh.nodeFreelistPending.Load())
	d.edgeFreelistPending.Store(fresh.edgeFreelistPending.Load())
	d.mu.Unlock()
	d.freelistClaimMu.Lock()
	d.freelistClaims = nil
	d.freelistClaimCount.Store(0)
	d.freelistClaimMu.Unlock()
	d.txnMu.Lock()
	d.txnCounters = make(map[*badger.Txn]*txnCounterState)
	d.txnCommitting = make(map[*badger.Txn]*txnCounterState)
	d.txnMu.Unlock()
}
