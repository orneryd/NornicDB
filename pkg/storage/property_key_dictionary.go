package storage

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// The property-key dictionary tokenizes property-key NAMES (not values)
// to per-namespace varint IDs so that node and edge bodies can encode
// `productName`, `unitPrice`, etc. as 1-2 byte integers instead of
// repeating the string in every record. The namespace boundary keeps
// multi-tenant ID spaces isolated; a tenant cannot exhaust another
// tenant's id space, and each dictionary compacts independently.
//
// Layout on disk:
//
//	[prefixPropKeyForward] + varint(len(ns)) + ns + varint(len(name)) + name -> varint(id)
//	[prefixPropKeyReverse] + varint(len(ns)) + ns + varint(id)               -> name
//	[prefixPropKeyCounter] + varint(len(ns)) + ns                            -> varint(next_id)
//
// Length-prefixing the namespace guarantees no ambiguity between e.g.
// namespaces "ab" and "a" + key "b...".
//
// The in-memory cache mirrors idDictionary's pattern: read paths take
// RLock; allocation takes Lock briefly to commit a new (name -> id)
// pair. Per-txn counter writes are batched via the same flushTxnCounters
// pattern so seed workloads pay one counter Set per dirty namespace per
// commit instead of one per allocation.
const (
	prefixPropKeyForward = byte(0x20)
	prefixPropKeyReverse = byte(0x21)
	prefixPropKeyCounter = byte(0x22)
)

// propertyKeyDictionary holds the per-namespace forward and reverse
// indexes for property-key tokenization.
type propertyKeyDictionary struct {
	mu        sync.RWMutex
	forward   map[string]map[string]uint64 // namespace -> (name -> id)
	reverse   map[string]map[uint64]string // namespace -> (id -> name)
	persisted map[string]map[string]bool   // namespace -> persisted names
	nextID    map[string]*atomic.Uint64    // namespace -> next id

	// Per-txn staged counter high-water marks and pending forward/
	// reverse entries, persisted out-of-band of the user transaction at
	// commit time (see resolveOrAllocateInTxn for why those writes
	// cannot ride the user txn).
	txnMu             sync.Mutex
	txnCounters       map[*badger.Txn]map[string]uint64
	txnPendingForward map[*badger.Txn][]propKeyPersistEntry
}

func newPropertyKeyDictionary() *propertyKeyDictionary {
	return &propertyKeyDictionary{
		forward:           make(map[string]map[string]uint64),
		reverse:           make(map[string]map[uint64]string),
		persisted:         make(map[string]map[string]bool),
		nextID:            make(map[string]*atomic.Uint64),
		txnCounters:       make(map[*badger.Txn]map[string]uint64),
		txnPendingForward: make(map[*badger.Txn][]propKeyPersistEntry),
	}
}

// propKeyForwardKey encodes a forward-map lookup key:
// prefix + varint(len(ns)) + ns + varint(len(name)) + name.
func propKeyForwardKey(namespace, name string) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(namespace)+binary.MaxVarintLen64+len(name))
	out = append(out, prefixPropKeyForward)
	out = binary.AppendUvarint(out, uint64(len(namespace)))
	out = append(out, namespace...)
	out = binary.AppendUvarint(out, uint64(len(name)))
	out = append(out, name...)
	return out
}

func propKeyReverseKey(namespace string, id uint64) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(namespace)+binary.MaxVarintLen64)
	out = append(out, prefixPropKeyReverse)
	out = binary.AppendUvarint(out, uint64(len(namespace)))
	out = append(out, namespace...)
	out = binary.AppendUvarint(out, id)
	return out
}

func propKeyCounterKey(namespace string) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(namespace))
	out = append(out, prefixPropKeyCounter)
	out = binary.AppendUvarint(out, uint64(len(namespace)))
	out = append(out, namespace...)
	return out
}

// ensureNamespace lazily initializes the per-namespace maps and
// counter on first use. NOT internally synchronized — callers must
// either (a) hold d.mu (write lock) when the dict is concurrently
// reachable, or (b) call only during construction (newPropertyKey-
// Dictionary + loadFromBadger), before *d is published to any other
// goroutine. The deliberate fine-grained locking scheme around
// resolveOrAllocateInTxn keeps the critical section tight and
// excludes the badger txn.Set I/O from it; do not widen this helper
// into self-locking, since that would force the caller to drop and
// re-acquire on every map touch.
func (d *propertyKeyDictionary) ensureNamespace(namespace string) {
	if _, ok := d.forward[namespace]; !ok {
		d.forward[namespace] = make(map[string]uint64)
		d.reverse[namespace] = make(map[uint64]string)
		d.persisted[namespace] = make(map[string]bool)
		d.nextID[namespace] = &atomic.Uint64{}
	}
}

// resolveOrAllocateInTxn returns the varint id for (namespace, name),
// allocating a new id and STAGING the forward+reverse entries for
// out-of-band persistence if the name has never been seen in this
// namespace. The persistence keys do NOT ride the user transaction:
// two concurrent writers in the same namespace both first-allocating
// the same property name would otherwise both write
// propKeyForwardKey(ns, name) inside their own user txns, triggering
// Badger's optimistic write-write conflict before the higher-level
// commit-time UNIQUE check could surface as the contracted
// "constraint violation: ... already exists" shape (see
// docs/plans/consumer-pinned-error-contract-plan.md §2.1).
//
// In-memory correctness: the d.mu Lock around the allocation makes the
// in-memory forward/reverse maps single-writer-correct — only one
// goroutine ever wins id=N for a given (namespace, name). Persistence
// is idempotent because every writer that subsequently reads the same
// (namespace, name) sees the same id from the map.
//
// Durability: new forward/reverse entries are persisted before entity
// records that reference them commit. A failed persistence aborts the
// entity transaction; a later entity-commit failure can leave an
// unused dictionary entry, which is safe and is not recycled.
//
// Lock discipline: reads happen under RLock; allocation upgrades to
// Lock and re-checks for the concurrent-create race. The losing
// goroutine's allocated ID becomes orphaned; nothing references it yet
// and there's no recycle by design.
func (d *propertyKeyDictionary) resolveOrAllocateInTxn(txn *badger.Txn, namespace, name string) (uint64, error) {
	d.mu.RLock()
	if forward, ok := d.forward[namespace]; ok {
		if id, ok := forward[name]; ok {
			persisted := d.persisted[namespace][name]
			d.mu.RUnlock()
			if !persisted {
				d.recordTxnPendingPersist(txn, namespace, name, id)
				d.recordTxnCounterUse(txn, namespace, id)
			}
			return id, nil
		}
	}
	d.mu.RUnlock()

	d.mu.Lock()
	d.ensureNamespace(namespace)
	if id, ok := d.forward[namespace][name]; ok {
		persisted := d.persisted[namespace][name]
		d.mu.Unlock()
		if !persisted {
			d.recordTxnPendingPersist(txn, namespace, name, id)
			d.recordTxnCounterUse(txn, namespace, id)
		}
		return id, nil
	}
	id := d.nextID[namespace].Add(1)
	d.forward[namespace][name] = id
	d.reverse[namespace][id] = name
	d.mu.Unlock()

	d.recordTxnPendingPersist(txn, namespace, name, id)
	d.recordTxnCounterUse(txn, namespace, id)
	return id, nil
}

// recordTxnPendingPersist stages a (namespace, name, id) tuple on the
// txn so the user's commit can later flush them via flushTxnCounters
// (which now drains BOTH the per-namespace counter high-water marks
// AND the staged forward/reverse entries) and persist them in a fresh
// badger transaction via persistTxnCounters.
func (d *propertyKeyDictionary) recordTxnPendingPersist(txn *badger.Txn, namespace, name string, id uint64) {
	d.txnMu.Lock()
	defer d.txnMu.Unlock()
	if d.txnPendingForward == nil {
		d.txnPendingForward = make(map[*badger.Txn][]propKeyPersistEntry)
	}
	for _, entry := range d.txnPendingForward[txn] {
		if entry.namespace == namespace && entry.name == name && entry.id == id {
			return
		}
	}
	d.txnPendingForward[txn] = append(d.txnPendingForward[txn], propKeyPersistEntry{
		namespace: namespace,
		name:      name,
		id:        id,
	})
}

type propKeyPersistEntry struct {
	namespace string
	name      string
	id        uint64
}

// lookup returns the property-key name for a given (namespace, id).
// Read-only — used by the decode path. Returns ("", false) if unknown.
func (d *propertyKeyDictionary) lookup(namespace string, id uint64) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if reverse, ok := d.reverse[namespace]; ok {
		name, ok := reverse[id]
		return name, ok
	}
	return "", false
}

// recordTxnCounterUse stages the per-namespace high-water mark on the
// txn so flushTxnCounters can write a single counter Set per dirty
// namespace at commit time.
func (d *propertyKeyDictionary) recordTxnCounterUse(txn *badger.Txn, namespace string, id uint64) {
	d.txnMu.Lock()
	state, ok := d.txnCounters[txn]
	if !ok {
		state = make(map[string]uint64)
		d.txnCounters[txn] = state
	}
	if id > state[namespace] {
		state[namespace] = id
	}
	d.txnMu.Unlock()
}

// propKeyTxnDrain holds the per-txn staged state — counter high-water
// marks and pending forward/reverse entries — drained at commit time
// for out-of-band persistence.
type propKeyTxnDrain struct {
	counters map[string]uint64
	pending  []propKeyPersistEntry
}

// flushTxnCounters detaches the staged counter state and pending
// forward/reverse entries for this txn and returns them for OUT-OF-TXN
// persistence by the caller. The persistence writes must NOT go
// through the user txn: every property-writing commit in the same
// namespace would otherwise touch propKeyCounterKey(ns) and the
// propKeyForwardKey(ns, name) keys for any newly-seen property name,
// triggering Badger's optimistic write-write conflict on concurrent
// commits and hiding genuine constraint violations from the
// per-namespace commit-time UNIQUE check (see
// consumer-pinned-error-contract-plan.md §2.1).
//
// Returns a zero-value drain if the txn allocated no fresh property
// keys.
func (d *propertyKeyDictionary) flushTxnCounters(txn *badger.Txn) propKeyTxnDrain {
	d.txnMu.Lock()
	state, hasCounters := d.txnCounters[txn]
	if hasCounters {
		delete(d.txnCounters, txn)
	}
	pending, hasPending := d.txnPendingForward[txn]
	if hasPending {
		delete(d.txnPendingForward, txn)
	}
	d.txnMu.Unlock()
	out := propKeyTxnDrain{}
	if hasCounters && len(state) > 0 {
		out.counters = make(map[string]uint64, len(state))
		for k, v := range state {
			out.counters[k] = v
		}
	}
	if hasPending && len(pending) > 0 {
		out.pending = pending
	}
	return out
}

// persistTxnCounters writes the staged forward/reverse entries and
// per-namespace counter keys in a fresh badger transaction.
func (d *propertyKeyDictionary) persistTxnCounters(db *badger.DB, drain propKeyTxnDrain) error {
	if len(drain.counters) == 0 && len(drain.pending) == 0 {
		return nil
	}
	if db == nil {
		return fmt.Errorf("property key dictionary persistence requires an open database")
	}
	if err := db.Update(func(txn *badger.Txn) error {
		var idBuf [binary.MaxVarintLen64]byte
		for _, entry := range drain.pending {
			n := binary.PutUvarint(idBuf[:], entry.id)
			if err := txn.Set(propKeyForwardKey(entry.namespace, entry.name), append([]byte(nil), idBuf[:n]...)); err != nil {
				return err
			}
			if err := txn.Set(propKeyReverseKey(entry.namespace, entry.id), []byte(entry.name)); err != nil {
				return err
			}
		}
		var buf [binary.MaxVarintLen64]byte
		for namespace, max := range drain.counters {
			n := binary.PutUvarint(buf[:], max)
			if err := txn.Set(propKeyCounterKey(namespace), append([]byte(nil), buf[:n]...)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	d.mu.Lock()
	for _, entry := range drain.pending {
		d.ensureNamespace(entry.namespace)
		d.persisted[entry.namespace][entry.name] = true
	}
	d.mu.Unlock()
	return nil
}

// discardTxnCounters drops staged counter state for a rolled-back txn.
// Safe to call multiple times.
func (d *propertyKeyDictionary) discardTxnCounters(txn *badger.Txn) {
	d.txnMu.Lock()
	delete(d.txnCounters, txn)
	delete(d.txnPendingForward, txn)
	d.txnMu.Unlock()
}

// loadFromBadger hydrates the in-memory dictionary from persisted
// forward, reverse, and counter keys. Called once on engine open.
func (d *propertyKeyDictionary) loadFromBadger(db *badger.DB) error {
	return db.View(func(txn *badger.Txn) error {
		// Forward map.
		{
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte{prefixPropKeyForward}
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				namespace, name, ok := decodePropKeyForwardKey(key)
				if !ok {
					continue
				}
				var id uint64
				if err := item.Value(func(val []byte) error {
					parsed, n := binary.Uvarint(val)
					if n <= 0 {
						return fmt.Errorf("property key forward value malformed for ns=%q name=%q", namespace, name)
					}
					id = parsed
					return nil
				}); err != nil {
					it.Close()
					return err
				}
				d.ensureNamespace(namespace)
				d.forward[namespace][name] = id
				d.reverse[namespace][id] = name
				d.persisted[namespace][name] = true
			}
			it.Close()
		}
		// Counters.
		{
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte{prefixPropKeyCounter}
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				namespace, ok := decodePropKeyCounterKey(key)
				if !ok {
					continue
				}
				var counter uint64
				if err := item.Value(func(val []byte) error {
					parsed, n := binary.Uvarint(val)
					if n <= 0 {
						return fmt.Errorf("property key counter malformed for ns=%q", namespace)
					}
					counter = parsed
					return nil
				}); err != nil {
					it.Close()
					return err
				}
				d.ensureNamespace(namespace)
				if cur := d.nextID[namespace].Load(); counter > cur {
					d.nextID[namespace].Store(counter)
				}
			}
			it.Close()
		}
		// Re-derive nextID floor from the loaded forward map in case any
		// counter key is missing for a namespace that already has IDs.
		for namespace, forward := range d.forward {
			counter := d.nextID[namespace]
			cur := counter.Load()
			for _, id := range forward {
				if id > cur {
					cur = id
				}
			}
			counter.Store(cur)
		}
		return nil
	})
}

// decodePropKeyForwardKey is the inverse of propKeyForwardKey. Returns
// ok=false on malformed keys (caller should skip them; nothing in the
// engine writes malformed keys, but a forward-compatible decoder
// silently ignores unknown trailing bytes from future versions).
func decodePropKeyForwardKey(key []byte) (string, string, bool) {
	if len(key) < 1 || key[0] != prefixPropKeyForward {
		return "", "", false
	}
	rest := key[1:]
	nsLen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < nsLen {
		return "", "", false
	}
	rest = rest[n:]
	namespace := string(rest[:nsLen])
	rest = rest[nsLen:]
	nameLen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) < nameLen {
		return "", "", false
	}
	rest = rest[n:]
	if uint64(len(rest)) != nameLen {
		return "", "", false
	}
	return namespace, string(rest), true
}

func decodePropKeyCounterKey(key []byte) (string, bool) {
	if len(key) < 1 || key[0] != prefixPropKeyCounter {
		return "", false
	}
	rest := key[1:]
	nsLen, n := binary.Uvarint(rest)
	if n <= 0 || uint64(len(rest)-n) != nsLen {
		return "", false
	}
	return string(rest[n:]), true
}

// PropKeyDictCounters exposes per-namespace counter usage for
// observability probes.
func (b *BadgerEngine) PropKeyDictCounters() map[string]uint64 {
	if b == nil || b.propKeyDict == nil {
		return nil
	}
	out := make(map[string]uint64)
	b.propKeyDict.mu.RLock()
	for ns, ctr := range b.propKeyDict.nextID {
		out[ns] = ctr.Load()
	}
	b.propKeyDict.mu.RUnlock()
	return out
}
