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
	mu      sync.RWMutex
	forward map[string]map[string]uint64 // namespace -> (name -> id)
	reverse map[string]map[uint64]string // namespace -> (id -> name)
	nextID  map[string]*atomic.Uint64    // namespace -> next id

	// Per-txn staged counter high-water marks, written once at commit
	// (mirrors idDictionary.txnCounters).
	txnMu       sync.Mutex
	txnCounters map[*badger.Txn]map[string]uint64
}

func newPropertyKeyDictionary() *propertyKeyDictionary {
	return &propertyKeyDictionary{
		forward:     make(map[string]map[string]uint64),
		reverse:     make(map[string]map[uint64]string),
		nextID:      make(map[string]*atomic.Uint64),
		txnCounters: make(map[*badger.Txn]map[string]uint64),
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

// ensureNamespaceLocked must be called under d.mu (write lock). Lazy
// initializes the per-namespace maps and counter on first use.
func (d *propertyKeyDictionary) ensureNamespaceLocked(namespace string) {
	if _, ok := d.forward[namespace]; !ok {
		d.forward[namespace] = make(map[string]uint64)
		d.reverse[namespace] = make(map[uint64]string)
		d.nextID[namespace] = &atomic.Uint64{}
	}
}

// resolveOrAllocateInTxn returns the varint id for (namespace, name),
// allocating a new id and persisting forward+reverse entries if the
// name has never been seen in this namespace. Counter writes are staged
// on the txn and flushed once at commit time.
//
// Lock discipline: reads happen under RLock; allocation upgrades to
// Lock and re-checks for the concurrent-create race. The losing
// goroutine's allocated ID becomes orphaned; nothing references it yet
// and there's no recycle by design.
func (d *propertyKeyDictionary) resolveOrAllocateInTxn(txn *badger.Txn, namespace, name string) (uint64, error) {
	d.mu.RLock()
	if forward, ok := d.forward[namespace]; ok {
		if id, ok := forward[name]; ok {
			d.mu.RUnlock()
			return id, nil
		}
	}
	d.mu.RUnlock()

	d.mu.Lock()
	d.ensureNamespaceLocked(namespace)
	if id, ok := d.forward[namespace][name]; ok {
		d.mu.Unlock()
		return id, nil
	}
	id := d.nextID[namespace].Add(1)
	d.forward[namespace][name] = id
	d.reverse[namespace][id] = name
	d.mu.Unlock()

	var idBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(idBuf[:], id)
	if err := txn.Set(propKeyForwardKey(namespace, name), append([]byte(nil), idBuf[:n]...)); err != nil {
		return 0, err
	}
	if err := txn.Set(propKeyReverseKey(namespace, id), []byte(name)); err != nil {
		return 0, err
	}
	d.recordTxnCounterUse(txn, namespace, id)
	return id, nil
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

// flushTxnCounters writes one counter key per dirty namespace and
// detaches the staged state. Caller must invoke this from the commit
// path BEFORE badgerTx.Commit(). Safe to call on a txn that never
// allocated — does nothing.
func (d *propertyKeyDictionary) flushTxnCounters(txn *badger.Txn) error {
	d.txnMu.Lock()
	state, ok := d.txnCounters[txn]
	if ok {
		delete(d.txnCounters, txn)
	}
	d.txnMu.Unlock()
	if !ok || len(state) == 0 {
		return nil
	}
	var buf [binary.MaxVarintLen64]byte
	for namespace, max := range state {
		n := binary.PutUvarint(buf[:], max)
		if err := txn.Set(propKeyCounterKey(namespace), append([]byte(nil), buf[:n]...)); err != nil {
			return err
		}
	}
	return nil
}

// discardTxnCounters drops staged counter state for a rolled-back txn.
// Safe to call multiple times.
func (d *propertyKeyDictionary) discardTxnCounters(txn *badger.Txn) {
	d.txnMu.Lock()
	delete(d.txnCounters, txn)
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
				d.ensureNamespaceLocked(namespace)
				d.forward[namespace][name] = id
				d.reverse[namespace][id] = name
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
				d.ensureNamespaceLocked(namespace)
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
