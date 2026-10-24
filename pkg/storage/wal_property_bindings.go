package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v4"

	"github.com/vmihailenco/msgpack/v5"
)

// A token has meaning only in the dictionary of the store that encoded it.
// WAL is a store boundary: even recovery on the same machine can follow a
// semantic snapshot that allocated the destination's tokens in another order.
type walPropertyToken struct {
	namespace string
	id        uint64
}

// importBufferedWALRecord is the only authority boundary between a transported
// physical body and a locally applicable commit. Complete tables belong to one
// record or its explicit immutable epoch. Unstamped legacy bytes need the
// own-WAL constructor authority; no matching token establishes that authority.
func (b *BadgerEngine) importBufferedWALRecord(data *WALBufferedCommitData, authority walPropertyAuthority) (bufferedCommit, error) {
	db, err := b.beginHelperTxn()
	if err != nil {
		return bufferedCommit{}, err
	}
	defer b.txnWG.Done()
	c := bufferedCommit{
		recovered: true,
		namespace: data.Namespace, txID: data.TxID, ops: data.Ops,
		version: data.Version, counterNodeMax: data.NodeMax,
		counterEdgeMax: data.EdgeMax, hasCounters: data.HasCounters,
		writes: data.Writes,
	}
	if len(data.Deletes) > 0 {
		c.deletes = make(map[string]bool, len(data.Deletes))
		for _, key := range data.Deletes {
			c.deletes[key] = true
		}
	}
	s := &b.walPropertySource
	s.Lock()
	defer s.Unlock()
	if authority == walForeignImport || data.PropertyDictionaryID != "" && data.PropertyDictionaryID != s.identity {
		if err := b.markPropertySourceForeignLocked(db); err != nil {
			return bufferedCommit{}, err
		}
	}
	legacy := len(data.PropertyBindings) == 0
	ownLegacy := authority == walOwnDirectoryAtOpen &&
		(data.PropertyDictionaryID != "" && data.PropertyDictionaryID == s.identity || data.PropertyDictionaryID == "" && (!s.foreign || s.clean))
	if legacy && ownLegacy {
		// The exact physical dictionary wrote these bytes. No body/value walk,
		// read transaction or binding map is needed. Only its own staged drain
		// may retain the source IDs; foreign branches never import that drain.
		c.propKeyDrain.counters = data.PropKeyCounters
		if len(data.PropKeys) != 0 {
			c.propKeyDrain.pending = make([]propKeyPersistEntry, len(data.PropKeys))
			for i, key := range data.PropKeys {
				c.propKeyDrain.pending[i] = propKeyPersistEntry{namespace: key.Namespace, name: key.Name, id: key.ID}
			}
		}
		return c, nil
	}
	// A historical typed sidecar can carry authoritative named properties even
	// when the old physical record omitted persisted-key bindings. Plain legacy
	// JSON is a lossy projection and is never treated as that authority.
	if legacy && data.namedPropertiesAuthoritative && len(data.Ops) > 0 {
		txn, readTS, err := db.beginTxn(false)
		if err != nil {
			return bufferedCommit{}, err
		}
		defer db.endRead(readTS)
		defer txn.Discard()
		defer b.propKeyDict.flushTxnCounters(txn)
		c.writes, err = b.rebindWALNamedProperties(txn, data)
		if err != nil {
			return bufferedCommit{}, err
		}
		c.propKeyDrain = b.propKeyDict.flushTxnCounters(txn)
		return c, nil
	}
	plan, cached := b.cachedWALPropertyPlan(data.PropertyBindings, data.propertyEpoch)
	bindings, resolve, remap := plan.bindings, plan.resolve, plan.remap
	revision, domain := b.propKeyDict.walBindingState()
	if cached {
		c.propKeyDrain = plan.drain
	} else {
		if !legacy {
			matches, err := b.completeWALBindingsMatch(data.PropertyBindings)
			if err != nil {
				return bufferedCommit{}, err
			}
			if matches {
				b.rememberWALPropertyPlan(data.PropertyBindings, nil, propKeyTxnDrain{}, false, false, revision, domain, data.propertyEpoch)
				return c, nil
			}
		}
		bindings, resolve, remap, err = b.bufferedWALRecordBindings(data)
		if err != nil {
			return bufferedCommit{}, err
		}
	}
	var txn *badger.Txn
	if resolve {
		var readTS uint64
		txn, readTS, err = db.beginTxn(false)
		if err != nil {
			return bufferedCommit{}, err
		}
		defer db.endRead(readTS)
		defer txn.Discard()
		defer b.propKeyDict.flushTxnCounters(txn)
		if data.propertyEpoch == nil {
			// Historical self-contained records bind only their own dependencies.
			for token, binding := range bindings {
				if binding.pending {
					binding.destination, err = b.propKeyDict.resolveOrAllocateInTxn(txn, token.namespace, binding.name)
					if err != nil {
						return bufferedCommit{}, err
					}
					bindings[token] = binding
				}
				remap = remap || token.id != binding.destination
			}
			resolve = false
		}
	}
	if legacy || remap || resolve {
		c.writes, err = rewriteWALPropertyBodies(data.Writes, func(namespace string, id uint64) (uint64, error) {
			token := walPropertyToken{namespace, id}
			binding, ok := bindings[token]
			if !ok {
				return 0, fmt.Errorf("wal: missing property name binding for namespace %q token %d; foreign historical record has no authoritative name", namespace, id)
			}
			if data.propertyEpoch != nil && binding.pending {
				// Resolve the first body's names in their normal first-use
				// order, then finish the remaining declared epoch names below.
				binding.destination, err = b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, binding.name)
				if err != nil {
					return 0, err
				}
				binding.pending = false
				bindings[token] = binding
				remap = remap || id != binding.destination
			}
			return binding.destination, nil
		})
		if err != nil {
			return bufferedCommit{}, err
		}
	}
	if resolve && data.propertyEpoch != nil {
		// Finish the explicit epoch once, after giving its first body's names
		// their normal first-use allocation order. Remaining names belong to
		// that same declared dictionary, including names of deleted entities.
		for token, binding := range bindings {
			if binding.pending {
				binding.destination, err = b.propKeyDict.resolveOrAllocateInTxn(txn, token.namespace, binding.name)
				if err != nil {
					return bufferedCommit{}, err
				}
				binding.pending = false
				bindings[token] = binding
			}
			remap = remap || token.id != binding.destination
		}
		resolve = false
	}
	if remap {
		// Re-allocation changes the physical dictionary domain even during an
		// original-directory import of an older self-contained record. Persist
		// that fact before any remapped body or destination drain can apply.
		if err := b.markPropertySourceForeignLocked(db); err != nil {
			return bufferedCommit{}, err
		}
	}
	if txn != nil {
		c.propKeyDrain = b.propKeyDict.flushTxnCounters(txn)
		revision = b.propKeyDict.walRevision()
	}
	if !legacy && (!cached || resolve) {
		b.rememberWALPropertyPlan(data.PropertyBindings, bindings, c.propKeyDrain, resolve, remap, revision, domain, data.propertyEpoch)
	}
	return c, nil
}

type walBoundProperty struct {
	name        string
	destination uint64
	pending     bool
}

// A matching complete table needs no per-record map or body traversal. Exact
// agreement in both dictionary directions also excludes conflicting duplicate
// bindings; any mismatch goes through the checked, record-local remap table.
func (b *BadgerEngine) completeWALBindingsMatch(data []byte) (bool, error) {
	d := b.propKeyDict
	d.mu.RLock()
	defer d.mu.RUnlock()
	matches := true
	err := visitWALPropertyBindings(data, nil, func(namespace string, id uint64, name string) error {
		matches = matches && d.forward[namespace][name] == id && d.reverse[namespace][id] == name && d.persisted[namespace][name]
		return nil
	}, func(namespace string, id uint64, value []byte) string {
		if name := d.reverse[namespace][id]; name == string(value) {
			return name
		}
		return string(value)
	})
	return matches, err
}

// Resolve the explicit table supplied by this record or its immutable epoch.
// Canonical dictionary strings are reused; no cross-epoch token map is retained.
func (b *BadgerEngine) bufferedWALRecordBindings(data *WALBufferedCommitData) (map[walPropertyToken]walBoundProperty, bool, bool, error) {
	bindings := make(map[walPropertyToken]walBoundProperty)
	resolve, remap := false, false
	d := b.propKeyDict
	d.mu.RLock()
	defer d.mu.RUnlock()
	remember := func(namespace string, id uint64, name string) error {
		token := walPropertyToken{namespace, id}
		if prior, exists := bindings[token]; exists && prior.name != name {
			return fmt.Errorf("wal: conflicting property bindings for namespace %q token %d in one record", namespace, id)
		}
		destination, found := d.forward[namespace][name]
		pending := !found || !d.persisted[namespace][name]
		resolve = resolve || pending
		remap = remap || found && destination != id
		bindings[token] = walBoundProperty{name: name, destination: destination, pending: pending}
		return nil
	}
	var err error
	if len(data.PropertyBindings) != 0 {
		err = visitWALPropertyBindings(data.PropertyBindings, nil, remember, func(namespace string, id uint64, value []byte) string {
			if name := d.reverse[namespace][id]; name == string(value) {
				return name
			}
			if local, ok := d.forward[namespace][string(value)]; ok {
				return d.reverse[namespace][local]
			}
			return string(value)
		})
	} else {
		for _, entry := range data.PropKeys {
			if err = remember(entry.Namespace, entry.ID, entry.Name); err != nil {
				break
			}
		}
	}
	return bindings, resolve, remap, err
}

// rewriteWALPropertyBodies owns the physical-body census. Only primary node
// and edge records use property tokens; MVCC images/undo records, snapshots,
// and replication operations contain named properties. Native backups transfer
// the entire store, including its dictionaries, through offline replacement.
func rewriteWALPropertyBodies(writes map[string][]byte, resolve func(string, uint64) (uint64, error), reserve ...func(string, int)) (map[string][]byte, error) {
	return rewriteWALPropertyPayloads(writes, func(_ string, namespace string, payload []byte) ([]byte, error) {
		if len(reserve) != 0 && len(payload) > 0 {
			count, _, err := readUvarint(payload)
			if err != nil || count > uint64(len(payload)) {
				return nil, fmt.Errorf("wal: invalid property count")
			}
			reserve[0](namespace, int(count))
		}
		return remapPropertyTokens(payload, func(id uint64) (uint64, error) { return resolve(namespace, id) })
	})
}

func rewriteWALPropertyPayloads(writes map[string][]byte, transform func(string, string, []byte) ([]byte, error)) (map[string][]byte, error) {
	out := writes
	copied := false
	for key, body := range writes {
		if len(key) == 0 || (key[0] != prefixNode && key[0] != prefixEdge) {
			continue
		}
		namespace, _, ok := ParseDatabasePrefix(key[1:])
		if !ok {
			return nil, fmt.Errorf("wal: body key lacks namespace")
		}
		var start, end int
		if key[0] == prefixNode {
			if len(body) == 0 || body[0] != nodeFormatTokenizedV1 {
				return nil, fmt.Errorf("wal: invalid tokenized node body")
			}
			size, n, err := readUvarint(body[1:])
			if err != nil {
				return nil, err
			}
			start = 1 + n
			if size > uint64(len(body)-start) {
				return nil, fmt.Errorf("wal: truncated node properties")
			}
			end = start + int(size)
		} else {
			_, _, _, offset, err := decodeEdgeCompactHeader(body, edgeFormatCompactV2)
			if err != nil {
				return nil, err
			}
			start, end = offset, len(body)
		}
		props, err := transform(key, namespace, body[start:end])
		if err != nil {
			return nil, fmt.Errorf("wal: property bindings for %q: %w", key[1:], err)
		}
		if bytes.Equal(props, body[start:end]) {
			continue
		}
		if !copied {
			out = make(map[string][]byte, len(writes))
			for key, value := range writes {
				out[key] = value
			}
			copied = true
		}
		var rebound []byte
		if key[0] == prefixNode {
			rebound = binary.AppendUvarint([]byte{nodeFormatTokenizedV1}, uint64(len(props)))
		} else {
			rebound = append(rebound, body[:start]...)
		}
		rebound = append(rebound, props...)
		out[key] = append(rebound, body[end:]...)
	}
	return out, nil
}

// remapPropertyTokens changes only key varints. Values retain their exact
// msgpack bytes (including temporal extensions, integer widths and vectors).
func remapPropertyTokens(data []byte, resolve func(uint64) (uint64, error)) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}
	count, n, err := readUvarint(data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(data[n:])
	dec := msgpack.GetDecoder()
	dec.Reset(reader)
	defer msgpack.PutDecoder(dec)
	var out []byte
	unchanged := 0
	for i := uint64(0); i < count; i++ {
		offset := len(data) - reader.Len()
		id, size, err := readUvarint(data[offset:])
		if err != nil {
			return nil, err
		}
		if _, err := reader.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
		mapped, err := resolve(id)
		if err != nil {
			return nil, err
		}
		if mapped != id {
			out = append(out, data[unchanged:offset]...)
			out = binary.AppendUvarint(out, mapped)
			unchanged = offset + size
		}
		if err := dec.Skip(); err != nil {
			return nil, err
		}
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("wal: trailing property bytes")
	}
	if out == nil {
		return data, nil
	}
	return append(out, data[unchanged:]...), nil
}
