package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// A token has meaning only in the dictionary of the store that encoded it.
// WAL is a store boundary: even recovery on the same machine can follow a
// semantic snapshot that allocated the destination's tokens in another order.
type walPropertyToken struct {
	namespace string
	id        uint64
}

// bufferedWALPropertyBindings captures every dependency, including names that
// were persisted before this transaction and therefore absent from its drain.
func bufferedWALPropertyBindings(writes map[string][]byte, dictionary *propertyKeyDictionary) ([]byte, error) {
	var seen map[walPropertyToken]bool
	bindings := []byte{1}
	dictionary.mu.RLock()
	defer dictionary.mu.RUnlock()
	_, err := rewriteWALPropertyBodies(writes, func(namespace string, id uint64) (uint64, error) {
		token := walPropertyToken{namespace, id}
		if !seen[token] {
			name, ok := dictionary.reverse[namespace][id]
			if !ok {
				return 0, fmt.Errorf("wal: property token %d has no source name in namespace %q", id, namespace)
			}
			bindings = appendWALPropertyBinding(bindings, namespace, id, name)
			seen[token] = true
		}
		return id, nil
	}, func(namespace string, count int) {
		if seen == nil {
			// Capacity depends on the first body, never the whole dictionary.
			seen = make(map[walPropertyToken]bool, count)
			bindings = make([]byte, 1, 1+count*(len(namespace)+24))
			bindings[0] = 1
		}
	})
	return bindings, err
}

// importBufferedWALRecord is the only conversion from transport records into
// locally applicable commits. Never install a source dictionary: doing so
// would change the meaning of bodies already restored into this store.
//
// Historical records inherit bindings from the original physical store or
// preceding source records. A semantic destination never supplies guessed IDs.
func (b *BadgerEngine) importBufferedWALRecord(data *WALBufferedCommitData) (bufferedCommit, error) {
	c := bufferedCommit{
		recovered:       true,
		embeddingRanges: data.EmbeddingRanges,
		namespace:       data.Namespace, txID: data.TxID, ops: data.Ops,
		version: data.Version, counterNodeMax: data.NodeMax,
		counterEdgeMax: data.EdgeMax, hasCounters: data.HasCounters,
	}
	b.walPropertySource.Lock()
	defer b.walPropertySource.Unlock()
	source, err := b.observePropertyBindingsLocked(data)
	if err != nil {
		return bufferedCommit{}, err
	}
	txn, readTS, err := b.db.beginTxn(false)
	if err != nil {
		return bufferedCommit{}, err
	}
	defer b.db.endRead(readTS)
	defer txn.Discard()
	defer b.propKeyDict.flushTxnCounters(txn)
	c.writes, err = rewriteWALPropertyBodies(data.Writes, func(namespace string, id uint64) (uint64, error) {
		token := walPropertyToken{namespace, id}
		name, ok := source.names[token]
		if !ok && source.original {
			name, ok = b.propKeyDict.lookup(namespace, id)
		}
		if !ok {
			return 0, fmt.Errorf("wal: missing property name binding for namespace %q token %d; cannot safely replay this historical record", namespace, id)
		}
		mapped, err := b.propKeyDict.resolveOrAllocateInTxn(txn, namespace, name)
		if err == nil && mapped != id {
			err = b.markPropertySourceForeignLocked(source)
		}
		return mapped, err
	})
	if err != nil {
		return bufferedCommit{}, err
	}
	c.propKeyDrain = b.propKeyDict.flushTxnCounters(txn)
	if len(data.Deletes) > 0 {
		c.deletes = make(map[string]bool, len(data.Deletes))
		for _, key := range data.Deletes {
			c.deletes[key] = true
		}
	}
	return c, nil
}

// rewriteWALPropertyBodies owns the physical-body census. Only primary node
// and edge records use property tokens; MVCC images/undo records, snapshots,
// and replication operations contain named properties. Native backups transfer
// the entire store, including its dictionaries, through offline replacement.
func rewriteWALPropertyBodies(writes map[string][]byte, resolve func(string, uint64) (uint64, error), reserve ...func(string, int)) (map[string][]byte, error) {
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
		if len(reserve) != 0 && end > start {
			count, _, err := readUvarint(body[start:end])
			if err != nil || count > uint64(end-start) {
				return nil, fmt.Errorf("wal: invalid property count")
			}
			reserve[0](namespace, int(count))
		}
		props, err := remapPropertyTokens(body[start:end], func(id uint64) (uint64, error) { return resolve(namespace, id) })
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
