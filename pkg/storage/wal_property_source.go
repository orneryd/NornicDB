package storage

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// Kept under the existing metadata prefix, separate from its numeric subkeys.
// A semantic rebuild/remap must never later masquerade as the original store.
var walPropertyForeignKey = append([]byte{prefixMVCCMeta}, []byte("wal-property-foreign")...)

type walPropertySource struct {
	sync.Mutex
	openedDictionary bool
	initialized      bool
	foreign          bool
	names            map[walPropertyToken]string
	original         bool
}

// resetWALPropertySource runs when a complete physical store is opened or
// replaced. Only a dictionary loaded with its original physical bodies is a
// candidate source for old records. Creating semantic rows is not such proof.
func (b *BadgerEngine) resetWALPropertySource() {
	s := &b.walPropertySource
	s.Lock()
	defer s.Unlock()
	s.initialized, s.foreign = false, false
	s.names, s.original = nil, false
	s.openedDictionary = len(b.propKeyDict.reverse) > 0
}

func (b *BadgerEngine) propertySourceLocked() (*walPropertySource, error) {
	s := &b.walPropertySource
	if s.initialized {
		return s, nil
	}
	if err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(walPropertyForeignKey)
		s.foreign = err == nil
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		return nil, err
	}
	s.names = make(map[walPropertyToken]string)
	s.initialized = true
	return s, nil
}

// Startup recovery supplies the original store/WAL pair. Generic entry replay
// and semantic recovery do not assert that provenance and cannot use this table.
func (b *BadgerEngine) beginOriginalPropertyRecovery() error {
	b.walPropertySource.Lock()
	defer b.walPropertySource.Unlock()
	s, err := b.propertySourceLocked()
	if err != nil {
		return err
	}
	// Startup runs before traffic; the loaded dictionary is the source until
	// a conflicting binding/remap revokes that identity. No whole-table copy.
	s.original = s.openedDictionary && !s.foreign
	return nil
}

func (b *BadgerEngine) endOriginalPropertyRecovery() {
	b.walPropertySource.Lock()
	b.walPropertySource.original = false
	b.walPropertySource.Unlock()
}

func (b *BadgerEngine) markPropertySourceForeignLocked(s *walPropertySource) error {
	if !s.foreign {
		if err := b.db.Update(func(txn *badger.Txn) error { return txn.Set(walPropertyForeignKey, []byte{1}) }); err != nil {
			return err
		}
		s.foreign = true
	}
	s.original = false
	return nil
}

// beginSemanticPropertyRestore is called before any snapshot row is encoded.
// Snapshot names alone cannot identify the source's original numeric tokens.
func (b *BadgerEngine) beginSemanticPropertyRestore() error {
	s := &b.walPropertySource
	s.Lock()
	defer s.Unlock()
	if _, err := b.propertySourceLocked(); err != nil {
		return err
	}
	s.names = make(map[walPropertyToken]string)
	return b.markPropertySourceForeignLocked(s)
}

func beginRecoveryPropertyRestore(destination Engine) error {
	if b, ok := UnwrapEngine(destination).(*BadgerEngine); ok {
		return b.beginSemanticPropertyRestore()
	}
	return nil
}

func observeRecoveryPropertyPrefix(destination Engine, entry WALEntry) error {
	if entry.Operation != OpBufferedCommit {
		return nil
	}
	if b, ok := UnwrapEngine(destination).(*BadgerEngine); ok {
		data, err := decodeBufferedWALRecord(entry)
		if err != nil {
			return err
		}
		return b.observeBufferedWALRecord(&data)
	}
	return nil
}

func (b *BadgerEngine) observePropertyBindingsLocked(data *WALBufferedCommitData) (*walPropertySource, error) {
	s, err := b.propertySourceLocked()
	if err != nil {
		return nil, err
	}
	remember := func(namespace string, id uint64, name string) error {
		token := walPropertyToken{namespace, id}
		if old, exists := s.names[token]; exists && old != name {
			return fmt.Errorf("wal: conflicting property bindings for namespace %q token %d", namespace, id)
		}
		if s.original {
			if original, exists := b.propKeyDict.lookup(namespace, id); exists && original != name {
				if err := b.markPropertySourceForeignLocked(s); err != nil {
					return err
				}
			}
		}
		s.names[token] = name
		return nil
	}
	if len(data.PropertyBindings) != 0 {
		err = visitWALPropertyBindings(data.PropertyBindings, s.names, remember)
	} else {
		for _, binding := range data.PropKeys {
			if err = remember(binding.Namespace, binding.ID, binding.Name); err != nil {
				break
			}
		}
	}
	return s, err
}

// observeBufferedWALRecord also runs for records skipped by snapshot sequence
// or durable flush markers. Their bindings remain evidence for later old WAL.
func (b *BadgerEngine) observeBufferedWALRecord(data *WALBufferedCommitData) error {
	b.walPropertySource.Lock()
	defer b.walPropertySource.Unlock()
	_, err := b.observePropertyBindingsLocked(data)
	return err
}

func decodeBufferedWALRecord(entry WALEntry) (WALBufferedCommitData, error) {
	var data WALBufferedCommitData
	payload, err := decompressWALBufferedCommitPayload(entry.Data)
	if err == nil {
		err = decodeRecoveryPayload(payload, &data)
	}
	if err != nil {
		return data, fmt.Errorf("wal: failed to unmarshal buffered commit %d: %w", entry.Sequence, err)
	}
	return data, nil
}

// Compact binding tuples avoid repeating JSON field names and allocating two
// decoded strings per key on every replay. Strings already known to the source
// context are reused. Only newly encountered names are retained by that context.
func visitWALPropertyBindings(data []byte, known map[walPropertyToken]string, visit func(string, uint64, string) error) error {
	if len(data) == 0 || data[0] != 1 {
		return fmt.Errorf("wal: unsupported property binding encoding")
	}
	data = data[1:]
	read := func() (uint64, error) {
		value, n, err := readUvarint(data)
		if err == nil {
			data = data[n:]
		}
		return value, err
	}
	readString := func() ([]byte, error) {
		n, err := read()
		if err != nil || n > uint64(len(data)) {
			return nil, fmt.Errorf("wal: truncated property binding string")
		}
		value := data[:n]
		data = data[n:]
		return value, nil
	}
	var namespace string
	for len(data) > 0 {
		ns, err := readString()
		if err != nil {
			return err
		}
		if namespace != string(ns) {
			namespace = string(ns)
		}
		id, err := read()
		if err != nil {
			return err
		}
		value, err := readString()
		if err != nil {
			return err
		}
		name, exists := known[walPropertyToken{namespace, id}]
		if !exists || name != string(value) {
			name = string(value)
		}
		if err := visit(namespace, id, name); err != nil {
			return err
		}
	}
	return nil
}

func appendWALPropertyBinding(out []byte, namespace string, id uint64, name string) []byte {
	out = binary.AppendUvarint(out, uint64(len(namespace)))
	out = append(out, namespace...)
	out = binary.AppendUvarint(out, id)
	out = binary.AppendUvarint(out, uint64(len(name)))
	return append(out, name...)
}
