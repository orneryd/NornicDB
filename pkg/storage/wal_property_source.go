package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

var walPropertyIdentityKey = append([]byte{prefixMVCCMeta}, []byte("wal-property-identity")...)
var walPropertyForeignKey = append([]byte{prefixMVCCMeta}, []byte("wal-property-foreign")...)
var walPropertyCleanKey = append([]byte{prefixMVCCMeta}, []byte("wal-property-clean")...)

// The entry point supplies authority. Neither matching tokens nor a loaded
// dictionary can establish that a destination wrote a historical record.
type walPropertyAuthority uint8

const (
	walForeignImport walPropertyAuthority = iota
	walOwnDirectoryAtOpen
)

type walPropertySource struct {
	sync.Mutex
	identity string
	foreign  bool
	clean    bool
	bindings walPropertyImportPlan
}

// initializeWALPropertyIdentity stamps unstamped stores on their first open.
// Old records still obtain their authority from the own-WAL constructor path.
// Native replacement and dictionary re-allocation explicitly rotate identity.
// The constructor has not published the engine; offline restore holds its
// exclusive barrier. Both pass their already protected handle explicitly.
func (b *BadgerEngine) initializeWALPropertyIdentity(db *managedBadgerDB, replaced, held bool) error {
	s := &b.walPropertySource
	s.Lock()
	defer s.Unlock()
	var identity string
	var foreign bool
	var cleanIdentity string
	read := db.View
	if held {
		read = db.viewHeld
	}
	if err := read(func(tx *badger.Txn) error {
		item, err := tx.Get(walPropertyIdentityKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			if err := item.Value(func(value []byte) error { identity = string(value); return nil }); err != nil {
				return err
			}
			if _, err := uuid.Parse(identity); err != nil {
				return fmt.Errorf("wal: invalid property dictionary identity: %w", err)
			}
		}
		if item, cleanErr := tx.Get(walPropertyCleanKey); cleanErr == nil {
			if err := item.Value(func(value []byte) error { cleanIdentity = string(value); return nil }); err != nil {
				return err
			}
		} else if cleanErr != badger.ErrKeyNotFound {
			return cleanErr
		}
		_, err = tx.Get(walPropertyForeignKey)
		foreign = err == nil
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	if identity == "" || replaced {
		return b.persistPropertyIdentityLocked(db, uuid.NewString(), foreign || replaced, held)
	}
	s.identity, s.foreign = identity, foreign
	s.clean = cleanIdentity == identity
	b.propKeyDict.walIdentity.Store(&identity)
	b.propKeyDict.walEpochRevision.Add(1)
	return nil
}

func (b *BadgerEngine) persistPropertyIdentityLocked(db *managedBadgerDB, identity string, foreign, held bool) error {
	if foreign && b.bufferedCommitWAL != nil {
		if err := b.bufferedCommitWAL.invalidateCleanPropertyEpoch(); err != nil {
			return err
		}
	}
	write := func(tx *badger.Txn) error {
		if err := tx.Set(walPropertyIdentityKey, []byte(identity)); err != nil {
			return err
		}
		if foreign {
			return tx.Set(walPropertyForeignKey, []byte{1})
		}
		return nil
	}
	var err error
	if held {
		// Restore already excludes readers. Use its held timestamp rather than
		// entering the ordinary read gate, which rejects reads during restore.
		ts := db.oracle.beginReadHeld()
		defer db.oracle.endRead(ts)
		tx := db.newTxnAt(ts, true)
		defer tx.Discard()
		if err = write(tx); err == nil {
			err = db.commit(context.Background(), tx, nil)
		}
	} else {
		err = db.Update(write)
	}
	if err != nil {
		return err
	}
	b.walPropertySource.identity, b.walPropertySource.foreign = identity, foreign
	b.walPropertySource.clean = false
	b.propKeyDict.walIdentity.Store(&identity)
	b.propKeyDict.walEpochRevision.Add(1)
	return nil
}

// Foreign provenance is durable before ANY imported body can apply, even if
// every observed name happens to have the same ID. The foreign marker stays;
// any later clean ownership proof is invalidated by another import or restore.
func (b *BadgerEngine) markPropertySourceForeignLocked(db *managedBadgerDB) error {
	if b.walPropertySource.foreign && !b.walPropertySource.clean {
		return nil
	}
	return b.persistPropertyIdentityLocked(db, uuid.NewString(), true, false)
}

func (b *BadgerEngine) beginSemanticPropertyRestore() error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	b.walPropertySource.Lock()
	defer b.walPropertySource.Unlock()
	return b.persistPropertyIdentityLocked(db, uuid.NewString(), true, false)
}

func beginRecoveryPropertyRestore(destination Engine) error {
	if b, ok := UnwrapEngine(destination).(*BadgerEngine); ok {
		return b.beginSemanticPropertyRestore()
	}
	return nil
}

func markRecoveryPropertyForeign(destination Engine) error {
	if b, ok := UnwrapEngine(destination).(*BadgerEngine); ok {
		db, err := b.beginHelperTxn()
		if err != nil {
			return err
		}
		defer b.txnWG.Done()
		b.walPropertySource.Lock()
		defer b.walPropertySource.Unlock()
		return b.markPropertySourceForeignLocked(db)
	}
	return nil
}

func decodeBufferedWALRecord(entry WALEntry) (WALBufferedCommitData, error) {
	var data WALBufferedCommitData
	payload, err := decompressWALBufferedCommitPayload(entry.Data)
	if err == nil {
		data.namedPropertiesAuthoritative, err = decodeRecoveryPayloadAuthority(payload, &data)
	}
	if err != nil {
		return data, fmt.Errorf("wal: failed to unmarshal buffered commit %d: %w", entry.Sequence, err)
	}
	if len(data.PropertyBindings) == 0 && data.PropertyDictionaryID == "" && entry.PropertyEpoch != nil {
		data.propertyEpoch = entry.PropertyEpoch.state
		if data.propertyEpoch == nil {
			return data, fmt.Errorf("wal: empty property dictionary epoch")
		}
		data.PropertyBindings = data.propertyEpoch.Bindings
		data.PropertyDictionaryID = data.propertyEpoch.DictionaryID
	}
	return data, nil
}

// Compact binding tuples avoid repeating JSON field names and allocating two
// decoded strings per key on every replay. A destination dictionary can supply
// canonical strings, without retaining a cross-record source-token context.
func visitWALPropertyBindings(data []byte, known map[walPropertyToken]string, visit func(string, uint64, string) error, canonical ...func(string, uint64, []byte) string) error {
	// Version 2 contains one independently compressed, complete version-1
	// table. It never refers to an earlier record or a segment's writer.
	if len(data) > 0 && data[0] == 2 {
		var err error
		data, err = decompressWALBufferedCommitPayload(data[1:])
		if err != nil {
			return err
		}
	}
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
			if len(canonical) != 0 {
				name = canonical[0](namespace, id, value)
			} else {
				name = string(value)
			}
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
