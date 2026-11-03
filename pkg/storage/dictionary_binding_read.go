package storage

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// storedBindingKind names one persisted name/number binding that a stored
// body or index entry references. A body stores the number; the binding is
// the only owner of the name it meant when the record was written.
type storedBindingKind uint8

const (
	// nodeIdentityBinding is a node's numeric identity to its stored ID.
	nodeIdentityBinding storedBindingKind = iota
	// propertyNameBinding is one namespace's property-key token to its name.
	propertyNameBinding
)

// storedBinding is one binding lookup: which dictionary, and the number the
// record stored.
type storedBinding struct {
	kind      storedBindingKind
	namespace string
	number    uint64
}

// readStoredBinding resolves a binding in the reader's physical view.
//
// txn is the Badger transaction whose snapshot the record was read from, so
// every binding resolves to the entry that snapshot holds: a namespace a DROP
// emptied and re-created cannot rename the properties or identities of a
// record stored before it. nil is the context-free standalone boundary (a body
// that belongs to no stored snapshot, such as the one a write has just
// encoded); it resolves through the in-memory dictionary.
//
// A stable current view may reuse the immutable current strings instead of
// re-reading the same persisted entry. It does so only when no lifecycle write
// is in progress and its snapshot is at or after the identity cut published by
// the last completed write that replaced stored token meanings (see
// lifecycleWrite.finish); the second sequence observation keeps a write that
// starts mid-lookup from making the borrowed string stale.
func (b *BadgerEngine) readStoredBinding(txn *badger.Txn, binding storedBinding) (name string, found bool, err error) {
	drops := &b.prefixDrops
	sequence := drops.seq.Load()
	if txn == nil || (sequence&1 == 0 && txn.ReadTs() >= drops.identityCut.Load()) {
		switch binding.kind {
		case nodeIdentityBinding:
			id, ok := b.idDict.lookupNodeIDByNum(binding.number)
			name, found = string(id), ok
		case propertyNameBinding:
			name, found = b.propKeyDict.lookup(binding.namespace, binding.number)
		default:
			return "", false, ErrInvalidData
		}
		if txn == nil || (found && drops.seq.Load() == sequence) {
			return name, found, nil
		}
	}
	if txn != nil && binding.kind == propertyNameBinding {
		// Staged names are persisted separately before the body commit. Read
		// only this writer's explicit claim, avoiding a self-conflict on the
		// dictionary commit without borrowing a future foreign name.
		d := b.propKeyDict
		name, found := d.lookup(binding.namespace, binding.number)
		if found {
			d.txnMu.Lock()
			number, owned := d.txnPendingForward[txn][propKeyName{namespace: binding.namespace, name: name}]
			d.txnMu.Unlock()
			if owned && number == binding.number {
				return name, true, nil
			}
		}
	}
	var fixed [9]byte
	var key []byte
	switch binding.kind {
	case nodeIdentityBinding:
		fixed[0] = prefixIDDictNodeReverse
		binary.BigEndian.PutUint64(fixed[1:], binding.number)
		key = fixed[:]
	case propertyNameBinding:
		key = propKeyReverseKey(binding.namespace, binding.number)
	default:
		return "", false, ErrInvalidData
	}
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	err = item.Value(func(data []byte) error {
		name = string(data)
		return nil
	})
	return name, err == nil && name != "", err
}
