// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package storage

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// metadataKV is what a derived-metadata writer needs from its transaction.
// *badger.Txn implements it; a publication tail hands its writers a guarded
// one instead.
type metadataKV interface {
	Get(key []byte) (*badger.Item, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}

// publicationTailKey reports whether a publication's tail may write key. A
// tail persists derived metadata that its publication's commit deliberately
// leaves out of the user transaction (allocateMVCCVersion, persistCounters,
// the positional count tier): the MVCC metadata family (sequence high-water
// marks, derived counts) and the numeric-ID counters. No node, relationship,
// index, pending-embedding, embedding, temporal, MVCC row/head, schema,
// dictionary or change-event key is a tail key, so a tail commit cannot change
// a row.
func publicationTailKey(key []byte) bool {
	return len(key) > 0 && (key[0] == prefixMVCCMeta || key[0] == prefixIDDictCounter)
}

// publicationTailWriter refuses every key outside the tail families, so a
// writer that strays into rows fails instead of committing a row change the
// commit oracle would not count.
type publicationTailWriter struct{ txn *badger.Txn }

func (w publicationTailWriter) Get(key []byte) (*badger.Item, error) { return w.txn.Get(key) }

func (w publicationTailWriter) Set(key, value []byte) error {
	if !publicationTailKey(key) {
		return publicationTailKeyError(key)
	}
	return w.txn.Set(key, value)
}

func (w publicationTailWriter) Delete(key []byte) error {
	if !publicationTailKey(key) {
		return publicationTailKeyError(key)
	}
	return w.txn.Delete(key)
}

func publicationTailKeyError(key []byte) error {
	family := byte(0)
	if len(key) > 0 {
		family = key[0]
	}
	return fmt.Errorf("storage: key family 0x%02x is not derived metadata and cannot be written by a publication tail: %w", family, ErrInvalidData)
}

// updatePublicationTail commits fn's derived-metadata writes as one commit of
// the publication that just finished. Such a commit changes no row, so the
// commit oracle does not count it as another reader-relevant publication.
func (m *managedBadgerDB) updatePublicationTail(fn func(w metadataKV) error) error {
	if m.DB.IsClosed() {
		return badger.ErrDBClosed
	}
	readTs, err := m.oracle.beginRead()
	if err != nil {
		return err
	}
	defer m.oracle.endRead(readTs)
	txn := m.DB.NewTransactionAt(readTs, true)
	defer txn.Discard()
	if err := fn(publicationTailWriter{txn: txn}); err != nil {
		return err
	}
	ts, err := m.commitAt(txn, nil, true)
	if err != nil {
		return err
	}
	return m.oracle.waitPublished(ts)
}

// updateKV is Update for the derived-metadata writers outside a publication
// tail; the commit stays reader-relevant.
func (m *managedBadgerDB) updateKV(fn func(w metadataKV) error) error {
	return m.Update(func(txn *badger.Txn) error { return fn(txn) })
}

// withPublicationTailUpdate is withUpdate for a publication's tail: the engine
// commit writer's usual bookkeeping, with only tail keys writable and the
// commit not counted as another reader-relevant publication.
// keys, when given, are the publication keys the commit serializes on.
func (b *BadgerEngine) withPublicationTailUpdate(fn func(w metadataKV) error, keys ...func() []any) error {
	return b.commitEngineWrite(func(cw *commitWriter) error {
		cw.publicationTail = true
		if err := cw.writeOnce(func(txn *badger.Txn) error { return fn(publicationTailWriter{txn: txn}) }); err != nil {
			return err
		}
		for _, k := range keys {
			cw.publishKeys = append(cw.publishKeys, k()...)
		}
		return nil
	})
}
