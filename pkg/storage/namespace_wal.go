package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// OpNamespaceLifecycle records the same namespace operation used by native and
// replicated CREATE/ALTER/DROP. It is an autonomous committed journal record.
const OpNamespaceLifecycle OperationType = "namespace_lifecycle"

type namespaceWALRecord struct {
	Prefix   string
	Mutation *NamespaceCatalogMutation
	Revision uint64
}

const prefixMVCCMetaNamespaceCompletion = byte(0x15)

func namespaceCompletionKey(namespace string) []byte {
	return append([]byte{prefixMVCCMeta, prefixMVCCMetaNamespaceCompletion}, namespace...)
}

func (b *BadgerEngine) persistNamespaceCompletion(namespace string, sequence uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, sequence)
	return b.withUpdate(func(txn *badger.Txn) error { return txn.Set(namespaceCompletionKey(namespace), value) })
}

func (b *BadgerEngine) namespaceCompleted(namespace string, sequence uint64) (bool, error) {
	completed := false
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(namespaceCompletionKey(namespace))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			if len(value) != 8 {
				return ErrInvalidData
			}
			completed = binary.BigEndian.Uint64(value) >= sequence
			return nil
		})
	})
	return completed, err
}

func (op *NamespaceDrop) bindNativeJournal(wal *WAL) {
	if wal == nil {
		return
	}
	prior := op.journal
	op.SetJournal(func() (bool, error) {
		admitted := op.committedEntry
		var priorErr error
		if prior != nil {
			admitted, priorErr = prior()
			if !admitted && priorErr != nil {
				return false, priorErr
			}
		}
		if op.namespace == "" {
			return admitted, priorErr
		}
		record := namespaceWALRecord{Prefix: op.prefix, Mutation: op.catalogMutation, Revision: op.catalogRevision}
		sequence, err := wal.appendWithDatabaseForcedFlush(OpNamespaceLifecycle, &record, op.namespace)
		if err == nil || sequence != 0 {
			op.nativeSequence = sequence
			admitted = true
		}
		if err == nil && wal.config.SyncMode == "immediate" {
			err = wal.Sync()
		}
		return admitted, errors.Join(priorErr, err)
	})
}

func replayNamespaceLifecycle(engine Engine, entry WALEntry) error {
	var record namespaceWALRecord
	if err := decodeRecoveryPayload(entry.Data, &record); err != nil {
		return fmt.Errorf("wal namespace lifecycle %d: %w", entry.Sequence, err)
	}
	owner := namespaceLifecycleOwner(engine)
	if owner == nil {
		return ErrNotImplemented
	}
	namespace := strings.TrimSuffix(record.Prefix, ":")
	done, err := owner.namespaceCompleted(namespace, entry.Sequence)
	if err != nil || done {
		return err
	}
	op := NewNamespaceDrop(nil)
	if record.Mutation != nil {
		op.SetCatalogMutation(*record.Mutation)
	}
	op.SetCatalogRevision(record.Revision)
	op.MarkCommittedEntry()
	op.nativeSequence = entry.Sequence
	ctx := WithNamespaceDrop(context.Background(), op)
	if record.Mutation != nil && len(record.Mutation.Value) != 0 {
		return PublishNamespaceCatalogContext(ctx, engine, *record.Mutation)
	}
	_, _, err = DeletePrefixContext(ctx, engine, record.Prefix)
	return err
}
