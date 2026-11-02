package storage

import (
	"context"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/vmihailenco/msgpack/v5"
)

// DeindexWorkItem is a pending deindex task for an entity whose visibility
// score has dropped below the threshold. The background cleanup job drains
// these items and writes tombstones for the entity's secondary-index keys.
type DeindexWorkItem = knowledgepolicy.DeindexWorkItem

func deindexWorkItemKey(workItemID string) []byte {
	return accessMetaKey(policyWorkMetaIDPrefix + workItemID)
}

func putDeindexWorkItemInTxn(txn *badger.Txn, item *DeindexWorkItem) error {
	entry := &knowledgepolicy.AccessMetaEntry{TargetID: item.TargetID, TargetScope: knowledgepolicy.ScopeType(item.TargetScope), DeindexWork: item}
	_, err := accessMetaInTxn(txn, policyWorkMetaIDPrefix+item.WorkItemID, accessMetaWrite, entry)
	return err
}

// PutDeindexWorkItem queues one retryable work item. It is the exported
// context-free queue entry: recording an item is accepted engine bookkeeping
// that runs under its own explicit unbounded lifetime, and making it cancelable
// needs its own context-first contract decision.
func (b *BadgerEngine) PutDeindexWorkItem(item *DeindexWorkItem) error {
	_, err := b.WriteAccessMeta(context.Background(), knowledgepolicy.AccessMetaWriteRequest{Mode: knowledgepolicy.RecordState, EntityID: policyWorkMetaIDPrefix + item.WorkItemID, Entry: &knowledgepolicy.AccessMetaEntry{
		TargetID: item.TargetID, TargetScope: knowledgepolicy.ScopeType(item.TargetScope), DeindexWork: item,
	}})
	return err
}

func (b *BadgerEngine) GetDeindexWorkItem(workItemID string) (*DeindexWorkItem, error) {
	result, err := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: policyWorkMetaIDPrefix + workItemID})
	if err != nil || result.Entry == nil {
		return nil, err
	}
	return result.Entry.DeindexWork, nil
}

// DeleteDeindexWorkItem retires one work item. As the context-free queue entry
// it runs under its own explicit unbounded lifetime, so an accepted retirement
// completes even when the job that produced it has stopped.
func (b *BadgerEngine) DeleteDeindexWorkItem(workItemID string) error {
	return b.DeleteAccessMeta(context.Background(), policyWorkMetaIDPrefix+workItemID)
}

// ScanPendingDeindexWorkItems returns all work items with status "pending".
func (b *BadgerEngine) ScanPendingDeindexWorkItems() ([]*DeindexWorkItem, error) {
	if err := b.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	var items []*DeindexWorkItem
	err := b.withView(func(txn *badger.Txn) error {
		prefix := accessMetaKey(policyWorkMetaIDPrefix)
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var entry knowledgepolicy.AccessMetaEntry
			if err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &entry)
			}); err != nil {
				return err
			}
			if entry.DeindexWork != nil && entry.DeindexWork.Status == "pending" {
				items = append(items, entry.DeindexWork)
			}
		}
		return nil
	})
	return items, err
}
