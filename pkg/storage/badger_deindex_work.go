package storage

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// DeindexWorkItem is a pending deindex task for an entity whose visibility
// score has dropped below the threshold. The background cleanup job drains
// these items and writes tombstones for the entity's secondary-index keys.
type DeindexWorkItem struct {
	WorkItemID    string `msgpack:"workItemId"`
	TargetID      string `msgpack:"targetId"`
	TargetScope   string `msgpack:"targetScope"`
	EnqueuedAt    int64  `msgpack:"enqueuedAt"`
	NextAttemptAt int64  `msgpack:"nextAttemptAt"`
	RetryCount    int    `msgpack:"retryCount"`
	Status        string `msgpack:"status"`
}

func deindexWorkItemKey(workItemID string) []byte {
	return append([]byte{prefixDeindexWorkItem}, []byte(workItemID)...)
}

func (b *BadgerEngine) PutDeindexWorkItem(item *DeindexWorkItem) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		data, err := msgpack.Marshal(item)
		if err != nil {
			return err
		}
		return txn.Set(deindexWorkItemKey(item.WorkItemID), data)
	})
}

func (b *BadgerEngine) GetDeindexWorkItem(workItemID string) (*DeindexWorkItem, error) {
	var item DeindexWorkItem
	found := false
	err := b.withView(func(txn *badger.Txn) error {
		it, err := txn.Get(deindexWorkItemKey(workItemID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return it.Value(func(val []byte) error {
			found = true
			return msgpack.Unmarshal(val, &item)
		})
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &item, nil
}

func (b *BadgerEngine) DeleteDeindexWorkItem(workItemID string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(deindexWorkItemKey(workItemID))
	})
}

// ScanPendingDeindexWorkItems returns all work items with status "pending".
func (b *BadgerEngine) ScanPendingDeindexWorkItems() ([]*DeindexWorkItem, error) {
	var items []*DeindexWorkItem
	err := b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixDeindexWorkItem}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var item DeindexWorkItem
			if err := it.Item().Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &item)
			}); err != nil {
				continue
			}
			if item.Status == "pending" {
				items = append(items, &item)
			}
		}
		return nil
	})
	return items, err
}
