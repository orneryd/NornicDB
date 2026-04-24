package storage

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

func accessMetaKey(entityID string) []byte {
	return append([]byte{prefixAccessMeta}, []byte(entityID)...)
}

func (b *BadgerEngine) GetAccessMeta(entityID string) (*knowledgepolicy.AccessMetaEntry, error) {
	var entry knowledgepolicy.AccessMetaEntry
	found := false

	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(accessMetaKey(entityID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if err := util.DecodeMsgpackBytes(val, &entry); err != nil {
				return err
			}
			found = true
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &entry, nil
}

func (b *BadgerEngine) PutAccessMeta(entityID string, entry *knowledgepolicy.AccessMetaEntry) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		data, err := msgpack.Marshal(entry)
		if err != nil {
			return err
		}
		return txn.Set(accessMetaKey(entityID), data)
	})
}

func (b *BadgerEngine) DeleteAccessMeta(entityID string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(accessMetaKey(entityID))
	})
}

func (b *BadgerEngine) ScanAccessMeta() ([]*knowledgepolicy.AccessMetaEntry, error) {
	var entries []*knowledgepolicy.AccessMetaEntry

	err := b.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixAccessMeta}
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var entry knowledgepolicy.AccessMetaEntry
				if err := util.DecodeMsgpackBytes(val, &entry); err != nil {
					return err
				}
				entries = append(entries, &entry)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}
