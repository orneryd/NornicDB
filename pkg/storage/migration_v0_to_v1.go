package storage

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/vmihailenco/msgpack/v5"
)

const migrationBatchSize = 1000

// legacyNodeForMigration captures the fields that existed on Node before Phase 7.
// Used only during migration to decode old bytes that include DecayScore/LastAccessed/AccessCount.
type legacyNodeForMigration struct {
	ID           NodeID    `msgpack:"ID"`
	DecayScore   float64   `msgpack:"DecayScore"`
	LastAccessed time.Time `msgpack:"LastAccessed"`
	AccessCount  int64     `msgpack:"AccessCount"`
}

func decodeLegacyNode(data []byte) (*legacyNodeForMigration, error) {
	_, payload, ok, err := splitSerializationHeader(data)
	if err != nil {
		return nil, err
	}
	var node legacyNodeForMigration
	if ok {
		if err := msgpack.Unmarshal(payload, &node); err != nil {
			return nil, err
		}
	} else {
		if err := decodeValue(data, &node); err != nil {
			return nil, err
		}
	}
	return &node, nil
}

// migrateV0ToV1 extracts legacy access state (DecayScore, LastAccessed, AccessCount)
// from node records into AccessMetaEntry records under prefixAccessMeta.
// Node bytes are NOT re-serialized — legacy keys are harmlessly ignored on future decodes.
func (b *BadgerEngine) migrateV0ToV1() error {
	type pendingEntry struct {
		entityID string
		entry    *knowledgepolicy.AccessMetaEntry
	}

	var batch []pendingEntry
	nowNanos := time.Now().UnixNano()

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := b.db.Update(func(txn *badger.Txn) error {
			for _, pe := range batch {
				data, err := msgpack.Marshal(pe.entry)
				if err != nil {
					return err
				}
				if err := txn.Set(accessMetaKey(pe.entityID), data); err != nil {
					return err
				}
			}
			return nil
		})
		batch = batch[:0]
		return err
	}

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = []byte{prefixNode}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(val) == 0 {
				continue
			}

			node, err := decodeLegacyNode(val)
			if err != nil {
				continue
			}

			if node.DecayScore == 0 && node.AccessCount == 0 && node.LastAccessed.IsZero() {
				continue
			}

			var lastAccessedNanos int64
			if !node.LastAccessed.IsZero() {
				lastAccessedNanos = node.LastAccessed.UnixNano()
			}

			entry := &knowledgepolicy.AccessMetaEntry{
				TargetID:    string(node.ID),
				TargetScope: knowledgepolicy.ScopeNode,
				Fixed: knowledgepolicy.AccessMetaFixedFields{
					AccessCount:    node.AccessCount,
					LastAccessedAt: lastAccessedNanos,
				},
				LastMutatedAt: nowNanos,
				MutationCount: 1,
			}

			batch = append(batch, pendingEntry{entityID: string(node.ID), entry: entry})

			if len(batch) >= migrationBatchSize {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := flushBatch(); err != nil {
		return err
	}

	return b.writeSchemaVersion(1)
}
