package storage

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
)

const embeddingRepairBatchSize = 128

// migrateV3ToV4 is the upgrade arm for stores written before the #1218
// cleanup fix, whose shrink and delete paths could leave chunks behind an
// inline or missing body. Reads trust body and metadata absence only from V4.
// It also upgrades a V3 backup during offline Restore, so it runs on the
// held read path; a crash before the version write repeats the remaining work.
func (b *BadgerEngine) migrateV3ToV4() error {
	if _, err := b.repairOrphanedEmbeddingKeysLocked(); err != nil {
		return err
	}
	return b.commitEngineWriteHeld(func(cw *commitWriter) error {
		return cw.writeOnce(func(txn *badger.Txn) error {
			version := make([]byte, 8)
			binary.BigEndian.PutUint64(version, uint64(storageVersionEmbedKeysV4))
			return txn.Set(mvccSchemaVersionKey(), version)
		})
	})
}

// RepairOrphanedEmbeddingKeys removes separately stored keys with no owning
// node, or with an inline owner and no sidecar metadata. It preserves ranges
// referenced by a separate body or a sidecar, including stale sidecars.
// The V3→V4 upgrade runs it once; it can also be run explicitly, for example:
//
//	removed, err := engine.RepairOrphanedEmbeddingKeys()
//
// Discovery retains at most 256 candidate owners and each deletion transaction
// copies at most 128 keys. The
// repair excludes writers and drains buffered data before inspecting owners.
func (b *BadgerEngine) RepairOrphanedEmbeddingKeys() (int, error) {
	for {
		if err := b.FlushWriteBehind(); err != nil {
			return 0, err
		}
		release, err := b.beginSchemaWrite()
		if err != nil {
			return 0, err
		}
		if b.writeBehind != nil && b.writeBehind.PendingOps() != 0 {
			release()
			continue
		}
		defer release()
		return b.repairOrphanedEmbeddingKeysLocked()
	}
}

func (b *BadgerEngine) repairOrphanedEmbeddingKeysLocked() (int, error) {
	removed := 0
	for _, family := range embeddingKeyFamilies {
		resume := []byte{family}
		for {
			var owners []NodeID
			scanned := 0
			err := b.db.viewHeld(func(txn *badger.Txn) error {
				opts := badgerPrefixIteratorOptions([]byte{family})
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				seen := make(map[NodeID]bool)
				referenced := make(map[NodeID]bool)
				for it.Seek(resume); it.Valid() && scanned < embeddingRepairBatchSize; it.Next() {
					key := it.Item().KeyCopy(nil)
					resume = append(key, 0) // strictly after this physical key
					scanned++
					candidates := embeddingKeyOwners(key)
					live := false
					for _, owner := range candidates {
						ref, known := referenced[owner.id]
						if !known {
							var err error
							ref, err = b.embeddingOwnerReferenced(txn, owner.id)
							if err != nil {
								return err
							}
							referenced[owner.id] = ref
						}
						live = live || ref
					}
					if live {
						continue
					}
					for _, owner := range candidates {
						if !seen[owner.id] {
							owners = append(owners, owner.id)
							seen[owner.id] = true
						}
					}
				}

				return nil
			})
			if err != nil {
				return removed, err
			}
			if scanned == 0 {
				break
			}
			for _, id := range owners {
				if id == "" {
					continue
				}
				var after []byte
				for {
					deleted := 0
					err := b.commitEngineWriteHeld(func(cw *commitWriter) error {
						referenced, err := b.embeddingOwnerReferenced(cw.txn, id)
						if err != nil || referenced {
							return err
						}
						deleted, err = cw.removeEmbeddingKeys(id, embeddingRepairBatchSize, embeddingKeyCleanupOptions{cursor: &after})
						return err
					})
					if err != nil {
						return removed, err
					}
					removed += deleted
					if deleted < embeddingRepairBatchSize {
						break
					}
				}
			}
		}
	}
	return removed, nil
}

// A stale sidecar is still owned. Decode the body through the store codec,
// rather than inferring absence from a delimiter or current hydration state.
func (b *BadgerEngine) embeddingOwnerReferenced(txn *badger.Txn, id NodeID) (bool, error) {
	return embeddingOwnerReferenced(txn, id)
}

// Discovery and every ambiguous legacy-key remover share this predicate.
func embeddingOwnerReferenced(txn *badger.Txn, id NodeID) (bool, error) {
	return (embeddingReferenceView{txn: txn}).ownerReferenced(id)
}
