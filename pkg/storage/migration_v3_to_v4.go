package storage

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// embeddingKeyMoveBatchSize bounds one move transaction: each moved record
// is copied with its shard parts, which hold at most maxEmbeddingValueBytes.
const embeddingKeyMoveBatchSize = 32

// migrateV3ToV4 brings every embedding record into the current key family.
// It first removes keys left without an owner by released shrink and delete
// code (#1218), then moves each remaining compatibility-family record to the
// current address of every referenced owner whose reader resolved it. Once
// the version is written, readers probe only the current family. Open runs it
// through the migration runner; Restore runs it for a restored V3 backup.
// Both hold off other readers and writers, so it uses the held paths.
func (b *BadgerEngine) migrateV3ToV4() error {
	if _, err := b.repairOrphanedEmbeddingKeysLocked(); err != nil {
		return fmt.Errorf("repair orphaned embedding keys: %w", err)
	}
	// Readers take the binary-length family before the released one, so a
	// record moved from it wins when both address the same current key.
	for _, family := range []byte{prefixEmbeddingBinary, prefixEmbedding} {
		for resume := []byte{family}; resume != nil; {
			var err error
			if resume, err = b.moveCompatibilityEmbeddingKeys(family, resume); err != nil {
				return fmt.Errorf("move embedding keys: %w", err)
			}
		}
	}
	return b.commitEngineWriteHeld(func(cw *commitWriter) error {
		return cw.writeOnce(func(txn *badger.Txn) error {
			return setSchemaVersion(txn, storageVersionEmbeddingKeysV4)
		})
	})
}

// moveCompatibilityEmbeddingKeys moves one batch of family's keys from
// resume onward and returns where the next batch starts, or nil at the end.
// A record moves as a chunk base, with the shard parts its value names; a
// part sorts after its base, so it is still present when the base moves. An
// existing current base is what readers already returned, so it is kept.
// Like the orphan repair, the move leaves keys that decode to no owner.
func (b *BadgerEngine) moveCompatibilityEmbeddingKeys(family byte, resume []byte) ([]byte, error) {
	var next []byte
	err := b.commitEngineWriteHeld(func(cw *commitWriter) error {
		return cw.writeOnce(func(txn *badger.Txn) error {
			type record struct{ key, value []byte }
			var batch []record
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{family}))
			for it.Seek(resume); it.ValidForPrefix([]byte{family}); it.Next() {
				if len(batch) == embeddingKeyMoveBatchSize {
					next = append(append([]byte(nil), batch[len(batch)-1].key...), 0) // strictly after the last batched key
					break
				}
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				batch = append(batch, record{it.Item().KeyCopy(nil), value})
			}
			it.Close()
			for _, rec := range batch {
				if len(embeddingKeyOwners(rec.key)) == 0 {
					continue
				}
				for _, base := range compatibilityEmbeddingBases(rec.key) {
					if err := moveCompatibilityEmbeddingBase(txn, base, rec.key, rec.value); err != nil {
						return err
					}
				}
				if err := txn.Delete(rec.key); err != nil {
					return err
				}
			}
			return nil
		})
	})
	return next, err
}

func moveCompatibilityEmbeddingBase(txn *badger.Txn, base embeddingKeyOwner, key, value []byte) error {
	referenced, err := embeddingOwnerReferenced(txn, base.id)
	if err != nil || !referenced {
		return err
	}
	target := embeddingKey(base.id, int(base.index))
	if _, err := txn.Get(target); err != badger.ErrKeyNotFound {
		return err
	}
	if err := txn.Set(target, value); err != nil {
		return err
	}
	parts, sharded := compatibilityEmbeddingShardParts(value)
	for part := 0; sharded && part < parts; part++ {
		item, err := txn.Get(embeddingPartKey(key, part))
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}
		payload, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := txn.Set(embeddingPartKey(target, part), payload); err != nil {
			return err
		}
	}
	return nil
}

// compatibilityEmbeddingShardParts reads a base value the way hydration
// does: a decodable vector has no parts.
func compatibilityEmbeddingShardParts(value []byte) (int, bool) {
	if _, err := decodeEmbedding(value); err == nil {
		return 0, false
	}
	return parseEmbeddingShardMarker(value)
}

// compatibilityEmbeddingBases returns the owners whose reader resolved key
// as a chunk base: every decoded base owner of a released key, and only IDs
// containing NUL for a binary-length key. Shard parts are resolved through
// their base, never on their own.
func compatibilityEmbeddingBases(key []byte) []embeddingKeyOwner {
	var bases []embeddingKeyOwner
	for _, owner := range embeddingKeyOwners(key) {
		if owner.part || (key[0] == prefixEmbeddingBinary && strings.IndexByte(string(owner.id), 0) < 0) {
			continue
		}
		bases = append(bases, owner)
	}
	return bases
}

// useCurrentEmbeddingKeys rewrites a buffered commit that a pre-V4 binary
// logged before a crash. Its embedding writes and deletes address
// compatibility keys, while open has already moved the store to the current
// family, so replay applies each one at the current addresses its readers
// used: a base at its owner's current base, and a shard part only together
// with its base in the same write or delete set.
func (c *bufferedCommit) useCurrentEmbeddingKeys() {
	if len(c.writes) > 0 {
		writes := make(map[string][]byte, len(c.writes))
		for key, value := range c.writes {
			addresses, ok := currentEmbeddingAddresses(key, func(base string) bool { _, ok := c.writes[base]; return ok })
			if !ok {
				addresses = []string{key}
			}
			for _, address := range addresses {
				writes[address] = value
			}
		}
		c.writes = writes
	}
	if len(c.deletes) > 0 {
		deletes := make(map[string]bool, len(c.deletes))
		for key := range c.deletes {
			addresses, ok := currentEmbeddingAddresses(key, func(base string) bool { return c.deletes[base] })
			if !ok {
				addresses = []string{key}
			}
			for _, address := range addresses {
				deletes[address] = true
			}
		}
		c.deletes = deletes
	}
}

// currentEmbeddingAddresses maps a compatibility-family key to current
// addresses; ok is false for any other key. hasBase reports whether the
// compatibility base of a shard part is in the same set.
func currentEmbeddingAddresses(key string, hasBase func(string) bool) ([]string, bool) {
	if key == "" || (key[0] != prefixEmbedding && key[0] != prefixEmbeddingBinary) {
		return nil, false
	}
	raw := []byte(key)
	var addresses []string
	for _, base := range compatibilityEmbeddingBases(raw) {
		addresses = append(addresses, string(embeddingKey(base.id, int(base.index))))
	}
	for _, owner := range embeddingKeyOwners(raw) {
		if !owner.part || (key[0] == prefixEmbeddingBinary && strings.IndexByte(string(owner.id), 0) < 0) || !hasBase(key[:len(key)-3]) {
			continue
		}
		part := int(key[len(key)-2])<<8 | int(key[len(key)-1])
		addresses = append(addresses, string(embeddingPartKey(embeddingKey(owner.id, int(owner.index)), part)))
	}
	return addresses, true
}
