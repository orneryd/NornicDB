package storage

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// storeBeforeV4 models a store that a binary before storage V4 wrote: its
// persisted version is V3, so readers still probe the compatibility families.
func storeBeforeV4(t testing.TB, b *BadgerEngine) {
	t.Helper()
	require.NoError(t, b.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
	b.storageVersion = storageVersionEdgeAdjacencyV3
}

func openWithStorageUpgrade(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: true})
	require.NoError(t, err)
	return b
}

// toReleasedEmbeddingKeys rewrites id's current records, shard parts
// included, at the released addresses a V3 store holds.
func toReleasedEmbeddingKeys(t *testing.T, b *BadgerEngine, id NodeID) {
	t.Helper()
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		type record struct{ key, value []byte }
		var records []record
		opts := badgerPrefixIteratorOptions(embeddingPrefix(id))
		it := txn.NewIterator(opts)
		for it.Rewind(); it.ValidForPrefix(embeddingPrefix(id)); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				it.Close()
				return err
			}
			records = append(records, record{it.Item().KeyCopy(nil), value})
		}
		it.Close()
		require.NotEmpty(t, records, "SETUP: %q has current records", id)
		for _, rec := range records {
			owners := embeddingKeyOwners(rec.key)
			require.Len(t, owners, 1)
			released := legacyEmbeddingKey(id, int(owners[0].index))
			if owners[0].part {
				part, err := strconv.ParseUint(string(rec.key[len(rec.key)-4:]), 16, 16)
				require.NoError(t, err)
				released = embeddingPartKey(released, int(part))
			}
			if err := txn.Delete(rec.key); err != nil {
				return err
			}
			if err := txn.Set(released, rec.value); err != nil {
				return err
			}
		}
		return nil
	}))
}

func compatibilityFamilyKeyCount(t *testing.T, b *BadgerEngine) int {
	t.Helper()
	count := 0
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		for _, family := range []byte{prefixEmbedding, prefixEmbeddingBinary} {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{family}))
			for it.Rewind(); it.ValidForPrefix([]byte{family}); it.Next() {
				count++
			}
			it.Close()
		}
		return nil
	}))
	return count
}

// A fresh store starts at V4, so a whole-node read of a node without
// embedding metadata costs one point read, as it did before the current
// family existed. A pre-V4 store still probes the compatibility families.
func TestEmbeddingRead_FreshStoreReadsCurrentKeyOnly(t *testing.T) {
	b := newTestEngine(t)
	require.Equal(t, storageVersionEmbeddingKeysV4, b.storageVersion)
	require.False(t, b.compatibilityEmbeddingKeys())
	for _, tc := range []struct {
		id            NodeID
		compatibility int
	}{{"test:plain", 2}, {"test:nul\x00owner", 3}} {
		_, err := b.CreateNode(&Node{ID: tc.id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}})
		require.NoError(t, err)
		lookups := func(compatibility bool) int {
			n := 0
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				_, _, err := readEmbeddingRecord(tc.id, reservedEmbeddingMetaChunkIndex, compatibility, func(key []byte) (*badger.Item, error) {
					n++
					return txn.Get(key)
				})
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
				return nil
			}))
			return n
		}
		require.Equal(t, 1, lookups(b.compatibilityEmbeddingKeys()), "a V4 store reads only the current key")
		require.Equal(t, tc.compatibility, lookups(true), "a pre-V4 store also probes the compatibility families")

		allocs := func() float64 {
			var got float64
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				node := &Node{ID: tc.id}
				got = testing.AllocsPerRun(5, func() {
					_, _, err := b.loadEmbeddingSidecar(txn, node, tc.id)
					require.NoError(t, err)
				})
				return nil
			}))
			return got
		}
		current := allocs()
		b.storageVersion = storageVersionEdgeAdjacencyV3
		compatibility := allocs()
		b.storageVersion = storageVersionEmbeddingKeysV4
		require.Less(t, current, compatibility, "the compatibility probe's key and lookup are gone from V4 whole-node reads")
	}
}

// Released stores hold separately stored chunks, sharded chunks and sidecar
// metadata in the released family, binary-length records from development
// builds, and orphans. They read correctly before the upgrade; the upgrade is
// gated, removes orphans, moves every live record to the current family, and
// the store then reads the same values through the current key alone.
func TestMigrationV3ToV4_MovesCompatibilityRecords(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	plain := separatelyStoredNode("test:released-plain")
	_, err = b.CreateNode(plain)
	require.NoError(t, err)
	// A second separate range puts more released keys in the family than
	// one move batch holds.
	second := separatelyStoredNode("test:released-second")
	_, err = b.CreateNode(second)
	require.NoError(t, err)
	sharded, _ := r8SeparateReplacementNode(t, "test:released-sharded", 0)
	_, err = b.CreateNode(sharded)
	require.NoError(t, err)
	sidecar := &Node{ID: "test:released-sidecar", Labels: []string{"Doc"}}
	_, err = b.CreateNode(sidecar)
	require.NoError(t, err)
	sidecar = embeddingCleanupColdNode(t, b, sidecar.ID)
	sidecar.ChunkEmbeddings = [][]float32{{1, 2}, {3, 4}}
	sidecar.EmbedMeta = map[string]any{"chunk_count": 2, "model": "fixture"}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar))
	binary := seedPriorEmbeddingNode(t, b, "test:r4\x00owner", true)
	ids := []NodeID{plain.ID, second.ID, sharded.ID, sidecar.ID}
	for _, id := range ids {
		toReleasedEmbeddingKeys(t, b, id)
		require.Zero(t, countEmbeddingKeys(t, b, id), "SETUP: no current record left for %q", id)
	}
	orphan := NodeID("test:released-orphan")
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		payload, err := encodeEmbedding([]float32{9, 9})
		if err != nil {
			return err
		}
		return txn.Set(legacyEmbeddingKey(orphan, 0), payload)
	}))
	require.Greater(t, compatibilityFamilyKeyCount(t, b)-len(binary.ChunkEmbeddings), embeddingKeyMoveBatchSize, "SETUP: the released family spans move batches")
	want := map[NodeID][][]float32{plain.ID: plain.ChunkEmbeddings, second.ID: second.ChunkEmbeddings, sharded.ID: sharded.ChunkEmbeddings, sidecar.ID: sidecar.ChunkEmbeddings, binary.ID: binary.ChunkEmbeddings}
	check := func(b *BadgerEngine) {
		t.Helper()
		for id, chunks := range want {
			require.Equal(t, chunks, embeddingCleanupColdNode(t, b, id).ChunkEmbeddings, "vectors of %q", id)
		}
		require.Equal(t, "fixture", embeddingCleanupColdNode(t, b, sidecar.ID).EmbedMeta["model"])
	}
	check(b)
	require.Positive(t, compatibilityFamilyKeyCount(t, b))
	require.NoError(t, b.Close())

	_, err = NewBadgerEngine(dir)
	var upgrade *ErrStorageUpgradeRequired
	require.True(t, errors.As(err, &upgrade), "a V3 store needs the operator's upgrade authorization: %v", err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, upgrade.OnDisk)
	require.Equal(t, storageVersionEmbeddingKeysV4, upgrade.Current)

	b = openWithStorageUpgrade(t, dir)
	require.Equal(t, storageVersionEmbeddingKeysV4, b.storageVersion)
	require.False(t, b.compatibilityEmbeddingKeys())
	require.Zero(t, compatibilityFamilyKeyCount(t, b), "every compatibility record moved or, as an orphan, removed")
	require.Zero(t, countEmbeddingKeys(t, b, orphan))
	for _, id := range append(ids, binary.ID) {
		require.Positive(t, countEmbeddingKeys(t, b, id), "%q now has current records", id)
	}
	check(b)
	require.NoError(t, b.Close())

	b, err = NewBadgerEngine(dir)
	require.NoError(t, err, "a V4 store opens without the upgrade flag")
	defer b.Close()
	check(b)
}

// A released key of an ID containing NUL can also decode as a shard part of
// a shorter ID. The move follows the readers: the key moves only as the
// chunk its owner read, and the shorter owner gains no part record.
func TestMigrationV3ToV4_AliasOwnersKeepTheirRanges(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	short, long, want := r7AExtensionOwners(t, b)
	shortKeys := countEmbeddingKeys(t, b, short)
	require.NoError(t, b.Close())
	b = openWithStorageUpgrade(t, dir)
	defer b.Close()
	require.Zero(t, r7AExtensionLegacyKeys(t, b))
	require.Equal(t, shortKeys, countEmbeddingKeys(t, b, short), "no alias part moved into the shorter owner's range")
	for _, id := range []NodeID{short, long} {
		cold := embeddingCleanupColdNode(t, b, id)
		require.Equal(t, want[id], cold.ChunkEmbeddings)
		require.Equal(t, string(id), cold.EmbedMeta["fixture_owner"])
	}
}

// Before the move, a current base is what readers returned, so its released
// twin is dropped rather than copied over it. A binary-length key whose owner
// has no NUL was never read, and a key that decodes to no owner stays.
func TestMigrationV3ToV4_KeepsWhatReadersReturned(t *testing.T) {
	b := newTestEngine(t)
	n := separatelyStoredNode("test:current-wins")
	_, err := b.CreateNode(n)
	require.NoError(t, err)
	storeBeforeV4(t, b)
	stale, err := encodeEmbedding([]float32{5, 5})
	require.NoError(t, err)
	unread := binaryEmbeddingPrefix(n.ID)
	unread = append(unread, 0, 0, 0, 0)
	opaque := []byte{prefixEmbedding, 0xff}
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		for _, key := range [][]byte{legacyEmbeddingKey(n.ID, 0), unread, opaque} {
			if err := txn.Set(key, stale); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, b.migrateV3ToV4())
	b.storageVersion = storageVersionEmbeddingKeysV4
	require.Equal(t, n.ChunkEmbeddings, embeddingCleanupColdNode(t, b, n.ID).ChunkEmbeddings)
	require.Equal(t, 1, compatibilityFamilyKeyCount(t, b), "only the undecodable key remains")
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(opaque)
		return err
	}))
}

// A buffered commit that a pre-V4 binary logged addresses released keys.
// Decoding it for a V4 store applies each write and delete at the current
// address; a shard part moves only with its base. A pre-V4 store replays the
// keys unchanged.
func TestBufferedCommit_ReplaysCompatibilityKeysAtCurrentAddresses(t *testing.T) {
	id := NodeID("test:replayed")
	base := legacyEmbeddingKey(id, 0)
	meta := legacyEmbeddingKey(id, reservedEmbeddingMetaChunkIndex)
	lonePart := embeddingPartKey(legacyEmbeddingKey(id, 7), 0)
	data := WALBufferedCommitData{
		TxID: "pre-v4",
		Writes: map[string][]byte{
			string(base): []byte("base"), string(embeddingPartKey(base, 1)): []byte("part"),
			string(meta): []byte("meta"), string(lonePart): []byte("lone"),
			string(nodeKey(id)): []byte("body"),
		},
		Deletes: []string{string(legacyEmbeddingKey(id, 5)), string(embeddingPartKey(legacyEmbeddingKey(id, 5), 0)), string(nodeKey("test:other"))},
	}
	b := newTestEngine(t)
	c := data.bufferedCommit(b)
	require.Equal(t, []byte("base"), c.writes[string(embeddingKey(id, 0))])
	require.Equal(t, []byte("part"), c.writes[string(embeddingChunkPartKey(id, 0, 1))])
	require.Equal(t, []byte("meta"), c.writes[string(embeddingKey(id, reservedEmbeddingMetaChunkIndex))])
	require.Equal(t, []byte("body"), c.writes[string(nodeKey(id))])
	require.NotContains(t, c.writes, string(embeddingChunkPartKey(id, 7, 0)), "a part without its base is not this owner's part")
	for key := range c.writes {
		require.NotEqual(t, prefixEmbedding, key[0], "no released key is replayed into a V4 store")
	}
	require.True(t, c.deletes[string(embeddingKey(id, 5))])
	require.True(t, c.deletes[string(embeddingChunkPartKey(id, 5, 0))])
	require.True(t, c.deletes[string(nodeKey("test:other"))])
	for key := range c.deletes {
		require.NotEqual(t, prefixEmbedding, key[0], "no released key is deleted in a V4 store")
	}

	storeBeforeV4(t, b)
	c = data.bufferedCommit(b)
	require.Equal(t, data.Writes, c.writes, "a pre-V4 store replays the logged keys")
	require.True(t, c.deletes[string(legacyEmbeddingKey(id, 5))])
}

// A released shard part that is missing was never readable; the move copies
// the parts that exist and does not invent the missing one.
func TestMigrationV3ToV4_MissingShardPart(t *testing.T) {
	b := newTestEngine(t)
	sharded, kvs := r8SeparateReplacementNode(t, "test:released-missing-part", 0)
	_, err := b.CreateNode(sharded)
	require.NoError(t, err)
	storeBeforeV4(t, b)
	toReleasedEmbeddingKeys(t, b, sharded.ID)
	require.Greater(t, len(kvs), 2, "SETUP: base plus at least two parts")
	released := legacyEmbeddingKey(sharded.ID, 0)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(embeddingPartKey(released, 1))
	}))
	require.NoError(t, b.migrateV3ToV4())
	require.Zero(t, compatibilityFamilyKeyCount(t, b))
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(embeddingChunkPartKey(sharded.ID, 0, 0))
		require.NoError(t, err, "an existing part moves")
		_, err = txn.Get(embeddingChunkPartKey(sharded.ID, 0, 1))
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "a missing part stays missing")
		return nil
	}))
}

// A V3 store whose arm cannot complete stays at V3, whether open or Restore
// runs the arm: an undecodable owner body cannot prove its chunks orphaned.
func TestMigrationV3ToV4_FailureKeepsV3(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	live := separatelyStoredNode("test:corrupt-upgrade")
	_, err = b.CreateNode(live)
	require.NoError(t, err)
	storeBeforeV4(t, b)
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(nodeKey(live.ID), []byte{0xff})
	}))
	backup := filepath.Join(t.TempDir(), "corrupt.backup")
	require.NoError(t, b.Backup(backup))
	require.NoError(t, b.Close())

	_, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: true})
	require.ErrorContains(t, err, "migration v3→v4 failed")

	target := newTestEngine(t)
	require.Error(t, target.Restore(backup))
	version, err := readSchemaVersionFrom(heldViewer{db: target.db})
	require.NoError(t, err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, version)
}
