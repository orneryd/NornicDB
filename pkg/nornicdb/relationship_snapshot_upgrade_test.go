package nornicdb

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type historicalRelationshipEntry struct {
	Source  uint64
	Vectors [][]float32
	Spans   []embeddingutil.ChunkSpan
}

type historicalRelationshipSnapshot struct {
	Format  int
	Entries map[string]*historicalRelationshipEntry
}

// The application owns persisted search generations outside Badger. Opening a
// V4 store must still migrate its old relationship snapshot when authorized.
func TestOpenUpgradesRelationshipSnapshotsOnExistingV4Store(t *testing.T) {
	for _, generation := range []bool{false, true} {
		name := "direct"
		if generation {
			name = "generation"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingEnabled = false
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.PersistSearchIndexes = false
			cfg.Database.WALAutoCompactionEnabled = false
			db, err := Open(dir, cfg)
			require.NoError(t, err)
			initial := db
			t.Cleanup(func() { _ = initial.Close() })
			_, err = db.GetStorage().CreateNode(&storage.Node{ID: "preserved", Labels: []string{"Preserved"}, Properties: map[string]any{"value": "original"}})
			require.NoError(t, err)
			require.NoError(t, db.Close())
			readStoredNode := func() []byte {
				t.Helper()
				store, err := fixtureowner.OwnResult[*badger.DB](t, badger.ErrDBClosed)(badger.Open(badger.DefaultOptions(dir).WithReadOnly(true).WithLogger(nil)))
				require.NoError(t, err)
				var value []byte
				require.NoError(t, store.View(func(tx *badger.Txn) error {
					item, err := tx.Get(append([]byte{1}, []byte("nornic:preserved")...))
					if err != nil {
						return err
					}
					value, err = item.ValueCopy(nil)
					return err
				}))
				require.NoError(t, store.Close())
				return value
			}
			beforeNode := readStoredNode()

			base := filepath.Join(dir, "search", "nornic")
			indexDir := base
			if generation {
				indexDir = filepath.Join(base, "g-0123456789abcdef")
			}
			require.NoError(t, os.MkdirAll(indexDir, 0o700))
			if generation {
				require.NoError(t, os.WriteFile(filepath.Join(base, "CURRENT"), []byte("g-0123456789abcdef\n"), 0o600))
			}
			path := filepath.Join(indexDir, "bm25.v2.relationships")
			want := historicalRelationshipSnapshot{Format: 2, Entries: map[string]*historicalRelationshipEntry{
				"r1": {Source: math.MaxUint64, Vectors: [][]float32{{math.Float32frombits(0x80000000), 1}}, Spans: []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 1, End: 5}}},
			}}
			var old bytes.Buffer
			require.NoError(t, gob.NewEncoder(&old).Encode(want))
			require.NoError(t, os.WriteFile(path, old.Bytes(), 0o600))

			cfg.Database.AllowStorageUpgrade = false
			db, err = Open(dir, cfg)
			if db != nil {
				unexpected := db
				t.Cleanup(func() { _ = unexpected.Close() })
			}
			require.Error(t, err, "an old snapshot must not be discarded when upgrade is not authorized")
			require.Nil(t, db)
			unchanged, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, old.Bytes(), unchanged)

			cfg.Database.AllowStorageUpgrade = true
			db, err = Open(dir, cfg)
			require.NoError(t, err)
			upgraded := db
			t.Cleanup(func() { _ = upgraded.Close() })
			converted, err := os.ReadFile(path)
			require.NoError(t, err)
			var got struct {
				Format  int
				Entries map[string]struct {
					Source  uint64
					Vectors [][]byte
					Spans   []embeddingutil.ChunkSpan
				}
			}
			require.NoError(t, msgpack.Unmarshal(converted, &got))
			require.Equal(t, want.Format, got.Format)
			require.Equal(t, want.Entries["r1"].Source, got.Entries["r1"].Source)
			require.Equal(t, want.Entries["r1"].Spans, got.Entries["r1"].Spans)
			require.Len(t, got.Entries["r1"].Vectors[0], 8)
			require.Equal(t, math.Float32bits(want.Entries["r1"].Vectors[0][0]), binary.LittleEndian.Uint32(got.Entries["r1"].Vectors[0][:4]))
			require.Equal(t, math.Float32bits(want.Entries["r1"].Vectors[0][1]), binary.LittleEndian.Uint32(got.Entries["r1"].Vectors[0][4:]))
			node, err := db.GetStorage().GetNode("preserved")
			require.NoError(t, err)
			require.Equal(t, "original", node.Properties["value"])
			require.NoError(t, db.Close())
			require.Equal(t, beforeNode, readStoredNode(), "search conversion must not rewrite the Badger node body")

			cfg.Database.AllowStorageUpgrade = false
			db, err = Open(dir, cfg)
			require.NoError(t, err)
			reopened := db
			t.Cleanup(func() { _ = reopened.Close() })
			require.NoError(t, db.Close())
			again, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, converted, again)
		})
	}
}

// A genuine pre-identity WAL fixture also exercises the V4 migration arm;
// search conversion must retain both recovered graph state and vector bytes.
func TestV4OpenUpgradesRelationshipSnapshotWithHistoricalWAL(t *testing.T) {
	dir := fixtureowner.ExtractZip(t, filepath.Join("..", "storage", "testdata", "wal-pre-identity", "release.zip"), "data/")
	indexDir := filepath.Join(dir, "search", "nornic")
	require.NoError(t, os.MkdirAll(indexDir, 0o700))
	path := filepath.Join(indexDir, "bm25.v2.relationships")
	old := historicalRelationshipSnapshot{Format: 2, Entries: map[string]*historicalRelationshipEntry{
		"edge": {Source: math.MaxUint64, Vectors: [][]float32{{math.Float32frombits(0x80000000), 1}}, Spans: []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: 1, End: 5}}},
	}}
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(old))
	require.NoError(t, os.WriteFile(path, encoded.Bytes(), 0o600))
	cfg := DefaultConfig()
	cfg.Database.AllowStorageUpgrade = true
	cfg.Database.StrictDurability = false
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Database.PersistSearchIndexes = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	first := db
	t.Cleanup(func() { _ = first.Close() })
	node, err := db.GetBaseStorageForManager().GetNode("upgrade:a")
	require.NoError(t, err)
	require.Equal(t, int64(20), node.Properties["value"])
	converted, err := os.ReadFile(path)
	require.NoError(t, err)
	var got struct {
		Format  int
		Entries map[string]struct {
			Source  uint64
			Vectors [][]byte
			Spans   []embeddingutil.ChunkSpan
		}
	}
	require.NoError(t, msgpack.Unmarshal(converted, &got))
	require.Equal(t, old.Format, got.Format)
	require.Equal(t, old.Entries["edge"].Source, got.Entries["edge"].Source)
	require.Equal(t, old.Entries["edge"].Spans, got.Entries["edge"].Spans)
	require.Len(t, got.Entries["edge"].Vectors[0], 8)
	require.Equal(t, uint32(0x80000000), binary.LittleEndian.Uint32(got.Entries["edge"].Vectors[0][:4]))
	require.NoError(t, db.Close())
	cfg.Database.AllowStorageUpgrade = false
	db, err = Open(dir, cfg)
	require.NoError(t, err)
	second := db
	t.Cleanup(func() { _ = second.Close() })
	node, err = db.GetBaseStorageForManager().GetNode("upgrade:a")
	require.NoError(t, err)
	require.Equal(t, int64(20), node.Properties["value"])
	require.NoError(t, db.Close())
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, converted, again)
}

func TestRelationshipSnapshotStartupReportsPostRenameMissingError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.DataDir = t.TempDir()
	dir := filepath.Join(cfg.Database.DataDir, "search", "nornic")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	path := filepath.Join(dir, "bm25.v2.relationships")
	var source bytes.Buffer
	require.NoError(t, gob.NewEncoder(&source).Encode(historicalRelationshipSnapshot{Format: 2}))
	require.NoError(t, os.WriteFile(path, source.Bytes(), 0o600))
	db := &DB{config: cfg}
	calls := 0
	err := db.upgradeRelationshipSnapshots(true, func(string) error {
		calls++
		return os.ErrNotExist
	})
	require.Equal(t, 1, calls)
	require.ErrorIs(t, err, os.ErrNotExist, "a failed durability step is not an absent source file")
	require.NoError(t, db.upgradeRelationshipSnapshots(true, nil))
}
