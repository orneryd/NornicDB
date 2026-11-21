//go:build closeout_l2_e33

package nornicdb

import (
	"context"
	"io/fs"
	"path/filepath"
	"sort"
	"testing"
	"time"

	l2e33cfg "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Report entry 33: a managed one-property update of a node with chunk vectors
// journals an entity_commit record holding the node's complete image, all
// chunk vectors included. The entry measured this through Bolt; Bolt hands the
// statement to the database manager's storage and the managed Cypher executor,
// which this test drives directly (the same commit path without the wire).

const (
	l2E33Chunks = 400  // the entry used 1,800 and 6,600; 400 keeps the test light
	l2E33Dims   = 1024 // as the entry
	l2E33Timed  = 3    // timed updates per case; the median growth is asserted
)

type l2E33Store struct {
	dir      string
	db       *DB
	manager  *multidb.DatabaseManager
	executor *cypher.StorageExecutor
	badger   *storage.BadgerEngine
	closed   bool
}

func l2E33Config() *Config {
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.StrictDurability = true
	cfg.Database.WALSyncMode = "immediate"
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Database.WALSnapshotInterval = time.Hour
	cfg.Memory.AutoLinksEnabled = false
	// As the entry's fixture: only `text` feeds the embedding text, labels do
	// not. Without this every property feeds it (the default policy), so the
	// timed SET n.flag changes the embedding source and the store correctly
	// drops the vectors (embedding_text_policy.go; db.go:1127 applies it).
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	cfg.EmbeddingWorker.IncludeLabels = false
	return cfg
}

func l2E33Open(t *testing.T, dir string) *l2E33Store {
	t.Helper()
	db, err := Open(dir, l2E33Config())
	require.NoError(t, err)
	s := &l2E33Store{dir: dir, db: db}
	t.Cleanup(func() { s.close(t) })
	manager, err := multidb.NewDatabaseManager(context.Background(), db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	s.manager = manager
	engine, err := manager.GetStorage(db.defaultDatabaseName())
	require.NoError(t, err)
	s.executor = cypher.NewStorageExecutor(engine)
	b, ok := storage.UnwrapEngine(db.GetBaseStorageForManager()).(*storage.BadgerEngine)
	require.True(t, ok, "base storage does not unwrap to the native engine")
	s.badger = b
	return s
}

func (s *l2E33Store) close(t *testing.T) {
	if s.closed {
		return
	}
	s.closed = true
	// db.Close closes the base storage; DatabaseManager.Close closes the same
	// engine (m.inner, multidb/manager.go:992), so it runs after the db and its
	// error (storage already closed) is ignored.
	require.NoError(t, s.db.Close())
	if s.manager != nil {
		_ = s.manager.Close()
	}
}

func (s *l2E33Store) run(t *testing.T, query string, params map[string]any) {
	t.Helper()
	_, err := s.executor.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
}

// l2E33JournalBytes is the total size of the files under <store>/wal, as the
// entry measured "journal per statement".
func l2E33JournalBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(filepath.Join(dir, "wal"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	require.NoError(t, err)
	return total
}

func l2E33Median(values []int64) int64 {
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// l2E33Growth runs query once per params entry and returns the journal growth
// of each statement.
func l2E33Growth(t *testing.T, s *l2E33Store, query string, params []map[string]any) []int64 {
	t.Helper()
	growth := make([]int64, 0, len(params))
	for _, p := range params {
		before := l2E33JournalBytes(t, s.dir)
		s.run(t, query, p)
		growth = append(growth, l2E33JournalBytes(t, s.dir)-before)
	}
	return growth
}

// l2E33AttachVectors writes the chunk vectors beside the Doc node on the native
// engine inside the journal engine, as the entry did, and returns the node ID.
func l2E33AttachVectors(t *testing.T, s *l2E33Store, chunks int) storage.NodeID {
	t.Helper()
	ctx := context.Background()
	nodes, err := s.badger.GetNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	node := nodes[0]
	vectors := make([][]float32, chunks)
	for i := range vectors {
		v := make([]float32, l2E33Dims)
		for j := range v {
			v[j] = 1 + float32(i*l2E33Dims+j+1)/3
		}
		vectors[i] = v
	}
	node.ChunkEmbeddings = vectors
	node.EmbedMeta = map[string]any{
		"has_embedding":        true,
		"chunk_count":          chunks,
		"embedding_dimensions": l2E33Dims,
		"embedding_model":      "synthetic",
	}
	require.NoError(t, s.badger.UpdateNodeEmbeddingSidecar(ctx, node))
	// Precondition: the full node read (BadgerEngine.GetNode loads the chunk
	// vectors from the sidecar) returns every vector just attached. Without
	// it the journal figure below would measure a node without vectors.
	stored, err := s.badger.GetNode(ctx, node.ID)
	require.NoError(t, err)
	if len(stored.ChunkEmbeddings) != chunks {
		t.Fatalf("harness: node has %d chunk vectors right after the attach, want %d (vectors not stored; no L2RESULT)", len(stored.ChunkEmbeddings), chunks)
	}
	return node.ID
}

func l2E33Parsers(t *testing.T, body func(t *testing.T)) {
	for _, parser := range []struct {
		name string
		set  func() func()
	}{{"nornic", l2e33cfg.WithNornicParser}, {"antlr", l2e33cfg.WithANTLRParser}} {
		t.Run(parser.name, func(t *testing.T) {
			restore := parser.set()
			defer restore()
			body(t)
		})
	}
}

// TestL2_e33_Repro: one-flag update of a node with 400 chunk vectors of 1,024
// values (1,638,400 bytes of vector data). Threshold: the median journal
// growth per update is a defect at >= 50 % of the vector bytes. The entry's
// defect journals about 102 % (9,222,424 B for 9,216,000 B of vectors at
// 1,800 chunks); a delta record needs about 1 KB (the small node update below
// journals 936 B), so 50 % separates the two by orders of magnitude.
func TestL2_e33_Repro(t *testing.T) {
	l2E33Parsers(t, func(t *testing.T) {
		s := l2E33Open(t, t.TempDir())
		s.run(t, "CREATE (:Doc {id: 'd', text: 'alpha', flag: 0})", nil)
		id := l2E33AttachVectors(t, s, l2E33Chunks)
		s.run(t, "MATCH (n:Doc {id: 'd'}) SET n.flag = $flag", map[string]any{"flag": int64(-1)})
		params := make([]map[string]any, l2E33Timed)
		for i := range params {
			params[i] = map[string]any{"flag": int64(i + 1)}
		}
		growth := l2E33Growth(t, s, "MATCH (n:Doc {id: 'd'}) SET n.flag = $flag", params)
		median := l2E33Median(growth)
		vectorBytes := int64(l2E33Chunks * l2E33Dims * 4)

		stored, err := s.badger.GetNode(context.Background(), id)
		require.NoError(t, err)
		chunksAfter := len(stored.ChunkEmbeddings)
		flagAfter := stored.Properties["flag"]

		ratio := float64(median) / float64(vectorBytes)
		if chunksAfter != l2E33Chunks {
			// The vectors were present after the attach (checked there) and
			// are gone after the one-flag SET, which does not touch `text`.
			t.Logf("L2RESULT E33 DEFECT vectors lost: %d of %d chunk vectors after the one-flag updates; journal growth %v (median %d B) for %d B of vector data; flag after %v",
				chunksAfter, l2E33Chunks, growth, median, vectorBytes, flagAfter)
			t.Errorf("node has %d chunk vectors after the update, want %d", chunksAfter, l2E33Chunks)
		} else if median*2 >= vectorBytes {
			t.Logf("L2RESULT E33 DEFECT journal growth per one-flag update %v (median %d B) for %d B of vector data (%.3f); chunks after %d, flag after %v",
				growth, median, vectorBytes, ratio, chunksAfter, flagAfter)
			t.Errorf("one-property update journaled %d B (median), >= 50%% of the %d B of unchanged chunk vectors", median, vectorBytes)
		} else {
			t.Logf("L2RESULT E33 OK journal growth per one-flag update %v (median %d B) for %d B of vector data (%.3f); chunks after %d, flag after %v",
				growth, median, vectorBytes, ratio, chunksAfter, flagAfter)
		}
		// The update itself must be correct either way.
		if flagAfter != int64(l2E33Timed) {
			t.Errorf("flag after the updates %#v, want %d", flagAfter, l2E33Timed)
		}
	})
}

// TestL2_e33_Control: the same managed path on nodes without vectors. The
// one-flag update of a vectorless node must journal under 64 KiB per statement
// on any build (the entry measured 416 B and 936 B). It also logs the small
// write figures of the entry's table (create node, update node).
func TestL2_e33_Control(t *testing.T) {
	l2E33Parsers(t, func(t *testing.T) {
		s := l2E33Open(t, t.TempDir())
		s.run(t, "CREATE (:Item {id: 'warm', name: 'n', v: 0})", nil)
		createParams := make([]map[string]any, 10)
		for i := range createParams {
			createParams[i] = map[string]any{"id": "i-" + string(rune('a'+i)), "v": int64(i)}
		}
		create := l2E33Growth(t, s, "CREATE (:Item {id: $id, name: 'n', v: $v})", createParams)

		s.run(t, "CREATE (:Item {id: 'x', name: 'n', v: 0})", nil)
		s.run(t, "MATCH (n:Item {id: 'x'}) SET n.v = -1", nil)
		updateParams := make([]map[string]any, 10)
		for i := range updateParams {
			updateParams[i] = map[string]any{"v": int64(i)}
		}
		update := l2E33Growth(t, s, "MATCH (n:Item {id: 'x'}) SET n.v = $v", updateParams)

		t.Logf("L2CONTROL E33 journal per statement: create node %v (median %d B), update node %v (median %d B)",
			create, l2E33Median(create), update, l2E33Median(update))
		require.Less(t, l2E33Median(update), int64(64<<10), "a one-property update of a vectorless node")
		require.Less(t, l2E33Median(create), int64(64<<10), "a one-node create")
	})
}

// TestL2_e33_Cause: after the timed updates, list the journal records of the
// last update (operation and data size). On the defect the update writes
// tx_begin, entity_commit and tx_commit, and the entity_commit data is about the
// size of the vector data: the complete node image built by the managed commit
// (pkg/storage/semantic_transaction_wire.go, update_node image with chunk
// embeddings and metadata).
func TestL2_e33_Cause(t *testing.T) {
	dir := t.TempDir()
	s := l2E33Open(t, dir)
	s.run(t, "CREATE (:Doc {id: 'd', text: 'alpha', flag: 0})", nil)
	l2E33AttachVectors(t, s, l2E33Chunks)
	s.run(t, "MATCH (n:Doc {id: 'd'}) SET n.flag = -1", nil)
	s.close(t)

	var lastSeq uint64
	require.NoError(t, storage.VisitWALEntriesAfterFromDir(context.Background(), filepath.Join(dir, "wal"), 0, func(e storage.WALEntry) error {
		lastSeq = e.Sequence
		return nil
	}))

	s = l2E33Open(t, dir)
	s.run(t, "MATCH (n:Doc {id: 'd'}) SET n.flag = 1", nil)
	s.close(t)

	type record struct {
		op   storage.OperationType
		size int
	}
	var records []record
	var entityBytes int
	require.NoError(t, storage.VisitWALEntriesAfterFromDir(context.Background(), filepath.Join(dir, "wal"), lastSeq, func(e storage.WALEntry) error {
		records = append(records, record{op: e.Operation, size: len(e.Data)})
		if e.Operation == storage.OpEntityCommit && len(e.Data) > entityBytes {
			entityBytes = len(e.Data)
		}
		return nil
	}))
	t.Logf("L2CAUSE E33 records journaled by one one-flag update (after seq %d, including any open/close records): %+v; largest entity_commit data %d B; vector data %d B",
		lastSeq, records, entityBytes, l2E33Chunks*l2E33Dims*4)
}
