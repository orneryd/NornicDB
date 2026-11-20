package nornicdb

// Untracked probe (batch 1b, follow-up run 3 / entries 40 and 41): entry 40's
// situation inside a database, the database's own rebuild from its snapshot
// and journal, then the database's own embedding worker with a stand-in
// embedder.
//
// 1. nornicdb.Open(root); through the database's journal-backed base storage:
//    node nornic:n {text: "alpha"}; the worker's read; a client update with
//    A (2 vectors along axis 0, has_embedding, chunk_count 2); the worker's
//    result B (3 vectors along axis 1) stored beside the node
//    (UpdateNodeEmbeddingSidecar).
// 2. The journal wrapper's automatic snapshot into <root>/snapshots (seam
//    EI1bAutoSnapshot: createSnapshotAndCompact, which also truncates the
//    journal); close the database.
// 3. Make the primary store unopenable (overwrite its Badger MANIFEST) and
//    reopen with NORNICDB_AUTO_RECOVER_ON_CORRUPTION=1: the database rebuilds
//    a fresh store from <root>/snapshots and <root>/wal by itself
//    (recoverBadgerFromSnapshotAndWAL).
// 4. Read nornic:n; create a control node {text: "beta"} through the database;
//    install the stand-in embedder (vectors along axis 2) with db.SetEmbedder,
//    which starts the database's embedding worker; wait up to 30 s for the
//    control node to be embedded, then read both nodes.
// Identical file on both builds (API differences in zz_ei1b_e40_adapt_test.go).

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func zzEI1bDBConfig() *Config {
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = zzEI1bDims
	cfg.EmbeddingWorker.PropertiesInclude = []string{"text"}
	cfg.EmbeddingWorker.IncludeLabels = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	return cfg
}

func TestZZEI1bE40DatabaseWorker(t *testing.T) {
	A, B := zzEI1bAxis(0, 2), zzEI1bAxis(1, 3)
	root := t.TempDir()
	db, err := Open(root, zzEI1bDBConfig())
	require.NoError(t, err)
	base, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
	if !ok {
		_ = db.Close()
		t.Fatalf("base storage is %T, not *storage.WALEngine", db.GetBaseStorageForManager())
	}
	require.NoError(t, ei1bCreate(base, &storage.Node{ID: "nornic:n", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}}))
	workerView, err := ei1bGet(base, "nornic:n")
	require.NoError(t, err)
	workerView = storage.CopyNode(workerView)
	client, err := ei1bGet(base, "nornic:n")
	require.NoError(t, err)
	client = storage.CopyNode(client)
	client.ChunkEmbeddings = A
	client.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 2, "embedding_dimensions": zzEI1bDims, "embedding_model": "synthetic"}
	require.NoError(t, ei1bUpdate(base, client))
	workerView.ChunkEmbeddings = B
	workerView.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 3, "embedding_dimensions": zzEI1bDims, "embedding_model": "synthetic"}
	werr := ei1bSidecar(base, workerView)
	live, err := ei1bGet(base, "nornic:n")
	require.NoError(t, err)
	sdir := filepath.Join(root, "snapshots")
	require.NoError(t, os.MkdirAll(sdir, 0o755))
	require.NoError(t, storage.EI1bAutoSnapshot(base, sdir))
	snaps, _ := filepath.Glob(filepath.Join(sdir, "*"))
	t.Logf("E40DB before: workerErr=%v live=%s liveMeta=%v snapshots=%d", werr, zzEI1bAxisOf(live.ChunkEmbeddings), live.EmbedMeta, len(snaps))
	require.NoError(t, db.Close())

	// Make the primary store unopenable.
	var corrupted []string
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Name() != "MANIFEST" {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if filepath.Dir(rel) == "wal" || filepath.Dir(rel) == "snapshots" {
			return nil
		}
		if os.WriteFile(path, []byte("not a manifest"), 0o600) == nil {
			corrupted = append(corrupted, rel)
		}
		return nil
	})
	t.Logf("E40DB corrupted MANIFEST files: %v", corrupted)
	t.Setenv("NORNICDB_AUTO_RECOVER_ON_CORRUPTION", "1")
	// The reopen enables embedding (as the package's own worker tests open
	// their database), so the database's embedding worker runs once an
	// embedder is installed; the first open had it off, so nothing could be
	// embedded before the snapshot.
	cfg2 := zzEI1bDBConfig()
	cfg2.Memory.EmbeddingEnabled = true
	db2, err := Open(root, cfg2)
	if err != nil {
		t.Logf("E40DB reopen error: %v", err)
		return
	}
	defer db2.Close()
	db2.embedWorkerConfig.BatchDelay = 0
	db2.embedWorkerConfig.ScanInterval = time.Hour
	base2 := db2.GetBaseStorageForManager()
	got, err := ei1bGet(base2, "nornic:n")
	require.NoError(t, err)
	t.Logf("E40DB rebuilt by the database: test node=%s meta=%v NodeNeedsEmbedding=%v", zzEI1bAxisOf(got.ChunkEmbeddings), got.EmbedMeta, storage.NodeNeedsEmbedding(got))

	ctrl, err := db2.CreateNode(context.Background(), []string{"Doc"}, map[string]interface{}{"text": "beta"})
	require.NoError(t, err)
	ctrlID := storage.NodeID("nornic:" + string(ctrl.ID))
	emb := &zzEI1bEmbedder{}
	accepted := db2.SetEmbedder(emb)
	_, trigErr := db2.EmbedExisting(context.Background())
	t.Logf("E40DB SetEmbedder accepted=%v; EmbedExisting err=%v; control node id %s", accepted, trigErr, ctrlID)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, _ := ei1bGet(base2, ctrlID)
		if c == nil {
			c, _ = ei1bGet(base2, storage.NodeID(ctrl.ID))
		}
		if c != nil && len(c.ChunkEmbeddings) > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // let the worker reach the other node too
	c, _ := ei1bGet(base2, ctrlID)
	if c == nil {
		c, _ = ei1bGet(base2, storage.NodeID(ctrl.ID))
	}
	ctrlVec := "missing"
	if c != nil {
		ctrlVec = zzEI1bAxisOf(c.ChunkEmbeddings)
	}
	after, err := ei1bGet(base2, "nornic:n")
	require.NoError(t, err)
	stats := db2.EmbedQueueStats()
	t.Logf("E40DB after the worker: embedder calls=%d queue stats=%+v; control node=%s; test node=%s meta=%v NodeNeedsEmbedding=%v",
		emb.calls, stats, ctrlVec, zzEI1bAxisOf(after.ChunkEmbeddings), after.EmbedMeta, storage.NodeNeedsEmbedding(after))
}
