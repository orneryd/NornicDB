//go:build closeout_l2_e53_5

package nornicdb_test

// Report entry 53.5: a store rebuilt from its periodic snapshot plus a short
// journal returns a worker-embedded node without its vectors.
//
// The node is embedded by the embedding worker (vectors stored beside the
// node), a periodic snapshot completes after that, automatic snapshots are
// then stopped, the store is closed, its MANIFEST is damaged, and the next
// Open rebuilds the store from the snapshot plus the journal (automatic
// recovery, on by default). Expected: the rebuilt node keeps its vectors, or
// at least its embedding metadata.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const l2E53_5Dims = 1024

// l2E53_5Provider is the stub provider of entry 53.4 with 1,024 dimensions:
// EmbedDocumentChunks returns `chunks` vectors whose values change with
// every call, so each set the worker stores is distinguishable.
type l2E53_5Provider struct {
	chunks int
	calls  atomic.Int32
}

func (p *l2E53_5Provider) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, l2E53_5Dims)
	v[0] = 1
	return v, nil
}

func (p *l2E53_5Provider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = p.Embed(ctx, texts[i])
	}
	return out, nil
}

func (p *l2E53_5Provider) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (p *l2E53_5Provider) Dimensions() int { return l2E53_5Dims }
func (p *l2E53_5Provider) Model() string   { return "l2-e53-5-stub" }
func (p *l2E53_5Provider) Backend() string { return "cpu" }

// l2E53_5First is the first value of the first vector of provider call `call`.
func l2E53_5First(call int) float32 { return float32(call*1000 + 1) }

func (p *l2E53_5Provider) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	call := int(p.calls.Add(1))
	result := &embed.DocumentChunkResult{Chunks: make([]string, p.chunks), Embeddings: make([][]float32, p.chunks), Model: p.Model()}
	for i := range result.Chunks {
		result.Chunks[i] = "chunk"
		v := make([]float32, l2E53_5Dims)
		for j := range v {
			// Largest value 6,600*1,024 + a few thousand, below 2^24: exact in float32.
			v[j] = float32(call*1000 + i*l2E53_5Dims + j + 1)
		}
		result.Embeddings[i] = v
	}
	return result, nil
}

var _ embed.Embedder = (*l2E53_5Provider)(nil)

// l2E53_5Config is step 1 of the entry: synchronous strict writes, journal
// sync immediate, automatic compaction every 200 ms, and the embedding
// settings of entry 53.4 step 1 with 1,024 dimensions.
func l2E53_5Config() *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.StrictDurability = true
	c.Database.WALSyncMode = "immediate"
	c.Database.WALAutoCompactionEnabled = true
	c.Database.WALSnapshotInterval = 200 * time.Millisecond
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = true
	c.Memory.EmbeddingDimensions = l2E53_5Dims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 0
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	return c
}

type l2E53_5Store struct {
	t    *testing.T
	root string
	db   *nornicdb.DB
}

func l2E53_5Open(t *testing.T, root string) *l2E53_5Store {
	t.Helper()
	db, err := nornicdb.Open(root, l2E53_5Config())
	require.NoError(t, err, "open %s", root)
	s := &l2E53_5Store{t: t, root: root, db: db}
	t.Cleanup(s.close)
	return s
}

func (s *l2E53_5Store) close() {
	if s.db == nil {
		return
	}
	db := s.db
	s.db = nil
	require.NoError(s.t, db.Close())
}

func (s *l2E53_5Store) exec(query string, params map[string]interface{}) {
	s.t.Helper()
	_, err := s.db.ExecuteCypher(context.Background(), query, params)
	require.NoError(s.t, err, query)
}

// node reads the Doc node with key 'd' in full (vectors included).
func (s *l2E53_5Store) node() *storage.Node {
	s.t.Helper()
	nodes, err := s.db.GetStorage().GetNodesByLabel(context.Background(), "Doc")
	require.NoError(s.t, err)
	for _, node := range nodes {
		if node.Properties["key"] == "d" {
			full, err := s.db.GetStorage().GetNode(context.Background(), node.ID)
			require.NoError(s.t, err)
			return full
		}
	}
	s.t.Fatalf("no Doc node with key 'd'")
	return nil
}

// setProvider is entry 53.4 step 2: the provider, then the search indexes.
func (s *l2E53_5Store) setProvider(p *l2E53_5Provider) {
	s.t.Helper()
	s.db.SetEmbedder(p)
	_, err := s.db.EnsureSearchIndexes(context.Background(), "nornic", nil, nornicdb.SearchIndexBuildWait)
	require.NoError(s.t, err)
}

// awaitVectors polls (bounded by limit) until the node holds p.chunks vectors
// whose first value is not `stale`. It reports whether that happened.
func (s *l2E53_5Store) awaitVectors(p *l2E53_5Provider, stale float32, limit time.Duration) bool {
	s.t.Helper()
	deadline := time.Now().Add(limit)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = s.db.WaitForEmbeddings(ctx)
		cancel()
		n := s.node()
		if len(n.ChunkEmbeddings) == p.chunks && len(n.ChunkEmbeddings[0]) > 0 && n.ChunkEmbeddings[0][0] != stale {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *l2E53_5Store) walEngine() *storage.WALEngine {
	s.t.Helper()
	w, ok := s.db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(s.t, ok, "the database's base storage is journaled")
	return w
}

// snapshot is step 3: let a periodic snapshot complete after everything
// written so far (a :_Tick marker written last must be in the newest
// snapshot file; another tick is written after every snapshot that lacks
// it), then DisableAutoCompaction. Returns the newest snapshot's path.
func (s *l2E53_5Store) snapshot() string {
	s.t.Helper()
	w := s.walEngine()
	marker := fmt.Sprintf("l2-e53-5-%d", time.Now().UnixNano())
	written, _ := w.GetSnapshotStats()
	s.exec("CREATE (:_Tick {key:$key})", map[string]interface{}{"key": marker})
	deadline := time.Now().Add(time.Minute)
	for {
		if path, err := l2E53_5NewestSnapshot(filepath.Join(s.root, "snapshots")); err == nil {
			if snap, err := storage.LoadSnapshot(context.Background(), path); err == nil && l2E53_5SnapNode(snap, "_Tick", marker) != nil {
				break
			}
		}
		if taken, _ := w.GetSnapshotStats(); taken > written {
			written = taken
			s.exec("CREATE (:_Tick)", nil)
		}
		require.True(s.t, time.Now().Before(deadline), "setup: no periodic snapshot holds the marker")
		time.Sleep(20 * time.Millisecond)
	}
	w.DisableAutoCompaction()
	path, err := l2E53_5NewestSnapshot(filepath.Join(s.root, "snapshots"))
	require.NoError(s.t, err)
	snap, err := storage.LoadSnapshot(context.Background(), path)
	require.NoError(s.t, err)
	require.NotNil(s.t, l2E53_5SnapNode(snap, "_Tick", marker), "setup: the newest snapshot holds the marker")
	return path
}

// l2E53_5NewestSnapshot is the snapshot file automatic recovery starts
// from: the newest snapshot-*.json by modification time.
func l2E53_5NewestSnapshot(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	newest, newestTime := "", time.Time{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if newest == "" || info.ModTime().After(newestTime) {
			newest, newestTime = filepath.Join(dir, name), info.ModTime()
		}
	}
	if newest == "" {
		return "", fmt.Errorf("no snapshot in %s", dir)
	}
	return newest, nil
}

func l2E53_5SnapNode(snap *storage.Snapshot, label string, key any) *storage.Node {
	for _, node := range snap.Nodes {
		for _, held := range node.Labels {
			if held == label && node.Properties["key"] == key {
				return node
			}
		}
	}
	return nil
}

func l2E53_5Describe(n *storage.Node) string {
	if n == nil {
		return "absent"
	}
	first := "none"
	if len(n.ChunkEmbeddings) > 0 && len(n.ChunkEmbeddings[0]) > 0 {
		first = fmt.Sprint(n.ChunkEmbeddings[0][0])
	}
	return fmt.Sprintf("chunks=%d first=%s meta=%v needs_embedding=%t props=%v",
		len(n.ChunkEmbeddings), first, n.EmbedMeta, storage.NodeNeedsEmbedding(n), n.Properties)
}

func l2E53_5JournalOps(t *testing.T, walDir string) string {
	t.Helper()
	entries, err := storage.ReadWALEntriesFromDir(context.Background(), walDir)
	if err != nil {
		return "read error: " + err.Error()
	}
	ops := make([]string, 0, len(entries))
	for _, e := range entries {
		ops = append(ops, fmt.Sprintf("%d:%s(%dB)", e.Sequence, e.Operation, len(e.Data)))
	}
	return fmt.Sprintf("%d entries %v", len(entries), ops)
}

// l2E53_5Seed runs steps 1-3: open, provider and indexes, CREATE the Doc
// node, wait until the worker stored all its vectors, let a periodic
// snapshot complete, stop snapshots. It returns the open store, the first
// vector value the worker stored, and the newest snapshot path.
func l2E53_5Seed(t *testing.T, chunks int) (*l2E53_5Store, float32, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "store") // the preserved sibling stays inside TempDir
	s := l2E53_5Open(t, root)
	p := &l2E53_5Provider{chunks: chunks}
	s.setProvider(p)
	s.exec("CREATE (:Doc {key:'d', text:'a long document'})", nil)
	if !s.awaitVectors(p, -1, 2*time.Minute) {
		t.Fatalf("setup (not the entry's defect): the worker stored no %d vectors within 2 min; node %s, provider calls %d",
			chunks, l2E53_5Describe(s.node()), p.calls.Load())
	}
	n := s.node()
	first := n.ChunkEmbeddings[0][0]
	t.Logf("step 2: worker stored the node's vectors: %s (provider calls %d)", l2E53_5Describe(n), p.calls.Load())
	snapPath := s.snapshot()
	t.Logf("step 3: periodic snapshot %s completed after the embedding; automatic compaction stopped", filepath.Base(snapPath))
	return s, first, snapPath
}

// l2E53_5Damage is step 5: MANIFEST overwritten, then Open; start-up moves the
// damaged store aside and rebuilds it from the snapshot plus the journal.
func l2E53_5Damage(t *testing.T, root string) *l2E53_5Store {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(root, "MANIFEST"), []byte("damaged"), 0o644))
	s := l2E53_5Open(t, root)
	preserved, err := filepath.Glob(root + ".corrupted-*")
	require.NoError(t, err)
	require.Len(t, preserved, 1, "setup: the start rebuilt the store and kept the damaged one aside")
	if manifest, err := os.ReadFile(filepath.Join(preserved[0], "recovery-manifest.json")); err == nil {
		t.Logf("recovery manifest: %s", manifest)
	}
	return s
}

func TestL2_e53_5_Repro(t *testing.T) {
	for _, chunks := range []int{1800, 6600} {
		t.Run(fmt.Sprintf("chunks=%d", chunks), func(t *testing.T) {
			s, first, _ := l2E53_5Seed(t, chunks)
			root := s.root
			s.close()
			t.Logf("journal after step 3: %s", l2E53_5JournalOps(t, filepath.Join(root, "wal")))

			// Step 4: ordinary restart (precondition of the entry: the node has its vectors).
			restarted := l2E53_5Open(t, root)
			n := restarted.node()
			step4 := l2E53_5Describe(n)
			restarted.close()
			require.True(t, len(n.ChunkEmbeddings) == chunks && n.ChunkEmbeddings[0][0] == first,
				"precondition (step 4, ordinary restart) failed: %s", step4)

			// Step 5: damaged MANIFEST, rebuild from snapshot plus journal.
			rebuilt := l2E53_5Damage(t, root)
			n = rebuilt.node()
			step5 := l2E53_5Describe(n)
			vectorsKept := len(n.ChunkEmbeddings) == chunks && len(n.ChunkEmbeddings[0]) > 0 && n.ChunkEmbeddings[0][0] == first
			metaKept := fmt.Sprint(n.EmbedMeta["has_embedding"]) == "true" || n.EmbedMeta["chunk_count"] != nil

			// Step 6 (information): the provider again, up to 90 s for re-embedding.
			reembed := "not run (vectors kept)"
			if !vectorsKept {
				p := &l2E53_5Provider{chunks: chunks}
				start := time.Now()
				rebuilt.setProvider(p)
				ok := rebuilt.awaitVectors(p, -1, 90*time.Second)
				reembed = fmt.Sprintf("re-embedded=%t after %s, provider calls %d", ok, time.Since(start).Round(time.Millisecond), p.calls.Load())
			}

			// OK: the rebuilt node keeps its vectors, or at least its embedding
			// metadata (the entry's Expected). DEFECT: no vectors and no metadata.
			if !vectorsKept && !metaKept {
				t.Logf("L2RESULT E53.5 DEFECT chunks=%d step4={%s} step5={%s} step6={%s}", chunks, step4, step5, reembed)
				t.Errorf("the rebuilt node lost its %d chunk vectors and its embedding metadata: %s", chunks, step5)
				return
			}
			t.Logf("L2RESULT E53.5 OK chunks=%d vectors_kept=%t metadata_kept=%t step4={%s} step5={%s} step6={%s}",
				chunks, vectorsKept, metaKept, step4, step5, reembed)
		})
	}
}

// Control: the same store and embedding, ordinary restart only (step 4):
// the node keeps all its vectors and needs no embedding.
func TestL2_e53_5_Control(t *testing.T) {
	const chunks = 1800
	s, first, _ := l2E53_5Seed(t, chunks)
	root := s.root
	s.close()
	restarted := l2E53_5Open(t, root)
	n := restarted.node()
	t.Logf("control: ordinary restart: %s", l2E53_5Describe(n))
	require.Len(t, n.ChunkEmbeddings, chunks, "ordinary restart keeps the worker's vectors")
	require.Equal(t, first, n.ChunkEmbeddings[0][0])
	require.False(t, storage.NodeNeedsEmbedding(n), "ordinary restart: node does not need embedding")
}

// Cause: where the vectors are lost. Three observations on the same store:
// (1) the Doc node in the snapshot file recovery starts from; (2) a seam that
// calls storage.RecoverIntoEngine directly on copies of that snapshot and the
// journal into a fresh engine (bypassing nornicdb.Open's recovery
// orchestration); (3) the node after the real damaged-store Open. The first
// of them that lacks the vectors names the step that drops them.
func TestL2_e53_5_Cause(t *testing.T) {
	const chunks = 1800
	ctx := context.Background()
	s, first, snapPath := l2E53_5Seed(t, chunks)
	root := s.root
	s.close()

	snap, err := storage.LoadSnapshot(ctx, snapPath)
	require.NoError(t, err)
	inSnapshot := l2E53_5SnapNode(snap, "Doc", "d")
	t.Logf("cause (1) snapshot %s, sequence %d: Doc node %s", filepath.Base(snapPath), snap.Sequence, l2E53_5Describe(inSnapshot))
	t.Logf("cause journal: %s", l2E53_5JournalOps(t, filepath.Join(root, "wal")))

	// (2) Seam: RecoverIntoEngine on copies, into a fresh engine.
	copyDir := t.TempDir()
	require.NoError(t, os.CopyFS(filepath.Join(copyDir, "wal"), os.DirFS(filepath.Join(root, "wal"))))
	require.NoError(t, os.CopyFS(filepath.Join(copyDir, "snapshots"), os.DirFS(filepath.Join(root, "snapshots"))))
	dest, err := storage.NewBadgerEngine(filepath.Join(t.TempDir(), "dest"))
	require.NoError(t, err)
	replay, status, recErr := storage.RecoverIntoEngine(ctx, dest, filepath.Join(copyDir, "wal"), filepath.Join(copyDir, "snapshots", filepath.Base(snapPath)))
	t.Logf("cause (2) RecoverIntoEngine: applied=%d skipped=%d failed=%d snapshot_nodes=%d wal_entries=%d err=%v",
		replay.Applied, replay.Skipped, replay.Failed, status.SnapshotNodes, status.WALEntries, recErr)
	var seamNode *storage.Node
	if recErr == nil {
		ns := storage.NewNamespacedEngine(dest, "nornic")
		docs, err := ns.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		for _, d := range docs {
			if d.Properties["key"] == "d" {
				seamNode, err = ns.GetNode(ctx, d.ID)
				require.NoError(t, err)
			}
		}
	}
	t.Logf("cause (2) node in the engine RecoverIntoEngine filled: %s", l2E53_5Describe(seamNode))
	require.NoError(t, dest.Close())

	// (3) The real rebuild.
	rebuilt := l2E53_5Damage(t, root)
	rebuiltNode := rebuilt.node()
	t.Logf("cause (3) node after the damaged-store Open: %s", l2E53_5Describe(rebuiltNode))

	has := func(n *storage.Node) bool {
		return n != nil && len(n.ChunkEmbeddings) == chunks && len(n.ChunkEmbeddings[0]) > 0 && n.ChunkEmbeddings[0][0] == first
	}
	switch {
	case !has(inSnapshot):
		t.Logf("L2CAUSE E53.5 snapshot-omits-vectors: the periodic snapshot's Doc node is {%s} (stored node had %d chunks); owner: snapshot write (WALEngine compaction -> storedSnapshotSource.StreamNodes / snapshotNodeVectorMetadata)", l2E53_5Describe(inSnapshot), chunks)
	case !has(seamNode):
		t.Logf("L2CAUSE E53.5 recovery-drops-vectors: snapshot holds them, RecoverIntoEngine's result does not (snapshot apply or journal replay after it)")
	case !has(rebuiltNode):
		t.Logf("L2CAUSE E53.5 open-drops-vectors: RecoverIntoEngine keeps them, nornicdb.Open's rebuild path does not (recoverBadgerFromSnapshotAndWAL or a later start-up step)")
	default:
		t.Logf("L2CAUSE E53.5 none: vectors present at every step")
	}
}
