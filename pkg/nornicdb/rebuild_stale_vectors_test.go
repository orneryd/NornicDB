package nornicdb_test

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

// A store whose files are damaged is rebuilt at start from the latest periodic
// snapshot plus the journal written after it (automatic recovery, on by
// default). The periodic snapshot keeps the vectors stored inside a node's
// record (WITH EMBEDDING stores them there), and the worker's vectors of a
// node updated after the worker embedded it; it leaves out the vectors of a
// node the worker embedded and nothing updated since. "Regenerate
// embeddings", the worker's write-back and an embedding reset change a
// node's vectors without a journal record that carries the new ones. In the
// sequences below the rebuilt node must not present the snapshot's older
// vectors as current: where the journal does not hold the vectors the node
// had at its end, the node comes back without vectors and needing embedding,
// and the worker embeds it again. Every sequence makes its changes one after
// another; none runs a clear at the same time as another write of the node.

const rsvDims = 16

type rsvProvider struct {
	chunks int
	calls  atomic.Int32
	fail   atomic.Bool
}

func (p *rsvProvider) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, rsvDims)
	v[0] = 1
	return v, nil
}

func (p *rsvProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = p.Embed(ctx, texts[i])
	}
	return out, nil
}

func (p *rsvProvider) ChunkText(text string, _, _ int) ([]string, error) { return []string{text}, nil }
func (p *rsvProvider) Dimensions() int                                   { return rsvDims }
func (p *rsvProvider) Model() string                                     { return "rebuild-stub" }
func (p *rsvProvider) Backend() string                                   { return "cpu" }

// EmbedDocumentChunks returns a different set of vectors on every call, so
// each embedding the worker stores is distinguishable.
func (p *rsvProvider) EmbedDocumentChunks(context.Context, string, int, int) (*embed.DocumentChunkResult, error) {
	if p.fail.Load() {
		return nil, &embed.ProviderError{Provider: "rebuild-stub", StatusCode: 400, Body: "rejected input"}
	}
	call := int(p.calls.Add(1))
	result := &embed.DocumentChunkResult{Chunks: make([]string, p.chunks), Embeddings: make([][]float32, p.chunks), Model: p.Model()}
	for i := range result.Chunks {
		result.Chunks[i] = "chunk"
		v := make([]float32, rsvDims)
		for j := range v {
			v[j] = float32(call*1000 + i*rsvDims + j + 1)
		}
		result.Embeddings[i] = v
	}
	return result, nil
}

var _ embed.Embedder = (*rsvProvider)(nil)

func rsvConfig() *nornicdb.Config {
	c := nornicdb.DefaultConfig()
	c.Database.AsyncWritesEnabled = false
	c.Database.WALAutoCompactionEnabled = true
	c.Database.WALSnapshotInterval = 50 * time.Millisecond
	c.Memory.DecayEnabled = false
	c.Memory.AutoLinksEnabled = false
	c.Memory.EmbeddingEnabled = true
	c.Memory.EmbeddingDimensions = rsvDims
	c.EmbeddingWorker.PropertiesInclude = []string{"text"}
	c.EmbeddingWorker.IncludeLabels = false
	c.EmbeddingWorker.NumWorkers = 1
	c.EmbeddingWorker.MaxRetries = 0
	c.EmbeddingWorker.BatchDelay = 0
	c.EmbeddingWorker.TriggerDebounceDelay = 0
	return c
}

type rsvLab struct {
	t    *testing.T
	root string
	db   *nornicdb.DB
}

// rsvOpen opens the server's database at root. Its embedding worker has no
// provider until setProvider, as when a server starts before its model loads.
func rsvOpen(t *testing.T, root string) *rsvLab {
	t.Helper()
	db, err := nornicdb.Open(root, rsvConfig())
	require.NoError(t, err)
	lab := &rsvLab{t: t, root: root, db: db}
	t.Cleanup(lab.close)
	return lab
}

func (l *rsvLab) close() {
	if l.db == nil {
		return
	}
	require.NoError(l.t, l.db.Close())
	l.db = nil
}

func (l *rsvLab) exec(query string, params map[string]any) {
	l.t.Helper()
	_, err := l.db.ExecuteCypher(context.Background(), query, params)
	require.NoError(l.t, err, query)
}

func (l *rsvLab) node(key string) *storage.Node {
	l.t.Helper()
	nodes, err := l.db.GetStorage().GetNodesByLabel(context.Background(), "Doc")
	require.NoError(l.t, err)
	for _, node := range nodes {
		if node.Properties["key"] == key {
			full, err := l.db.GetStorage().GetNode(context.Background(), node.ID)
			require.NoError(l.t, err)
			return full
		}
	}
	l.t.Fatalf("no Doc node with key %q", key)
	return nil
}

func (l *rsvLab) setProvider(provider *rsvProvider) {
	l.t.Helper()
	l.db.SetEmbedder(provider)
	_, err := l.db.EnsureSearchIndexes(context.Background(), "nornic", nil, nornicdb.SearchIndexBuildWait)
	require.NoError(l.t, err)
}

func rsvPrint(vectors [][]float32) string {
	if len(vectors) == 0 || len(vectors[0]) == 0 {
		return "none"
	}
	last := vectors[len(vectors)-1]
	return fmt.Sprintf("chunks=%d first=%v last=%v", len(vectors), vectors[0][0], last[len(last)-1])
}

// awaitVectors waits until the node holds a full set of provider vectors
// other than previous and returns their print.
func (l *rsvLab) awaitVectors(provider *rsvProvider, key, previous string) string {
	l.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = l.db.WaitForEmbeddings(ctx)
		cancel()
		node := l.node(key)
		got := rsvPrint(node.ChunkEmbeddings)
		if len(node.ChunkEmbeddings) == provider.chunks && got != previous {
			return got
		}
		require.True(l.t, time.Now().Before(deadline), "the worker stored no new vectors; node %s has %s", key, got)
		time.Sleep(50 * time.Millisecond)
	}
}

// awaitFailureMarker waits until the worker has parked the node as failed.
func (l *rsvLab) awaitFailureMarker(key string) {
	l.t.Helper()
	id := string(l.node(key).ID)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		failures, err := l.db.EmbeddingFailures(context.Background(), 0)
		require.NoError(l.t, err)
		for _, failure := range failures {
			if strings.HasSuffix(string(failure.NodeID), id) {
				require.Empty(l.t, l.node(key).ChunkEmbeddings)
				return
			}
		}
		require.True(l.t, time.Now().Before(deadline), "the worker parked no failure for node %s", key)
		time.Sleep(50 * time.Millisecond)
	}
}

// snapshot establishes the periodic snapshot that a rebuild of this store
// starts from, and stops further snapshots: what follows is in the journal
// only, as for any change in the minutes between two snapshots. That
// snapshot must hold everything written so far. A periodic snapshot reads
// the store as it was when it began, and one may already be under way when
// the last write commits, so a completed snapshot alone proves nothing. The
// fixture writes a marker node after everything else and waits until the
// newest completed snapshot file holds the marker. Such a snapshot may
// complete at any time after the marker commits, even before the write
// returns, so every pass looks at the newest file first, whatever the
// snapshot count did. A periodic snapshot is skipped while the journal holds
// nothing new since the last one, so when a snapshot has completed since a
// write of the fixture began and the newest file still lacks the marker, the
// fixture writes again; the count it compares with is taken before each
// write. After stopping snapshots it checks the newest snapshot file, the one
// recovery starts from: it holds the marker and every Doc node with the
// properties the store holds now.
func (l *rsvLab) snapshot() {
	l.t.Helper()
	walEngine := l.walEngine()
	marker := fmt.Sprintf("snapshot-%d", time.Now().UnixNano())
	// Snapshots completed before the marker's write began.
	written, _ := walEngine.GetSnapshotStats()
	l.exec("CREATE (:_Tick {key:$key})", map[string]any{"key": marker}) // internal label: never embedded
	deadline := time.Now().Add(time.Minute)
	for {
		if snapshot, err := l.newestSnapshot(); err == nil && rsvHoldsMarker(snapshot, marker) {
			break
		}
		if taken, _ := walEngine.GetSnapshotStats(); taken > written {
			written = taken
			l.exec("CREATE (:_Tick)", nil)
		}
		require.True(l.t, time.Now().Before(deadline), "no periodic snapshot holds the marker written after the fixture's writes")
		time.Sleep(20 * time.Millisecond)
	}
	walEngine.DisableAutoCompaction()

	snapshot, err := l.newestSnapshot()
	require.NoError(l.t, err)
	require.True(l.t, rsvHoldsMarker(snapshot, marker), "the snapshot a rebuild starts from holds the marker")
	nodes, err := l.db.GetStorage().GetNodesByLabel(context.Background(), "Doc")
	require.NoError(l.t, err)
	for _, live := range nodes {
		held := rsvSnapshotDoc(snapshot, live.Properties["key"])
		require.NotNil(l.t, held, "the snapshot a rebuild starts from holds Doc %v", live.Properties["key"])
		for name, value := range live.Properties {
			require.Equal(l.t, fmt.Sprint(value), fmt.Sprint(held.Properties[name]), "Doc %v, property %s in the snapshot", live.Properties["key"], name)
		}
	}
}

// newestSnapshot loads the snapshot file a rebuild would start from: the
// newest by modification time, the rule automatic recovery applies.
func (l *rsvLab) newestSnapshot() (*storage.Snapshot, error) {
	dir := filepath.Join(l.root, "snapshots")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("no snapshot in %s", dir)
	}
	return storage.LoadSnapshot(context.Background(), newest)
}

func rsvHoldsMarker(snapshot *storage.Snapshot, marker string) bool {
	return rsvSnapshotNode(snapshot, "_Tick", marker) != nil
}

func rsvSnapshotDoc(snapshot *storage.Snapshot, key any) *storage.Node {
	return rsvSnapshotNode(snapshot, "Doc", key)
}

func rsvSnapshotNode(snapshot *storage.Snapshot, label string, key any) *storage.Node {
	for _, node := range snapshot.Nodes {
		for _, held := range node.Labels {
			if held == label && node.Properties["key"] == key {
				return node
			}
		}
	}
	return nil
}

// awaitSnapshotInFlight returns the temporary file of a periodic snapshot
// that is being written and has already written node records (more than the
// writer's 4 KiB buffer), so it reads the store as it was before now. While
// no snapshot is being written it writes internal marker nodes, so that the
// next periodic snapshot is not skipped as having nothing new.
func (l *rsvLab) awaitSnapshotInFlight() string {
	l.t.Helper()
	dir := filepath.Join(l.root, "snapshots")
	deadline := time.Now().Add(2 * time.Minute)
	lastTick := time.Time{}
	for {
		writing := false
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if !strings.HasSuffix(entry.Name(), ".json.tmp") {
				continue
			}
			writing = true
			if info, err := entry.Info(); err == nil && info.Size() > 4096 {
				return filepath.Join(dir, entry.Name())
			}
		}
		require.True(l.t, time.Now().Before(deadline), "no periodic snapshot was seen being written")
		if !writing && time.Since(lastTick) > 20*time.Millisecond {
			l.exec("CREATE (:_Tick)", nil)
			lastTick = time.Now()
		}
		time.Sleep(time.Millisecond)
	}
}

func (l *rsvLab) walEngine() *storage.WALEngine {
	l.t.Helper()
	walEngine, ok := l.db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(l.t, ok, "the server's storage is journaled")
	return walEngine
}

// rsvDamageAndRecover damages the closed store's files, then starts the
// server on them: automatic recovery rebuilds the store from the snapshot and
// the journal.
func rsvDamageAndRecover(t *testing.T, root string) *rsvLab {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(root, "MANIFEST"), []byte("damaged"), 0o644))
	lab := rsvOpen(t, root)
	preserved, err := filepath.Glob(root + ".corrupted-*")
	require.NoError(t, err)
	require.Len(t, preserved, 1, "the start rebuilt the store and kept the damaged one")
	manifest, err := os.ReadFile(filepath.Join(preserved[0], "recovery-manifest.json"))
	require.NoError(t, err)
	t.Logf("recovery manifest: %s", manifest)
	if entries, err := storage.ReadWALEntriesFromDir(t.Context(), filepath.Join(preserved[0], "wal")); err == nil {
		ops := make([]string, 0, len(entries))
		for _, entry := range entries {
			ops = append(ops, fmt.Sprintf("%d:%s", entry.Sequence, entry.Operation))
		}
		t.Logf("journal of the damaged store: %v", ops)
	}
	return lab
}

// requireReembedded checks the rebuilt node holds no managed vectors and
// needs embedding, and that the worker then embeds it with vectors other than
// stale (the snapshot's, or "none" when the snapshot held none).
func (l *rsvLab) requireReembedded(provider *rsvProvider, key, stale string) {
	l.t.Helper()
	got := l.node(key)
	require.Empty(l.t, got.ChunkEmbeddings, "the rebuilt node holds no managed vectors; the node holds %s, the snapshot held %s", rsvPrint(got.ChunkEmbeddings), stale)
	require.True(l.t, storage.NodeNeedsEmbedding(got), "the rebuilt node needs embedding; embedding metadata %v", got.EmbedMeta)
	provider.fail.Store(false)
	l.setProvider(provider)
	require.NotEqual(l.t, stale, l.awaitVectors(provider, key, stale))
}

// rsvSeed creates the Doc node with key and text and embeds it. "with the
// node" embeds it inline in the creating statement (WITH EMBEDDING), which
// stores the vectors inside the node's record, where the periodic snapshot
// keeps them; "worker" leaves it to the embedding worker, which stores them
// beside the node; "worker, then updated" also updates another property of
// the node afterwards, after which the periodic snapshot holds its vectors.
// It returns the vectors' print.
func (l *rsvLab) rsvSeed(provider *rsvProvider, origin, key, text string) string {
	l.t.Helper()
	l.setProvider(provider)
	if origin == "with the node" {
		l.exec("CREATE (:Doc {key:$key, text:$text}) WITH EMBEDDING", map[string]any{"key": key, "text": text})
		got := rsvPrint(l.node(key).ChunkEmbeddings)
		require.NotEqual(l.t, "none", got)
		return got
	}
	l.exec("CREATE (:Doc {key:$key, text:$text})", map[string]any{"key": key, "text": text})
	got := l.awaitVectors(provider, key, "")
	if origin == "worker, then updated" {
		node := storage.CopyNode(l.node(key))
		node.Properties["seen"] = true
		require.NoError(l.t, l.db.GetStorage().UpdateNode(context.Background(), storage.NodeUpdateRequest{After: node}))
		require.Equal(l.t, got, rsvPrint(l.node(key).ChunkEmbeddings), "an update of another property keeps the vectors")
	}
	return got
}

type rsvOrigin struct{ name, origin, text string }

func rsvOrigins() []rsvOrigin {
	return []rsvOrigin{
		{name: "vectors inside the node (WITH EMBEDDING)", origin: "with the node", text: "small text"},
		{name: "worker vectors beside the node", origin: "worker", text: "small text"},
		{name: "worker vectors, then the node updated", origin: "worker, then updated", text: "small text"},
		// The node's record exceeds the inline limit.
		{name: "worker vectors, large text", origin: "worker", text: strings.Repeat("large text ", 6000)},
	}
}

func TestRebuildFromSnapshotDoesNotPresentReplacedVectorsAsCurrent(t *testing.T) {
	for _, o := range rsvOrigins() {
		t.Run(o.name+"/embeddings regenerated, the node not updated afterwards", func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")
			provider := &rsvProvider{chunks: 3}
			lab := rsvOpen(t, root)
			first := lab.rsvSeed(provider, o.origin, "d", o.text)
			lab.snapshot()
			_, err := lab.db.ClearAllEmbeddings()
			require.NoError(t, err)
			second := lab.awaitVectors(provider, "d", first)
			require.NotEqual(t, first, second)
			lab.close()

			lab = rsvDamageAndRecover(t, root)
			lab.requireReembedded(provider, "d", first)
		})

		t.Run(o.name+"/embeddings cleared, not yet regenerated", func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")
			provider := &rsvProvider{chunks: 3}
			lab := rsvOpen(t, root)
			first := lab.rsvSeed(provider, o.origin, "d", o.text)
			lab.snapshot()
			lab.close()
			// The server restarts and clears the embeddings before its model
			// loads, so nothing embeds the node again before the damage.
			lab = rsvOpen(t, root)
			lab.walEngine().DisableAutoCompaction()
			_, err := lab.db.ClearAllEmbeddings()
			require.NoError(t, err)
			require.Empty(t, lab.node("d").ChunkEmbeddings)
			lab.close()

			lab = rsvDamageAndRecover(t, root)
			lab.requireReembedded(provider, "d", first)
		})

		if o.origin != "with the node" {
			continue // one origin: the failure marker's record is the worker's
		}
		t.Run(o.name+"/embeddings regenerated, the provider rejected the text", func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")
			provider := &rsvProvider{chunks: 3}
			lab := rsvOpen(t, root)
			first := lab.rsvSeed(provider, o.origin, "d", o.text)
			lab.snapshot()
			provider.fail.Store(true)
			_, err := lab.db.ClearAllEmbeddings()
			require.NoError(t, err)
			lab.awaitFailureMarker("d")
			lab.close()

			lab = rsvDamageAndRecover(t, root)
			lab.requireReembedded(provider, "d", first)
		})
	}

	t.Run("snapshot holds a parked failure, the node embedded after a retry", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "data")
		provider := &rsvProvider{chunks: 3}
		provider.fail.Store(true)
		lab := rsvOpen(t, root)
		lab.exec("CREATE (:Doc {key:'d', text:'small text'})", nil)
		lab.setProvider(provider)
		lab.awaitFailureMarker("d")
		lab.snapshot()
		provider.fail.Store(false)
		retried, err := lab.db.RetryEmbeddingFailures(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, 1, retried)
		lab.awaitVectors(provider, "d", "none")
		lab.close()

		lab = rsvDamageAndRecover(t, root)
		lab.requireReembedded(provider, "d", "none")
	})

	t.Run("embeddings regenerated, the node updated afterwards", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "data")
		provider := &rsvProvider{chunks: 3}
		lab := rsvOpen(t, root)
		first := lab.rsvSeed(provider, "with the node", "d", "small text")
		lab.snapshot()
		_, err := lab.db.ClearAllEmbeddings()
		require.NoError(t, err)
		second := lab.awaitVectors(provider, "d", first)
		// An update through the journaled storage path: its record carries
		// the node's whole image, the regenerated vectors included.
		updated := lab.node("d")
		updated.Properties["hidden"] = true
		require.NoError(t, lab.db.GetStorage().UpdateNode(context.Background(), storage.NodeUpdateRequest{After: updated}))
		live := lab.node("d")
		require.Equal(t, true, live.Properties["hidden"])
		require.Equal(t, second, rsvPrint(live.ChunkEmbeddings))
		lab.close()

		lab = rsvDamageAndRecover(t, root)
		got := lab.node("d")
		require.Equal(t, true, got.Properties["hidden"], "the update is replayed")
		require.Equal(t, second, rsvPrint(got.ChunkEmbeddings), "the rebuilt node holds the regenerated vectors")
		require.False(t, storage.NodeNeedsEmbedding(got))
	})

	// The "named" node holds named vectors only, so the clear writes no
	// record for it: this checks that its named vectors, updated after the
	// snapshot, are the ones the rebuilt node holds, and that it does not
	// need embedding. Named vectors of a node that also holds managed ones
	// are in the storage test's shapes.
	t.Run("named vectors survive regeneration", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "data")
		provider := &rsvProvider{chunks: 3}
		lab := rsvOpen(t, root)
		named := func(seed float32) map[string][]float32 {
			v := make([]float32, rsvDims)
			for i := range v {
				v[i] = seed + float32(i)
			}
			return map[string][]float32{"title": v}
		}
		ctx := context.Background()
		_, err := lab.db.GetStorage().CreateNode(ctx, &storage.Node{ID: "named", Labels: []string{"Doc"}, Properties: map[string]any{"key": "named", "text": "named text"}, NamedEmbeddings: named(1)})
		require.NoError(t, err)
		lab.rsvSeed(provider, "with the node", "d", "small text")
		lab.snapshot()
		node := lab.node("named")
		node.NamedEmbeddings = named(2)
		require.NoError(t, lab.db.GetStorage().UpdateNode(ctx, storage.NodeUpdateRequest{After: node}))
		_, err = lab.db.ClearAllEmbeddings()
		require.NoError(t, err)
		lab.close()

		lab = rsvDamageAndRecover(t, root)
		got := lab.node("named")
		require.Equal(t, named(2), got.NamedEmbeddings)
		require.False(t, storage.NodeNeedsEmbedding(got))
	})

	t.Run("ordinary restart keeps regenerated vectors", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "data")
		provider := &rsvProvider{chunks: 3}
		lab := rsvOpen(t, root)
		first := lab.rsvSeed(provider, "with the node", "d", "small text")
		lab.snapshot()
		_, err := lab.db.ClearAllEmbeddings()
		require.NoError(t, err)
		second := lab.awaitVectors(provider, "d", first)
		lab.close()

		lab = rsvOpen(t, root)
		got := lab.node("d")
		require.Equal(t, second, rsvPrint(got.ChunkEmbeddings))
		require.False(t, storage.NodeNeedsEmbedding(got))
	})
}

// The recovery fixture's snapshot holds the fixture's writes even when a
// periodic snapshot is already being written as the node is created: that
// snapshot reads the store as it was before the node existed, and a rebuild
// from it alone has no node. The snapshot boundary itself orders the phases:
// a periodic snapshot holds the store's write barrier while it writes, so the
// statement that creates the node is admitted only after that snapshot
// completed, and the file that snapshot writes cannot hold a node created
// after its own cut. The store is filled first so that writing a snapshot
// takes long enough to reach it while it is under way; the provider and its
// indexes are set up before, so creating the node is one statement.
func TestRebuildFixtureSnapshotHoldsWritesMadeDuringASnapshot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	lab := rsvOpen(t, root)
	lab.setProvider(&rsvProvider{chunks: 3})
	lab.exec("UNWIND range(1, $n) AS i CREATE (:_Fill {i: i, pad: $pad})", map[string]any{"n": 20000, "pad": strings.Repeat("p", 1024)})
	// The snapshot being written now cut the store before the statement below,
	// and that statement is admitted only once this snapshot released the
	// store, so the file this snapshot writes holds no node.
	inFlight := lab.awaitSnapshotInFlight()
	key := "d"
	lab.exec("CREATE (:Doc {key:$key, text:'small text'}) WITH EMBEDDING", map[string]any{"key": key})
	during, err := storage.LoadSnapshot(context.Background(), strings.TrimSuffix(inFlight, ".tmp"))
	require.NoError(t, err, "the snapshot that was being written when the node was created completed")
	require.Nil(t, rsvSnapshotDoc(during, key), "the snapshot that was being written when the node was created holds no node")
	vectors := rsvPrint(lab.node(key).ChunkEmbeddings)
	require.NotEqual(t, "none", vectors)
	lab.snapshot()
	snapshot, err := lab.newestSnapshot()
	require.NoError(t, err)
	require.NotNil(t, rsvSnapshotDoc(snapshot, key), "the snapshot a rebuild starts from holds the node")
	lab.close()

	lab = rsvDamageAndRecover(t, root)
	require.Equal(t, vectors, rsvPrint(lab.node(key).ChunkEmbeddings), "the rebuilt node holds the vectors it was created with")
}

// A closed journal refuses a node's reset record. Both clears then return the
// journal's error and a count of 0, and the node keeps its vectors.
func TestEmbeddingClearReportsARefusedJournalRecord(t *testing.T) {
	c := rsvConfig()
	c.Memory.EmbeddingEnabled = false
	c.Database.WALAutoCompactionEnabled = false
	db, err := nornicdb.Open(filepath.Join(t.TempDir(), "data"), c)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	vectors := [][]float32{make([]float32, rsvDims)}
	vectors[0][0] = 1
	_, err = db.GetStorage().CreateNode(ctx, &storage.Node{ID: "v", Labels: []string{"Doc"}, Properties: map[string]any{"key": "v", "text": "t"},
		ChunkEmbeddings: vectors, EmbedMeta: map[string]any{"has_embedding": true}})
	require.NoError(t, err)
	walEngine, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
	require.True(t, ok)
	require.NoError(t, walEngine.GetWAL().Close())

	for name, clear := range map[string]func() (int, error){
		"ClearAllEmbeddings":      db.ClearAllEmbeddings,
		"ClearDatabaseEmbeddings": func() (int, error) { return db.ClearDatabaseEmbeddings("nornic", db.GetStorage()) },
	} {
		cleared, err := clear()
		require.ErrorIs(t, err, storage.ErrWALClosed, name)
		require.Zero(t, cleared, name)
		node, err := db.GetStorage().GetNode(ctx, "v")
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 1, "%s: the node keeps its vectors", name)
	}
}
