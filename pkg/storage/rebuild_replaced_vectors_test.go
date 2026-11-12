package storage

import (
	"bufio"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// In the cases below, a rebuild of an empty store from a snapshot plus the
// journal must not hold managed vectors other than those the live store held
// when it was closed. The embedding worker's write-back, an embedding reset
// and "regenerate embeddings" replace a node's managed vectors with a
// journal record that does not carry the new ones. After replay of that
// record the rebuilt node holds the live vectors (always, when a later
// record carries them) or none; a node without managed vectors must need
// embedding, unless it holds named vectors or the same parked failure as the
// live node. Each case writes through the journal wrapper and compares the
// rebuilt node's properties, named vectors and managed vectors with the live
// store's.
//
// Each case applies its writes one after another; none runs a clear at the
// same time as another write of the node. "Worker vectors" are written with
// the storage call the embedding worker uses (UpdateNodeEmbeddingSidecar); no
// worker runs here. The shape names say how each node is written; the
// storage layout that results is not asserted.
//
// The node shapes follow the kept-vectors shape builders of the NOR-412 lane
// (scratch commit 25e3c2ace), written against this tree's API.

const rrvNode NodeID = "test:shape"

const rrvDims = 8

func rrvVectors(chunks int, seed float32) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, rrvDims)
		for j := range v {
			v[j] = seed + float32(i*rrvDims+j)/7
		}
		out[i] = v
	}
	return out
}

func rrvWorkerMeta(chunks int) map[string]any {
	return map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": rrvDims, "embedding_model": "synthetic"}
}

type rrvLab struct {
	t       *testing.T
	dir     string
	engine  *BadgerEngine
	wal     *WAL
	journal *WALEngine
}

// rrvOpen opens an engine at dir behind the journal wrapper, synchronous
// commits, with "text" as the only embedded property.
func rrvOpen(t *testing.T, dir string) *rrvLab {
	t.Helper()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true})
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	return &rrvLab{t: t, dir: dir, engine: engine, wal: engine.bufferedCommitWAL, journal: NewWALEngine(engine, engine.bufferedCommitWAL)}
}

func (l *rrvLab) node() *Node {
	l.t.Helper()
	node, err := l.journal.GetNode(context.Background(), rrvNode)
	require.NoError(l.t, err)
	return CopyNode(node)
}

func (l *rrvLab) sidecar(edit func(*Node)) {
	l.t.Helper()
	node := l.node()
	edit(node)
	require.NoError(l.t, l.journal.UpdateNodeEmbeddingSidecar(context.Background(), node))
}

func (l *rrvLab) create(text string, chunks [][]float32, meta map[string]any, named map[string][]float32) {
	l.t.Helper()
	_, err := l.journal.CreateNode(context.Background(), &Node{ID: rrvNode, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false},
		ChunkEmbeddings: chunks, EmbedMeta: meta, NamedEmbeddings: named})
	require.NoError(l.t, err)
}

// regenerate clears the managed embeddings of the namespace, as "regenerate
// embeddings" does.
func (l *rrvLab) regenerate() {
	l.t.Helper()
	_, err := l.journal.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(l.t, err)
}

// snapshotPeriodic takes the snapshot automatic compaction takes, and
// truncates the journal as it does.
func (l *rrvLab) snapshotPeriodic() string {
	l.t.Helper()
	l.journal.snapshotDir = filepath.Join(l.dir, "snapshots")
	require.NoError(l.t, os.MkdirAll(l.journal.snapshotDir, 0o755))
	require.NoError(l.t, l.journal.createSnapshotAndCompact(context.Background()))
	entries, err := os.ReadDir(l.journal.snapshotDir)
	require.NoError(l.t, err)
	require.Len(l.t, entries, 1)
	return filepath.Join(l.journal.snapshotDir, entries[0].Name())
}

// snapshotFull takes a snapshot that also holds the vectors stored beside
// nodes.
func (l *rrvLab) snapshotFull() string {
	l.t.Helper()
	snapshot, err := l.wal.CreateSnapshot(l.engine)
	require.NoError(l.t, err)
	path := filepath.Join(l.dir, "full-snapshot.json")
	require.NoError(l.t, SaveSnapshot(snapshot, path))
	return path
}

// rrvRead reads the node as the store holds it: a stored read in a fresh
// transaction, not the engine's node cache.
func rrvRead(t *testing.T, engine *BadgerEngine) *Node {
	t.Helper()
	ctx := context.Background()
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, rrvNode)
	require.NoError(t, err)
	return node
}

func rrvBits(vectors [][]float32) [][]uint32 {
	out := make([][]uint32, len(vectors))
	for i, v := range vectors {
		out[i] = make([]uint32, len(v))
		for j, f := range v {
			out[i][j] = math.Float32bits(f)
		}
	}
	return out
}

func rrvNamedBits(named map[string][]float32) map[string][]uint32 {
	if len(named) == 0 {
		return nil
	}
	out := make(map[string][]uint32, len(named))
	for name, v := range named {
		out[name] = rrvBits([][]float32{v})[0]
	}
	return out
}

func rrvMeta(meta map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range meta {
		if key != "chunk_count" && key != embeddingContentUpdatedAtKey && key != embeddingRangeManifestKey {
			out[key] = value
		}
	}
	return out
}

func requireSameStoredMap(t *testing.T, want, got map[string]any, what string) {
	t.Helper()
	require.Equal(t, len(want), len(got), "%s: want %v, got %v", what, want, got)
	for key, value := range want {
		require.True(t, sameStoredValue(value, got[key]), "%s %q: want %#v, got %#v", what, key, value, got[key])
	}
}

// requireSameNode compares two stored reads: vectors bit for bit, metadata
// without the store's layout keys, properties by stored value.
func requireSameNode(t *testing.T, want, got *Node) {
	t.Helper()
	require.Equal(t, rrvBits(want.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings), "managed vectors")
	require.Equal(t, rrvNamedBits(want.NamedEmbeddings), rrvNamedBits(got.NamedEmbeddings), "named vectors")
	requireSameStoredMap(t, rrvMeta(want.EmbedMeta), rrvMeta(got.EmbedMeta), "embedding metadata")
	requireSameStoredMap(t, want.Properties, got.Properties, "properties")
}

type rrvShape struct {
	name string
	seed func(l *rrvLab)
}

func rrvShapes() []rrvShape {
	large := strings.Repeat("x", 300<<10)
	named := map[string][]float32{"title": {1, math.Float32frombits(0x80000000), 3}}
	worker := func(text string) func(l *rrvLab) {
		return func(l *rrvLab) {
			l.create(text, nil, nil, nil)
			l.sidecar(func(n *Node) { n.ChunkEmbeddings, n.EmbedMeta = rrvVectors(4, 1), rrvWorkerMeta(4) })
		}
	}
	return []rrvShape{
		{name: "vectors inside the node", seed: func(l *rrvLab) {
			l.create("alpha", rrvVectors(2, 3), map[string]any{"has_embedding": true, "embedding_model": "inline"}, nil)
		}},
		{name: "worker vectors beside the node", seed: worker("alpha")},
		{name: "worker vectors, then the node updated", seed: func(l *rrvLab) {
			worker("alpha")(l)
			node := l.node()
			node.Properties["flag"] = true
			require.NoError(l.t, l.journal.UpdateNode(context.Background(), NodeUpdateRequest{After: node}))
		}},
		{name: "large node, worker vectors beside it", seed: worker(large)},
		{name: "large node, vectors stored apart", seed: func(l *rrvLab) {
			l.create(large, rrvVectors(3, 5), map[string]any{"has_embedding": true, "embedding_model": "inline"}, nil)
		}},
		{name: "named vectors", seed: func(l *rrvLab) { l.create("alpha", nil, nil, named) }},
		{name: "named vectors and worker vectors", seed: func(l *rrvLab) {
			worker("alpha")(l)
			node := l.node()
			node.NamedEmbeddings = named
			require.NoError(l.t, l.journal.UpdateNode(context.Background(), NodeUpdateRequest{After: node}))
		}},
		{name: "parked embedding failure", seed: func(l *rrvLab) {
			l.create("alpha", nil, nil, nil)
			l.sidecar(func(n *Node) {
				n.ChunkEmbeddings, n.EmbedMeta = nil, map[string]any{"has_embedding": false, "embedding_failed": true, "embedding_error": "rejected"}
			})
		}},
	}
}

type rrvWrite struct {
	name string
	// exact: the journal holds the node's final vectors, so the rebuilt node
	// must hold the live node's vectors and not need embedding.
	exact bool
	apply func(l *rrvLab)
}

func rrvWrites() []rrvWrite {
	reembed := func(seed float32) func(l *rrvLab) {
		return func(l *rrvLab) {
			l.sidecar(func(n *Node) { n.ChunkEmbeddings, n.EmbedMeta = rrvVectors(3, seed), rrvWorkerMeta(3) })
		}
	}
	return []rrvWrite{
		{name: "embeddings regenerated", apply: func(l *rrvLab) { l.regenerate(); reembed(9)(l) }},
		{name: "embeddings cleared, not yet regenerated", apply: func(l *rrvLab) { l.regenerate() }},
		{name: "embeddings regenerated, the provider rejected the text", apply: func(l *rrvLab) {
			l.regenerate()
			l.sidecar(func(n *Node) {
				n.ChunkEmbeddings, n.EmbedMeta = nil, map[string]any{"has_embedding": false, "embedding_failed": true, "embedding_error": "rejected"}
			})
		}},
		{name: "embedding reset, then embedded", apply: func(l *rrvLab) {
			l.sidecar(func(n *Node) { n.ChunkEmbeddings, n.EmbedMeta = nil, nil })
			reembed(11)(l)
		}},
		{name: "embeddings regenerated, then the node updated", exact: true, apply: func(l *rrvLab) {
			l.regenerate()
			reembed(13)(l)
			node := l.node()
			node.Properties["flag"] = true
			require.NoError(l.t, l.journal.UpdateNode(context.Background(), NodeUpdateRequest{After: node}))
		}},
	}
}

func TestRebuildDoesNotPresentReplacedVectorsAsCurrent(t *testing.T) {
	for _, shape := range rrvShapes() {
		for _, write := range rrvWrites() {
			for _, periodic := range []bool{true, false} {
				snapshotKind := "full snapshot"
				if periodic {
					snapshotKind = "periodic snapshot"
				}
				t.Run(shape.name+"/"+write.name+"/"+snapshotKind, func(t *testing.T) {
					dir := t.TempDir()
					lab := rrvOpen(t, dir)
					shape.seed(lab)
					snapshot := ""
					if periodic {
						snapshot = lab.snapshotPeriodic()
					} else {
						snapshot = lab.snapshotFull()
					}
					write.apply(lab)
					live := rrvRead(t, lab.engine)
					crashClose(t, lab.engine, lab.wal)

					destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
					require.NoError(t, err)
					result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
					require.NoError(t, err)
					require.Zero(t, result.Failed, "%v", result.Errors)
					got := rrvRead(t, destination)

					requireSameStoredMap(t, live.Properties, got.Properties, "properties")
					require.Equal(t, rrvNamedBits(live.NamedEmbeddings), rrvNamedBits(got.NamedEmbeddings), "named vectors equal the live store's")
					failed := func(n *Node) bool { f, _ := n.EmbedMeta["embedding_failed"].(bool); return f }
					switch {
					case write.exact:
						require.NotEmpty(t, got.ChunkEmbeddings)
						require.Equal(t, rrvBits(live.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings), "the rebuilt node holds the live vectors")
						require.False(t, NodeNeedsEmbedding(got))
					case len(got.ChunkEmbeddings) > 0:
						require.Equal(t, rrvBits(live.ChunkEmbeddings), rrvBits(got.ChunkEmbeddings), "no managed vectors other than the live ones")
					case len(got.NamedEmbeddings) > 0:
						// Not managed: the worker does not embed it.
					case len(live.ChunkEmbeddings) == 0 && failed(live) && failed(got):
						// Unchanged since the snapshot: the same parked failure.
					default:
						require.True(t, NodeNeedsEmbedding(got), "a rebuilt node without managed vectors needs embedding; metadata %v", got.EmbedMeta)
					}

					// An ordinary restart of the live store gives the node it held
					// before the close.
					restarted := rrvOpen(t, dir)
					_, err = RecoverBufferedCommitsAtOpen(restarted.engine, restarted.wal)
					require.NoError(t, err)
					requireSameNode(t, live, rrvRead(t, restarted.engine))
				})
			}
		}
	}
}

// The clear through the journal wrapper journals one update_embedding record
// for the node it clears, naming that node, without vectors and with
// has_embedding false, and none for a node without managed vectors or outside
// the prefix.
func TestJournaledEmbeddingClearRecordsEachClearedNode(t *testing.T) {
	ctx := context.Background()
	lab := rrvOpen(t, t.TempDir())
	_, err := lab.journal.CreateNode(ctx, &Node{ID: "test:a", Properties: map[string]any{"text": "a"}, ChunkEmbeddings: rrvVectors(2, 1), EmbedMeta: rrvWorkerMeta(2)})
	require.NoError(t, err)
	_, err = lab.journal.CreateNode(ctx, &Node{ID: "test:b", Properties: map[string]any{"text": "b"}})
	require.NoError(t, err)
	_, err = lab.journal.CreateNode(ctx, &Node{ID: "other:c", Properties: map[string]any{"text": "c"}, ChunkEmbeddings: rrvVectors(1, 2), EmbedMeta: rrvWorkerMeta(1)})
	require.NoError(t, err)
	before := lab.wal.sequence.Load()

	cleared, err := lab.journal.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 1, cleared)
	require.NoError(t, lab.wal.Sync())
	entries, err := ReadWALEntriesFromDir(ctx, filepath.Join(lab.dir, "wal"))
	require.NoError(t, err)
	var records []WALNodeData
	for _, entry := range entries {
		if entry.Sequence <= before {
			continue
		}
		require.Equal(t, OpUpdateEmbedding, entry.Operation)
		require.Equal(t, "test", entry.Database)
		var data WALNodeData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		records = append(records, data)
	}
	require.Len(t, records, 1)
	require.Equal(t, NodeID("a"), records[0].Node.ID)
	require.Empty(t, records[0].Node.ChunkEmbeddings)
	require.Equal(t, false, records[0].Node.EmbedMeta["has_embedding"])

	got, err := lab.journal.GetNode(ctx, "test:a")
	require.NoError(t, err)
	require.Empty(t, got.ChunkEmbeddings)
	other, err := lab.journal.GetNode(ctx, "other:c")
	require.NoError(t, err)
	require.Len(t, other.ChunkEmbeddings, 1, "another namespace keeps its vectors")
}

// A closed journal refuses a node's reset record: the clear returns the
// journal's error and a count of 0, and the node keeps its vectors.
// (Independent review of #1795, round 1, TestR1795ClearJournalFailure.)
func TestJournaledEmbeddingClearReportsARefusedRecord(t *testing.T) {
	lab := rrvOpen(t, t.TempDir())
	lab.create("alpha", rrvVectors(2, 1), rrvWorkerMeta(2), nil)
	require.NoError(t, lab.wal.Close())

	cleared, err := lab.journal.ClearAllEmbeddingsForPrefix("test:")
	require.ErrorIs(t, err, ErrWALClosed)
	require.Zero(t, cleared)
	require.Len(t, rrvRead(t, lab.engine).ChunkEmbeddings, 2, "the node whose record was refused keeps its vectors")
}

// rrvFullDisk accepts allowed writes, then fails like a full disk.
type rrvFullDisk struct {
	w       io.Writer
	allowed int
}

func (d *rrvFullDisk) Write(p []byte) (int, error) {
	if d.allowed <= 0 {
		return 0, syscall.ENOSPC
	}
	d.allowed--
	return d.w.Write(p)
}

// A journal write error part-way through, injected by replacing the journal's
// writer with one that returns ENOSPC after one write: the node cleared
// before it stays cleared and journaled, the refused node and the one after
// it keep their vectors, and the error comes back with the number cleared.
func TestJournaledEmbeddingClearStopsAtAJournalWriteError(t *testing.T) {
	ctx := context.Background()
	lab := rrvOpen(t, t.TempDir())
	ids := []NodeID{"test:a", "test:b", "test:c"}
	for i, id := range ids {
		_, err := lab.journal.CreateNode(ctx, &Node{ID: id, Properties: map[string]any{"text": string(id)}, ChunkEmbeddings: rrvVectors(2, float32(i)), EmbedMeta: rrvWorkerMeta(2)})
		require.NoError(t, err)
	}
	before := lab.wal.sequence.Load()
	lab.wal.mu.Lock()
	require.NoError(t, lab.wal.writer.Flush())
	working := lab.wal.writer
	lab.wal.writer = bufio.NewWriter(&rrvFullDisk{w: lab.wal.file, allowed: 1})
	lab.wal.mu.Unlock()
	t.Cleanup(func() {
		lab.wal.mu.Lock()
		lab.wal.writer = working
		lab.wal.mu.Unlock()
	})

	cleared, err := lab.journal.ClearAllEmbeddingsForPrefix("test:")
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Equal(t, 1, cleared)
	for i, id := range ids {
		node, err := lab.journal.GetNode(ctx, id)
		require.NoError(t, err)
		if i == 0 {
			require.Empty(t, node.ChunkEmbeddings, "cleared before the error")
		} else {
			require.Len(t, node.ChunkEmbeddings, 2, "%s keeps its vectors", id)
		}
	}
	entries, err := ReadWALEntriesFromDir(ctx, filepath.Join(lab.dir, "wal"))
	require.NoError(t, err)
	var journaled []NodeID
	for _, entry := range entries {
		if entry.Sequence > before && entry.Operation == OpUpdateEmbedding {
			var data WALNodeData
			require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
			journaled = append(journaled, data.Node.ID)
		}
	}
	require.Equal(t, []NodeID{"a"}, journaled, "the cleared node's record is in the journal")
}
