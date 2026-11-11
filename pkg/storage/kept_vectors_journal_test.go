package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412: under WALRecordsLarge a transaction that changes a node's
// properties or labels but not its vectors journals the node without those
// vectors, and replay restores them from the stored predecessor. These tests
// drive the journal owner directly: the record a commit writes, the crash boundary after the record
// and before its application, a clean reopen, version-3 records, and a
// transaction mixing kept, replaced and invalidated vectors.

const keptVectorDims = 256

func keptVectors(chunks int, seed float32) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, keptVectorDims)
		for j := range v {
			v[j] = seed + float32(i*keptVectorDims+j)/7
		}
		out[i] = v
	}
	return out
}

// keptVectorsEngine opens an engine and WAL that write large records, as a
// single-node server's do.
func keptVectorsEngine(t *testing.T, dir string) (*BadgerEngine, *WAL) {
	t.Helper()
	return keptVectorsEngineWith(t, dir, WALRecordsLarge)
}

func keptVectorsEngineWith(t *testing.T, dir string, records WALRecordPolicy) (*BadgerEngine, *WAL) {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: records})
	// Only "text" feeds the embedding, so other changes keep the vectors.
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	return b, b.bufferedCommitWAL
}

func keptVectorsReopen(t *testing.T, b *BadgerEngine, w *WAL, dir string) (*BadgerEngine, *WAL) {
	t.Helper()
	crashClose(t, b, w)
	b, w = keptVectorsEngine(t, dir)
	_, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	return b, w
}

// keptVectorsSeed stores an embedded node whose vectors the embedding worker
// wrote to the sidecar, as a deployed server has them.
func keptVectorsSeed(t *testing.T, b *BadgerEngine, id NodeID, text string, chunks int, seed float32) {
	t.Helper()
	ctx := t.Context()
	_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false, "a": int64(1)}})
	require.NoError(t, err)
	stored, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	embedded := CopyNode(stored)
	embedded.ChunkEmbeddings = keptVectors(chunks, seed)
	embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": keptVectorDims, "embedding_model": "synthetic"}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(ctx, embedded))
	got, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, keptVectors(chunks, seed), got.ChunkEmbeddings)
}

// keptVectorsPrepare stages edit on each node in one explicit transaction and
// returns the journal payload its commit would write, without applying it:
// exactly the crash boundary after the record and before native application.
func keptVectorsPrepare(t *testing.T, b *BadgerEngine, logicalID string, edits map[NodeID]func(*Node)) []byte {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for id, edit := range edits {
		node, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		node = CopyNode(node)
		edit(node)
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	}
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	prepared.Abort()
	return payload
}

func keptVectorsJournal(t *testing.T, w *WAL, payload []byte) {
	t.Helper()
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
}

func requireKeptVectors(t *testing.T, b *BadgerEngine, id NodeID, chunks int, seed float32) *Node {
	t.Helper()
	got, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, keptVectors(chunks, seed), got.ChunkEmbeddings, "stored vectors of %s", id)
	require.Equal(t, true, got.EmbedMeta["has_embedding"], "embedding metadata of %s", id)
	return got
}

func TestKeptVectorsJournalOmitsUnchangedVectors(t *testing.T) {
	b, _ := keptVectorsEngine(t, t.TempDir())
	keptVectorsSeed(t, b, "test:keep", "alpha", 400, 1)
	payload := keptVectorsPrepare(t, b, "omit", map[NodeID]func(*Node){
		"test:keep": func(n *Node) { n.Properties["flag"] = true; n.Labels = append(n.Labels, "Hidden") },
	})
	// 400 x 256 float32 vectors are 400 KiB raw; the record carries none.
	require.Less(t, len(payload), 4096, "the record must not grow with vectors the update kept")
	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Equal(t, uint16(6), envelope.Version, "a record that omits kept vectors is version 6")
	require.Len(t, ops, 1)
	require.Equal(t, OpUpdateNode, ops[0].Type)
	require.Empty(t, ops[0].Node.ChunkEmbeddings)
	require.Nil(t, ops[0].Node.EmbedMeta)
	require.Equal(t, true, ops[0].Node.Properties["flag"])
}

func TestKeptVectorsReplayAfterCrashBeforeApplication(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
	keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "crash-before-apply", map[NodeID]func(*Node){
		"test:keep": func(n *Node) {
			n.Properties["flag"] = true
			n.Labels = append(n.Labels, "Hidden")
			delete(n.Properties, "a")
		},
	}))
	got, err := b.GetNode(t.Context(), "test:keep")
	require.NoError(t, err)
	require.Equal(t, false, got.Properties["flag"], "the record is not applied before the crash")

	b, w = keptVectorsReopen(t, b, w, dir)
	got = requireKeptVectors(t, b, "test:keep", 300, 1)
	require.Equal(t, true, got.Properties["flag"])
	require.NotContains(t, got.Properties, "a")
	require.ElementsMatch(t, []string{"Doc", "Hidden"}, got.Labels)

	// The sidecar stays fresh for the replayed body: a further unrelated
	// update keeps the vectors, and a clean reopen still has them.
	keptVectorsJournal(t, w, keptVectorsPrepare(t, b, "after-replay", map[NodeID]func(*Node){
		"test:keep": func(n *Node) { n.Properties["second"] = int64(2) },
	}))
	b, w = keptVectorsReopen(t, b, w, dir)
	got = requireKeptVectors(t, b, "test:keep", 300, 1)
	require.Equal(t, int64(2), got.Properties["second"])
	require.NoError(t, b.Close())
	require.NoError(t, w.Close())
	b, _ = keptVectorsEngine(t, dir)
	requireKeptVectors(t, b, "test:keep", 300, 1)
}

func TestKeptVectorsCommittedUpdateSurvivesCrashAndCleanReopen(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	keptVectorsSeed(t, b, "test:keep", "alpha", 300, 1)
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, "test:keep")
	require.NoError(t, err)
	node = CopyNode(node)
	node.Properties["flag"] = true
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "committed"})
	require.NoError(t, err)
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(ctx))
	requireKeptVectors(t, b, "test:keep", 300, 1)

	// Crash after application: replay finds the receipt and changes nothing.
	b, w = keptVectorsReopen(t, b, w, dir)
	got := requireKeptVectors(t, b, "test:keep", 300, 1)
	require.Equal(t, true, got.Properties["flag"])
	require.NoError(t, b.Close())
	require.NoError(t, w.Close())
	b, _ = keptVectorsEngine(t, dir)
	got = requireKeptVectors(t, b, "test:keep", 300, 1)
	require.Equal(t, true, got.Properties["flag"])
}

// A record written before this change carries the complete image, vectors
// included, as version 3. It still replays as that complete image.
func TestKeptVectorsVersion3RecordStillReplays(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	keptVectorsSeed(t, b, "test:keep", "alpha", 50, 1)
	stored, err := b.GetNode(t.Context(), "test:keep")
	require.NoError(t, err)
	image := CopyNode(stored)
	image.Properties["flag"] = true
	require.Len(t, image.ChunkEmbeddings, 50)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "v3-record", Mode: CommitModeExplicit, Operations: []Operation{{Type: OpUpdateNode, NodeID: image.ID, Node: image}}})
	require.NoError(t, err)
	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Equal(t, uint16(3), envelope.Version, "a record that omits nothing keeps the version-3 format")
	require.Len(t, ops[0].Node.ChunkEmbeddings, 50)
	keptVectorsJournal(t, w, payload)

	b, _ = keptVectorsReopen(t, b, w, dir)
	got := requireKeptVectors(t, b, "test:keep", 50, 1)
	require.Equal(t, true, got.Properties["flag"])
}

// One transaction: one node keeps its vectors, one receives new vectors from
// the client, one changes its embedded text and loses them. Replay after a
// crash before application reproduces each outcome.
func TestKeptVectorsMixedTransactionReplay(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	keptVectorsSeed(t, b, "test:keep", "alpha", 120, 1)
	keptVectorsSeed(t, b, "test:replace", "beta", 120, 2)
	keptVectorsSeed(t, b, "test:invalidate", "gamma", 120, 3)
	payload := keptVectorsPrepare(t, b, "mixed", map[NodeID]func(*Node){
		"test:keep":    func(n *Node) { n.Properties["flag"] = true },
		"test:replace": func(n *Node) { n.Properties["flag"] = true; n.ChunkEmbeddings = keptVectors(30, 9) },
		"test:invalidate": func(n *Node) {
			n.Properties["text"] = "changed"
		},
	})
	_, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	byID := map[NodeID]Operation{}
	for _, op := range ops {
		byID[op.NodeID] = op
	}
	require.Empty(t, byID["test:keep"].Node.ChunkEmbeddings, "kept vectors are not journaled")
	require.Equal(t, keptVectors(30, 9), byID["test:replace"].Node.ChunkEmbeddings, "new vectors are journaled")
	require.Empty(t, byID["test:invalidate"].Node.ChunkEmbeddings)
	keptVectorsJournal(t, w, payload)

	b, w = keptVectorsReopen(t, b, w, dir)
	got := requireKeptVectors(t, b, "test:keep", 120, 1)
	require.Equal(t, true, got.Properties["flag"])
	replaced, err := b.GetNode(t.Context(), "test:replace")
	require.NoError(t, err)
	require.Equal(t, keptVectors(30, 9), replaced.ChunkEmbeddings)
	require.Equal(t, true, replaced.Properties["flag"])
	invalidated, err := b.GetNode(t.Context(), "test:invalidate")
	require.NoError(t, err)
	require.Empty(t, invalidated.ChunkEmbeddings)
	require.Equal(t, "changed", invalidated.Properties["text"])
	require.NoError(t, b.Close())
	require.NoError(t, w.Close())

	b, _ = keptVectorsEngine(t, dir)
	requireKeptVectors(t, b, "test:keep", 120, 1)
	replaced, err = b.GetNode(t.Context(), "test:replace")
	require.NoError(t, err)
	require.Equal(t, keptVectors(30, 9), replaced.ChunkEmbeddings)
	invalidated, err = b.GetNode(t.Context(), "test:invalidate")
	require.NoError(t, err)
	require.Empty(t, invalidated.ChunkEmbeddings)
}

// Named vectors the update carries unchanged are omitted and restored too.
func TestKeptVectorsNamedEmbeddingsReplay(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	named := map[string][]float32{"title": keptVectors(1, 4)[0], "body": keptVectors(1, 5)[0]}
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, NamedEmbeddings: named})
	require.NoError(t, err)
	payload := keptVectorsPrepare(t, b, "named", map[NodeID]func(*Node){
		"test:named": func(n *Node) { n.Properties["flag"] = true },
	})
	_, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	require.Empty(t, ops[0].Node.NamedEmbeddings, "kept named vectors are not journaled")
	keptVectorsJournal(t, w, payload)

	b, _ = keptVectorsReopen(t, b, w, dir)
	got, err := b.GetNode(t.Context(), "test:named")
	require.NoError(t, err)
	require.Equal(t, true, got.Properties["flag"])
	require.Equal(t, named, got.NamedEmbeddings)
}

// The defect itself at the journal owner: kept vectors of 17 MB (above the
// 16 MiB WAL frame) on one node. The commit was refused with "wal: entry
// exceeds maximum WAL frame size"; it must commit and keep them.
func TestKeptVectorsCommitAboveFrameLimit(t *testing.T) {
	dir := t.TempDir()
	b, w := keptVectorsEngine(t, dir)
	const chunks = 17000 // x 256 dims x 4 bytes = 17.4 MB
	keptVectorsSeed(t, b, "test:keep", "alpha", chunks, 1)
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, "test:keep")
	require.NoError(t, err)
	node = CopyNode(node)
	node.Properties["flag"] = true
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "above-frame"})
	require.NoError(t, err)
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err, "the update keeps its vectors and must commit")
	require.NoError(t, receipt.Wait(ctx))
	got := requireKeptVectors(t, b, "test:keep", chunks, 1)
	require.Equal(t, true, got.Properties["flag"])
	b, _ = keptVectorsReopen(t, b, w, dir)
	got = requireKeptVectors(t, b, "test:keep", chunks, 1)
	require.Equal(t, true, got.Properties["flag"])
}
