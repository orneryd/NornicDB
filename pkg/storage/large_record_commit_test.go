package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-412 at the journal owner: under WALRecordsLarge, as a single-node
// server opens its WAL, a transaction whose record is larger than one 16 MiB
// frame commits, and the record carries the node's whole image, vectors
// included, exactly as the standard policy encodes it.

const largeRecordDims = 256

func largeRecordVectors(chunks int, seed float32) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, largeRecordDims)
		for j := range v {
			v[j] = seed + float32(i*largeRecordDims+j)/7
		}
		out[i] = v
	}
	return out
}

// largeRecordEngine opens an engine and a WAL under records.
func largeRecordEngine(t *testing.T, dir string, records WALRecordPolicy) (*BadgerEngine, *WAL) {
	t.Helper()
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: true, manualDrain: true, records: records})
	// Only "text" feeds the embedding, so other changes keep the vectors.
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	return b, b.bufferedCommitWAL
}

// largeRecordReopen crashes the engine and its WAL and opens them again with
// the recovery a server runs at open.
func largeRecordReopen(t *testing.T, b *BadgerEngine, w *WAL, dir string) (*BadgerEngine, *WAL) {
	t.Helper()
	crashClose(t, b, w)
	b, w = largeRecordEngine(t, dir, WALRecordsLarge)
	_, err := RecoverBufferedCommitsAtOpen(b, w)
	require.NoError(t, err)
	return b, w
}

// largeRecordSeed stores an embedded node whose vectors the embedding worker
// wrote to the sidecar, as a deployed server has them.
func largeRecordSeed(t *testing.T, b *BadgerEngine, id NodeID, text string, chunks int, seed float32) {
	t.Helper()
	ctx := t.Context()
	_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false, "a": int64(1)}})
	require.NoError(t, err)
	stored, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	embedded := CopyNode(stored)
	embedded.ChunkEmbeddings = largeRecordVectors(chunks, seed)
	embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": largeRecordDims, "embedding_model": "synthetic"}
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(ctx, embedded))
	got, err := b.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, largeRecordVectors(chunks, seed), got.ChunkEmbeddings)
}

// largeRecordPrepare stages edit on each node in one explicit transaction and
// returns the journal payload its commit would write, without applying it:
// exactly the crash point after the record and before native application.
func largeRecordPrepare(t *testing.T, b *BadgerEngine, logicalID string, edits map[NodeID]func(*Node)) []byte {
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

// largeRecordJournal appends payload as a transaction record of namespace
// "test".
func largeRecordJournal(t *testing.T, w *WAL, payload []byte) {
	t.Helper()
	require.NoError(t, w.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
}

func requireLargeRecordVectors(t *testing.T, b *BadgerEngine, id NodeID, chunks int, seed float32) *Node {
	t.Helper()
	got, err := b.GetNode(t.Context(), id)
	require.NoError(t, err)
	require.Equal(t, largeRecordVectors(chunks, seed), got.ChunkEmbeddings, "stored vectors of %s", id)
	require.Equal(t, true, got.EmbedMeta["has_embedding"], "embedding metadata of %s", id)
	return got
}

// The defect itself: one property changed on a node with 17 MB of vectors
// (above the 16 MiB WAL frame). main refuses the commit with "wal: entry
// exceeds maximum WAL frame size"; it must commit, journal the whole image,
// and keep the vectors through a crash and the recovery at open.
func TestLargeNodeUpdateAboveFrameLimitCommits(t *testing.T) {
	dir := t.TempDir()
	b, w := largeRecordEngine(t, dir, WALRecordsLarge)
	const chunks = 17000 // x 256 dims x 4 bytes = 17.4 MB
	largeRecordSeed(t, b, "test:large", "alpha", chunks, 1)
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, "test:large")
	require.NoError(t, err)
	node = CopyNode(node)
	node.Properties["flag"] = true
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "above-frame"})
	require.NoError(t, err)
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err, "a record larger than one frame must commit")
	require.NoError(t, receipt.Wait(ctx))
	got := requireLargeRecordVectors(t, b, "test:large", chunks, 1)
	require.Equal(t, true, got.Properties["flag"])

	// The journal holds the whole image as one logical record.
	entries, err := ReadWALEntriesFromDir(ctx, w.config.Dir)
	require.NoError(t, err)
	var records int
	for _, entry := range entries {
		if entry.Operation != OpEntityCommit {
			continue
		}
		var data walEntityCommitData
		require.NoError(t, decodeRecoveryPayload(entry.Data, &data))
		envelope, ops, err := DecodeEntityTransaction(data.Payload)
		require.NoError(t, err)
		if envelope.LogicalTxID != "above-frame" {
			continue
		}
		records++
		require.Greater(t, len(data.Payload), int(walMaxEntrySize), "the record is larger than one frame")
		require.Equal(t, EntityTransactionVersion, envelope.Version)
		require.Len(t, ops, 1)
		require.Equal(t, largeRecordVectors(chunks, 1), ops[0].Node.ChunkEmbeddings, "the record carries the whole image")
	}
	require.Equal(t, 1, records)

	b, _ = largeRecordReopen(t, b, w, dir)
	got = requireLargeRecordVectors(t, b, "test:large", chunks, 1)
	require.Equal(t, true, got.Properties["flag"])
}
