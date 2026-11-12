package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// An update that leaves a node's vectors alone must leave the node holding
// exactly the vectors and embedding metadata it held before: whichever way the
// engine stores them (inside the body, apart from a large body, in the
// embedding sidecar, as named vectors), on every commit path (a plain commit,
// an accepted commit, an accepted commit whose first attempt conflicts and is
// staged again from its published images, the replay at open of an accepted
// commit journaled but not applied before a crash, the recovery of a buffered
// commit after a crash) and in a store rebuilt from a snapshot plus the
// journal, with one and two updates in the transaction, with synchronous
// writes and with the write buffer.
//
// The commit published by a large node whose vectors live in the sidecar
// carried the body codec's {chunk_count: 0} as its embedding metadata. Every
// path that installs that image again as stored state (the retried accepted
// commit, journal replay, a snapshot taken from the write buffer) cleared the
// sidecar and stored a body that loads no vectors.
func TestUpdateKeepingVectorsKeepsStoredEmbeddingState(t *testing.T) {
	const id = NodeID("test:doc")
	large := strings.Repeat("large text ", 30000)
	vectors := [][]float32{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}}
	named := map[string][]float32{"title": {1, 2, 3, 4}}
	managed := func() map[string]any {
		return map[string]any{"has_embedding": true, "chunk_count": 3, "embedding_model": "kept-state-test", "embedding_dimensions": 4}
	}
	sidecar := func(text string, chunks [][]float32, meta map[string]any, namedVectors map[string][]float32) func(t *testing.T, b *BadgerEngine) {
		return func(t *testing.T, b *BadgerEngine) {
			seedKeptStateNode(t, b, &Node{ID: id, Properties: map[string]any{"text": text}, NamedEmbeddings: namedVectors})
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(context.Background(), embeddingWriteback(t, b, id, chunks, meta, time.Time{})))
		}
	}
	inBody := func(text string, chunks [][]float32, meta map[string]any, namedVectors map[string][]float32) func(t *testing.T, b *BadgerEngine) {
		return func(t *testing.T, b *BadgerEngine) {
			seedKeptStateNode(t, b, &Node{ID: id, Properties: map[string]any{"text": text}, ChunkEmbeddings: chunks, EmbedMeta: meta, NamedEmbeddings: namedVectors})
		}
	}
	shapes := []struct {
		name string
		seed func(t *testing.T, b *BadgerEngine)
		// grow, when set, is a value the update adds, so the update moves
		// the node past the inline size.
		grow string
	}{
		{name: "sidecar, small text", seed: sidecar("small text", vectors, managed(), nil)},
		{name: "sidecar, large text", seed: sidecar(large, vectors, managed(), nil)},
		{name: "sidecar and named, large text", seed: sidecar(large, vectors, managed(), named)},
		{name: "sidecar metadata without vectors, large text", seed: sidecar(large, nil, map[string]any{"chunk_count": 0, "marker": "keep"}, nil)},
		{name: "in the body, small text", seed: inBody("small text", vectors, managed(), nil)},
		{name: "in the body, the update outgrows the body", seed: inBody("small text", vectors, managed(), nil), grow: strings.Repeat("y", 80<<10)},
		{name: "apart from the body, large text", seed: inBody(large, vectors, managed(), nil)},
		{name: "named, small text", seed: inBody("small text", nil, nil, named)},
		{name: "named, large text", seed: inBody(large, nil, nil, named)},
		{name: "no vectors, large text", seed: inBody(large, nil, nil, nil)},
	}
	paths := []string{"plain commit", "accepted commit", "retried accepted commit",
		"journal replay after a crash", "journal replay after a crash, two updates",
		"buffered commit recovered after a crash", "buffered commit recovered after a crash, two updates",
		"rebuild from a snapshot and the journal", "rebuild from a snapshot and the journal, two updates",
		"rebuild from a snapshot taken after the update"}
	for _, writeBuffer := range []bool{false, true} {
		for _, shape := range shapes {
			for _, path := range paths {
				t.Run(fmt.Sprintf("writeBuffer=%v/%s/%s", writeBuffer, shape.name, path), func(t *testing.T) {
					dir := t.TempDir()
					open := func() *BadgerEngine {
						b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, wal: true, synchronous: !writeBuffer, manualDrain: true})
						b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
						return b
					}
					b := open()
					shape.seed(t, b)
					before := keptStateRead(t, b, id, true)

					b = keptStateUpdate(t, b, id, path, shape.grow, dir, open)

					rebuilt := strings.HasPrefix(path, "rebuild")
					require.Equal(t, before, keptStateRead(t, b, id, !rebuilt), "after the update")
					if rebuilt {
						return
					}
					require.NoError(t, b.Close())
					require.NoError(t, b.bufferedCommitWAL.Close())
					b = open()
					require.Equal(t, before, keptStateRead(t, b, id, true), "after a reopen")
				})
			}
		}
	}
}

func seedKeptStateNode(t *testing.T, b *BadgerEngine, node *Node) {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	node.Labels, node.CreatedAt, node.UpdatedAt = []string{"Doc"}, at, at
	node.Properties["flag"] = false
	_, err := b.CreateNode(context.Background(), node)
	require.NoError(t, err)
}

// keptStateRead is the node's embedding state as a new transaction reads it
// from storage; with engine set, the engine's own read must return the same.
// The metadata is compared without the keys the store writes by itself to lay
// out and date it (the chunk count, the content stamp, the range manifest):
// the vectors themselves show the count.
func keptStateRead(t *testing.T, b *BadgerEngine, id NodeID, engine bool) string {
	t.Helper()
	ctx := context.Background()
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	stored, err := tx.GetNode(ctx, id)
	require.NoError(t, err)
	state := func(n *Node) string {
		meta := map[string]any{}
		for key, value := range n.EmbedMeta {
			if key != "chunk_count" && key != embeddingContentUpdatedAtKey && key != embeddingRangeManifestKey {
				meta[key] = value
			}
		}
		return fmt.Sprintf("vectors=%v named=%v meta=%v", n.ChunkEmbeddings, n.NamedEmbeddings, meta)
	}
	if engine {
		read, err := b.GetNode(ctx, id)
		require.NoError(t, err)
		require.Equal(t, state(stored), state(read), "a stored read and the engine's read")
	}
	return state(stored)
}

// keptStateUpdate changes properties of the node, leaving its embedded text
// and its vectors alone, on the named path, and returns the engine that holds
// the result.
func keptStateUpdate(t *testing.T, b *BadgerEngine, id NodeID, path, grow, dir string, open func() *BadgerEngine) *BadgerEngine {
	t.Helper()
	ctx := context.Background()
	update := func(tx *BadgerTransaction, read *Node, property string) {
		n := CopyNode(read)
		n.Properties[property] = true
		if grow != "" {
			n.Properties["grown"] = grow
		}
		n.UpdatedAt = n.UpdatedAt.Add(time.Second)
		require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: n}))
	}
	read := func(tx *BadgerTransaction) *Node {
		n, err := tx.GetNode(ctx, id)
		require.NoError(t, err)
		return n
	}
	// stage makes the transaction's updates: one, or two when path says so.
	stage := func(tx *BadgerTransaction) {
		update(tx, read(tx), "flag")
		if strings.HasSuffix(path, ", two updates") {
			update(tx, read(tx), "second")
		}
	}
	// begin starts an accepted transaction, or else a plain one: with the
	// write buffer on, an autocommit one, which the buffer takes.
	begin := func(accepted bool) *BadgerTransaction {
		var tx *BadgerTransaction
		var err error
		if accepted {
			tx, err = BeginEntityTransaction(ctx, b, CommitModeExplicit)
		} else {
			tx, err = b.BeginTransaction(ctx)
			if err == nil && b.writeBehind != nil {
				err = tx.SetImplicit(true)
			}
		}
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		return tx
	}
	submit := func(tx *BadgerTransaction, logicalID string) {
		prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
		require.NoError(t, err)
		receipt, err := NewWALEngine(b, b.bufferedCommitWAL).SubmitEntityCommit(ctx, prepared, nil)
		require.NoError(t, err)
		require.NoError(t, receipt.WaitOutcome(ctx).Err)
	}
	// journal stages an accepted transaction and journals its record without
	// applying it: the server stops between the journal and the store.
	journal := func() {
		tx := begin(true)
		stage(tx)
		prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "replayed"})
		require.NoError(t, err)
		record, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
		require.NoError(t, err)
		prepared.Abort()
		require.NoError(t, b.bufferedCommitWAL.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: record}, "test"))
		require.NoError(t, b.bufferedCommitWAL.Sync())
	}
	// recoverAtOpen stops the server without draining it and opens the
	// store again, which applies what the journal holds.
	recoverAtOpen := func() {
		crashClose(t, b, b.bufferedCommitWAL)
		b = open()
		_, err := RecoverBufferedCommitsAtOpen(b, b.bufferedCommitWAL)
		require.NoError(t, err)
	}
	snapshot := func() string {
		snapshot, err := b.bufferedCommitWAL.CreateSnapshot(b)
		require.NoError(t, err)
		path := filepath.Join(dir, "snapshot.json")
		require.NoError(t, SaveSnapshot(snapshot, path))
		return path
	}
	// rebuild writes an empty store from the snapshot plus the journal, as
	// the rebuild of a damaged store does.
	rebuild := func(snapshotPath string) *BadgerEngine {
		destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
		require.NoError(t, err)
		result, _, err := RecoverIntoEngine(ctx, destination, filepath.Join(dir, "wal"), snapshotPath)
		require.NoError(t, err)
		require.Zero(t, result.Failed, "%v", result.Errors)
		return destination
	}
	switch strings.TrimSuffix(path, ", two updates") {
	case "plain commit":
		tx := begin(false)
		stage(tx)
		require.NoError(t, tx.Commit())
	case "accepted commit":
		tx := begin(true)
		stage(tx)
		submit(tx, "update")
	case "retried accepted commit":
		// The waiter reads the node, a writer commits an update of it, and the
		// waiter then updates the node under its write lock. Its first attempt
		// conflicts with the writer's commit and is staged again from the
		// images it published.
		waiter := begin(true)
		read(waiter)
		writer := begin(true)
		update(writer, read(writer), "writer")
		submit(writer, "writer")
		latest, err := waiter.LockNodeForWrite(ctx, id)
		require.NoError(t, err)
		update(waiter, latest, "waiter")
		submit(waiter, "waiter")
	case "journal replay after a crash":
		journal()
		recoverAtOpen()
	case "buffered commit recovered after a crash":
		// With the write buffer on, the commit is acknowledged from the
		// buffer and journaled; the server stops before the buffer drains.
		tx := begin(false)
		stage(tx)
		require.NoError(t, tx.Commit())
		recoverAtOpen()
	case "rebuild from a snapshot and the journal":
		snapshotPath := snapshot()
		journal()
		crashClose(t, b, b.bufferedCommitWAL)
		return rebuild(snapshotPath)
	case "rebuild from a snapshot taken after the update":
		// With the write buffer on, the snapshot takes the node from the
		// buffer, as the image the commit published.
		tx := begin(false)
		stage(tx)
		require.NoError(t, tx.Commit())
		snapshotPath := snapshot()
		require.NoError(t, b.Close())
		require.NoError(t, b.bufferedCommitWAL.Close())
		return rebuild(snapshotPath)
	default:
		t.Fatalf("unknown commit path %q", path)
	}
	return b
}
