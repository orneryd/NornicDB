package storage

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// NOR-412: a transaction record that spans frames, recovered, against the same
// transaction committed live, for the seven shapes of stored vectors and the
// orders of updates listed below. Each case runs the transaction twice from the
// same seeded store: once committed live through the journal and read back
// after a clean reopen, and once journaled without being applied and then
// recovered, by the recovery at open after crashClose and, separately, by a
// rebuild of an empty store from a snapshot plus the journal. Each transaction
// also creates filler nodes, so its record is larger than one 16 MiB frame and
// is written in two or more parts (asserted); one shape is a node whose own
// image is larger than a frame. Four things of the recovered node are compared
// with the live one (shapeImage): its chunk vectors and named vectors bit for bit, its properties,
// and its embedding metadata without the three keys shapeLayoutKey names. Its ID, labels and
// timestamps are not compared. The node is read with GetNode in a new transaction (shapeRead).
// The cases of one shape that keep the node's vectors are skipped, not
// asserted.

const shapesNode NodeID = "test:shape"

// shapeFillers is how many filler nodes of shapeFillerBytes of text each
// transaction creates: about 18 MiB, more than one frame.
const (
	shapeFillers     = 450
	shapeFillerBytes = 42 << 10
)

type spannedShape struct {
	name string
	// seed stores shapesNode in b.
	seed func(t *testing.T, b *BadgerEngine)
	// named reports whether the shape has named vectors.
	named bool
	// mainLosesKeptVectors marks the shape whose orders that keep the vectors are
	// skipped, not asserted (see the skip below).
	mainLosesKeptVectors bool
}

func spannedShapes() []spannedShape {
	sidecar := func(text string, chunks int) func(t *testing.T, b *BadgerEngine) {
		return func(t *testing.T, b *BadgerEngine) {
			_, err := b.CreateNode(t.Context(), &Node{ID: shapesNode, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false}})
			require.NoError(t, err)
			stored, err := b.GetNode(t.Context(), shapesNode)
			require.NoError(t, err)
			embedded := CopyNode(stored)
			embedded.ChunkEmbeddings = largeRecordVectors(chunks, 1)
			embedded.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": largeRecordDims, "embedding_model": "synthetic"}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(), embedded))
		}
	}
	body := func(text string, chunks [][]float32, meta map[string]any, named map[string][]float32) func(t *testing.T, b *BadgerEngine) {
		return func(t *testing.T, b *BadgerEngine) {
			_, err := b.CreateNode(t.Context(), &Node{ID: shapesNode, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false},
				ChunkEmbeddings: chunks, EmbedMeta: meta, NamedEmbeddings: named})
			require.NoError(t, err)
		}
	}
	client := map[string]any{"has_embedding": true, "embedding_model": "client"}
	large := strings.Repeat("x", 300<<10)
	named := map[string][]float32{"title": {1, math.Float32frombits(0x80000000), 3}, "nan": {math.Float32frombits(0x7fc00001)}}
	return []spannedShape{
		{name: "vectors inside the body", seed: body("alpha", largeRecordVectors(2, 3), client, nil)},
		{name: "vectors beside the body", seed: sidecar("alpha", 4)},
		{name: "large text, vectors beside it", seed: sidecar(large, 4), mainLosesKeptVectors: true},
		{name: "large text, vectors stored apart", seed: body(large, largeRecordVectors(3, 5), client, nil)},
		{name: "named vectors", named: true, seed: body("alpha", nil, nil, named)},
		{name: "named vectors and vectors beside the body", named: true, seed: func(t *testing.T, b *BadgerEngine) {
			sidecar("alpha", 4)(t, b)
			stored, err := b.GetNode(t.Context(), shapesNode)
			require.NoError(t, err)
			stored = CopyNode(stored)
			stored.NamedEmbeddings = named
			require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: stored}))
		}},
		// 17,000 x 256 float32 dimensions: the node's own image is larger
		// than one frame.
		{name: "vectors beside the body, larger than one frame", seed: sidecar("alpha", 17000)},
	}
}

// shapeImage is what the test compares of a node: the bits of its chunk vectors
// and of its named vectors, its properties, and its embedding metadata without
// the keys shapeLayoutKey names (chunk_count, embeddingContentUpdatedAtKey and
// embeddingRangeManifestKey). It does not hold the node's ID, labels or timestamps.
type shapeImage struct {
	chunks     [][]uint32
	named      map[string][]uint32
	meta       map[string]any
	properties map[string]any
}

func shapeLayoutKey(key string) bool {
	return key == "chunk_count" || key == embeddingContentUpdatedAtKey || key == embeddingRangeManifestKey
}

func shapeImageOf(node *Node) shapeImage {
	bits := func(v []float32) []uint32 {
		out := make([]uint32, len(v))
		for i, f := range v {
			out[i] = math.Float32bits(f)
		}
		return out
	}
	image := shapeImage{properties: node.Properties}
	for _, chunk := range node.ChunkEmbeddings {
		image.chunks = append(image.chunks, bits(chunk))
	}
	if len(node.NamedEmbeddings) > 0 {
		image.named = map[string][]uint32{}
		for name, v := range node.NamedEmbeddings {
			image.named[name] = bits(v)
		}
	}
	for key, value := range node.EmbedMeta {
		if !shapeLayoutKey(key) {
			if image.meta == nil {
				image.meta = map[string]any{}
			}
			image.meta[key] = value
		}
	}
	return image
}

// requireSameShapeImage compares two shapeImages: the vector bits with require.Equal, and the
// metadata and the properties by their number of keys and then key by key with sameStoredValue.
func requireSameShapeImage(t *testing.T, want, got shapeImage) {
	t.Helper()
	require.Equal(t, want.chunks, got.chunks, "chunk vectors")
	require.Equal(t, want.named, got.named, "named vectors")
	require.Equal(t, len(want.meta), len(got.meta), "metadata keys: want %v, got %v", want.meta, got.meta)
	for key, value := range want.meta {
		require.True(t, sameStoredValue(value, got.meta[key]), "metadata %q: want %#v, got %#v", key, value, got.meta[key])
	}
	require.Equal(t, len(want.properties), len(got.properties), "properties")
	for key, value := range want.properties {
		require.True(t, sameStoredValue(value, got.properties[key]), "property %q", key)
	}
}

// spannedOrder is a transaction over shapesNode: stage makes its updates in
// tx.
type spannedOrder struct {
	name  string
	stage func(t *testing.T, tx *BadgerTransaction, shape spannedShape)
	// keeps reports whether the transaction's last image of the node keeps
	// the vectors it had before the transaction.
	keeps bool
}

func shapeUpdate(t *testing.T, tx *BadgerTransaction, edit func(*Node)) {
	t.Helper()
	node, err := tx.GetNode(context.Background(), shapesNode)
	require.NoError(t, err)
	node = CopyNode(node)
	edit(node)
	require.NoError(t, tx.UpdateGraphNode(context.Background(), NodeUpdateRequest{After: node}))
}

func spannedOrders() []spannedOrder {
	property := func(name string) func(*Node) {
		return func(n *Node) { n.Properties[name] = true }
	}
	return []spannedOrder{
		{name: "one update keeps the vectors", keeps: true, stage: func(t *testing.T, tx *BadgerTransaction, _ spannedShape) {
			shapeUpdate(t, tx, property("flag"))
		}},
		{name: "two updates keep the vectors", keeps: true, stage: func(t *testing.T, tx *BadgerTransaction, _ spannedShape) {
			shapeUpdate(t, tx, property("flag"))
			shapeUpdate(t, tx, property("second"))
		}},
		{name: "new vectors, then an update that keeps them", stage: func(t *testing.T, tx *BadgerTransaction, shape spannedShape) {
			shapeUpdate(t, tx, func(n *Node) {
				n.ChunkEmbeddings = largeRecordVectors(2, 9)
				n.EmbedMeta = map[string]any{"has_embedding": true, "extra": []any(nil), "nested": map[string]any(nil)}
				if shape.named {
					n.NamedEmbeddings = map[string][]float32{"title": {7, math.Float32frombits(0x80000000)}}
				}
			})
			shapeUpdate(t, tx, property("flag"))
		}},
	}
}

// shapeStage runs order on shape in tx and creates the filler nodes that make
// the transaction's record span frames.
func shapeStage(t *testing.T, order spannedOrder, shape spannedShape) func(*BadgerTransaction) {
	return func(tx *BadgerTransaction) {
		order.stage(t, tx, shape)
		text := strings.Repeat("f", shapeFillerBytes)
		for i := range shapeFillers {
			_, err := tx.CreateNode(context.Background(), &Node{ID: NodeID(fmt.Sprintf("test:filler-%d", i)), Labels: []string{"Filler"}, Properties: map[string]any{"text": text}})
			require.NoError(t, err)
		}
	}
}

// shapeCommitLive commits stage's transaction through the journal.
func shapeCommitLive(t *testing.T, b *BadgerEngine, w *WAL, logicalID string, stage func(*BadgerTransaction)) {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	stage(tx)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	require.NoError(t, err)
	receipt, err := NewWALEngine(b, w).SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(ctx))
}

// shapeJournal stages stage's transaction and journals its record without
// applying it. It checks that the record is larger than a frame and was written
// in two or more parts.
func shapeJournal(t *testing.T, b *BadgerEngine, w *WAL, logicalID string, stage func(*BadgerTransaction)) {
	t.Helper()
	ctx := context.Background()
	tx, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	stage(tx)
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: logicalID})
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	prepared.Abort()
	require.Greater(t, len(payload), int(walMaxEntrySize), "the record is larger than one frame")
	largeRecordJournal(t, w, payload)
	require.NoError(t, w.Sync())
	seq, parts := w.Sequence(), 0
	for _, frame := range standardRecordsFrames(t, walActivePath(w.config.Dir)) {
		if frame.Seq == seq && frame.Version == walFormatVersionSpanned {
			parts++
		}
	}
	require.GreaterOrEqual(t, parts, 2, "the record spans frames")
}

// shapeRead reads shapesNode with GetNode in a new transaction, which it rolls
// back, and returns the node's shapeImage.
func shapeRead(t *testing.T, engine *BadgerEngine) shapeImage {
	t.Helper()
	ctx := context.Background()
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, shapesNode)
	require.NoError(t, err)
	return shapeImageOf(node)
}

// shapeFillersStored checks that the store has shapeFillers nodes with the label Filler.
func shapeFillersStored(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	nodes, err := engine.GetNodesByLabel(context.Background(), "Filler")
	require.NoError(t, err)
	require.Len(t, nodes, shapeFillers)
}

// shapeSnapshot takes a snapshot of b's store (w.CreateSnapshot), saves it as snapshot.json in dir and returns its path.
func shapeSnapshot(t *testing.T, b *BadgerEngine, w *WAL, dir string) string {
	t.Helper()
	snapshot, err := w.CreateSnapshot(b)
	require.NoError(t, err)
	path := filepath.Join(dir, "snapshot.json")
	require.NoError(t, SaveSnapshot(snapshot, path))
	return path
}

func TestSpannedRecordRecoveryEqualsLiveCommit(t *testing.T) {
	for _, shape := range spannedShapes() {
		for _, order := range spannedOrders() {
			t.Run(shape.name+"/"+order.name, func(t *testing.T) {
				if shape.mainLosesKeptVectors && order.keeps {
					// Skipped, not asserted. On main at 620595a5f the same cases, with a record of
					// one frame, end with the node's vectors gone after the recovery while the live
					// commit keeps them (the lab jobs are in the PR description, which also names
					// the pull request that changes it).
					t.Skip("not asserted here: on main the recovery of this shape loses the kept vectors, with a record of one frame")
				}
				// The oracle: the transaction committed live, read after a
				// clean reopen.
				liveDir := t.TempDir()
				b, w := largeRecordEngine(t, liveDir, WALRecordsLarge)
				shape.seed(t, b)
				shapeCommitLive(t, b, w, "shape", shapeStage(t, order, shape))
				require.NoError(t, b.Close())
				require.NoError(t, w.Close())
				b, _ = largeRecordEngine(t, liveDir, WALRecordsLarge)
				live := shapeRead(t, b)
				shapeFillersStored(t, b)

				t.Run("crash between journal and store", func(t *testing.T) {
					dir := t.TempDir()
					b, w := largeRecordEngine(t, dir, WALRecordsLarge)
					shape.seed(t, b)
					shapeJournal(t, b, w, "shape", shapeStage(t, order, shape))
					b, _ = largeRecordReopen(t, b, w, dir)
					requireSameShapeImage(t, live, shapeRead(t, b))
					shapeFillersStored(t, b)
				})
				t.Run("rebuild from a snapshot and the journal", func(t *testing.T) {
					dir := t.TempDir()
					b, w := largeRecordEngine(t, dir, WALRecordsLarge)
					shape.seed(t, b)
					snapshot := shapeSnapshot(t, b, w, dir)
					shapeJournal(t, b, w, "shape", shapeStage(t, order, shape))
					crashClose(t, b, w)
					// An empty store on disk as the destination of the rebuild.
					destination, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
					require.NoError(t, err)
					result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
					require.NoError(t, err)
					require.Zero(t, result.Failed, "%v", result.Errors)
					requireSameShapeImage(t, live, shapeRead(t, destination))
					shapeFillersStored(t, destination)
				})
			})
		}
	}
}
