package storage

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// NOR-412, round 7: a record that leaves vectors out identifies the image an
// intact store holds when the record is replayed, for every shape of stored
// vector state and every order of updates. Each case runs the same
// transaction twice from the same seeded store: once committed live and read
// back after a clean reopen (the oracle), and once journaled without being
// applied, then recovered, either by the crash recovery that runs at open or by
// a rebuild of an empty store from a snapshot plus the journal. The recovered
// node must equal the live one: chunk vectors and named vectors bit for bit,
// embedding metadata value for value without the store's layout keys, and
// properties. Every read is a stored read in a fresh transaction: the
// engine's node cache can present another image of a large body whose vectors
// live in the sidecar (writtenNodeImage keeps the body codec's placeholder
// metadata), which is not what the store holds.

const shapesNode NodeID = "test:shape"

// shapeAwkwardMetadata holds values the storage and record codecs represent
// differently in memory: containers left nil, nested lists and maps, numbers of
// several Go types, a float32 NaN payload, bytes and an instant.
func shapeAwkwardMetadata(chunks int) map[string]any {
	return map[string]any{
		"has_embedding": true, "chunk_count": chunks, "embedding_dimensions": keptVectorDims, "embedding_model": "synthetic",
		"embedded_at": "2026-11-11T00:00:00Z", "nil_list": []any(nil), "nil_map": map[string]any(nil), "empty_list": []any{},
		"chunk_spans": []any{[]any{int64(0), "text", int64(0), int64(5)}},
		"source":      map[string]any{"properties": []string{"text"}, "labels": []any(nil), "version": uint16(2)},
		"score":       float32(0.25), "nan": math.Float32frombits(0x7fc00001), "big": uint64(1) << 63,
		"bytes": []byte{1, 2, 3}, "at": time.Date(2026, 11, 11, 12, 0, 0, 5, time.UTC),
	}
}

type vectorShape struct {
	name string
	// seed stores shapesNode in b.
	seed func(t *testing.T, b *BadgerEngine)
	// keeps names the groups an update of one property keeps as stored, so
	// its record leaves them out.
	keeps vectorRetention
	// grow, when set, is a property each update adds to the node.
	grow string
}

const (
	keepsManaged = retainedManagedEmbeddings
	keepsNamed   = retainedNamedEmbeddings
)

func vectorShapes() []vectorShape {
	sidecar := func(text string, meta map[string]any) func(t *testing.T, b *BadgerEngine) {
		return func(t *testing.T, b *BadgerEngine) {
			_, err := b.CreateNode(t.Context(), &Node{ID: shapesNode, Labels: []string{"Doc"}, Properties: map[string]any{"text": text, "flag": false}})
			require.NoError(t, err)
			stored, err := b.GetNode(t.Context(), shapesNode)
			require.NoError(t, err)
			embedded := CopyNode(stored)
			embedded.ChunkEmbeddings = keptVectors(4, 1)
			embedded.EmbedMeta = meta
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
	workerMeta := func() map[string]any {
		return map[string]any{"has_embedding": true, "chunk_count": 4, "embedding_dimensions": keptVectorDims, "embedding_model": "synthetic"}
	}
	large := strings.Repeat("x", 300<<10)
	named := map[string][]float32{"title": {1, math.Float32frombits(0x80000000), 3}, "nan": {math.Float32frombits(0x7fc00001)}}
	return []vectorShape{
		{name: "inline client vectors", keeps: keepsManaged, seed: body("alpha", keptVectors(2, 3), map[string]any{"has_embedding": true, "embedding_model": "client"}, nil)},
		{name: "inline client vectors, awkward metadata", keeps: keepsManaged, seed: body("alpha", keptVectors(2, 3), shapeAwkwardMetadata(2), nil)},
		{name: "sidecar worker vectors", keeps: keepsManaged, seed: sidecar("alpha", workerMeta())},
		{name: "sidecar worker vectors, awkward metadata", keeps: keepsManaged, seed: sidecar("alpha", shapeAwkwardMetadata(4))},
		{name: "large body, sidecar worker vectors", keeps: keepsManaged, seed: sidecar(large, workerMeta())},
		{name: "large body, vectors stored apart", keeps: keepsManaged, seed: body(large, keptVectors(3, 5), map[string]any{"has_embedding": true, "embedding_model": "client"}, nil)},
		{name: "inline vectors, the update outgrows the body", keeps: keepsManaged, grow: strings.Repeat("y", 80<<10),
			seed: body("alpha", keptVectors(2, 3), map[string]any{"has_embedding": true, "embedding_model": "client"}, nil)},
		// A large body without vectors stores the codec's metadata
		// {"chunk_count": 0}: metadata without vectors, kept.
		{name: "large body without vectors", keeps: keepsManaged, seed: body(large, nil, nil, nil)},
		// A permanent embedding failure is not carried by an update (main's
		// rule): the update drops it and its record is whole.
		{name: "metadata of a failed embedding", seed: body("alpha", nil, map[string]any{"has_embedding": false, "embedding_failed": true, "embedding_error": "too long"}, nil)},
		{name: "named vectors", keeps: keepsNamed, seed: body("alpha", nil, nil, named)},
		{name: "named and sidecar vectors", keeps: keepsManaged | keepsNamed, seed: func(t *testing.T, b *BadgerEngine) {
			sidecar("alpha", workerMeta())(t, b)
			stored, err := b.GetNode(t.Context(), shapesNode)
			require.NoError(t, err)
			stored = CopyNode(stored)
			stored.NamedEmbeddings = named
			require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: stored}))
		}},
	}
}

// shapeImage is a node's state as a stored read returns it, for comparison:
// metadata without the store's layout keys.
type shapeImage struct {
	chunks     [][]uint32
	named      map[string][]uint32
	meta       map[string]any
	properties map[string]any
}

func shapeImageOf(t *testing.T, node *Node) shapeImage {
	t.Helper()
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
		if !embeddingLayoutKey(key) {
			if image.meta == nil {
				image.meta = map[string]any{}
			}
			image.meta[key] = value
		}
	}
	return image
}

// requireSameShapeImage compares two stored reads; metadata values by value,
// as the stored-value comparison does.
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

// vectorOrder is a transaction over shapesNode. stage makes its updates in tx;
// kept says which groups the journal record of its last update must mark.
type vectorOrder struct {
	name  string
	stage func(t *testing.T, tx *BadgerTransaction, shape vectorShape)
	// kept reports, for a shape, which groups the record's updates leave out.
	kept func(shape vectorShape) vectorRetention
}

func shapeUpdate(t *testing.T, tx *BadgerTransaction, edit func(*Node)) {
	t.Helper()
	node, err := tx.GetNode(context.Background(), shapesNode)
	require.NoError(t, err)
	node = CopyNode(node)
	edit(node)
	require.NoError(t, tx.UpdateGraphNode(context.Background(), NodeUpdateRequest{After: node}))
}

func vectorOrders() []vectorOrder {
	keptGroups := func(shape vectorShape) vectorRetention { return shape.keeps }
	property := func(name string, shape vectorShape) func(*Node) {
		return func(n *Node) {
			n.Properties[name] = true
			if shape.grow != "" {
				n.Properties["grown"] = shape.grow
			}
		}
	}
	return []vectorOrder{
		{name: "one update keeps the vectors", kept: keptGroups, stage: func(t *testing.T, tx *BadgerTransaction, shape vectorShape) {
			shapeUpdate(t, tx, property("flag", shape))
		}},
		{name: "two updates keep the vectors", kept: keptGroups, stage: func(t *testing.T, tx *BadgerTransaction, shape vectorShape) {
			shapeUpdate(t, tx, property("flag", shape))
			shapeUpdate(t, tx, property("second", shape))
		}},
		{name: "new vectors, then an update that keeps them", kept: func(vectorShape) vectorRetention { return 0 },
			stage: func(t *testing.T, tx *BadgerTransaction, shape vectorShape) {
				shapeUpdate(t, tx, func(n *Node) {
					n.ChunkEmbeddings = keptVectors(2, 9)
					n.EmbedMeta = map[string]any{"has_embedding": true, "extra": []any(nil), "nested": map[string]any(nil)}
					if shape.keeps&keepsNamed != 0 {
						n.NamedEmbeddings = map[string][]float32{"title": {7, math.Float32frombits(0x80000000)}}
					}
				})
				shapeUpdate(t, tx, property("flag", shape))
			}},
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
// applying it, the crash point between journal and store.
func shapeJournal(t *testing.T, b *BadgerEngine, w *WAL, logicalID string, stage func(*BadgerTransaction)) []byte {
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
	keptVectorsJournal(t, w, payload)
	return payload
}

func requireRecordMarks(t *testing.T, payload []byte, want vectorRetention) {
	t.Helper()
	envelope, ops, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	var marks vectorRetention
	for _, op := range ops {
		marks |= op.retainedVectors
	}
	require.Equal(t, want, marks, "groups the record leaves out")
	if want != 0 {
		require.Equal(t, EntityTransactionVersion, envelope.Version)
		require.NotNil(t, envelope.retainedVectorIDs)
	}
}

// shapeRead reads the node as the store holds it: a stored read in a fresh
// transaction, not the engine's node cache.
func shapeRead(t *testing.T, engine *BadgerEngine) shapeImage {
	t.Helper()
	ctx := context.Background()
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(ctx, shapesNode)
	require.NoError(t, err)
	return shapeImageOf(t, node)
}

// shapeRebuildDestination is an empty store on disk, as the rebuild of a
// damaged store writes one.
func shapeRebuildDestination(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	return engine
}

func TestKeptVectorsRecoveryEqualsLiveCommit(t *testing.T) {
	for _, shape := range vectorShapes() {
		for _, order := range vectorOrders() {
			stage := func(t *testing.T) func(*BadgerTransaction) {
				return func(tx *BadgerTransaction) { order.stage(t, tx, shape) }
			}
			t.Run(shape.name+"/"+order.name, func(t *testing.T) {
				// The oracle: the transaction committed live, read after a clean reopen.
				liveDir := t.TempDir()
				b, w := keptVectorsEngine(t, liveDir)
				shape.seed(t, b)
				shapeCommitLive(t, b, w, "shape", stage(t))
				require.NoError(t, b.Close())
				require.NoError(t, w.Close())
				b, _ = keptVectorsEngine(t, liveDir)
				live := shapeRead(t, b)

				t.Run("crash between journal and store", func(t *testing.T) {
					dir := t.TempDir()
					b, w := keptVectorsEngine(t, dir)
					shape.seed(t, b)
					payload := shapeJournal(t, b, w, "shape", stage(t))
					requireRecordMarks(t, payload, order.kept(shape))
					b, _ = keptVectorsReopen(t, b, w, dir)
					requireSameShapeImage(t, live, shapeRead(t, b))
				})
				t.Run("rebuild from a snapshot and the journal", func(t *testing.T) {
					dir := t.TempDir()
					b, w := keptVectorsEngine(t, dir)
					shape.seed(t, b)
					snapshot := keptVectorsSnapshot(t, b, w, dir, nil)
					shapeJournal(t, b, w, "shape", stage(t))
					crashClose(t, b, w)
					destination := shapeRebuildDestination(t)
					result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
					require.NoError(t, err)
					require.Zero(t, result.Failed, "%v", result.Errors)
					requireSameShapeImage(t, live, shapeRead(t, destination))
				})
			})
		}
	}
}

// An update journaled after an earlier one was applied: its predecessor is the
// store the earlier commit left, and the record identifies that.
func TestKeptVectorsRecoveryAfterAnAppliedUpdate(t *testing.T) {
	for _, shape := range vectorShapes() {
		t.Run(shape.name, func(t *testing.T) {
			first := func(tx *BadgerTransaction) { shapeUpdate(t, tx, func(n *Node) { n.Properties["first"] = true }) }
			second := func(tx *BadgerTransaction) { shapeUpdate(t, tx, func(n *Node) { n.Properties["second"] = true }) }

			liveDir := t.TempDir()
			b, w := keptVectorsEngine(t, liveDir)
			shape.seed(t, b)
			shapeCommitLive(t, b, w, "first", first)
			shapeCommitLive(t, b, w, "second", second)
			require.NoError(t, b.Close())
			require.NoError(t, w.Close())
			b, _ = keptVectorsEngine(t, liveDir)
			live := shapeRead(t, b)

			t.Run("crash between journal and store", func(t *testing.T) {
				dir := t.TempDir()
				b, w := keptVectorsEngine(t, dir)
				shape.seed(t, b)
				shapeCommitLive(t, b, w, "first", first)
				requireRecordMarks(t, shapeJournal(t, b, w, "second", second), vectorOrders()[0].kept(shape))
				b, _ = keptVectorsReopen(t, b, w, dir)
				requireSameShapeImage(t, live, shapeRead(t, b))
			})
			t.Run("rebuild from a snapshot and the journal", func(t *testing.T) {
				dir := t.TempDir()
				b, w := keptVectorsEngine(t, dir)
				shape.seed(t, b)
				snapshot := keptVectorsSnapshot(t, b, w, dir, nil)
				shapeCommitLive(t, b, w, "first", first)
				shapeJournal(t, b, w, "second", second)
				crashClose(t, b, w)
				destination := shapeRebuildDestination(t)
				result, _, err := RecoverIntoEngine(t.Context(), destination, filepath.Join(dir, "wal"), snapshot)
				require.NoError(t, err)
				require.Zero(t, result.Failed, "%v", result.Errors)
				requireSameShapeImage(t, live, shapeRead(t, destination))
			})
		})
	}
}

// A transaction that waited for a concurrent writer of the node reads its
// predecessor as last committed, from the engine's node cache, which holds
// the image the other commit published: for a large body whose vectors live
// in the sidecar, that image carries the body codec's placeholder metadata,
// not the sidecar record. The record must still identify what the store
// holds, so recovery keeps the stored vectors.
//
// The oracle is the store before the waiter's update, not the waiter
// committed live: on main as here, the accepted commit of such a waiter loses
// the vectors of a large body with sidecar vectors
// (TestZZR7WaiterOnLargeSidecarNodeThroughTheJournal, lane evidence), which
// this change does not touch.
func TestKeptVectorsRecoveryAfterWaitingForAWriter(t *testing.T) {
	for _, shape := range vectorShapes() {
		if shape.keeps == 0 {
			continue // nothing is kept, so there is nothing to identify
		}
		t.Run(shape.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			b, w := keptVectorsEngine(t, dir)
			shape.seed(t, b)
			waiter, err := BeginEntityTransaction(ctx, b, CommitModeExplicit)
			require.NoError(t, err)
			require.NoError(t, waiter.SetNamespace("test"))
			_, err = waiter.GetNode(ctx, shapesNode)
			require.NoError(t, err)
			shapeCommitLive(t, b, w, "writer", func(tx *BadgerTransaction) {
				shapeUpdate(t, tx, func(n *Node) { n.Properties["writer"] = true })
			})
			want := shapeRead(t, b)
			latest, err := waiter.LockNodeForWrite(ctx, shapesNode)
			require.NoError(t, err)
			require.NotNil(t, latest, "the waiter reads the node as last committed")
			node := CopyNode(latest)
			node.Properties["waiter"] = true
			require.NoError(t, waiter.UpdateGraphNode(ctx, NodeUpdateRequest{After: node}))
			prepared, err := waiter.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "waiter"})
			require.NoError(t, err)
			payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
			require.NoError(t, err)
			prepared.Abort()
			requireRecordMarks(t, payload, shape.keeps)
			keptVectorsJournal(t, w, payload)
			b, _ = keptVectorsReopen(t, b, w, dir)
			got := shapeRead(t, b)
			require.Equal(t, true, got.properties["waiter"], "the waiter's update is applied")
			want.properties, got.properties = nil, nil
			requireSameShapeImage(t, want, got)
		})
	}
}
