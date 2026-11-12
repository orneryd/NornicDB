package storage

// Tests of the vector-sized work a commit no longer repeats (NOR-412, bug 35):
// a stored chunk value within the memo's bounds is decoded once per
// transaction (embeddingChunkDecodes), and encodeNodeInTxn does not encode
// chunk vectors holding more than maxNodeSize values only to learn that the
// body cannot hold them. Neither changes anything a commit produces.

import (
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func chunkVectors(chunks, dims int, seed float32) [][]float32 {
	out := make([][]float32, chunks)
	for i := range out {
		v := make([]float32, dims)
		for j := range v {
			v[j] = seed + float32(i*dims+j+1)/7
		}
		out[i] = v
	}
	return out
}

func sameFloatBits(t *testing.T, want, got []float32) {
	t.Helper()
	require.Equal(t, want == nil, got == nil, "nil-ness")
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, math.Float32bits(want[i]), math.Float32bits(got[i]), "value %d", i)
	}
}

// TestEmbeddingChunkDecodesReuseOnlyIdenticalValues: the memo returns, for a
// byte-identical value only, a private copy of exactly what decodeEmbedding
// returns for that value, bit for bit (signed zero, NaN payloads, infinities
// included; the stored codec's own round trip is not the question here).
func TestEmbeddingChunkDecodesReuseOnlyIdenticalValues(t *testing.T) {
	vector := []float32{1.5, float32(math.Copysign(0, -1)), math.Float32frombits(0x7fc00001), math.Float32frombits(0xffa00000), float32(math.Inf(-1)), math.SmallestNonzeroFloat32}
	value, err := encodeEmbedding(vector)
	require.NoError(t, err)
	key := embeddingKey("test:n", 0)

	decoded, err := decodeEmbedding(value)
	require.NoError(t, err)
	require.Len(t, decoded, len(vector))

	counts := observeChunkDecodes(t)
	var memo embeddingChunkDecodes
	first, err := memo.decode(key, append([]byte(nil), value...))
	require.NoError(t, err)
	sameFloatBits(t, decoded, first)
	first[0] = 99 // the caller owns what it got

	second, err := memo.decode(key, append([]byte(nil), value...))
	require.NoError(t, err)
	sameFloatBits(t, decoded, second)
	require.Equal(t, int64(1), counts.decodes.Load())
	require.Equal(t, int64(1), counts.reuses.Load())
	second[1] = 98
	third, err := memo.decode(key, append([]byte(nil), value...))
	require.NoError(t, err)
	sameFloatBits(t, decoded, third)

	// Another value under the same key is decoded, never answered from the memo.
	other := []float32{2, 3, 4, 5, 6, 7}
	otherValue, err := encodeEmbedding(other)
	require.NoError(t, err)
	otherDecoded, err := decodeEmbedding(otherValue)
	require.NoError(t, err)
	got, err := memo.decode(key, otherValue)
	require.NoError(t, err)
	sameFloatBits(t, otherDecoded, got)
	require.Equal(t, int64(2), counts.decodes.Load())

	// A value decodeEmbedding rejects is rejected the same way and not kept.
	corrupt := append([]byte(nil), value[:len(value)-3]...)
	_, wantErr := decodeEmbedding(corrupt)
	require.Error(t, wantErr)
	_, err = memo.decode(embeddingKey("test:n", 1), corrupt)
	require.EqualError(t, err, wantErr.Error())
	_, err = memo.decode(embeddingKey("test:n", 1), corrupt)
	require.EqualError(t, err, wantErr.Error())

	// The empty and the nil vector keep their form.
	empty, err := encodeEmbedding([]float32{})
	require.NoError(t, err)
	for range 2 {
		got, err := memo.decode(embeddingKey("test:n", 2), append([]byte(nil), empty...))
		require.NoError(t, err)
		want, err := decodeEmbedding(empty)
		require.NoError(t, err)
		require.Equal(t, want == nil, got == nil)
		require.Len(t, got, 0)
	}

	// A nil memo, and the test switch, decode every value.
	var none *embeddingChunkDecodes
	got, err = none.decode(key, value)
	require.NoError(t, err)
	sameFloatBits(t, decoded, got)
	decodeEveryChunkValue.Store(true)
	t.Cleanup(func() { decodeEveryChunkValue.Store(false) })
	decodesBefore, reusesBefore := counts.decodes.Load(), counts.reuses.Load()
	_, err = memo.decode(key, otherValue)
	require.NoError(t, err)
	require.Equal(t, decodesBefore, counts.decodes.Load())
	require.Equal(t, reusesBefore, counts.reuses.Load())
	decodeEveryChunkValue.Store(false)

	// release drops what the memo keeps: the same value is decoded again.
	decodesBefore = counts.decodes.Load()
	memo.release()
	_, err = memo.decode(key, otherValue)
	require.NoError(t, err)
	require.Equal(t, decodesBefore+1, counts.decodes.Load())
}

// TestEmbeddingChunkDecodesStayBounded: a value larger than
// maxChunkDecodeValueBytes, a value that decodes to more than
// maxChunkDecodeValueBytes/4 values, and a value not already kept once the
// memo holds maxChunkDecodes entries, is decoded every time, exactly as
// without the memo; a value kept before that is still answered.
func TestEmbeddingChunkDecodesStayBounded(t *testing.T) {
	counts := observeChunkDecodes(t)
	var memo embeddingChunkDecodes
	large := make([]float32, maxChunkDecodeValueBytes/4)
	for i := range large {
		large[i] = float32(i) + 0.5
	}
	largeValue, err := encodeEmbedding(large)
	require.NoError(t, err)
	require.Greater(t, len(largeValue), maxChunkDecodeValueBytes)
	for range 2 {
		got, err := memo.decode(embeddingKey("test:large", 0), largeValue)
		require.NoError(t, err)
		sameFloatBits(t, large, got)
	}
	require.Equal(t, int64(2), counts.decodes.Load())
	require.Equal(t, int64(0), counts.reuses.Load())

	// Stored integers decode as float32 values, and an int from 0 to 127 is
	// stored in one byte: a value within maxChunkDecodeValueBytes can decode
	// to a vector four times its size, so the vector is bounded too.
	compact := make([]int, 20000)
	for i := range compact {
		compact[i] = i % 100
	}
	compactValue, err := encodeValue(compact)
	require.NoError(t, err)
	require.LessOrEqual(t, len(compactValue), maxChunkDecodeValueBytes)
	compactVector, err := decodeEmbedding(compactValue)
	require.NoError(t, err)
	require.Greater(t, len(compactVector), maxChunkDecodeValueBytes/4)
	for range 2 {
		got, err := memo.decode(embeddingKey("test:compact", 0), compactValue)
		require.NoError(t, err)
		sameFloatBits(t, compactVector, got)
	}
	require.Equal(t, int64(4), counts.decodes.Load())
	require.Equal(t, int64(0), counts.reuses.Load())

	small, err := encodeEmbedding([]float32{1, 2, 3})
	require.NoError(t, err)
	for i := 0; i < maxChunkDecodes; i++ {
		_, err := memo.decode(embeddingKey("test:many", i), small)
		require.NoError(t, err)
	}
	decodes := counts.decodes.Load()
	for range 2 {
		got, err := memo.decode(embeddingKey("test:many", maxChunkDecodes), small)
		require.NoError(t, err)
		sameFloatBits(t, []float32{1, 2, 3}, got)
	}
	require.Equal(t, decodes+2, counts.decodes.Load(), "a value past the bound is not kept")
	_, err = memo.decode(embeddingKey("test:many", 0), small)
	require.NoError(t, err)
	require.Equal(t, int64(1), counts.reuses.Load(), "a value kept before the bound is still answered")
}

// TestEmbeddingChunkDecodesKeepNoChunkKey: what a memo keeps does not grow
// with the node ID. maxChunkDecodes chunk keys of a 60,000-character node ID
// (about 469 MiB of key bytes) leave the memo holding its entries' values and
// vectors and its map, less than 16 MiB of heap, and no key.
func TestEmbeddingChunkDecodesKeepNoChunkKey(t *testing.T) {
	id := NodeID("test:" + strings.Repeat("n", 60000))
	value, err := encodeEmbedding([]float32{1, 2})
	require.NoError(t, err)
	heapInUse := func() int64 {
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return int64(stats.HeapAlloc)
	}
	var memo embeddingChunkDecodes
	before := heapInUse()
	for i := 0; i < maxChunkDecodes; i++ {
		_, err := memo.decode(embeddingKey(id, i), append([]byte(nil), value...))
		require.NoError(t, err)
	}
	kept := heapInUse() - before
	require.Len(t, memo.byKeyHash, maxChunkDecodes)
	require.Less(t, kept, int64(16<<20), "the memo keeps %d bytes for %d chunks", kept, maxChunkDecodes)
	runtime.KeepAlive(&memo)
}

// chunkDecodeCounts counts what every memo decodes and saves while a test
// observes it (chunkDecodeObserved).
type chunkDecodeCounts struct{ decodes, reuses atomic.Int64 }

func observeChunkDecodes(t *testing.T) *chunkDecodeCounts {
	t.Helper()
	counts := &chunkDecodeCounts{}
	observe := func(reused bool) {
		if reused {
			counts.reuses.Add(1)
		} else {
			counts.decodes.Add(1)
		}
	}
	chunkDecodeObserved.Store(&observe)
	t.Cleanup(func() { chunkDecodeObserved.Store(nil) })
	return counts
}

// referenceEncodeNodeInTxn is encodeNodeInTxn as it was before NOR-412 bug 35:
// it always encodes the body with its vectors first and measures it.
func referenceEncodeNodeInTxn(b *BadgerEngine, txn *badger.Txn, namespace string, n *Node) ([]byte, bool, error) {
	if err := validatePropertiesForStorage(n.Properties); err != nil {
		return nil, false, err
	}
	propsBytes, err := b.encodeTokenizedProperties(txn, namespace, n.Properties)
	if err != nil {
		return nil, false, err
	}
	bodyCopy := *n
	bodyCopy.Properties = nil
	assemble := func(bodyBytes []byte) []byte {
		out := []byte{nodeFormatTokenizedV1}
		out = binary.AppendUvarint(out, uint64(len(propsBytes)))
		out = append(out, propsBytes...)
		return append(out, bodyBytes...)
	}
	bodyBytes, err := encodeValue(&bodyCopy)
	if err != nil {
		return nil, false, err
	}
	data := assemble(bodyBytes)
	if len(data) <= maxNodeSize {
		return data, false, nil
	}
	chunkCount := len(bodyCopy.ChunkEmbeddings)
	bodyCopy.ChunkEmbeddings = nil
	metadata := bodyCopy.RuntimeMetadata()
	metadata.EmbeddingsStoredSeparately = true
	bodyCopy.SetRuntimeMetadata(metadata)
	if bodyCopy.EmbedMeta == nil {
		bodyCopy.EmbedMeta = make(map[string]any)
	}
	if _, has := bodyCopy.EmbedMeta["chunk_count"]; !has {
		bodyCopy.EmbedMeta["chunk_count"] = chunkCount
	}
	bodyBytes, err = encodeValue(&bodyCopy)
	if err != nil {
		return nil, false, err
	}
	return assemble(bodyBytes), true, nil
}

// TestEncodeNodeInTxnMatchesMeasuredEncoding: encodeNodeInTxn, which no
// longer encodes chunk vectors holding more than maxNodeSize values to
// measure the body, gives what the measured encoding gave: the same branch,
// the same bytes (the same decoded body where a map of several keys makes
// the byte order vary between any two encodings), the same change to the
// caller's metadata map and the same error (its type and its message),
// around the inline limit and far beyond it.
func TestEncodeNodeInTxnMatchesMeasuredEncoding(t *testing.T) {
	engine := createTestBadgerEngine(t)
	at := time.Date(2026, 11, 12, 10, 0, 0, 123456789, time.UTC)
	exactly := chunkVectors(maxNodeSize/1024, 1024, 1) // maxNodeSize values: the measured path
	oneMore := append(chunkVectors(maxNodeSize/1024, 1024, 1), []float32{0.5})
	cases := []struct {
		name string
		node func() *Node
	}{
		{"small inline", func() *Node {
			return &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: chunkVectors(3, 8, 1), CreatedAt: at, UpdatedAt: at}
		}},
		{"values equal to the limit, metadata nil", func() *Node {
			return &Node{ID: "test:b", Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: exactly, CreatedAt: at, UpdatedAt: at}
		}},
		{"one value over the limit, metadata nil", func() *Node {
			return &Node{ID: "test:c", Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: oneMore, CreatedAt: at, UpdatedAt: at}
		}},
		{"far over the limit, metadata without chunk_count", func() *Node {
			return &Node{ID: "test:d", Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: chunkVectors(60, 1024, 2), EmbedMeta: map[string]any{"embedding_model": "m"}, CreatedAt: at, UpdatedAt: at}
		}},
		{"far over the limit, metadata with chunk_count, named vectors", func() *Node {
			return &Node{ID: "test:e", Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: chunkVectors(60, 1024, 3), NamedEmbeddings: map[string][]float32{"default": chunkVectors(1, 128, 4)[0]}, EmbedMeta: map[string]any{"chunk_count": 60}, CreatedAt: at, UpdatedAt: at}
		}},
		{"text just under the limit, few values", func() *Node {
			return &Node{ID: "test:f", Properties: map[string]any{"text": strings.Repeat("x", maxNodeSize-1200)}, ChunkEmbeddings: chunkVectors(2, 16, 5), CreatedAt: at, UpdatedAt: at}
		}},
		{"text just over the limit, few values", func() *Node {
			return &Node{ID: "test:g", Properties: map[string]any{"text": strings.Repeat("x", maxNodeSize+10)}, ChunkEmbeddings: chunkVectors(2, 16, 6), CreatedAt: at, UpdatedAt: at}
		}},
		{"text over the limit, no vectors", func() *Node {
			return &Node{ID: "test:h", Properties: map[string]any{"text": strings.Repeat("x", maxNodeSize+10)}, CreatedAt: at, UpdatedAt: at}
		}},
		{"metadata that cannot be encoded, far over the limit", func() *Node {
			return &Node{ID: "test:i", ChunkEmbeddings: chunkVectors(60, 1024, 7), EmbedMeta: map[string]any{"bad": make(chan int)}, CreatedAt: at, UpdatedAt: at}
		}},
		{"metadata that cannot be encoded, inline", func() *Node {
			return &Node{ID: "test:j", ChunkEmbeddings: chunkVectors(2, 4, 8), EmbedMeta: map[string]any{"bad": make(chan int)}, CreatedAt: at, UpdatedAt: at}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, got := tc.node(), tc.node()
			var wantData, gotData []byte
			var wantSeparate, gotSeparate bool
			var wantErr, gotErr error
			require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
				wantData, wantSeparate, wantErr = referenceEncodeNodeInTxn(engine, txn, "test", want)
				gotData, gotSeparate, gotErr = engine.encodeNodeInTxn(txn, "test", got)
				_ = engine.propKeyDict.flushTxnCounters(txn)
				return nil
			}))
			if wantErr != nil {
				require.Error(t, gotErr)
				require.Equal(t, reflect.TypeOf(wantErr), reflect.TypeOf(gotErr), "error type")
				require.EqualError(t, gotErr, wantErr.Error())
			} else {
				require.NoError(t, gotErr)
			}
			// The caller's node, including its metadata map, ends the same.
			require.Equal(t, want.EmbedMeta == nil, got.EmbedMeta == nil)
			if wantErr == nil {
				require.Equal(t, want.EmbedMeta, got.EmbedMeta)
			} else {
				_, wantHas := want.EmbedMeta["chunk_count"]
				_, gotHas := got.EmbedMeta["chunk_count"]
				require.Equal(t, wantHas, gotHas)
				return
			}
			require.Equal(t, wantSeparate, gotSeparate)
			require.Equal(t, len(wantData), len(gotData))
			if len(want.EmbedMeta) <= 1 {
				require.Equal(t, wantData, gotData)
			}
			wantNode, err := engine.decodeNodeProjected(nil, "test", wantData, nil)
			require.NoError(t, err)
			gotNode, err := engine.decodeNodeProjected(nil, "test", gotData, nil)
			require.NoError(t, err)
			require.Equal(t, wantNode, gotNode)
		})
	}
}

// commitImage is the part of a published stored image a test compares
// between two engines: everything a stored read returns and the image's
// encoded size, which storage accounting folds (calculateNodeSize).
type commitImage struct {
	ID                 NodeID
	Labels             []string
	Properties         map[string]any
	CreatedAt          int64
	UpdatedAt          int64
	Named              map[string][]uint32
	Chunks             [][]uint32
	Meta               map[string]any
	Runtime            NodeRuntimeMetadata
	EncodedSize        int
	NilChunks, NilMeta bool
}

func commitImageOf(t *testing.T, node *Node) *commitImage {
	t.Helper()
	if node == nil {
		return nil
	}
	bits := func(v []float32) []uint32 {
		if v == nil {
			return nil
		}
		out := make([]uint32, len(v))
		for i, f := range v {
			out[i] = math.Float32bits(f)
		}
		return out
	}
	image := &commitImage{ID: node.ID, Labels: node.Labels, Properties: node.Properties, CreatedAt: node.CreatedAt.UnixNano(), UpdatedAt: node.UpdatedAt.UnixNano(), Meta: node.EmbedMeta, Runtime: node.RuntimeMetadata(), NilChunks: node.ChunkEmbeddings == nil, NilMeta: node.EmbedMeta == nil}
	if node.NamedEmbeddings != nil {
		image.Named = make(map[string][]uint32, len(node.NamedEmbeddings))
		for name, v := range node.NamedEmbeddings {
			image.Named[name] = bits(v)
		}
	}
	for _, chunk := range node.ChunkEmbeddings {
		image.Chunks = append(image.Chunks, bits(chunk))
	}
	encoded, err := msgpack.Marshal(node)
	require.NoError(t, err)
	image.EncodedSize = len(encoded)
	return image
}

type publishedChange struct{ Before, After *commitImage }

// commitImagesFixture runs one scenario on a fresh engine and returns every
// stored image its commits published, in order, and the chunk decodes the
// marked transaction made.
type commitImagesFixture struct {
	t       *testing.T
	base    *BadgerEngine
	owner   *WALEngine // nil: commits go through the engine's own commit
	at      time.Time
	changes []publishedChange

	// counts are the measured step's decodes (measure).
	counts *chunkDecodeCounts
}

func newCommitImagesFixture(t *testing.T, buffered bool) *commitImagesFixture {
	t.Helper()
	f := &commitImagesFixture{t: t, at: time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)}
	if buffered {
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		f.base = b
	} else {
		t.Cleanup(config.WithWALEnabled())
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
		require.NoError(t, err)
		dir := t.TempDir()
		wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
		require.NoError(t, err)
		f.owner = NewWALEngine(b, wal)
		t.Cleanup(func() { _ = f.owner.Close() })
		f.base = b
	}
	require.NoError(t, f.base.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(publication StoredEntityPublication) (func(), error) {
			for _, change := range publication.Changes {
				if change.BeforeNode == nil && change.AfterNode == nil {
					continue
				}
				f.changes = append(f.changes, publishedChange{commitImageOf(t, change.BeforeNode), commitImageOf(t, change.AfterNode)})
			}
			return nil, nil
		}, nil
	}, nil))
	return f
}

func (f *commitImagesFixture) tick() time.Time {
	f.at = f.at.Add(time.Second)
	return f.at
}

// commit stages one transaction and commits it: through the journal's
// accepted route (preparation, then the native application) or, on a
// buffered engine, through the engine's own commit.
func (f *commitImagesFixture) commit(stage func(tx *BadgerTransaction)) *BadgerTransaction {
	t := f.t
	tx, err := f.base.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	stage(tx)
	if f.owner == nil {
		require.NoError(t, tx.Commit())
		return tx
	}
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	r, err := f.owner.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	require.NoError(t, r.WaitOutcome(t.Context()).Err)
	return tx
}

// measure runs step with every chunk decode counted (chunkDecodeObserved).
func (f *commitImagesFixture) measure(step func()) {
	counts := &chunkDecodeCounts{}
	observe := func(reused bool) {
		if reused {
			counts.reuses.Add(1)
		} else {
			counts.decodes.Add(1)
		}
	}
	chunkDecodeObserved.Store(&observe)
	defer chunkDecodeObserved.Store(nil)
	step()
	f.counts = counts
}

func (f *commitImagesFixture) create(node *Node) {
	node.CreatedAt, node.UpdatedAt = f.tick(), f.at
	f.commit(func(tx *BadgerTransaction) {
		_, err := tx.CreateNode(f.t.Context(), node)
		require.NoError(f.t, err)
	})
}

func (f *commitImagesFixture) embed(id NodeID, chunks [][]float32) {
	payload := embeddingWriteback(f.t, f.base, id, chunks, map[string]any{"has_embedding": true, "chunk_count": len(chunks), "embedding_dimensions": len(chunks[0]), "embedding_model": "synthetic"}, time.Time{})
	require.NoError(f.t, f.base.UpdateNodeEmbeddingSidecar(context.Background(), payload))
}

// update changes the node as a client edit does: an image without its
// vectors, so the store keeps them (reconcileManagedEmbeddingsLocked).
func (f *commitImagesFixture) update(tx *BadgerTransaction, id NodeID, edit func(*Node)) {
	stored, err := f.base.GetStoredNode(f.t.Context(), id)
	require.NoError(f.t, err)
	after := CopyNode(stored)
	after.ChunkEmbeddings, after.EmbedMeta = nil, nil
	after.UpdatedAt = f.tick()
	edit(after)
	require.NoError(f.t, tx.UpdateGraphNode(f.t.Context(), NodeUpdateRequest{After: after}))
}

func (f *commitImagesFixture) updateCommit(id NodeID, edit func(*Node)) *BadgerTransaction {
	return f.commit(func(tx *BadgerTransaction) { f.update(tx, id, edit) })
}

// TestCommitImagesMatchWithoutChunkDecodes: every stored image a commit
// publishes to storage accounting is the same with the chunk-decode memo as
// without it (each value decoded, as before NOR-412 bug 35), field for field,
// vector bit for bit and in encoded size, for these storage shapes and
// commit routes that write a node's vectors or keep them. With the memo, each
// measured kept update here, whose chunk values all fit the memo's bounds,
// decodes each stored chunk value once.
func TestCommitImagesMatchWithoutChunkDecodes(t *testing.T) {
	setFlag := func(v any) func(*Node) { return func(n *Node) { n.Properties["flag"] = v } }
	scenarios := []struct {
		name     string
		buffered bool
		run      func(f *commitImagesFixture) (chunks int)
	}{
		{"sidecar vectors kept by property set, label add and remove, property remove", false, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:sidecar", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha", "flag": false, "extra": 1}})
			f.embed("test:sidecar", chunkVectors(30, 64, 1))
			f.measure(func() { f.updateCommit("test:sidecar", setFlag(true)) })
			f.updateCommit("test:sidecar", func(n *Node) { n.Labels = append(n.Labels, "Seen") })
			f.updateCommit("test:sidecar", func(n *Node) { n.Labels = n.Labels[:1] })
			f.updateCommit("test:sidecar", func(n *Node) { delete(n.Properties, "extra") })
			return 30
		}},
		{"sidecar vectors and text over the inline limit", false, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:large-text", Labels: []string{"Doc"}, Properties: map[string]any{"text": strings.Repeat("y", maxNodeSize+100), "flag": false}})
			f.embed("test:large-text", chunkVectors(20, 64, 2))
			f.measure(func() { f.updateCommit("test:large-text", setFlag(true)) })
			return 20
		}},
		{"vectors stored apart by the codec, kept, then replaced, then deleted", false, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:apart", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha", "flag": false}, ChunkEmbeddings: chunkVectors(60, 1024, 3), EmbedMeta: map[string]any{"has_embedding": true, "chunk_count": 60}})
			f.measure(func() { f.updateCommit("test:apart", setFlag(true)) })
			f.updateCommit("test:apart", func(n *Node) { n.ChunkEmbeddings = chunkVectors(61, 1024, 4) })
			f.commit(func(tx *BadgerTransaction) {
				require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:apart", CascadeEdges))
			})
			return 60
		}},
		{"vectors inside the record moved apart by a large property and back", false, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:move", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha"}, ChunkEmbeddings: chunkVectors(4, 16, 5), EmbedMeta: map[string]any{"has_embedding": true}})
			f.updateCommit("test:move", func(n *Node) { n.Properties["blob"] = strings.Repeat("z", maxNodeSize+500) })
			f.updateCommit("test:move", func(n *Node) { delete(n.Properties, "blob") })
			return 0
		}},
		{"named vectors with sidecar vectors, two updates in one transaction", false, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:named", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha", "flag": false}, NamedEmbeddings: map[string][]float32{"default": chunkVectors(1, 128, 6)[0]}})
			f.embed("test:named", chunkVectors(25, 64, 7))
			f.measure(func() {
				f.commit(func(tx *BadgerTransaction) {
					f.update(tx, "test:named", setFlag(true))
					f.update(tx, "test:named", setFlag(2))
				})
			})
			return 25
		}},
		{"buffered commits: sidecar vectors kept, vectors apart kept", true, func(f *commitImagesFixture) int {
			f.create(&Node{ID: "test:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha", "flag": false}})
			f.embed("test:buffered", chunkVectors(30, 64, 8))
			f.updateCommit("test:buffered", setFlag(true))
			f.create(&Node{ID: "test:buffered-apart", Labels: []string{"Doc"}, Properties: map[string]any{"text": "beta", "flag": false}, ChunkEmbeddings: chunkVectors(60, 1024, 9), EmbedMeta: map[string]any{"has_embedding": true, "chunk_count": 60}})
			f.updateCommit("test:buffered-apart", setFlag(true))
			require.NoError(t, f.base.FlushWriteBehind(context.Background()))
			f.updateCommit("test:buffered", setFlag(false))
			return 0
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			run := func(memo bool) ([]publishedChange, *chunkDecodeCounts, int) {
				decodeEveryChunkValue.Store(!memo)
				defer decodeEveryChunkValue.Store(false)
				f := newCommitImagesFixture(t, sc.buffered)
				chunks := sc.run(f)
				return f.changes, f.counts, chunks
			}
			without, _, _ := run(false)
			with, counts, chunks := run(true)
			require.NotEmpty(t, without)
			require.Equal(t, len(without), len(with))
			for i := range without {
				require.True(t, reflect.DeepEqual(without[i], with[i]), "published change %d differs:\nwithout: %+v\nwith:    %+v", i, without[i], with[i])
			}
			if counts != nil {
				decodes, reuses := counts.decodes.Load(), counts.reuses.Load()
				t.Logf("kept update of %d chunks: %d decodes, %d reuses", chunks, decodes, reuses)
				require.Equal(t, int64(chunks), decodes, "each stored chunk value is decoded once")
				require.GreaterOrEqual(t, reuses, int64(2*chunks))
			}
		})
	}
}
