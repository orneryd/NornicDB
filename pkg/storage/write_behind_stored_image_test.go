package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"math"
	"reflect"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Until a flush lands a commit, the write-behind overlay serves every reader,
// so it must serve the body a read from storage decodes afterwards: property
// values as the codec stores them, and separately stored vectors hydrated.
// Visible and stored reads, created and rewritten bodies all see that image.
func TestWriteBehindOverlayServesStoredImage(t *testing.T) {
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	zone := time.FixedZone("plus-one", 3600)
	properties := func() map[string]any {
		return map[string]any{
			"list":   []any{"stored", "x"},
			"width":  int32(7),
			"at":     time.Date(2024, 1, 2, 3, 4, 5, 6, zone),
			"nested": map[string]any{"x": []any{int64(1), int64(2)}},
		}
	}
	// Large enough that the codec stores the vectors apart from the body.
	vectors := make([][]float32, 2)
	for c := range vectors {
		vectors[c] = make([]float32, 10000)
		for i := range vectors[c] {
			vectors[c][i] = float32((c + i) % 7)
		}
	}
	created, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, created.SetImplicit(true))
	// Bodies whose values the codec keeps as given are served without a decode.
	plain := func() map[string]any {
		return map[string]any{"name": "x", "count": int64(3), "score": 0.5, "flag": true, "none": nil}
	}
	for _, node := range []*Node{
		{ID: "test:a", Labels: []string{"A"}, Properties: properties()},
		{ID: "test:big", Labels: []string{"A"}, Properties: properties(), ChunkEmbeddings: vectors},
		{ID: "test:plain", Labels: []string{"A"}, Properties: plain()},
	} {
		_, err = created.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, created.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:big", Type: "LINK", Properties: properties()}))
	require.NoError(t, created.CreateEdge(&Edge{ID: "test:plain-r", StartNode: "test:plain", EndNode: "test:a", Type: "LINK"}))
	require.NoError(t, created.Commit())
	rewrite, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, rewrite.SetImplicit(true))
	require.NoError(t, rewrite.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:a", Labels: []string{"A"}, Properties: properties()}, Policy: ClientNodeEdits}))
	require.NoError(t, rewrite.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:big", Type: "LINK", Properties: properties()}}))
	require.NoError(t, rewrite.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:plain", Labels: []string{"A"}, Properties: plain()}, Policy: ClientNodeEdits}))
	require.NoError(t, rewrite.Commit())

	type image struct {
		nodes []*Node
		edges []*Edge
	}
	read := func() image {
		var out image
		for _, id := range []NodeID{"test:a", "test:big", "test:plain"} {
			stored, err := engine.GetStoredNode(id)
			require.NoError(t, err)
			visible, err := engine.GetNode(id)
			require.NoError(t, err)
			out.nodes = append(out.nodes, publicNodeImage(stored), publicNodeImage(visible))
		}
		for _, id := range []EdgeID{"test:r", "test:plain-r"} {
			stored, err := engine.GetStoredEdge(id)
			require.NoError(t, err)
			visible, err := engine.GetEdge(id)
			require.NoError(t, err)
			out.edges = append(out.edges, comparableStoredEdge(stored), comparableStoredEdge(visible))
		}
		return out
	}
	overlay := read()
	require.NoError(t, engine.FlushWriteBehind())
	// Drop the caches the flush filled, so the second read decodes storage.
	engine.invalidateCachesAfterRestore()
	physical := read()
	require.Equal(t, physical.nodes, overlay.nodes)
	require.Equal(t, physical.edges, overlay.edges)
	require.Len(t, overlay.nodes[2].ChunkEmbeddings, len(vectors), "separately stored vectors are hydrated")
}

// A body enters undecoded only when its source is exactly the image the codec
// returns: every value kind below is written through the real codec and
// decoded, and wherever writtenNodeSource or writtenEdgeSource accepts the
// body, the source must equal that image bit for bit (property values,
// timestamps in the codec's zone and precision, every other field). Every
// kind outside the accepted scalar set must take the decode.
func TestWrittenSourceIsStoredImage(t *testing.T) {
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	zone := time.FixedZone("minus-three-thirty", -(3*3600 + 1800))
	times := []time.Time{
		{},
		time.Now(),
		time.Date(2024, 1, 2, 3, 4, 5, 6, zone),
		time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
		time.Date(1960, 6, 7, 8, 9, 10, 11, time.UTC),
	}
	kinds := map[string]any{
		"nil": nil, "string": "x", "empty string": "", "true": true, "false": false,
		"int64": int64(-3), "int64 max": int64(math.MaxInt64), "int64 min": int64(math.MinInt64),
		"float64": -0.5, "float64 zero": 0.0, "float64 negative zero": math.Copysign(0, -1),
		"float64 inf": math.Inf(1), "float64 nan": math.NaN(), "float64 max": math.MaxFloat64,
		"int": int(7), "int8": int8(7), "int16": int16(7), "int32": int32(7), "uint": uint(7),
		"uint8": uint8(7), "uint16": uint16(7), "uint32": uint32(7), "uint64": uint64(7), "uint64 max": uint64(math.MaxUint64),
		"float32": float32(0.5), "bytes": []byte("ab"),
		"time utc": time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC), "time zoned": time.Date(2024, 1, 2, 3, 4, 5, 6, zone),
		"time local": time.Now(), "time zero": time.Time{},
		"generic strings": []any{"a", "b"}, "generic int64s": []any{int64(1), int64(2)}, "generic ints": []any{1, 2},
		"generic floats": []any{0.5, 1.5}, "generic bools": []any{true, false}, "generic mixed": []any{int64(1), 2.5, "x"},
		"generic nested": []any{[]any{"a"}, []any{int64(1)}}, "generic empty": []any{}, "generic with nil": []any{nil, "x"},
		"generic times": []any{time.Date(2024, 1, 2, 3, 4, 5, 6, zone)},
		"strings":       []string{"a", "b"}, "empty strings": []string{}, "int64s": []int64{1, 2}, "ints": []int{1, 2},
		"float64s": []float64{0.5, 1.5}, "float32s": []float32{0.5}, "bools": []bool{true}, "int32s": []int32{1},
		"map": map[string]any{"x": "y"}, "map nested": map[string]any{"x": []any{int64(1)}, "y": map[string]any{"z": 1}},
		"map empty": map[string]any{}, "maps": []any{map[string]any{"x": int64(1)}},
		"generic float32s": []any{float32(0.5)},
		"int max":          int(math.MaxInt64), "int min": int(math.MinInt64), "uint big": uint(math.MaxUint64), "uint small": uint(3),
		"int 127": 127, "int 128": 128, "int 255": 255, "int 256": 256, "int 65535": 65535, "int 65536": 65536,
		"int 4294967295": 4294967295, "int 4294967296": 4294967296, "int -1": -1, "int -32": -32, "int -33": -33,
		"int -128": -128, "int -129": -129, "int -32768": -32768, "int -32769": -32769, "int -2147483648": -2147483648,
		"int -2147483649": -2147483649, "int8 min": int8(math.MinInt8), "int16 min": int16(math.MinInt16), "int32 min": int32(math.MinInt32),
		"uint16 max": uint16(math.MaxUint16), "uint32 max": uint32(math.MaxUint32),
		"uint64 maxint64": uint64(math.MaxInt64), "uint64 maxint64+1": uint64(math.MaxInt64) + 1, "uint maxint64+1": uint(math.MaxInt64) + 1,
	}
	// Exactly the scalar kinds take the fast path.
	accepted := map[string]bool{}
	for name, value := range kinds {
		switch value.(type) {
		case nil, string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			accepted[name] = true
		}
	}
	codec := func(node *Node, edge *Edge) (*Node, *Edge) {
		var nodeImage *Node
		var edgeImage *Edge
		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			if node != nil {
				data, _, err := engine.encodeNodeInTxn(txn, "test", node)
				require.NoError(t, err)
				nodeImage, err = engine.decodeWrittenNode(node.ID, data)
				require.NoError(t, err)
				keepSourceEmbeddings(nodeImage, node)
			}
			if edge != nil {
				data, err := engine.encodeEdgeInTxn(txn, "test", edge)
				require.NoError(t, err)
				edgeImage, err = engine.decodeWrittenEdge(data, edge)
				require.NoError(t, err)
			}
			return nil
		}))
		return nodeImage, edgeImage
	}
	sameNode := func(name string, want, got *Node) {
		t.Helper()
		require.True(t, sameStoredBits(want.Properties, got.Properties), "%s: properties %#v vs %#v", name, want.Properties, got.Properties)
		want, got = copyNode(want), copyNode(got)
		want.Properties, got.Properties = nil, nil
		require.Equal(t, want, got, name)
	}
	sameEdge := func(name string, want, got *Edge) {
		t.Helper()
		require.True(t, sameStoredBits(want.Properties, got.Properties), "%s: properties %#v vs %#v", name, want.Properties, got.Properties)
		want, got = copyEdge(want), copyEdge(got)
		want.Properties, got.Properties = nil, nil
		require.Equal(t, want, got, name)
	}
	check := func(name string, props map[string]any, at time.Time, labels []string, flag bool) {
		t.Helper()
		node := &Node{ID: "test:n", Labels: labels, Properties: props, CreatedAt: at, UpdatedAt: at.Add(time.Second)}
		metadata := node.RuntimeMetadata()
		metadata.VisibilitySuppressed = flag
		node.SetRuntimeMetadata(metadata)
		edge := &Edge{ID: "test:e", StartNode: "test:s", EndNode: "test:t", Type: "LINK", Properties: props,
			CreatedAt: at, UpdatedAt: at.Add(time.Second), Confidence: 0.25, AutoGenerated: true, VisibilitySuppressed: flag}
		nodeImage, edgeImage := codec(node, edge)
		if source, ok := writtenNodeSource(node); ok {
			sameNode(name, nodeImage, source)
		}
		if source, ok := writtenEdgeSource(edge); ok {
			sameEdge(name, edgeImage, source)
		}
	}
	for name, value := range kinds {
		_, nodeOK := writtenNodeSource(&Node{ID: "test:n", Properties: map[string]any{"v": value}})
		_, edgeOK := writtenEdgeSource(&Edge{ID: "test:e", Properties: map[string]any{"v": value}})
		require.Equal(t, accepted[name], nodeOK, "node fast path for %s (%T)", name, value)
		require.Equal(t, accepted[name], edgeOK, "edge fast path for %s (%T)", name, value)
		for i, at := range times {
			check(name, map[string]any{"v": value, "s": "x"}, at, []string{"A"}, i%2 == 0)
		}
	}
	for i, at := range times {
		for _, props := range []map[string]any{nil, {}} {
			for _, labels := range [][]string{nil, {}, {"A", "B"}} {
				check("empty", props, at, labels, i%2 == 0)
			}
		}
	}
	// Embedding state the body stores apart or types anew takes the decode.
	for _, node := range []*Node{
		{ID: "test:n", ChunkEmbeddings: [][]float32{{1}}},
		{ID: "test:n", NamedEmbeddings: map[string][]float32{"default": {1}}},
		{ID: "test:n", EmbedMeta: map[string]any{"chunk_count": 1}},
		{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{EmbeddingsStoredSeparately: true}}, ID: "test:n"},
	} {
		_, ok := writtenNodeSource(node)
		require.False(t, ok)
	}
}

// sameStoredBits reports whether a and b are the same stored value: equal
// types and, for floats, equal bits (NaN and negative zero included).
func sameStoredBits(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for k, v := range x {
			w, found := y[k]
			if !found || !sameStoredBits(v, w) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// publicNodeImage is node's exported state with comparable timestamps; read
// paths may differ in their internal embedding-key bookkeeping.
func publicNodeImage(node *Node) *Node {
	node = comparableStoredNode(node)
	return &Node{
		ID: node.ID, Labels: node.Labels, Properties: node.Properties,
		CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
		NamedEmbeddings: node.NamedEmbeddings, ChunkEmbeddings: node.ChunkEmbeddings,
		runtime:   &nodeRuntime{metadata: node.RuntimeMetadata()},
		EmbedMeta: node.EmbedMeta,
	}
}

// An update that keeps a managed embedding stores the body without it while
// the sidecar holds it (#963). Until the flush, the overlay serves the node a
// stored read returns afterwards: stored property values and the embedding.
func TestWriteBehindOverlayKeepsSidecarEmbedding(t *testing.T) {
	engine, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetEmbeddingsEnabled(true)
	engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
	now := time.Now().UTC().Truncate(time.Millisecond)
	node := sidecarTestNode(t, engine, "keep", map[string]any{"text": "boiler invoice", "tags": []any{"a"}}, now)
	require.NoError(t, engine.FlushWriteBehind())
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, engine, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, now)))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetImplicit(true))
	current, err := tx.GetNode(node.ID)
	require.NoError(t, err)
	current.Properties["tags"] = []any{"a", "b"}
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: current, Policy: ClientNodeEdits}))
	require.NoError(t, tx.Commit())

	overlay, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.1, 0.2}}, overlay.ChunkEmbeddings)
	require.NoError(t, engine.FlushWriteBehind())
	physical, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, physical.ChunkEmbeddings, overlay.ChunkEmbeddings)
	require.Equal(t, physical.EmbedMeta, overlay.EmbedMeta)
	require.Equal(t, physical.Properties, overlay.Properties)
}
