package storage

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// frozenTransportNode pins the pre-consolidation eleven-field wire contract.
// It is a test oracle, not a production decoder or another stored-node codec.
type frozenTransportNode struct {
	Database                   string
	ID                         NodeID
	Labels                     []string
	Properties                 transportPropertyMap
	CreatedAt                  transportTime
	UpdatedAt                  transportTime
	NamedEmbeddings            map[string][]float32
	ChunkEmbeddings            [][]float32
	EmbedMeta                  transportPropertyMap
	EmbeddingsStoredSeparately bool
	VisibilitySuppressed       bool
}

func frozenNodeWire(node *Node) *frozenTransportNode {
	if node == nil {
		return nil
	}
	return &frozenTransportNode{
		Database: node.Database, ID: node.ID, Labels: node.Labels,
		Properties: transportPropertyMap(node.Properties),
		CreatedAt:  transportTime(node.CreatedAt), UpdatedAt: transportTime(node.UpdatedAt),
		NamedEmbeddings: node.NamedEmbeddings, ChunkEmbeddings: node.ChunkEmbeddings,
		EmbedMeta:                  transportPropertyMap(node.EmbedMeta),
		EmbeddingsStoredSeparately: node.EmbeddingsStoredSeparately,
		VisibilitySuppressed:       node.VisibilitySuppressed,
	}
}

func TestTransportNodeWirePreservesCurrentFormat(t *testing.T) {
	// One key in each unordered map permits an exact byte comparison without
	// imposing sorting or deterministic map order on the production codec.
	values := []struct {
		name  string
		value any
	}{
		{"nil", nil}, {"bool", true}, {"string", "text"},
		{"invalid UTF8", "bad\xff"}, {"unpaired UTF16", "\xed\xa0\x80"},
		{"int", int(-7)}, {"int8", int8(-8)}, {"int16", int16(-16)},
		{"int32", int32(-32)}, {"int64", int64(math.MinInt64)},
		{"uint", uint(7)}, {"uint8", uint8(8)}, {"uint16", uint16(16)},
		{"uint32", uint32(32)}, {"uint64", uint64(math.MaxUint64)},
		{"float32", float32(1.5)}, {"float64", float64(-2.25)},
		{"bytes", []byte{0, 255}}, {"strings", []string{"a", ""}},
		{"ints", []int{-1, 0}}, {"int32s", []int32{-1, 0}},
		{"int64s", []int64{math.MinInt64, math.MaxInt64}},
		{"float32s", []float32{1.25, -1.25}}, {"float64s", []float64{math.Inf(1)}},
		{"bools", []bool{true, false}}, {"list", []any{int8(2), "s", nil}},
		{"map", map[string]any{"nested": []any{int16(3), nil}}},
		{"nil list", []any(nil)}, {"empty list", []any{}},
		{"empty map", map[string]any{}},
	}
	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			node := &Node{
				Database: "provenance", ID: "test:wire", Labels: []string{"Doc", "Item"},
				Properties:                 map[string]any{"p": tc.value},
				CreatedAt:                  time.Date(2024, 3, 9, 23, 30, 45, 123456789, time.FixedZone("", -5*3600)),
				UpdatedAt:                  time.Date(2024, 11, 3, 1, 30, 15, 987654321, time.FixedZone("", 5*3600+45*60)),
				NamedEmbeddings:            map[string][]float32{"v": {0.5, -0.5}},
				ChunkEmbeddings:            [][]float32{{1, 0}, nil, {}},
				EmbedMeta:                  map[string]any{"count": int(2)},
				EmbeddingsStoredSeparately: true, VisibilitySuppressed: true,
			}
			previous, err := EncodeCommandPayload(frozenNodeWire(node))
			require.NoError(t, err)
			current, err := EncodeNodePayload(node)
			require.NoError(t, err)
			require.Equal(t, previous, current)
			decoded, err := DecodeNodePayload(current)
			require.NoError(t, err)
			require.Equal(t, node, decoded)
			require.IsType(t, tc.value, decoded.Properties["p"])

			// Snapshot callers use the same codec, including nil elements.
			var expected bytes.Buffer
			require.NoError(t, msgpack.NewEncoder(&expected).Encode(struct {
				WALPosition uint64
				Nodes       []*frozenTransportNode
				Edges       []*transportEdgeImage
			}{WALPosition: 42, Nodes: []*frozenTransportNode{frozenNodeWire(node), nil}}))
			snapshot, err := EncodeCommandPayload(ReplicationSnapshot{WALPosition: 42, Nodes: []*Node{node, nil}})
			require.NoError(t, err)
			require.Equal(t, expected.Bytes(), snapshot)
			var restored ReplicationSnapshot
			require.NoError(t, DecodeCommandPayload(snapshot, &restored))
			require.Equal(t, uint64(42), restored.WALPosition)
			require.Equal(t, []*Node{node, nil}, restored.Nodes)
		})
	}
	for _, node := range []*Node{
		{ID: "test:nil"},
		{ID: "test:empty", Labels: []string{}, Properties: map[string]any{}, NamedEmbeddings: map[string][]float32{}, ChunkEmbeddings: [][]float32{}, EmbedMeta: map[string]any{}},
	} {
		before, err := EncodeCommandPayload(frozenNodeWire(node))
		require.NoError(t, err)
		after, err := EncodeNodePayload(node)
		require.NoError(t, err)
		require.Equal(t, before, after)
		decoded, err := DecodeNodePayload(after)
		require.NoError(t, err)
		require.Equal(t, node, decoded)
	}
}

func TestTransportNodeWireRetainsValidationAndAllocationBounds(t *testing.T) {
	node := &Node{ID: "test:wire", Properties: map[string]any{"p": int64(3)}}
	before := frozenNodeWire(node)
	var sink []byte
	previous := testing.AllocsPerRun(50, func() {
		var err error
		sink, err = EncodeCommandPayload(before)
		require.NoError(t, err)
	})
	current := testing.AllocsPerRun(50, func() {
		var err error
		sink, err = EncodeNodePayload(node)
		require.NoError(t, err)
	})
	require.NotEmpty(t, sink)
	require.LessOrEqual(t, current, previous, "wire consolidation must not add encode allocations")
	t.Logf("encode allocations: frozen %.0f, current %.0f", previous, current)
	for _, malformed := range [][]byte{nil, {0x82, 0xa2, 'I', 'D'}, append(append([]byte(nil), sink...), 0)} {
		decoded, err := DecodeNodePayload(malformed)
		require.Error(t, err)
		require.Nil(t, decoded)
	}
	nilValue, err := EncodeCommandPayload(nil)
	require.NoError(t, err)
	_, err = DecodeNodePayload(nilValue)
	require.ErrorIs(t, err, ErrInvalidID)
	var frame bytes.Buffer
	enc := msgpack.NewEncoder(&frame)
	require.NoError(t, enc.EncodeMapLen(3))
	require.NoError(t, enc.EncodeString("unknown"))
	require.NoError(t, enc.Encode([]any{"ignored", map[string]any{"nested": true}}))
	require.NoError(t, enc.EncodeString("ID"))
	require.NoError(t, enc.EncodeString("first"))
	require.NoError(t, enc.EncodeString("ID"))
	require.NoError(t, enc.EncodeString("last"))
	decoded, err := DecodeNodePayload(frame.Bytes())
	require.NoError(t, err)
	require.Equal(t, NodeID("last"), decoded.ID)
}
