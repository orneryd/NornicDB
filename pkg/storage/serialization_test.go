package storage

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageSerializerMsgpackRoundTrip(t *testing.T) {
	node := &Node{
		ID:         NodeID("node-1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"age": int64(42), "name": "Alice"},
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
	}

	data, _, err := encodeNode(node)
	require.NoError(t, err)

	decoded, err := decodeNode(data)
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.Properties, decoded.Properties)
	require.True(t, decoded.CreatedAt.Equal(node.CreatedAt))
}

func TestDecodeNode_LegacyGobFallback(t *testing.T) {
	node := &Node{
		ID:         NodeID("legacy-node"),
		Labels:     []string{"Legacy"},
		Properties: map[string]any{"count": int64(7)},
	}

	// Synthesize a header-less gob body the way pre-msgpack writers
	// produced them. The decoder is required to handle these for the
	// in-place migration tool, even though the engine never emits them.
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(node))

	decoded, err := decodeNode(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.Properties, decoded.Properties)
}

func TestSplitSerializationHeader(t *testing.T) {
	// Short data — no header.
	id, payload, ok, err := splitSerializationHeader([]byte("tiny"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, id)
	require.Nil(t, payload)

	// Wrong magic — no header.
	id, payload, ok, err = splitSerializationHeader([]byte("plain-gob-data"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, id)
	require.Nil(t, payload)

	badVersion := append([]byte(serializationMagic), byte(99), serializerIDMsgpack)
	_, _, _, err = splitSerializationHeader(badVersion)
	require.ErrorContains(t, err, "unsupported serialization version")
}

func TestSerializeEdge_RoundTrip(t *testing.T) {
	edge := &Edge{
		ID:        EdgeID("edge-1"),
		StartNode: NodeID("node-1"),
		EndNode:   NodeID("node-2"),
		Type:      "KNOWS",
		Properties: map[string]any{
			"weight": int64(3),
		},
	}

	data, err := serializeEdge(edge)
	require.NoError(t, err)

	decoded, err := deserializeEdge(data)
	require.NoError(t, err)
	require.Equal(t, edge.ID, decoded.ID)
	require.Equal(t, edge.Type, decoded.Type)
	require.Equal(t, edge.Properties, decoded.Properties)

	_, err = deserializeEdge([]byte("not-a-valid-edge"))
	require.ErrorContains(t, err, "decoding edge")

	_, err = deserializeNode([]byte("not-a-valid-node"))
	require.ErrorContains(t, err, "decoding node")
}

func TestSerializeNode_RejectsUnencodableProperty(t *testing.T) {
	node := &Node{
		ID:     "n-bad",
		Labels: []string{"Bad"},
		Properties: map[string]any{
			"bad": make(chan int),
		},
	}
	_, err := serializeNode(node)
	require.ErrorContains(t, err, "encoding node")

	edge := &Edge{
		ID:        "e-bad",
		StartNode: "n1",
		EndNode:   "n2",
		Type:      "REL",
		Properties: map[string]any{
			"bad": make(chan int),
		},
	}
	_, err = serializeEdge(edge)
	require.ErrorContains(t, err, "encoding edge")
}
