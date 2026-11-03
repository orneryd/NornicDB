package storage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSemanticPropertyEmptyMapsUseCanonicalEncodingWithoutAllocation(t *testing.T) {
	reference, err := msgpack.Marshal(map[string]any{})
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		values map[string]any
	}{{name: "nil"}, {name: "empty", values: map[string]any{}}} {
		t.Run(test.name, func(t *testing.T) {
			var buffer bytes.Buffer
			encoder := msgpack.NewEncoder(&buffer)
			values := semanticPropertyValues{values: test.values}
			allocations := testing.AllocsPerRun(100, func() {
				buffer.Reset()
				if err := values.EncodeMsgpack(encoder); err != nil {
					panic(err)
				}
			})
			require.Equal(t, reference, buffer.Bytes(), "nil and empty preserve the existing canonical empty-map bytes")
			require.Zero(t, allocations, "no Go map needs materializing to write an empty MessagePack map")
		})
	}
}

func TestSemanticTransactionNilAndEmptyMapsPreservePublicWire(t *testing.T) {
	request := EntityTransactionRequest{Namespace: "test", LogicalID: "empty-map-wire", Mode: CommitModeDirect, Operations: []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:empty"}}}}
	absent, err := EncodeEntityTransaction(request)
	require.NoError(t, err)
	request.Operations[0].Node.Properties = map[string]any{}
	request.Operations[0].Node.EmbedMeta = map[string]any{}
	present, err := EncodeEntityTransaction(request)
	require.NoError(t, err)
	require.Equal(t, absent, present, "public normalization remains exactly byte-equivalent for nil and empty maps")
	_, ops, err := DecodeEntityTransaction(absent)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.NotNil(t, ops[0].Node.Properties)
	require.Empty(t, ops[0].Node.Properties)
	require.NotNil(t, ops[0].Node.EmbedMeta)
	require.Empty(t, ops[0].Node.EmbedMeta)
}
