package multidb

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestQuotaSizeUsesCanonicalMessagePackImage(t *testing.T) {
	node := &storage.Node{ID: "n", Labels: []string{"Item"}, Properties: map[string]any{
		"number": int64(9), "vector": []float32{1, 2}, "nested": map[string]any{"items": []string{"a", "b"}}, "time": time.Unix(12345, 67).UTC(),
	}}
	node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{Database: "query-provenance"})
	edge := &storage.Edge{ID: "e", Database: "query-provenance", StartNode: "n", EndNode: "m", Type: "LINK", Properties: map[string]any{"count": int64(3)}}
	nodeCanonical, edgeCanonical := *node, *edge
	nodeCanonical.SetRuntimeMetadata(storage.NodeRuntimeMetadata{})
	edgeCanonical.Database = ""
	nodeBytes, err := msgpack.Marshal(&nodeCanonical)
	require.NoError(t, err)
	edgeBytes, err := msgpack.Marshal(&edgeCanonical)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		nodeSize, err := calculateNodeSize(node)
		require.NoError(t, err)
		edgeSize, err := calculateEdgeSize(edge)
		require.NoError(t, err)
		require.EqualValues(t, len(nodeBytes), nodeSize)
		require.EqualValues(t, len(edgeBytes), edgeSize)
	}
	require.Equal(t, "query-provenance", node.RuntimeMetadata().Database)
	require.Equal(t, "query-provenance", edge.Database)
}

func BenchmarkQuotaNodeSizeCodec(b *testing.B) {
	node := &storage.Node{ID: "node-123", Labels: []string{"Item"}, Properties: map[string]any{"name": "northwind", "count": int64(9), "vector": []float32{1, 2, 3}}}
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"gob", func() error {
			canonical := *node
			{
				metadata := canonical.RuntimeMetadata()
				metadata.Database = ""
				canonical.SetRuntimeMetadata(metadata)
			}
			if len(canonical.Properties) == 0 {
				canonical.Properties = nil
			}
			var out bytes.Buffer
			return gob.NewEncoder(&out).Encode(&canonical)
		}},
		{"current", func() error { _, err := calculateNodeSize(node); return err }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := tc.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
