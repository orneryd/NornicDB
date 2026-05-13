package storage

import (
	"testing"
	"time"
)

func BenchmarkSerializeNode(b *testing.B) {
	node := benchmarkNode()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := encodeNode(node); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeNode(b *testing.B) {
	node := benchmarkNode()
	data, _, err := encodeNode(node)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeNode(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeEdge(b *testing.B) {
	edge := benchmarkEdge()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := encodeEdge(edge); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeEdge(b *testing.B) {
	edge := benchmarkEdge()
	data, err := encodeEdge(edge)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeEdge(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeEmbedding(b *testing.B) {
	embedding := benchmarkEmbedding()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := encodeEmbedding(embedding); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeEmbedding(b *testing.B) {
	embedding := benchmarkEmbedding()
	data, err := encodeEmbedding(embedding)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := decodeEmbedding(data); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkNode() *Node {
	return &Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person", "User"},
		Properties: map[string]any{
			"name":    "Alice",
			"age":     int64(30),
			"score":   float64(9.5),
			"active":  true,
			"created": time.Unix(1700000000, 0).UTC(),
			"tags":    []string{"a", "b", "c"},
			"nums":    []int64{1, 2, 3, 4, 5},
			"attrs": map[string]any{
				"role":   "admin",
				"height": float64(1.75),
			},
		},
	}
}

func benchmarkEdge() *Edge {
	return &Edge{
		ID:        EdgeID("edge-1"),
		StartNode: NodeID("node-1"),
		EndNode:   NodeID("node-2"),
		Type:      "KNOWS",
		Properties: map[string]any{
			"since":  int64(2020),
			"weight": float64(0.42),
			"tags":   []string{"friend", "colleague"},
		},
	}
}

func benchmarkEmbedding() []float32 {
	emb := make([]float32, 384)
	for i := range emb {
		emb[i] = float32(i) * 0.001
	}
	return emb
}
