package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// benchEngine spins up a fresh in-memory engine for codec benchmarks.
// All benches share this helper so they exercise the V2 codec end-to-
// end (dictionary allocation included).
func benchEngine(b *testing.B) *BadgerEngine {
	b.Helper()
	eng, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { eng.Close() })
	return eng
}

func BenchmarkSerializeNode(b *testing.B) {
	eng := benchEngine(b)
	node := benchmarkNode()
	ns := namespaceForNodeID(node.ID)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := eng.withUpdate(func(txn *badger.Txn) error {
			_, _, err := eng.encodeNodeInTxn(txn, ns, node)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeNode(b *testing.B) {
	eng := benchEngine(b)
	node := benchmarkNode()
	ns := namespaceForNodeID(node.ID)
	var data []byte
	if err := eng.withUpdate(func(txn *badger.Txn) error {
		var encErr error
		data, _, encErr = eng.encodeNodeInTxn(txn, ns, node)
		if encErr != nil {
			return encErr
		}
		return eng.propKeyDict.flushTxnCounters(txn)
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := eng.decodeNode(ns, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSerializeEdge(b *testing.B) {
	eng := benchEngine(b)
	edge := benchmarkEdge()
	ns := namespaceForEdgeID(edge.ID)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := eng.withUpdate(func(txn *badger.Txn) error {
			_, err := eng.encodeEdgeInTxn(txn, ns, edge)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserializeEdge(b *testing.B) {
	eng := benchEngine(b)
	edge := benchmarkEdge()
	ns := namespaceForEdgeID(edge.ID)
	var data []byte
	if err := eng.withUpdate(func(txn *badger.Txn) error {
		var encErr error
		data, encErr = eng.encodeEdgeInTxn(txn, ns, edge)
		if encErr != nil {
			return encErr
		}
		if err := eng.idDict.flushTxnCounters(txn); err != nil {
			return err
		}
		return eng.propKeyDict.flushTxnCounters(txn)
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := eng.decodeEdgeBodyWithID(ns, data, edge.ID); err != nil {
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
		ID:     NodeID("test:node-1"),
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
		ID:        EdgeID("test:edge-1"),
		StartNode: NodeID("test:node-1"),
		EndNode:   NodeID("test:node-2"),
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
