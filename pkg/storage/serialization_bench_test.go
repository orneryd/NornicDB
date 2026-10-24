package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// benchEngine spins up a fresh in-memory engine for codec benchmarks.
// All benches share this helper so they exercise the V2 codec end-to-
// end (dictionary allocation included).
func benchEngine(b *testing.B) *BadgerEngine {
	b.Helper()
	eng, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
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
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
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

func BenchmarkDecodeTokenizedPropertiesGraphitiVector(b *testing.B) {
	eng := benchEngine(b)
	props := benchmarkGraphitiProperties()
	var data []byte
	if err := eng.withUpdate(func(txn *badger.Txn) error {
		var encErr error
		data, encErr = eng.encodeTokenizedProperties(txn, "test", props)
		if encErr != nil {
			return encErr
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := eng.decodeTokenizedProperties("test", data)
		if err != nil {
			b.Fatal(err)
		}
		if vec, ok := out["embedding"].([]float64); !ok || len(vec) != 1024 {
			b.Fatalf("unexpected embedding shape: %T len=%d", out["embedding"], len(vec))
		}
	}
}

func BenchmarkDeserializeNodeGraphitiVector(b *testing.B) {
	eng := benchEngine(b)
	node := benchmarkNode()
	node.Properties = benchmarkGraphitiProperties()
	ns := namespaceForNodeID(node.ID)
	var data []byte
	if err := eng.withUpdate(func(txn *badger.Txn) error {
		var encErr error
		data, _, encErr = eng.encodeNodeInTxn(txn, ns, node)
		if encErr != nil {
			return encErr
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := eng.decodeNode(ns, data)
		if err != nil {
			b.Fatal(err)
		}
		if vec, ok := out.Properties["embedding"].([]float64); !ok || len(vec) != 1024 {
			b.Fatalf("unexpected embedding shape: %T", out.Properties["embedding"])
		}
	}
}

func BenchmarkDeserializeNodeGraphitiVectorProjectedScalars(b *testing.B) {
	eng := benchEngine(b)
	node := benchmarkNode()
	node.Properties = benchmarkGraphitiProperties()
	ns := namespaceForNodeID(node.ID)
	var data []byte
	if err := eng.withUpdate(func(txn *badger.Txn) error {
		var encErr error
		data, _, encErr = eng.encodeNodeInTxn(txn, ns, node)
		if encErr != nil {
			return encErr
		}
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	include := propertyProjectionSet([]string{"name", "group_id"})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := eng.decodeNodeProjected(ns, data, include)
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := out.Properties["embedding"]; ok {
			b.Fatal("projected decode materialized embedding")
		}
		if out.Properties["group_id"] != "episode-100" {
			b.Fatalf("unexpected group_id: %v", out.Properties["group_id"])
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
		_, _ = eng.idDict.flushTxnCounters(txn)
		_ = eng.propKeyDict.flushTxnCounters(txn)
		return nil
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

func benchmarkGraphitiProperties() map[string]any {
	embedding := make([]float64, 1024)
	for i := range embedding {
		embedding[i] = float64(i%97) * 0.001
	}
	return map[string]any{
		"name":          "Alice Johnson",
		"group_id":      "episode-100",
		"summary":       "Entity extracted from a Graphiti episode with client-supplied embeddings.",
		"entity_source": "message",
		"embedding":     embedding,
	}
}
