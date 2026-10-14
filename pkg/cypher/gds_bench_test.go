package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	gdsBenchmarkNodes            = 5000
	gdsBenchmarkRelationships    = gdsBenchmarkNodes * 4
	gdsBenchmarkScalarProjection = "CALL gds.graph.project('gds-bench', {BenchGDS: {properties: ['integer', 'float']}}, {GDS_LINK: {properties: 'weight'}})"
	gdsBenchmarkListProjection   = "CALL gds.graph.project('gds-bench', {BenchGDS: {properties: ['integer', 'float', 'list']}}, {GDS_LINK: {properties: 'weight'}})"
)

var gdsBenchmarkResult *ExecuteResult

// newGDSBenchmarkExecutor builds one deterministic storage graph, outside the
// measured interval. Every node has Integer, Float and four-element Float-list
// properties. Four fixed hops per node give 20,000 weighted relationships.
// Use only the public Cypher API so this file also builds on main 3abe406c.
func newGDSBenchmarkExecutor(b *testing.B) (*StorageExecutor, []string) {
	b.Helper()
	b.StopTimer()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench-"+strings.ReplaceAll(b.Name(), "/", "-"))
	ids := make([]string, gdsBenchmarkNodes)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%05d", i)
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID(ids[i]), Labels: []string{"BenchGDS"},
			Properties: map[string]interface{}{
				"integer": int64(i%23 + 1),
				"float":   float64(i%17+1) / 8,
				"list":    []float64{float64(i % 11), float64(i%7) / 4, 0.5, 1.5},
			},
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	for i, id := range ids {
		for j, hop := range []int{1, 7, 17, 97} {
			err := engine.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("edge-%05d", 4*i+j)),
				StartNode: storage.NodeID(id), EndNode: storage.NodeID(ids[(i+hop)%len(ids)]), Type: "GDS_LINK",
				Properties: map[string]interface{}{"weight": float64(1+(i+j)%8) / 8},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	exec := NewStorageExecutor(engine)
	b.Cleanup(func() {
		if _, err := exec.Execute(context.Background(), "CALL gds.graph.drop('gds-bench', false)", nil); err != nil {
			b.Error(err)
		}
	})
	return exec, ids
}

// Main 3abe406c implements these procedures but cannot project list properties.
// Keep common scalar cases comparable on both versions; explicitly skip the
// additional list cases on that API, instead of substituting scalar features
// and reporting a misleading list-cost comparison. The capability check is
// outside timing and accepts only the known unsupported-property error.
func requireGDSBenchmarkLists(b *testing.B, exec *StorageExecutor) {
	b.Helper()
	b.StopTimer()
	_, err := exec.Execute(context.Background(), gdsBenchmarkListProjection, nil)
	if err != nil {
		if strings.Contains(err.Error(), "Node properties not found: 'list'") {
			b.Skip("this API does not project numeric lists; compare the scalar case")
		}
		b.Fatal(err)
	}
	if _, err := exec.Execute(context.Background(), "CALL gds.graph.drop('gds-bench')", nil); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkGDSProjectNative measures import and property conversion of 5,000
// nodes and 20,000 weighted relationships. Catalog deletion is not timed.
func BenchmarkGDSProjectNative(b *testing.B) {
	for _, withLists := range []bool{false, true} {
		name, query := "scalars", gdsBenchmarkScalarProjection
		if withLists {
			name, query = "lists", gdsBenchmarkListProjection
		}
		b.Run(name, func(b *testing.B) {
			exec, _ := newGDSBenchmarkExecutor(b)
			if withLists {
				requireGDSBenchmarkLists(b, exec)
			}
			params := map[string]interface{}{"nonce": 0}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				params["nonce"] = i
				result, err := exec.Execute(context.Background(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0][3] != int64(gdsBenchmarkNodes) || result.Rows[0][4] != int64(gdsBenchmarkRelationships) {
					b.Fatalf("unexpected projection counts: %v", result.Rows)
				}
				gdsBenchmarkResult = result
				b.StopTimer()
				if _, err := exec.Execute(context.Background(), "CALL gds.graph.drop('gds-bench')", nil); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()
		})
	}
}

// BenchmarkGDSFastRPStream measures weighted propagation and feature extraction
// at 64 dimensions with a fixed seed, including list features on supported APIs.
func BenchmarkGDSFastRPStream(b *testing.B) {
	for _, withLists := range []bool{false, true} {
		name, projection, features := "scalar_features", gdsBenchmarkScalarProjection, "['integer', 'float']"
		if withLists {
			name, projection, features = "list_features", gdsBenchmarkListProjection, "['integer', 'float', 'list']"
		}
		b.Run(name, func(b *testing.B) {
			exec, _ := newGDSBenchmarkExecutor(b)
			if withLists {
				requireGDSBenchmarkLists(b, exec)
			}
			if _, err := exec.Execute(context.Background(), projection, nil); err != nil {
				b.Fatal(err)
			}
			query := "CALL gds.fastRP.stream('gds-bench', {embeddingDimension: 64, randomSeed: 42, iterationWeights: [1.0, 1.0, 1.0], propertyRatio: 0.5, featureProperties: " + features + ", relationshipWeightProperty: 'weight'})"
			params := map[string]interface{}{"nonce": 0}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				params["nonce"] = i
				result, err := exec.Execute(context.Background(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != gdsBenchmarkNodes {
					b.Fatalf("streamed %d nodes, want %d", len(result.Rows), gdsBenchmarkNodes)
				}
				gdsBenchmarkResult = result
			}
			b.StopTimer()
		})
	}
}

// BenchmarkGDSNodePropertyRead measures the Cypher utility's typed property reads
// over the same graph. Changing nonce prevents query-result cache hits when node
// IDs wrap, so every operation reaches gds.util.nodeProperty.
func BenchmarkGDSNodePropertyRead(b *testing.B) {
	for _, property := range []string{"integer", "float", "list"} {
		b.Run(property, func(b *testing.B) {
			exec, ids := newGDSBenchmarkExecutor(b)
			projection := gdsBenchmarkScalarProjection
			if property == "list" {
				requireGDSBenchmarkLists(b, exec)
				projection = gdsBenchmarkListProjection
			}
			if _, err := exec.Execute(context.Background(), projection, nil); err != nil {
				b.Fatal(err)
			}
			query := "RETURN gds.util.nodeProperty('gds-bench', $node, $property)"
			params := map[string]interface{}{"node": ids[0], "property": property, "nonce": 0}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				params["node"], params["nonce"] = ids[i%len(ids)], i
				result, err := exec.Execute(context.Background(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
					b.Fatalf("unexpected property result: %v", result.Rows)
				}
				gdsBenchmarkResult = result
			}
			b.StopTimer()
		})
	}
}
