package search

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkBuildIndexesWithRelationships times a build over 200 nodes and
// 2,000 relationships, each with one 1024-dimension vector and a text
// property: the node phase and the relationship phase together.
func BenchmarkBuildIndexesWithRelationships(b *testing.B) {
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	engine := storage.NewMemoryEngine()
	defer engine.Close()
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i%7) / 7
	}
	const nodes, relationships = 200, 2000
	for i := range nodes {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"},
			Properties: map[string]any{"text": fmt.Sprintf("node %d", i)}, NamedEmbeddings: map[string][]float32{"v": vec}}
		if _, err := engine.CreateNode(node); err != nil {
			b.Fatal(err)
		}
	}
	for i := range relationships {
		edge := &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nornic:r%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("nornic:n%d", i%nodes)), EndNode: storage.NodeID(fmt.Sprintf("nornic:n%d", (i+1)%nodes)),
			Type: "REL", Properties: map[string]any{"v": vec, "note": strings.Repeat("text ", 40)}}
		if err := engine.CreateEdge(edge); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		svc := NewServiceWithDimensions(engine, 1024)
		b.StartTimer()
		if err := svc.BuildIndexes(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		_ = svc.Close()
		b.StartTimer()
	}
}
