package adminimport

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"path/filepath"
	"strconv"
	"testing"
)

// BenchmarkCSVEntityExport measures complete CSV row construction and file
// output for 1,000 entities. The fixture is immutable and built before timing;
// each iteration overwrites one task-owned file rather than accumulating files.
func BenchmarkCSVEntityExport(b *testing.B) {
	const count = 1000
	nodes := make([]*storage.Node, count)
	edges := make([]*storage.Edge, count)
	properties := map[string]any{"name": "example", "blank": "", "scores": []float64{1.25, 2.5}, "active": true}
	for i := range count {
		id := strconv.Itoa(i)
		nodes[i] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: properties}
		edges[i] = &storage.Edge{ID: storage.EdgeID(id), StartNode: "a", EndNode: "b", Type: "LINK", Properties: properties}
	}
	opts := Neo4jCSVExportOptions{Delimiter: ',', ArrayDelimiter: ';', VectorDelimiter: '|'}
	b.Run("nodes", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "nodes.csv")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := exportNodesCSV(path, nodes, opts); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("relationships", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "relationships.csv")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := exportRelationshipsCSV(path, edges, opts); err != nil {
				b.Fatal(err)
			}
		}
	})
}
