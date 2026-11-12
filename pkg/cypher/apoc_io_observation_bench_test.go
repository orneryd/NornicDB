package cypher

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkAPOCIOReadObservation qualifies ordinary public I/O operations.
// Result caching is disabled; file and graph preparation precede timing.
func BenchmarkAPOCIOReadObservation(b *testing.B) {
	for _, mode := range []string{"load-json", "export-json", "export-csv", "export-query"} {
		b.Run(mode, func(b *testing.B) {
			base, err := testfixture.OwnResult[*storage.BadgerEngine](b, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			store := storage.NewNamespacedEngine(base, "io_observation_bench")
			for i := 0; i < 128; i++ {
				_, err := store.CreateNode(b.Context(), &storage.Node{ID: storage.NodeID("n" + strconv.Itoa(i)), Labels: []string{"ObservedIO"}, Properties: map[string]interface{}{"v": int64(i), "text": strings.Repeat("body", 16)}})
				if err != nil {
					b.Fatal(err)
				}
			}
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			exec.SetAllowLocalAPOCFileAccess(true)
			query := "CALL apoc.export.json.all('')"
			var params map[string]interface{}
			switch mode {
			case "load-json":
				file := filepath.Join(b.TempDir(), "input.json")
				if err := os.WriteFile(file, []byte(`{"v":"one","items":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}`), 0600); err != nil {
					b.Fatal(err)
				}
				query = "CALL apoc.load.json($file) YIELD value RETURN value"
				params = map[string]interface{}{"file": file}
			case "export-csv":
				query = "CALL apoc.export.csv.all('')"
			case "export-query":
				query = "CALL apoc.export.json.query('MATCH (n:ObservedIO) RETURN n.v AS v','')"
			}
			first, err := exec.Execute(b.Context(), query, params)
			if err != nil {
				b.Fatal(err)
			}
			if len(first.Rows) != 1 {
				b.Fatalf("unexpected public result: %#v", first.Rows)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := exec.Execute(b.Context(), query, params)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 {
					b.Fatalf("unexpected public result: %#v", result.Rows)
				}
			}
		})
	}
}
