package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

// BenchmarkIndexedTransactionRead compares complete indexed results in an open
// transaction and autocommit. Setup, BEGIN and ROLLBACK are outside timing;
// result caches are disabled and every result is consumed and checked.
func BenchmarkIndexedTransactionRead(b *testing.B) {
	for _, size := range []int{512, 12000, 24000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			base := newTestMemoryEngine(b)
			store := storage.NewNamespacedEngine(base, "txread")
			setup := NewStorageExecutor(store)
			ctx := b.Context()
			for _, q := range []string{
				"CREATE CONSTRAINT pd_id FOR (m:PDGraphNode) REQUIRE m.id IS UNIQUE",
				"CREATE INDEX pd_document FOR (m:PDGraphNode) ON (m.document_id)",
				"UNWIND range(1,$size) AS i CREATE (:PDGraphNode:PDEntity {id: toString(i), document_id: 'target', value: i})",
				"UNWIND range(1,256) AS i CREATE (:PDGraphNode:PDEntity {id: 'other-'+toString(i), document_id: 'other', value: i})",
			} {
				if _, err := setup.Execute(ctx, q, map[string]any{"size": int64(size)}); err != nil {
					b.Fatal(err)
				}
			}
			for _, result := range []string{"count", "nodes"} {
				q := "MATCH (m:PDGraphNode:PDEntity {document_id:$d}) RETURN m"
				if result == "count" {
					q = "MATCH (m:PDGraphNode:PDEntity {document_id:$d}) RETURN count(m)"
				}
				for _, explicit := range []bool{false, true} {
					name := "auto"
					if explicit {
						name = "explicit"
					}
					b.Run(result+"/"+name, func(b *testing.B) {
						exec := NewStorageExecutor(store)
						exec.cache = nil
						if explicit {
							if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
								b.Fatal(err)
							}
							b.Cleanup(func() {
								if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
									b.Error(err)
								}
							})
						}
						params := map[string]any{"d": "target"}
						read := func() {
							got, err := exec.Execute(ctx, q, params)
							if err != nil {
								b.Fatal(err)
							}
							if result == "count" {
								if len(got.Rows) != 1 || got.Rows[0][0] != int64(size) {
									b.Fatalf("bad count: %v", got.Rows)
								}
							} else {
								if len(got.Rows) != size {
									b.Fatalf("rows=%d want %d", len(got.Rows), size)
								}
								var total int64
								for _, row := range got.Rows {
									node, ok := row[0].(*storage.Node)
									if !ok {
										b.Fatalf("node type %T", row[0])
									}
									if node.Properties["document_id"] != "target" {
										b.Fatal("foreign document")
									}
									total += node.Properties["value"].(int64)
								}
								if total != int64(size*(size+1)/2) {
									b.Fatalf("sum=%d", total)
								}
							}
						}
						read()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							read()
						}
					})
				}
			}
		})
	}
}
