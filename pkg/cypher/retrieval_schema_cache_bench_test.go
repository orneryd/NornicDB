package cypher

import (
	"context"
	"fmt"
	"reflect"
	"runtime/pprof"
	"sync/atomic"
	"testing"
)

var retrievalAdmissionSequence atomic.Uint64

// BenchmarkRetrieveAfterSchemaPublication separates post-DDL cache revocation
// from first-use query admission on the public 12.5K-source fixture. Cold reads
// use unique projection identifiers, which cannot share a successful syntax
// cache entry through literal normalization. DDL, text construction and exact
// result assertions remain outside timing. Result caches are disabled.
func BenchmarkRetrieveAfterSchemaPublication(b *testing.B) {
	for _, parser := range relationshipEqualityParsers {
		b.Run(parser.name, func(b *testing.B) {
			b.Cleanup(parser.setup())
			fixture := newNOR406RetrievalFixture(b, 12500)
			fixture.service.SetSearchResultCachePolicy(0, 0)
			exec := fixture.newExecutor()
			const query = "CALL db.retrieve($request) YIELD node RETURN node.id"
			request := fixture.oneShotRequest(false)
			request["limit"] = int64(3)
			parameters := fixture.parameters(request)
			expected, err := exec.Execute(b.Context(), query, parameters)
			if err != nil {
				b.Fatal(err)
			}
			if len(expected.Rows) != 3 {
				b.Fatalf("expected three hits, got %d", len(expected.Rows))
			}
			for _, workload := range []struct {
				name           string
				cold, afterDDL bool
			}{
				{name: "warm"},
				{name: "after-schema", afterDDL: true},
				{name: "cold", cold: true},
				{name: "cold-after-schema", cold: true, afterDDL: true},
			} {
				b.Run(workload.name, func(b *testing.B) {
					labels := pprof.Labels("phase", "retrieve-query", "parser", parser.name, "read", workload.name)
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						sequence := retrievalAdmissionSequence.Add(1)
						if workload.afterDDL {
							ddl := fmt.Sprintf("CREATE INDEX retrieval_cache_%d FOR (d:Document) ON (d.probe_%d)", sequence, sequence)
							if _, err := exec.Execute(b.Context(), ddl, nil); err != nil {
								b.Fatal(err)
							}
						}
						statement, columns := query, expected.Columns
						if workload.cold {
							alias := fmt.Sprintf("retrieval_admission_%s_%d", parser.name, sequence)
							statement += " AS " + alias
							columns = []string{alias}
						}
						pprof.Do(b.Context(), labels, func(ctx context.Context) {
							b.StartTimer()
							result, err := exec.Execute(ctx, statement, parameters)
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							if !reflect.DeepEqual(expected.Rows, result.Rows) || !reflect.DeepEqual(columns, result.Columns) {
								b.Fatalf("schema/admission changed results: got %v/%v want %v/%v", result.Columns, result.Rows, columns, expected.Rows)
							}
						})
					}
				})
			}
		})
	}
}
