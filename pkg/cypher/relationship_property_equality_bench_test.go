package cypher

import (
	"context"
	"fmt"
	"runtime/pprof"
	"testing"
)

// BenchmarkRelationshipPropertyEquality reuses the public correctness fixture.
// Constraint preparation and parser admission precede timing; result caches are
// disabled. Both transaction modes return the identical single relationship.
func BenchmarkRelationshipPropertyEquality(b *testing.B) {
	for _, parser := range relationshipEqualityParsers {
		b.Run(parser.name, func(b *testing.B) {
			b.Cleanup(parser.setup())
			for _, population := range []int{128, 4096} {
				for _, explicit := range []bool{false, true} {
					b.Run(fmt.Sprintf("unrelated-%d/explicit-%v", population, explicit), func(b *testing.B) {
						b.StopTimer()
						fixture := newRelationshipEqualityAccessFixture(b, relationshipEqualityFixtureOptions{unrelatedRelationships: population})
						ctx := context.Background()
						if explicit {
							ctx = beginRelationshipEqualityTransaction(b, fixture.exec)
						}
						params := map[string]interface{}{"id": "r289999"}
						read := func() {
							result, err := fixture.exec.Execute(ctx, relationshipEqualityAccessQuery, params)
							if err != nil {
								b.Fatal(err)
							}
							if len(result.Rows) != 1 || len(result.Rows[0]) != 3 || result.Rows[0][0] != "r289999" || result.Rows[0][1] != "n73" || result.Rows[0][2] != "n128561" {
								b.Fatalf("unexpected result: %v", result.Rows)
							}
						}
						read()
						pprof.Do(ctx, pprof.Labels("phase", "relationship-read"), func(context.Context) {
							b.ReportAllocs()
							b.ResetTimer()
							b.StartTimer()
							for i := 0; i < b.N; i++ {
								read()
							}
							b.StopTimer()
						})
					})
				}
			}
		})
	}
}
