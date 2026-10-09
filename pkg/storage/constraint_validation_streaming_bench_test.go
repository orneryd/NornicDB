package storage

import (
	"testing"
)

// Constraint DDL on a server that holds one large database ("b") and creates
// constraints in a new, empty database ("a"). Creation-time validation used
// to scan every edge (AllEdges) / node (AllNodes) of the whole server; it now
// streams only the constrained relationship type / labels of database "a".
//
// go test ./pkg/storage/ -run '^$' -bench 'WithLargeOtherDatabase' -benchmem -count=3
//
// Apple M-series (14 cores), 20k nodes + 100k edges (1k of the constrained
// type) in "b", -benchtime=5x:
//
//	CreateRelationshipConstraint  before: ~55,000,000 ns/op  72.7 MB/op  1,405,000 allocs/op (AllEdges)
//	                              after:      ~130,000 ns/op   7.4 KB/op         55 allocs/op (scoped type stream)
//	RefreshUniqueConstraint       before: ~22,600,000 ns/op  25.2 MB/op    336,600 allocs/op (AllNodes)
//	                              after:    ~2,050,000 ns/op  426 KB/op     20,400 allocs/op (scoped label stream)

const (
	benchOtherDBNodes     = 20_000
	benchOtherDBEdges     = 100_000
	benchOtherDBSameTypes = 1_000
)

func setupLargeOtherDatabase(b *testing.B) *NamespacedEngine {
	b.Helper()
	inner := NewMemoryEngine()
	b.Cleanup(func() { _ = inner.Close() })
	populateOtherDatabase(b, inner, benchOtherDBNodes, benchOtherDBEdges-benchOtherDBSameTypes, benchOtherDBSameTypes)
	return NewNamespacedEngine(inner, "a")
}

func BenchmarkCreateRelationshipConstraint_WithLargeOtherDatabase(b *testing.B) {
	nsA := setupLargeOtherDatabase(b)
	c := Constraint{Name: "card", Type: ConstraintCardinality, EntityType: ConstraintEntityRelationship, Label: "REL", Direction: "OUTGOING", MaxCount: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ValidateConstraintOnCreationForEngine(nsA, c); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRefreshUniqueConstraint_WithLargeOtherDatabase(b *testing.B) {
	nsA := setupLargeOtherDatabase(b)
	schema := nsA.GetSchema()
	if err := schema.AddConstraint(Constraint{Name: "uniq_email", Type: ConstraintUnique, Label: "User", Properties: []string{"email"}}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := RefreshUniqueConstraintValuesForEngine(nsA, schema); err != nil {
			b.Fatal(err)
		}
	}
}
