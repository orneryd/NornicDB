package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkCreateConstraint benchmarks CREATE CONSTRAINT parsing
func BenchmarkCreateConstraint(b *testing.B) {
	baseStore := newTestMemoryEngine(b)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	cypher := "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE"

	fixtureAdmission1786Context, fixtureAdmission1786 := statementTestContext(ctx, cypher)
	b.ResetTimer()

	_ = fixtureAdmission1786Context
	for i := 0; i < b.N; i++ {
		exec.executeCreateConstraint(fixtureAdmission1786, fixtureAdmission1786Context, cypher)
	}
}

// BenchmarkCreateIndex benchmarks CREATE INDEX parsing
func BenchmarkCreateIndex(b *testing.B) {
	baseStore := newTestMemoryEngine(b)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	cypher := "CREATE INDEX user_email IF NOT EXISTS FOR (n:User) ON (n.email)"

	fixtureAdmission1787Context, fixtureAdmission1787 := statementTestContext(ctx, cypher)
	b.ResetTimer()

	_ = fixtureAdmission1787Context
	for i := 0; i < b.N; i++ {
		exec.executeCreateIndex(fixtureAdmission1787, fixtureAdmission1787Context, cypher)
	}
}

// BenchmarkCreateVectorIndex benchmarks CREATE VECTOR INDEX parsing
func BenchmarkCreateVectorIndex(b *testing.B) {
	baseStore := newTestMemoryEngine(b)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	cypher := `CREATE VECTOR INDEX embedding_idx IF NOT EXISTS FOR (n:Document) ON (n.embedding) 
		OPTIONS {indexConfig: {` + "`vector.dimensions`" + `: 1024, ` + "`vector.similarity_function`" + `: 'cosine'}}`

	fixtureAdmission1788Context, fixtureAdmission1788 := statementTestContext(ctx, cypher)
	b.ResetTimer()

	_ = fixtureAdmission1788Context
	for i := 0; i < b.N; i++ {
		exec.executeCreateVectorIndex(fixtureAdmission1788, fixtureAdmission1788Context, cypher)
	}
}

// BenchmarkParseDuration benchmarks ISO 8601 duration parsing
func BenchmarkParseDuration(b *testing.B) {
	durations := []string{
		"P1Y",
		"P1M",
		"P1D",
		"PT1H",
		"PT1M",
		"PT1S",
		"P1Y2M3D",
		"PT4H5M6S",
		"P1Y2M3DT4H5M6S",
		"P1Y2M3DT4H5M6.789S",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, d := range durations {
			parseDuration(d)
		}
	}
}

// BenchmarkFulltextQueryParsing benchmarks fulltext query phrase extraction
func BenchmarkFulltextQueryParsing(b *testing.B) {
	queries := []string{
		`simple query`,
		`"exact phrase"`,
		`word1 "phrase one" word2 "phrase two"`,
		`complex AND query OR "multiple phrases" NOT excluded`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			parseFulltextQuery(q)
		}
	}
}
