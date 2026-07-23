package cypher

// Benchmarks for the traversal-seeded OPTIONAL MATCH projection path
// (executeTraversalSeededOptionalMatch). These cover the query shape whose
// projection was previously corrupted (function-call expressions echoed as
// literal text): a relationship-bound MATCH followed by trailing OPTIONAL
// MATCH clauses with function-call projections in RETURN.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// seedTraversalOptionalBenchGraph creates classCount source classes, each
// inheriting from 2 targets, every class contained in a file, and each file
// contained in a repository — the Eshu entity-context handler shape.
func seedTraversalOptionalBenchGraph(b *testing.B, exec *StorageExecutor, ctx context.Context, classCount int) {
	b.Helper()
	for i := 0; i < classCount; i++ {
		_, err := exec.Execute(ctx, fmt.Sprintf(`
			CREATE (c:BenchClass {uid:"cls:src%[1]d", name:"Src%[1]d", language:"python"})
			CREATE (d:BenchClass {uid:"cls:base%[1]d", name:"Base%[1]d"})
			CREATE (m:BenchClass {uid:"cls:mixin%[1]d", name:"Mixin%[1]d"})
			CREATE (f:BenchFile {uid:"file:f%[1]d", relative_path:"pkg/f%[1]d.py", language:"python"})
			CREATE (r:BenchRepo {id:"repo:r%[1]d", name:"repo%[1]d"})
			CREATE (c)-[:INHERITS {weight: %[1]d}]->(d)
			CREATE (c)-[:INHERITS {weight: %[1]d}]->(m)
			CREATE (f)-[:CONTAINS]->(c)
			CREATE (f)-[:CONTAINS]->(d)
			CREATE (f)-[:CONTAINS]->(m)
			CREATE (r)-[:REPO_CONTAINS]->(f)
		`, i), nil)
		if err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}
}

// BenchmarkTraversalOptionalMatch_SingleSeedChained is the exact
// relationship-context handler shape: one anchored source, four chained
// OPTIONAL MATCH clauses, function-call projections.
func BenchmarkTraversalOptionalMatch_SingleSeedChained(b *testing.B) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	// Disable the query result cache so every iteration measures real
	// routing, join, and projection work instead of a cache hit.
	exec.cache = nil
	ctx := context.Background()
	seedTraversalOptionalBenchGraph(b, exec, ctx, 50)

	query := `
		MATCH (e:BenchClass {uid:"cls:src25"})-[rel:INHERITS]->(target)
		OPTIONAL MATCH (e)<-[:CONTAINS]-(sourceFile:BenchFile)
		OPTIONAL MATCH (sourceRepo:BenchRepo)-[:REPO_CONTAINS]->(sourceFile)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(targetFile:BenchFile)
		OPTIONAL MATCH (targetRepo:BenchRepo)-[:REPO_CONTAINS]->(targetFile)
		RETURN type(rel) AS type,
		       coalesce(e.id, e.uid) AS source_id,
		       target.name AS target_name,
		       coalesce(target.id, target.uid) AS target_id,
		       targetFile.relative_path AS target_file,
		       targetRepo.id AS target_repo`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) == 0 {
			b.Fatal("expected rows")
		}
	}
}

// BenchmarkTraversalOptionalMatch_FanOutProjection stresses per-row projection
// cost: every class row (150 relationship rows over 50 sources) flows through
// the OPTIONAL MATCH join and the function-call projection.
func BenchmarkTraversalOptionalMatch_FanOutProjection(b *testing.B) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "bench")
	exec := NewStorageExecutor(store)
	// Disable the query result cache so every iteration measures real
	// routing, join, and projection work instead of a cache hit.
	exec.cache = nil
	ctx := context.Background()
	seedTraversalOptionalBenchGraph(b, exec, ctx, 50)

	query := `
		MATCH (e:BenchClass)-[rel:INHERITS]->(target)
		OPTIONAL MATCH (target)<-[:CONTAINS]-(tf:BenchFile)
		RETURN type(rel) AS type,
		       coalesce(target.id, target.uid) AS target_id,
		       target.name AS target_name,
		       tf.relative_path AS target_file`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		if err != nil {
			b.Fatalf("execute failed: %v", err)
		}
		if len(res.Rows) == 0 {
			b.Fatal("expected rows")
		}
	}
}
