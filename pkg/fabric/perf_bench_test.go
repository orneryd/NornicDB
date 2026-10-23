package fabric

import (
	"fmt"
	"testing"
)

func BenchmarkQueryIsWrite(b *testing.B) {
	readQ := "MATCH (n:Translation) WHERE n.textKey128 = $id RETURN n.textKey AS textKey, n.textKey128 AS textKey128 LIMIT 25"
	writeQ := "MATCH (n:Translation) WHERE n.textKey128 = $id SET n.flag = true RETURN n.textKey AS textKey"

	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = queryIsWrite(readQ)
		}
	})
	b.Run("write", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = queryIsWrite(writeQ)
		}
	})
}

func BenchmarkRewriteLeadingWithImports(b *testing.B) {
	query := "WITH textKey128 MATCH (tt:TranslationText) WHERE tt.textKey128 = textKey128 RETURN collect(tt) AS texts"
	imports := []string{"textKey128"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = rewriteLeadingWithImports(query, imports)
	}
}

func BenchmarkInCompositeScope(b *testing.B) {
	catalog := NewCatalog()
	for i := 0; i < 100; i++ {
		catalog.Register(fmt.Sprintf("translations.%d", i), &LocationLocal{DBName: "shard"})
	}
	planner := NewFabricPlanner(catalog)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = planner.inCompositeScope("translations")
	}
}
