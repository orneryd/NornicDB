package fabric

import (
	"fmt"
	"testing"
)

func BenchmarkInferReturnColumnsFromQuery(b *testing.B) {
	query := `CALL {
  USE translations.tr
  MATCH (t:Translation)
  RETURN t.id AS id, t.textKey AS textKey, t.textKey128 AS textKey128, t.lang AS lang
}
RETURN id, textKey, textKey128, lang
ORDER BY textKey128
LIMIT 25`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = inferReturnColumnsFromQuery(query)
	}
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
