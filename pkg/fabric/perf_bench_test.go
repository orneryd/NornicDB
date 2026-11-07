package fabric

import (
	"fmt"
	"testing"
)

func BenchmarkInCompositeScope(b *testing.B) {
	catalog := NewCatalog()
	for i := 0; i < 100; i++ {
		catalog.Register(fmt.Sprintf("translations.%d", i), &LocationLocal{DBName: "shard"})
	}
	planner := NewFabricPlanner(catalog, readDeclaredEffects())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = planner.inCompositeScope("translations")
	}
}
