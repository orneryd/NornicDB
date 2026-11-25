package cypher

import (
	"context"
	"testing"
)

// BenchmarkBoundSemanticValidation measures repeated admission of one
// parameterized statement, the C61 regression's causal path.
func BenchmarkBoundSemanticValidation(b *testing.B) {
	const query = "UNWIND $ids AS id MATCH (n:Person {id: id}) WITH n, id WHERE n.active = true RETURN n.name AS name, id ORDER BY name LIMIT 20"
	params := map[string]interface{}{"ids": []string{"a", "b", "c"}}
	exec := &StorageExecutor{
		matchSemanticValidationCache: newBoundedCache[string, nativeFunctionInputs](500, boundedCacheStringShardIndex),
		boundSemanticValidationCache: newBoundedCache[string, nativeFunctionInputs](500, boundedCacheStringShardIndex),
	}
	ctx := context.Background()
	if err := exec.validateBoundParameterExpressions(ctx, query, params); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := exec.validateBoundParameterExpressions(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}
