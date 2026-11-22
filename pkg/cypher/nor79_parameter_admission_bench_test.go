package cypher

import "testing"

// The same public query reuses compiled syntax while each call supplies a new
// parameter value. Disable result caching so admission is paid on every call.
func BenchmarkNOR79BoundParameterRebinding(b *testing.B) {
	exec, _ := newClauseSemanticsBenchmarkExecutor(b)
	const query = "WITH $p AS v RETURN v"
	params := map[string]interface{}{"p": int64(1)}
	if _, err := exec.Execute(b.Context(), query, params); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var want interface{} = int64(i)
		if i%2 == 1 {
			want = "string"
		}
		params["p"] = want
		result, err := exec.Execute(b.Context(), query, params)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != want {
			b.Fatalf("expected current binding %v, got %v", want, result.Rows)
		}
	}
}
