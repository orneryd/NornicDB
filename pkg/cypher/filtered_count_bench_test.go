package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkPipelineFilteredCount measures MATCH (n:Label) WHERE … RETURN
// count(n) on the fused label-stream count path.
func BenchmarkPipelineFilteredCount(b *testing.B) {
	ctx := context.Background()
	exec, _ := newTestExecutor(b)
	_, err := exec.Execute(ctx, "UNWIND range(1, 2000) AS i CREATE (:BC {id: i, name: 'n' + toString(i % 10), v: i % 7})", nil)
	require.NoError(b, err)
	for _, bc := range []struct {
		name   string
		where  string
		params map[string]interface{}
	}{
		{"literal_eq", "n.v = 3", nil},
		{"in_list", "n.v IN [1, 2, 3]", nil},
		{"in_param", "n.v IN $p", map[string]interface{}{"p": []interface{}{int64(1), int64(2), int64(3)}}},
		{"param_and_prop", "$q = 1 AND n.v < 4", map[string]interface{}{"q": int64(1)}},
		{"starts_with", "n.name STARTS WITH 'n1'", nil},
	} {
		b.Run(bc.name, func(b *testing.B) {
			query := "MATCH (n:BC) WHERE " + bc.where + " RETURN count(n) AS c"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query, bc.params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
