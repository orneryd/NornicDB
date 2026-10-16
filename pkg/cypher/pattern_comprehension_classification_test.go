package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nestedListClassificationQuery(depth int, atom string) string {
	return "RETURN " + strings.Repeat("[(", depth) + atom + strings.Repeat(")]", depth) + " AS value"
}

func TestPatternComprehensionNestedListControls(t *testing.T) {
	for _, depth := range []int{1, 10, 100, 1000} {
		query := nestedListClassificationQuery(depth, "'|'")
		require.False(t, singleNodePatternComprehension(query))
		require.False(t, expressionReadsGraphText(query))
		if depth <= 10 {
			exec, ctx := newUnitExecutor(t)
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			value := result.Rows[0][0]
			for i := 0; i < depth; i++ {
				list, ok := value.([]interface{})
				require.True(t, ok)
				require.Len(t, list, 1)
				value = list[0]
			}
			require.Equal(t, "|", value)
		}
	}
}

func TestPatternComprehensionClassifierLexicalBoundaries(t *testing.T) {
	for _, tc := range []struct {
		query              string
		pattern, singleton bool
	}{
		{"RETURN [1, 2]", false, false},
		{"RETURN ['|', 'WHERE']", false, false},
		{"RETURN [/* | WHERE */ 1]", false, false},
		{"RETURN [// | WHERE\n 1]", false, false},
		{"WITH [1] AS values RETURN values[(1)]", false, false},
		{"RETURN [(n) | n]", true, true},
		{"RETURN [/* gap */ (n) /* gap */ WHERE true | n]", true, true},
		{"RETURN [(n) WHERE true]", true, true},
		{"RETURN [(a)-->(b) | b]", true, false},
		{"RETURN [(a)<--(b) | b]", true, false},
		{"RETURN [(a)-[:R]->(b) | b]", true, false},
		{"RETURN [(a)<-[r]-(b) | b]", true, false},
		{"RETURN [(n", false, false},
		{"RETURN [)] |", false, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			pattern, singleton := classifyPatternComprehensions(tc.query)
			require.Equal(t, tc.pattern, pattern)
			require.Equal(t, tc.singleton, singleton)
		})
	}
}

// BenchmarkPatternComprehensionGuard measures the syntax guard alone on valid
// ordinary lists, including quoted pipes which require real lexical scanning.
func BenchmarkPatternComprehensionGuard(b *testing.B) {
	for _, depth := range []int{100, 1000} {
		b.Run(fmt.Sprintf("depth%d/realPipe", depth), func(b *testing.B) {
			query := "WITH " + strings.TrimPrefix(nestedListClassificationQuery(depth, "'|'"), "RETURN ") + " RETURN value, [(n)-->(m) | m] AS paths"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if singleNodePatternComprehension(query) {
					b.Fatal("valid relationship comprehension classified as a singleton")
				}
			}
		})
		for _, atom := range []string{"1", "'|'"} {
			b.Run(fmt.Sprintf("depth%d/atom%s", depth, atom), func(b *testing.B) {
				query := nestedListClassificationQuery(depth, atom)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if singleNodePatternComprehension(query) {
						b.Fatal("ordinary nested list classified as a pattern comprehension")
					}
				}
			})
		}
	}
}
