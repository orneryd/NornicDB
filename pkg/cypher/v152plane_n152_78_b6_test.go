package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type nor357RequestKey struct{}

// Cancel inside one expression, rather than between rows or during parsing.
// The callee intentionally ignores cancellation; the evaluator must stop
// before a fourth item and preserve the original statement's context.
func TestNOR357SingleExpressionPreservesRequestAndStopsAtCancellation(t *testing.T) {
	expression := "reduce(s = 0, x IN $items | s + PROBE(x))"
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for i, shape := range []struct {
				name, query string
				want        [][]interface{}
				calls       int
			}{
				{"return", "RETURN EXPR AS v", [][]interface{}{{int64(36)}}, 8},
				{"with", "WITH EXPR AS v RETURN v", [][]interface{}{{int64(36)}}, 8},
				{"case", "WITH 1 AS seed RETURN CASE WHEN seed = 1 THEN EXPR ELSE 0 END AS v", [][]interface{}{{int64(36)}}, 8},
				{"where", "WITH 1 AS seed WHERE EXPR > 0 RETURN seed AS v", [][]interface{}{{int64(1)}}, 8},
				{"aggregate_return", "UNWIND [1] AS seed RETURN sum(EXPR) AS v", [][]interface{}{{int64(36)}}, 8},
				{"aggregate_with", "UNWIND [1] AS seed WITH sum(EXPR) AS v RETURN v", [][]interface{}{{int64(36)}}, 8},
				{"order_by", "UNWIND [1,2] AS seed RETURN seed AS v ORDER BY EXPR", [][]interface{}{{int64(1)}, {int64(2)}}, 16},
			} {
				t.Run(shape.name, func(t *testing.T) {
					name := fmt.Sprintf("zz.nor357_%s_%d", parser, i)
					query := strings.ReplaceAll(shape.query, "EXPR", strings.ReplaceAll(expression, "PROBE", name))
					marker := &struct{ name string }{shape.name}
					var seen []context.Context
					var cancelOnThird context.CancelFunc
					withPluginFunctions(t, map[string]interface{}{name: func(ctx context.Context, x int64) int64 {
						seen = append(seen, ctx)
						if cancelOnThird != nil && len(seen) == 3 {
							cancelOnThird()
						}
						return x
					}})
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor357"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
					params := map[string]interface{}{"items": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8)}}
					request := context.WithValue(t.Context(), nor357RequestKey{}, marker)
					verifyContext := func() {
						for _, ctx := range seen {
							require.Same(t, marker, ctx.Value(nor357RequestKey{}), "the expression callback must inherit the request")
						}
					}
					result, err := exec.Execute(request, query, params)
					require.NoError(t, err)
					require.Equal(t, shape.want, result.Rows)
					require.Len(t, seen, shape.calls)
					verifyContext()

					seen = nil
					canceled, cancel := context.WithCancel(request)
					defer cancel()
					cancelOnThird = cancel
					result, err = exec.Execute(canceled, query, params)
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, result)
					require.Len(t, seen, 3, "one expression must stop before evaluating a fourth item")
					verifyContext()

					cancelOnThird = nil
					seen = nil
					result, err = exec.Execute(request, query, params)
					require.NoError(t, err)
					require.Equal(t, shape.want, result.Rows)
					require.Len(t, seen, shape.calls, "the canceled run must not poison a healthy retry")
					verifyContext()
				})
			}
		})
	}
}
