package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// largeDecisionStatement builds a statement shaped like a large generated
// decision query: nested CALL subqueries whose WHERE clauses and projections
// are long chains of CASE expressions, label tests and list membership, with
// an empty correlated UNWIND in front. Its size is about 40 KB at parts=60.
func largeDecisionStatement(parts int) string {
	var b strings.Builder
	b.WriteString("CALL {\n  UNWIND $ids AS id\n  MATCH (c:Correction {id: id})\n  RETURN collect(c) AS active\n}\n")
	b.WriteString("CALL (active) {\n  UNWIND active AS c\n  WITH c WHERE c.operation = 'different'\n")
	for i := 0; i < parts; i++ {
		fmt.Fprintf(&b, "  OPTIONAL MATCH (m%d:Node {id: c.targets[0]})\n", i)
		fmt.Fprintf(&b, "  WITH c, m%d, CASE WHEN m%d:Identity THEN 'identity' WHEN m%d:Event THEN 'event' ELSE 'other' END AS kind%d,\n", i, i, i, i)
		fmt.Fprintf(&b, "       [x IN coalesce(m%d.keys, []) WHERE x IN $keys AND NOT x IN ['a', 'b'] | toLower(x)] AS keys%d\n", i, i)
		fmt.Fprintf(&b, "  WHERE any(k IN keys%d WHERE k IN c.targets) OR (m%d IS NULL AND c.status IN ['active', 'pending'])\n", i, i)
	}
	b.WriteString("  RETURN collect(c.id) AS remote\n}\n")
	b.WriteString("RETURN active, remote, [m IN $mentions | m.id] AS current\n")
	return b.String()
}

func BenchmarkLargeStatementFixedCost(b *testing.B) {
	query := largeDecisionStatement(60)
	params := map[string]interface{}{"ids": []interface{}{}, "keys": []interface{}{"k"}, "mentions": []interface{}{}}
	ctx := context.Background()
	b.Run("label_rewrite", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := desugarLabelExpressions(query, params, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("bound_parameters", func(b *testing.B) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench"))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := exec.validateBoundParameterExpressions(ctx, query, params); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("execute", func(b *testing.B) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench"))
		if _, err := exec.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := exec.Execute(ctx, query, params); err != nil {
				b.Fatal(err)
			}
		}
	})
}
