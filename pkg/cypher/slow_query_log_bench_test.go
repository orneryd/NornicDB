package cypher

import (
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// slowQueryLargeStatementPath is a ~99 KB generated statement: nested CALL
// subqueries, list comprehensions, reduce() and many string literals, the
// size real applications build from query templates.
const slowQueryLargeStatementPath = "testdata/slow_query_large_statement.cypher"

func loadSlowQueryLargeStatement(tb testing.TB) string {
	tb.Helper()
	text, err := os.ReadFile(slowQueryLargeStatementPath)
	if err != nil {
		tb.Fatalf("read %s: %v", slowQueryLargeStatementPath, err)
	}
	return string(text)
}

// BenchmarkSlowQueryLogEmission measures one slow-query log record: the
// redaction, the shape hash and the record itself, for a small statement and
// for the large one.
func BenchmarkSlowQueryLogEmission(b *testing.B) {
	cases := []struct {
		name  string
		query string
	}{
		{"small", `MATCH (n:Person {name: 'alice'}) WHERE n.age > 30 RETURN n.name AS who LIMIT 10`},
		{"large", loadSlowQueryLargeStatement(b)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			exec := NewStorageExecutor(newTestMemoryEngine(b))
			exec.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
			exec.SetSlowQueryThreshold(time.Nanosecond)
			b.SetBytes(int64(len(tc.query)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec.emitSlowQueryLog(b.Context(), tc.query, nil, time.Second)
			}
		})
	}
}
