package cypher

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Long-statement benchmarks on the public path: Execute, with the result
// cache off, of the 99 KB application statement the parser benchmarks use
// (antlr/testdata/prediction_large_statement.cypher), on an empty store. The
// time is what the engine spends on the statement itself: reading it, the
// entry rewrites and checks, admission and planning.
//
// BenchmarkLongStatementFirstRun measures a process's first run, so run it
// alone with -benchtime 1x. BenchmarkLongStatementRepeatRun measures every
// later run of the same text.

func longStatement(tb testing.TB) (string, map[string]interface{}) {
	tb.Helper()
	data, err := os.ReadFile("antlr/testdata/prediction_large_statement.cypher")
	if err != nil {
		tb.Fatal(err)
	}
	query := string(data)
	params := map[string]interface{}{}
	for _, match := range regexp.MustCompile(`\$([A-Za-z_]\w*)`).FindAllStringSubmatch(query, -1) {
		name := match[1]
		switch {
		case name == "limit" || name == "offset" || name == "per_side":
			params[name] = int64(10)
		case strings.HasSuffix(name, "_id") || name == "query" || name == "separator":
			params[name] = "x"
		default:
			params[name] = []interface{}{}
		}
	}
	return query, params
}

func newLongStatementExecutor(tb testing.TB) *StorageExecutor {
	tb.Helper()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(tb), "bench"))
	executor.cache = nil
	return executor
}

func BenchmarkLongStatementFirstRun(b *testing.B) {
	query, params := longStatement(b)
	executor := newLongStatementExecutor(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executor.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLongStatementRepeatRun(b *testing.B) {
	query, params := longStatement(b)
	executor := newLongStatementExecutor(b)
	ctx := context.Background()
	if _, err := executor.Execute(ctx, query, params); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executor.Execute(ctx, query, params); err != nil {
			b.Fatal(err)
		}
	}
}
