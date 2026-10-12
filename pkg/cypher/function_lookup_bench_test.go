package cypher

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// functionLookupStatements are statements whose function calls the static
// checks look up: plain scalar functions, namespaced ones, many calls in one
// statement and nested calls.
var functionLookupStatements = []struct{ name, query string }{
	{"Scalar", "RETURN toUpper('a') AS a, substring('abc', 1) AS b, coalesce(null, 1) AS c, size([1, 2]) AS d, abs(-1) AS e, round(1.5) AS f, toString(1) AS g, trim(' a ') AS h"},
	{"Namespaced", "RETURN date.truncate('day', date('2020-01-02')) AS a, duration.between(date('2020-01-01'), date('2020-01-02')) AS b, point.distance(point({x: 0, y: 0}), point({x: 3, y: 4})) AS c, datetime.fromepoch(1, 0) AS d"},
	{"ManyCalls", "RETURN toUpper('a') AS a1, toLower('B') AS a2, replace('abc', 'b', 'x') AS a3, left('abc', 1) AS a4, right('abc', 1) AS a5, split('a,b', ',') AS a6, reverse('ab') AS a7, ltrim(' a') AS a8, rtrim('a ') AS a9, size('abc') AS a10, sqrt(4) AS a11, sign(-2) AS a12, ceil(1.2) AS a13, floor(1.8) AS a14, exp(0) AS a15, log(1) AS a16, toInteger('5') AS a17, toFloat('1.5') AS a18, toBoolean('true') AS a19, head([1]) AS a20, last([1]) AS a21, range(1, 3) AS a22, keys({k: 1}) AS a23, coalesce(null, 'x') AS a24"},
	{"Nested", "RETURN toUpper(substring(trim(toString(abs(round(coalesce(null, -1.5))))), 0, 2)) AS v"},
}

// BenchmarkFunctionLookupStaticCheck measures the compile-time check of a
// statement's function calls: finding each call, looking its name up and
// checking its argument count and literal argument types.
func BenchmarkFunctionLookupStaticCheck(b *testing.B) {
	for _, statement := range functionLookupStatements {
		b.Run(statement.name, func(b *testing.B) {
			if err := validateStaticFunctionArguments(statement.query, false); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := validateStaticFunctionArguments(statement.query, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFunctionLookupFirstExecution runs each statement as a query text
// the executor hasn't seen, so its validation and planning aren't cached and
// the function lookups run every time.
func BenchmarkFunctionLookupFirstExecution(b *testing.B) {
	for _, statement := range functionLookupStatements {
		b.Run(statement.name, func(b *testing.B) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"))
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, statement.query+", "+strconv.Itoa(i)+" AS n", nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFunctionCallRows evaluates function calls on every row of a
// repeated statement (validation cached), which exercises the evaluators and
// their argument splitting.
func BenchmarkFunctionCallRows(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "test"))
	ctx := context.Background()
	if _, err := exec.Execute(ctx, "UNWIND range(1, 1000) AS i CREATE (:FnRow {name: 'name ' + toString(i), n: i, d: date('2020-01-01') + duration({days: i % 300})})", nil); err != nil {
		b.Fatal(err)
	}
	for _, workload := range []struct{ name, query string }{
		{"String", "MATCH (r:FnRow) RETURN toUpper(r.name) AS a, substring(r.name, 1, 3) AS b, replace(r.name, 'a', 'b') AS c, split(r.name, ' ') AS d"},
		{"Numeric", "MATCH (r:FnRow) RETURN abs(r.n - 500) AS a, round(r.n / 7.0) AS b, coalesce(r.missing, r.n) AS c, sqrt(r.n) AS d"},
		{"Temporal", "MATCH (r:FnRow) RETURN date.truncate('month', r.d) AS a, duration.between(date('2020-01-01'), r.d) AS b, r.d.year AS c"},
	} {
		b.Run(workload.name, func(b *testing.B) {
			result, err := exec.Execute(ctx, workload.query, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Rows) != 1000 {
				b.Fatalf("got %d rows, want 1000", len(result.Rows))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, workload.query, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
