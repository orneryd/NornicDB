package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Public query workloads use in-memory fixtures and disable query-result caching.
// Warm runs reuse statement text; cold runs vary a constant-true predicate, so
// text-keyed caches miss while parser process state remains warm. Cold predicates
// and per-iteration result assertions are included in both revision measurements.
// Node lookup caches may be warm in either mode. Setup is outside the timer.

const (
	// callBindingBaselineRows is the fixture size for the single-size workloads.
	callBindingBaselineRows = 128

	// callBindingNamespace isolates the benchmark graph inside the in-memory
	// engine, matching the package's existing benchmark fixture pattern.
	callBindingNamespace = "call_binding_bench"
)

var (
	// callBindingCorrelatedRowCounts are the outer row counts CALL binding qualifies
	// for the correlated CALL shape.
	callBindingCorrelatedRowCounts = []int{1, 128, 4096}

	// callBindingConditionalArmCounts are the literal CASE arm counts CALL binding
	// qualifies.
	callBindingConditionalArmCounts = []int{2, 16, 64}

	// callBindingProjectionItemCounts are the projection widths CALL binding
	// qualifies.
	callBindingProjectionItemCounts = []int{128, 512}

	// callBindingBenchSequence makes every cold statement text unique within the
	// process, so text-keyed statement caches cannot serve an earlier variant.
	callBindingBenchSequence atomic.Uint64
)

// callBindingBenchFixture is one pure-memory graph plus the executor the
// workloads are measured through. Node i carries id=i and value=i+1, so each
// generated workload has one exact expected result that the assertions pin.
type callBindingBenchFixture struct {
	exec *StorageExecutor
	ctx  context.Context
	rows int
}

// newCallBindingBenchFixture builds an ephemeral namespaced in-memory :Bench
// graph, fixes the parser to nornic for the benchmark's lifetime and returns a
// cache-disabled public executor. Storage is owned and closed by
// newTestMemoryEngine's b.Cleanup.
func newCallBindingBenchFixture(tb testing.TB, rows int) *callBindingBenchFixture {
	tb.Helper()
	if rows < 1 {
		tb.Fatalf("call-binding fixture rows: got %d, want at least 1", rows)
	}
	callBindingFixParser(tb)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(tb), callBindingNamespace)
	ctx := tb.Context()
	for i := 0; i < rows; i++ {
		id := int64(i)
		if _, err := store.CreateNode(ctx, &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("call-binding-%d", i)),
			Labels: []string{"Bench"},
			Properties: map[string]any{
				"id":    id,
				"value": id + 1,
			},
		}); err != nil {
			tb.Fatalf("create call-binding fixture node %d: %v", i, err)
		}
	}
	// QueryCachePolicy{MaxEntries: 0} is the existing supported construction
	// option that disables the database-scoped query result cache. A repeated
	// statement therefore still parses, admits and evaluates on every call.
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	return &callBindingBenchFixture{exec: exec, ctx: ctx, rows: rows}
}

// callBindingFixParser fixes the nornic parser for this benchmark and restores
// the caller's previous selection through tb.Cleanup.
func callBindingFixParser(tb testing.TB) {
	tb.Helper()
	previous := config.GetParserType()
	config.SetParserType(config.ParserTypeNornic)
	tb.Cleanup(func() { config.SetParserType(previous) })
	if got := config.GetParserType(); got != config.ParserTypeNornic {
		tb.Fatalf("call-binding fixture parser: got %q, want %q", got, config.ParserTypeNornic)
	}
}

// callBindingDistinctPredicate returns the cold variant's constant-true
// predicate. It changes the statement text without changing the rows; the warm
// form contributes nothing.
func callBindingDistinctPredicate(distinct bool) string {
	if !distinct {
		return ""
	}
	variant := callBindingBenchSequence.Add(1)
	return fmt.Sprintf("WHERE %d = %d ", variant, variant)
}

// callBindingOrdinaryReadQuery is a plain labeled read of the whole fixture.
func callBindingOrdinaryReadQuery(distinct bool) string {
	return "MATCH (n:Bench) " + callBindingDistinctPredicate(distinct) +
		"RETURN n.id AS id, n.value AS value"
}

// callBindingCorrelatedCallQuery correlates one CALL per outer row and returns a
// fresh alias that does not exist in the outer scope.
func callBindingCorrelatedCallQuery(distinct bool) string {
	return "MATCH (n:Bench) " + callBindingDistinctPredicate(distinct) +
		"CALL (n) { RETURN n.value + 1 AS freshValue } " +
		"RETURN n.id AS id, freshValue"
}

// callBindingRetainedScopedImportQuery reads the scoped import n after the CALL
// body's own WITH horizon; the body's WITH declares only its local.
func callBindingRetainedScopedImportQuery(distinct bool) string {
	return "MATCH (n:Bench) " + callBindingDistinctPredicate(distinct) +
		"CALL (n) { WITH 2 AS local RETURN n.value * local AS carried } " +
		"RETURN n.id AS id, carried"
}

// callBindingScalarCountQuery is a scalar aggregate with explicit output
// columns: count(n) and sum(n.value), no count(*) and no COLLECT(*).
func callBindingScalarCountQuery(distinct bool) string {
	return "MATCH (n:Bench) " + callBindingDistinctPredicate(distinct) +
		"RETURN count(n) AS total, sum(COUNT { UNWIND range(1,n.value) AS item RETURN item }) AS total_value"
}

// callBindingConditionalLiteralQuery is a simple top-level CASE over literal
// arms. The fixture node selected by the inline id has value == arms, so the
// last arm matches and the expected value is arms*10.
func callBindingConditionalLiteralQuery(arms int, distinct bool) string {
	var query strings.Builder
	fmt.Fprintf(&query, "MATCH (n:Bench {id: %d}) ", arms-1)
	query.WriteString(callBindingDistinctPredicate(distinct))
	query.WriteString("RETURN CASE n.value")
	for arm := 1; arm <= arms; arm++ {
		fmt.Fprintf(&query, " WHEN %d THEN %d", arm, arm*10)
	}
	query.WriteString(" ELSE 0 END AS arm_value")
	return query.String()
}

// callBindingProjectionQuery projects `items` columns from the fixture node with
// id 0 (value 1). The first column is named "next" and the remaining columns
// use distinct next_<i> aliases; no ORDER BY follows them. Item i yields
// n.value + i, so the expected value of column i is i+1.
func callBindingProjectionQuery(items int, distinct bool) string {
	var query strings.Builder
	query.WriteString("MATCH (n:Bench {id: 0}) ")
	query.WriteString(callBindingDistinctPredicate(distinct))
	query.WriteString("RETURN ")
	for item := 1; item <= items; item++ {
		if item > 1 {
			query.WriteString(", ")
		}
		if item == 1 {
			query.WriteString("n.value + 1 AS next")
			continue
		}
		fmt.Fprintf(&query, "n.value + %d AS next_%d", item, item)
	}
	return query.String()
}

// callBindingVariantName names the warm and cold text forms.
func callBindingVariantName(distinct bool) string {
	if distinct {
		return "cold"
	}
	return "warm"
}

// callBindingQueryLabel bounds failure output for the wide generated queries.
func callBindingQueryLabel(query string) string {
	const limit = 160
	if len(query) <= limit {
		return fmt.Sprintf("%q", query)
	}
	return fmt.Sprintf("%q...(%d bytes)", query[:limit], len(query))
}

// callBindingExecuteAndVerify runs one statement through the public Execute path
// and checks the exact expected result. It runs inside the timed loop, so it
// does not call tb.Helper(); failure lines point at this helper and the message
// names the workload.
func callBindingExecuteAndVerify(tb testing.TB, fixture *callBindingBenchFixture, query string, verify func(testing.TB, *ExecuteResult)) {
	result, err := fixture.exec.Execute(fixture.ctx, query, nil)
	if err != nil {
		tb.Fatalf("execute %s: %v", callBindingQueryLabel(query), err)
	}
	verify(tb, result)
}

// callBindingInt64 reads an integer result value, failing with the supplied
// description when the engine returns another type. It runs per row, so it does
// not call tb.Helper().
func callBindingInt64(tb testing.TB, value interface{}, what string) int64 {
	got, ok := value.(int64)
	if !ok {
		tb.Fatalf("%s: got %T (%v), want int64", what, value, value)
	}
	return got
}

// The assertion helpers below run inside the timed benchmark loop and therefore
// avoid tb.Helper(), which walks the stack and takes a lock on every call.

// callBindingAssertOrdinaryRead checks the plain labeled read: every fixture id
// appears once with value == id+1, and the values sum to rows*(rows+1)/2. seen
// is caller-owned scratch that the assertion clears, so repeated iterations do
// not allocate for coverage tracking.
func callBindingAssertOrdinaryRead(tb testing.TB, result *ExecuteResult, rows int, seen []bool) {
	if len(result.Columns) != 2 || result.Columns[0] != "id" || result.Columns[1] != "value" {
		tb.Fatalf("ordinary labeled read columns: got %v, want [id value]", result.Columns)
	}
	if len(result.Rows) != rows {
		tb.Fatalf("ordinary labeled read rows: got %d, want %d", len(result.Rows), rows)
	}
	if len(seen) != rows {
		tb.Fatalf("ordinary labeled read coverage scratch: got %d, want %d", len(seen), rows)
	}
	clear(seen)
	var sum int64
	for _, row := range result.Rows {
		if len(row) != 2 {
			tb.Fatalf("ordinary labeled read row width: got %d, want 2 (%v)", len(row), row)
		}
		id := callBindingInt64(tb, row[0], "ordinary labeled read id")
		value := callBindingInt64(tb, row[1], "ordinary labeled read value")
		if id < 0 || id >= int64(rows) {
			tb.Fatalf("ordinary labeled read id out of range: %d", id)
		}
		if seen[id] {
			tb.Fatalf("ordinary labeled read duplicate id: %d", id)
		}
		seen[id] = true
		if value != id+1 {
			tb.Fatalf("ordinary labeled read value: id %d gave %d, want %d", id, value, id+1)
		}
		sum += value
	}
	if want := int64(rows) * int64(rows+1) / 2; sum != want {
		tb.Fatalf("ordinary labeled read value sum: got %d, want %d", sum, want)
	}
}

// callBindingAssertCorrelatedCall checks the correlated CALL result: each outer
// row's id appears once with freshValue == id+2, and the fresh values sum to
// rows*(rows+3)/2.
func callBindingAssertCorrelatedCall(tb testing.TB, result *ExecuteResult, rows int, seen []bool) {
	if len(result.Columns) != 2 || result.Columns[0] != "id" || result.Columns[1] != "freshValue" {
		tb.Fatalf("correlated CALL columns: got %v, want [id freshValue]", result.Columns)
	}
	if len(result.Rows) != rows {
		tb.Fatalf("correlated CALL rows: got %d, want %d", len(result.Rows), rows)
	}
	if len(seen) != rows {
		tb.Fatalf("correlated CALL coverage scratch: got %d, want %d", len(seen), rows)
	}
	clear(seen)
	var sum int64
	for _, row := range result.Rows {
		if len(row) != 2 {
			tb.Fatalf("correlated CALL row width: got %d, want 2 (%v)", len(row), row)
		}
		id := callBindingInt64(tb, row[0], "correlated CALL id")
		fresh := callBindingInt64(tb, row[1], "correlated CALL fresh value")
		if id < 0 || id >= int64(rows) {
			tb.Fatalf("correlated CALL id out of range: %d", id)
		}
		if seen[id] {
			tb.Fatalf("correlated CALL duplicate id: %d", id)
		}
		seen[id] = true
		if fresh != id+2 {
			tb.Fatalf("correlated CALL fresh value: id %d gave %d, want %d", id, fresh, id+2)
		}
		sum += fresh
	}
	if want := int64(rows) * int64(rows+3) / 2; sum != want {
		tb.Fatalf("correlated CALL fresh value sum: got %d, want %d", sum, want)
	}
}

// callBindingAssertRetainedScopedImport checks that the import stayed readable
// after the body's WITH: carried == 2*(id+1), summing to rows*(rows+1).
func callBindingAssertRetainedScopedImport(tb testing.TB, result *ExecuteResult, rows int, seen []bool) {
	if len(result.Columns) != 2 || result.Columns[0] != "id" || result.Columns[1] != "carried" {
		tb.Fatalf("retained scoped import columns: got %v, want [id carried]", result.Columns)
	}
	if len(result.Rows) != rows {
		tb.Fatalf("retained scoped import rows: got %d, want %d", len(result.Rows), rows)
	}
	if len(seen) != rows {
		tb.Fatalf("retained scoped import coverage scratch: got %d, want %d", len(seen), rows)
	}
	clear(seen)
	var sum int64
	for _, row := range result.Rows {
		if len(row) != 2 {
			tb.Fatalf("retained scoped import row width: got %d, want 2 (%v)", len(row), row)
		}
		id := callBindingInt64(tb, row[0], "retained scoped import id")
		carried := callBindingInt64(tb, row[1], "retained scoped import carried")
		if id < 0 || id >= int64(rows) {
			tb.Fatalf("retained scoped import id out of range: %d", id)
		}
		if seen[id] {
			tb.Fatalf("retained scoped import duplicate id: %d", id)
		}
		seen[id] = true
		if want := 2*id + 2; carried != want {
			tb.Fatalf("retained scoped import carried: id %d gave %d, want %d", id, carried, want)
		}
		sum += carried
	}
	if want := int64(rows) * int64(rows+1); sum != want {
		tb.Fatalf("retained scoped import carried sum: got %d, want %d", sum, want)
	}
}

// callBindingAssertScalarCount checks the scalar aggregate: one row with the
// explicit total and total_value columns for the fixture population.
func callBindingAssertScalarCount(tb testing.TB, result *ExecuteResult, rows int) {
	if len(result.Columns) != 2 || result.Columns[0] != "total" || result.Columns[1] != "total_value" {
		tb.Fatalf("scalar count columns: got %v, want [total total_value]", result.Columns)
	}
	if len(result.Rows) != 1 {
		tb.Fatalf("scalar count rows: got %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if len(row) != 2 {
		tb.Fatalf("scalar count row width: got %d, want 2 (%v)", len(row), row)
	}
	total := callBindingInt64(tb, row[0], "scalar count total")
	totalValue := callBindingInt64(tb, row[1], "scalar count total value")
	if total != int64(rows) {
		tb.Fatalf("scalar count total: got %d, want %d", total, rows)
	}
	if want := int64(rows) * int64(rows+1) / 2; totalValue != want {
		tb.Fatalf("scalar count total value: got %d, want %d", totalValue, want)
	}
}

// callBindingAssertConditionalLiteral checks the top-level CASE: one arm_value
// column holding the matching last arm's literal, arms*10.
func callBindingAssertConditionalLiteral(tb testing.TB, result *ExecuteResult, arms int) {
	if len(result.Columns) != 1 || result.Columns[0] != "arm_value" {
		tb.Fatalf("conditional literal columns: got %v, want [arm_value]", result.Columns)
	}
	if len(result.Rows) != 1 {
		tb.Fatalf("conditional literal rows: got %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if len(row) != 1 {
		tb.Fatalf("conditional literal row width: got %d, want 1 (%v)", len(row), row)
	}
	got := callBindingInt64(tb, row[0], "conditional literal arm value")
	if want := int64(arms * 10); got != want {
		tb.Fatalf("conditional literal arm value: got %d, want %d", got, want)
	}
}

// callBindingProjectionExpectation precomputes the wide projection's column
// names, values and sum once per benchmark so per-iteration assertions do not
// build formatted strings.
type callBindingProjectionExpectation struct {
	columns []string
	values  []int64
	sum     int64
}

func newCallBindingProjectionExpectation(items int) *callBindingProjectionExpectation {
	expectation := &callBindingProjectionExpectation{
		columns: make([]string, items),
		values:  make([]int64, items),
	}
	for item := 1; item <= items; item++ {
		if item == 1 {
			expectation.columns[0] = "next"
		} else {
			expectation.columns[item-1] = fmt.Sprintf("next_%d", item)
		}
		expectation.values[item-1] = int64(item + 1)
		expectation.sum += expectation.values[item-1]
	}
	return expectation
}

// callBindingAssertProjection checks every projected column name and value,
// including the "next" column and the distinct next_<i> aliases, and their sum.
func callBindingAssertProjection(tb testing.TB, result *ExecuteResult, expectation *callBindingProjectionExpectation) {
	if len(result.Columns) != len(expectation.columns) {
		tb.Fatalf("wide projection columns: got %d, want %d (%v)", len(result.Columns), len(expectation.columns), result.Columns)
	}
	for i, want := range expectation.columns {
		if result.Columns[i] != want {
			tb.Fatalf("wide projection column %d: got %q, want %q", i+1, result.Columns[i], want)
		}
	}
	if len(result.Rows) != 1 {
		tb.Fatalf("wide projection rows: got %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if len(row) != len(expectation.values) {
		tb.Fatalf("wide projection row width: got %d, want %d", len(row), len(expectation.values))
	}
	var sum int64
	for i, want := range expectation.values {
		got, ok := row[i].(int64)
		if !ok {
			tb.Fatalf("wide projection item %d: got %T (%v), want int64", i+1, row[i], row[i])
		}
		if got != want {
			tb.Fatalf("wide projection item %d: got %d, want %d", i+1, got, want)
		}
		sum += got
	}
	if sum != expectation.sum {
		tb.Fatalf("wide projection value sum: got %d, want %d", sum, expectation.sum)
	}
}

// benchmarkCallBindingWarm runs the warm form: the exact statement text is
// executed once as setup to populate this executor's compile caches, then the
// same text is repeated inside the timed loop. Result caching is disabled, so
// every timed call still executes the query.
func benchmarkCallBindingWarm(b *testing.B, fixture *callBindingBenchFixture, query string, verify func(testing.TB, *ExecuteResult)) {
	b.Helper()
	callBindingExecuteAndVerify(b, fixture, query, verify)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		callBindingExecuteAndVerify(b, fixture, query, verify)
	}
}

// benchmarkCallBindingCold runs the cold form: every iteration builds a
// statement with a fresh constant-true predicate before executing it. There is
// no warm-up because each text is meant to miss text-keyed compile caches.
func benchmarkCallBindingCold(b *testing.B, fixture *callBindingBenchFixture, build func() string, verify func(testing.TB, *ExecuteResult)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		callBindingExecuteAndVerify(b, fixture, build(), verify)
	}
}

// TestCallBindingWorkloadResults executes every workload once in its warm and
// cold text form and checks the exact generated result. It guards the query
// builders, fixture and assertions the benchmarks share, and gives the parent a
// cheap correctness check on both the baseline and the current head without
// running a benchmark.
func TestCallBindingWorkloadResults(t *testing.T) {
	t.Run("distinct_literal_text", func(t *testing.T) {
		warm := callBindingScalarCountQuery(false)
		if warm != callBindingScalarCountQuery(false) {
			t.Fatal("warm statement text must be stable")
		}
		cold := callBindingScalarCountQuery(true)
		if cold == warm {
			t.Fatal("cold statement text must differ from the warm text")
		}
		if next := callBindingScalarCountQuery(true); next == cold {
			t.Fatal("cold statement text must differ between iterations")
		}
	})

	t.Run("ordinary_labeled_read", func(t *testing.T) {
		fixture := newCallBindingBenchFixture(t, callBindingBaselineRows)
		seen := make([]bool, fixture.rows)
		for _, distinct := range []bool{false, true} {
			t.Run(callBindingVariantName(distinct), func(t *testing.T) {
				callBindingExecuteAndVerify(t, fixture, callBindingOrdinaryReadQuery(distinct), func(tb testing.TB, result *ExecuteResult) {
					callBindingAssertOrdinaryRead(tb, result, fixture.rows, seen)
				})
			})
		}
	})

	t.Run("correlated_call", func(t *testing.T) {
		for _, rows := range callBindingCorrelatedRowCounts {
			t.Run(fmt.Sprintf("rows%d", rows), func(t *testing.T) {
				fixture := newCallBindingBenchFixture(t, rows)
				seen := make([]bool, fixture.rows)
				for _, distinct := range []bool{false, true} {
					t.Run(callBindingVariantName(distinct), func(t *testing.T) {
						callBindingExecuteAndVerify(t, fixture, callBindingCorrelatedCallQuery(distinct), func(tb testing.TB, result *ExecuteResult) {
							callBindingAssertCorrelatedCall(tb, result, fixture.rows, seen)
						})
					})
				}
			})
		}
	})

	t.Run("retained_scoped_import", func(t *testing.T) {
		fixture := newCallBindingBenchFixture(t, callBindingBaselineRows)
		seen := make([]bool, fixture.rows)
		for _, distinct := range []bool{false, true} {
			t.Run(callBindingVariantName(distinct), func(t *testing.T) {
				callBindingExecuteAndVerify(t, fixture, callBindingRetainedScopedImportQuery(distinct), func(tb testing.TB, result *ExecuteResult) {
					callBindingAssertRetainedScopedImport(tb, result, fixture.rows, seen)
				})
			})
		}
	})

	t.Run("scalar_count", func(t *testing.T) {
		fixture := newCallBindingBenchFixture(t, callBindingBaselineRows)
		for _, distinct := range []bool{false, true} {
			t.Run(callBindingVariantName(distinct), func(t *testing.T) {
				callBindingExecuteAndVerify(t, fixture, callBindingScalarCountQuery(distinct), func(tb testing.TB, result *ExecuteResult) {
					callBindingAssertScalarCount(tb, result, fixture.rows)
				})
			})
		}
	})

	t.Run("conditional_literal", func(t *testing.T) {
		for _, arms := range callBindingConditionalArmCounts {
			t.Run(fmt.Sprintf("arms%d", arms), func(t *testing.T) {
				fixture := newCallBindingBenchFixture(t, callBindingBaselineRows)
				for _, distinct := range []bool{false, true} {
					t.Run(callBindingVariantName(distinct), func(t *testing.T) {
						callBindingExecuteAndVerify(t, fixture, callBindingConditionalLiteralQuery(arms, distinct), func(tb testing.TB, result *ExecuteResult) {
							callBindingAssertConditionalLiteral(tb, result, arms)
						})
					})
				}
			})
		}
	})

	t.Run("wide_projection", func(t *testing.T) {
		for _, items := range callBindingProjectionItemCounts {
			t.Run(fmt.Sprintf("items%d", items), func(t *testing.T) {
				fixture := newCallBindingBenchFixture(t, callBindingBaselineRows)
				expectation := newCallBindingProjectionExpectation(items)
				for _, distinct := range []bool{false, true} {
					t.Run(callBindingVariantName(distinct), func(t *testing.T) {
						callBindingExecuteAndVerify(t, fixture, callBindingProjectionQuery(items, distinct), func(tb testing.TB, result *ExecuteResult) {
							callBindingAssertProjection(tb, result, expectation)
						})
					})
				}
			})
		}
	})
}

// callBindingWorkload parameterizes one measured CALL-binding workload. A single
// runner then owns fixture construction, width enumeration and warm/cold
// dispatch, while every benchmark entry keeps the recorded name of its shape.
type callBindingWorkload struct {
	// sizes are the measured widths. nil marks a single-size shape, measured
	// once at callBindingBaselineRows without a sub-benchmark.
	sizes []int
	// name builds the sub-benchmark name for a width, e.g. "rows128".
	name func(width int) string
	// rows maps a width to the fixture size that width needs.
	rows func(width int) int
	// query builds the statement text; distinct selects the cold text form.
	query func(width int, distinct bool) string
	// verify returns the per-iteration result assertion for one fixture.
	verify func(fixture *callBindingBenchFixture, width int) func(testing.TB, *ExecuteResult)
}

// callBindingBaselineFixtureRows is the fixture size of the single-size shapes
// and of the shapes whose width varies something other than the graph size.
func callBindingBaselineFixtureRows(int) int { return callBindingBaselineRows }

// benchmarkCallBindingWorkload is the single parameterized entry every CALL
// binding benchmark funnels through: it runs one workload warm (one statement
// text, repeated) or cold (fresh text on every iteration) at each configured
// width. A shape without sizes is measured at the top level, so the single-size
// benchmarks keep the top-level name they record today.
func benchmarkCallBindingWorkload(b *testing.B, workload callBindingWorkload, distinct bool) {
	b.Helper()
	if len(workload.sizes) == 0 {
		benchmarkCallBindingSize(b, workload, callBindingBaselineRows, distinct)
		return
	}
	for _, width := range workload.sizes {
		b.Run(workload.name(width), func(b *testing.B) {
			benchmarkCallBindingSize(b, workload, width, distinct)
		})
	}
}

// benchmarkCallBindingSize builds one fixture plus its assertion for a workload
// width and hands them to the warm or the cold runner. Setup stays outside the
// timer in both runners.
func benchmarkCallBindingSize(b *testing.B, workload callBindingWorkload, width int, distinct bool) {
	b.Helper()
	fixture := newCallBindingBenchFixture(b, workload.rows(width))
	verify := workload.verify(fixture, width)
	if distinct {
		benchmarkCallBindingCold(b, fixture, func() string { return workload.query(width, true) }, verify)
		return
	}
	benchmarkCallBindingWarm(b, fixture, workload.query(width, false), verify)
}

// callBindingOrdinaryReadWorkload reads every fixture node's id and value.
var callBindingOrdinaryReadWorkload = callBindingWorkload{
	rows:  callBindingBaselineFixtureRows,
	query: func(_ int, distinct bool) string { return callBindingOrdinaryReadQuery(distinct) },
	verify: func(fixture *callBindingBenchFixture, _ int) func(testing.TB, *ExecuteResult) {
		seen := make([]bool, fixture.rows)
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertOrdinaryRead(tb, result, fixture.rows, seen)
		}
	},
}

// callBindingCorrelatedCallWorkload correlates one CALL per outer row, so the
// measured width is both the outer row count and the fixture size.
var callBindingCorrelatedCallWorkload = callBindingWorkload{
	sizes: callBindingCorrelatedRowCounts,
	name:  func(rows int) string { return fmt.Sprintf("rows%d", rows) },
	rows:  func(rows int) int { return rows },
	query: func(_ int, distinct bool) string { return callBindingCorrelatedCallQuery(distinct) },
	verify: func(fixture *callBindingBenchFixture, _ int) func(testing.TB, *ExecuteResult) {
		seen := make([]bool, fixture.rows)
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertCorrelatedCall(tb, result, fixture.rows, seen)
		}
	},
}

// callBindingRetainedScopedImportWorkload reads the scoped import after the
// CALL body's own WITH horizon.
var callBindingRetainedScopedImportWorkload = callBindingWorkload{
	rows:  callBindingBaselineFixtureRows,
	query: func(_ int, distinct bool) string { return callBindingRetainedScopedImportQuery(distinct) },
	verify: func(fixture *callBindingBenchFixture, _ int) func(testing.TB, *ExecuteResult) {
		seen := make([]bool, fixture.rows)
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertRetainedScopedImport(tb, result, fixture.rows, seen)
		}
	},
}

// callBindingScalarCountWorkload aggregates count(n) and a COUNT{}-sum with
// explicit output columns.
var callBindingScalarCountWorkload = callBindingWorkload{
	rows:  callBindingBaselineFixtureRows,
	query: func(_ int, distinct bool) string { return callBindingScalarCountQuery(distinct) },
	verify: func(fixture *callBindingBenchFixture, _ int) func(testing.TB, *ExecuteResult) {
		rows := fixture.rows
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertScalarCount(tb, result, rows)
		}
	},
}

// callBindingConditionalLiteralWorkload varies the number of literal CASE arms
// over the same fixture.
var callBindingConditionalLiteralWorkload = callBindingWorkload{
	sizes: callBindingConditionalArmCounts,
	name:  func(arms int) string { return fmt.Sprintf("arms%d", arms) },
	rows:  callBindingBaselineFixtureRows,
	query: func(arms int, distinct bool) string { return callBindingConditionalLiteralQuery(arms, distinct) },
	verify: func(_ *callBindingBenchFixture, arms int) func(testing.TB, *ExecuteResult) {
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertConditionalLiteral(tb, result, arms)
		}
	},
}

// callBindingWideProjectionWorkload varies the projection width over the same
// fixture.
var callBindingWideProjectionWorkload = callBindingWorkload{
	sizes: callBindingProjectionItemCounts,
	name:  func(items int) string { return fmt.Sprintf("items%d", items) },
	rows:  callBindingBaselineFixtureRows,
	query: func(items int, distinct bool) string { return callBindingProjectionQuery(items, distinct) },
	verify: func(_ *callBindingBenchFixture, items int) func(testing.TB, *ExecuteResult) {
		expectation := newCallBindingProjectionExpectation(items)
		return func(tb testing.TB, result *ExecuteResult) {
			callBindingAssertProjection(tb, result, expectation)
		}
	},
}

// BenchmarkCallBindingWorkloads qualifies every CALL binding workload through
// one registration and one measured runner. Each shape retains warm and cold
// variants and every recorded row, arm and projection-width population.
func BenchmarkCallBindingWorkloads(b *testing.B) {
	for _, shape := range []struct {
		name     string
		workload callBindingWorkload
	}{
		{"OrdinaryLabeledRead", callBindingOrdinaryReadWorkload},
		{"CorrelatedCall", callBindingCorrelatedCallWorkload},
		{"RetainedScopedImport", callBindingRetainedScopedImportWorkload},
		{"ScalarCount", callBindingScalarCountWorkload},
		{"ConditionalLiteral", callBindingConditionalLiteralWorkload},
		{"WideProjection", callBindingWideProjectionWorkload},
	} {
		for _, mode := range []struct {
			name     string
			distinct bool
		}{{"Warm", false}, {"Cold", true}} {
			b.Run(shape.name+"/"+mode.name, func(b *testing.B) {
				benchmarkCallBindingWorkload(b, shape.workload, mode.distinct)
			})
		}
	}
}
