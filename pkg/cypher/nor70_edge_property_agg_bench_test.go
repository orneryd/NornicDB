package cypher

// NOR-70 follow-up: a bounded, deterministic benchmark for the detector-supported
// edge-property aggregate shape
//
//	MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r)
//
// The fixture is small (50 product groups) and moderate (500 product groups), ten
// REVIEWED relationships per group, and ten reused Customer start nodes. The
// ratings are mixed integer/float, some relationships carry no rating at all, and
// one group has no rated relationship. Nothing here is a stress or scale fixture.
//
// The two arms are the exported entry points ExecuteOptimized and Execute over
// the exact same statement text and the exact same store:
//
//   - Query-result caching is disabled at construction for both arms with
//     QueryCachePolicy{MaxEntries: 0} ("Zero MaxEntries disables caching",
//     NewStorageExecutor). With cache nil, canUseResultCache (cache_policy.go)
//     is false for every call on both paths, so neither arm can serve a cached
//     result and no arm pays a per-iteration cache clear the other does not.
//     nor70BenchFixture.executor asserts the disabled cache before measuring.
//   - Each arm runs on its own executor over the shared read-only store, so the
//     comparison is not an artifact of one executor's analyzer, plan or node
//     lookup caches being warmed by the other arm. Each arm is warmed by the
//     untimed validation call below.
//   - The timed region for ExecuteOptimized excludes DetectQueryPattern, which
//     is the cost of the API's own signature (the caller supplies PatternInfo).
//     A caller therefore pays detection on top of the measurement here.
//
// Validation happens before any timing and is also a standalone test
// (TestNOR70EdgePropertyAggBenchFixtureParity). It pins the exact columns, the
// row width, the group values and the row population against an expectation
// derived from the fixture specification only: every group keeps a row
// (including the null-only group), count(r) is ten for every group, and avg is
// the rated sum over the rated count, nil for the group without a rated
// relationship.
//
// avg is compared within a tight bound rather than bit for bit because the
// aggregate is Neo4j 5.26's running mean - mean += (value-mean)/count in
// pipeline_aggregate_stream.go - which rounds differently from one division of a
// total. A dropped relationship or a counted-in unrated one moves an average by
// orders of magnitude more than the bound. For the same reason two groups can
// share an average (both skip an unrated relationship carrying the same value),
// so the descending-ordered statement is asserted by its rank values and its
// non-increasing order rather than by one particular group per position.
//
// # Old release optimizer capture (invalid correctness baseline)
//
// The pre-fix reducer (executeEdgePropertyAggOptimized) exists only as the
// release version of optimized_executors.go. Capture its actual output before
// reading any timing, and label it invalid:
//
//	git show e8cd436e540ed421327bb72478d4d98479f7a2ac:pkg/cypher/optimized_executors.go > _nor70-bench-scratch/optimized_executors.go
//	go test ./pkg/cypher -overlay _nor70-bench-scratch/overlay.json \
//	  -run 'TestNOR70EdgePropertyAggBenchFixtureParity$' -v
//
// overlay.json maps this package's optimized_executors.go to that immutable file
// (the underscore directory is ignored by package patterns, so it is never
// built). The parity test then fails, and the recorded capture is the old
// optimizer's real, wrong answer: rows=49 of 50 and rows=499 of 500 with the
// null-only group absent, every row two values wide while the reported columns
// stay three wide (name and average only, count omitted), and every statement -
// including ORDER BY p.name and ORDER BY a DESC LIMIT 10 - answered in the
// reducer's own average-descending order. That output is an invalid correctness
// baseline and is never a performance reference either: the old reducer answers
// a different question than the one asked.
//
// # Scope
//
// This file measures and validates only. It changes no production behavior, and
// it makes no performance claim: the numbers are workload- and machine-scoped
// and are taken with a fixed modest iteration count. Nothing in production
// currently calls ExecuteOptimized, so the arms compare two exported entry
// points, not a shipped query route.

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// nor70BenchCustomers is the number of Customer start nodes the fixture
	// reuses: ten of them however many product groups the fixture has.
	nor70BenchCustomers = 10

	// nor70BenchEdgesPerGroup is the number of REVIEWED relationships each
	// product group receives, so count(r) is ten for every group.
	nor70BenchEdgesPerGroup = 10

	// nor70BenchAverageTolerance is the relative bound within which an observed
	// average must meet the fixture's rated-sum-over-rated-count expectation.
	// The running mean lands a few units in the last place away from that
	// division; a wrong rated value, a dropped relationship or a counted-in
	// unrated one moves the average by at least 1e-2, eight orders of magnitude
	// more than this bound.
	nor70BenchAverageTolerance = 1e-12
)

// nor70BenchCustomerName is the name of reused customer i.
func nor70BenchCustomerName(i int) string { return fmt.Sprintf("Customer %02d", i) }

// nor70BenchProductName is the name of product group i. Zero padding makes
// ascending name order equal to ascending group order, so the ORDER BY p.name
// expectation is readable.
func nor70BenchProductName(i int) string { return fmt.Sprintf("Product %04d", i) }

// nor70BenchRatingValue is one relationship's rating: an integer rating keeps
// its int64 type so the fixture's population really is mixed, a float rating
// carries the x.125 fraction, and an unrated relationship is absent rather than
// zero.
type nor70BenchRatingValue struct {
	integer int64
	float   float64
	isFloat bool
	present bool
}

// property is the stored property value: an int64, a float64, or nothing at all
// for an unrated relationship.
func (r nor70BenchRatingValue) property() any {
	switch {
	case !r.present:
		return nil
	case r.isFloat:
		return r.float
	default:
		return r.integer
	}
}

// numeric is the value the aggregate reads, expressed the way avg(r.prop) reads
// an integer and a float alike.
func (r nor70BenchRatingValue) numeric() float64 {
	if r.isFloat {
		return r.float
	}
	return float64(r.integer)
}

// nor70BenchRating is the deterministic rating of relationship edge of product
// group in a fixture with the given number of groups.
//
//   - The last group carries no rating on any of its ten relationships: the
//     null-only group.
//   - (group+edge) % 7 == 3 removes a rating from one relationship of most
//     groups, so the fixture also has partially rated groups.
//   - Even (group+edge) yields an int64 rating; odd yields the group's base plus
//     a fraction that depends only on the group.
func nor70BenchRating(groups, group, edge int) nor70BenchRatingValue {
	if group == groups-1 {
		return nor70BenchRatingValue{}
	}
	if (group+edge)%7 == 3 {
		return nor70BenchRatingValue{}
	}
	base := int64(group%9 + 1)
	if (group+edge)%2 == 0 {
		return nor70BenchRatingValue{integer: base, present: true}
	}
	return nor70BenchRatingValue{float: float64(base) + float64((group/9)%8)/8, isFloat: true, present: true}
}

// nor70BenchGroupAggregate is the expected aggregate of one product group,
// computed from the fixture specification rather than from any executor.
type nor70BenchGroupAggregate struct {
	relationships int64   // matched relationships: count(r)
	values        int64   // relationships that carried a rating
	sum           float64 // sum of the rated values
}

// average is avg(r.rating) for the group: nil when no relationship carried a
// rating, otherwise the rated sum over the rated count.
func (a nor70BenchGroupAggregate) average() any {
	if a.values == 0 {
		return nil
	}
	return a.sum / float64(a.values)
}

// nor70BenchExpectedAggregates derives every group's expected aggregate from the
// fixture specification alone.
func nor70BenchExpectedAggregates(groups int) map[string]nor70BenchGroupAggregate {
	expected := make(map[string]nor70BenchGroupAggregate, groups)
	for group := 0; group < groups; group++ {
		aggregate := nor70BenchGroupAggregate{relationships: nor70BenchEdgesPerGroup}
		for edge := 0; edge < nor70BenchEdgesPerGroup; edge++ {
			rating := nor70BenchRating(groups, group, edge)
			if !rating.present {
				continue
			}
			aggregate.sum += rating.numeric()
			aggregate.values++
		}
		expected[nor70BenchProductName(group)] = aggregate
	}
	return expected
}

// nor70BenchOrder is the row order a statement asks for.
type nor70BenchOrder int

const (
	// nor70BenchUnordered: the statement defines no row order, so rows are
	// compared per group instead of by position.
	nor70BenchUnordered nor70BenchOrder = iota
	// nor70BenchNameOrder: ORDER BY p.name ascending.
	nor70BenchNameOrder
	// nor70BenchAverageDescOrder: ORDER BY the average descending.
	nor70BenchAverageDescOrder
)

// nor70BenchQuery is one measured statement: its label, its exact text, its
// expected columns, the order it asks for and its LIMIT.
type nor70BenchQuery struct {
	label   string
	text    string
	columns []string
	order   nor70BenchOrder
	limit   int
}

// nor70BenchQueries returns the three measured statements. They project the same
// three values in the same column order - the group name, avg(r.rating) and
// count(r) - so every query's rows are read as [name, average, count].
func nor70BenchQueries() []nor70BenchQuery {
	const match = "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) "
	return []nor70BenchQuery{
		{
			label:   "avg-count",
			text:    match + "RETURN p.name, avg(r.rating), count(r)",
			columns: []string{"p.name", "avg(r.rating)", "count(r)"},
		},
		{
			label:   "name-order",
			text:    match + "RETURN p.name, avg(r.rating), count(r) ORDER BY p.name",
			columns: []string{"p.name", "avg(r.rating)", "count(r)"},
			order:   nor70BenchNameOrder,
		},
		{
			label:   "avg-order-limit-10",
			text:    match + "RETURN p.name, avg(r.rating) AS a, count(r) AS c ORDER BY a DESC LIMIT 10",
			columns: []string{"p.name", "a", "c"},
			order:   nor70BenchAverageDescOrder,
			limit:   10,
		},
	}
}

// nor70BenchFixture is the read-only store every arm reads.
type nor70BenchFixture struct {
	groups int
	store  storage.Engine
}

// executor returns a fresh executor over the fixture's store with the
// query-result cache disabled, and holds that disabled cache as an assertion so
// no measured call can be served from the result cache.
func (f *nor70BenchFixture) executor(tb testing.TB) *StorageExecutor {
	tb.Helper()
	exec := NewStorageExecutor(f.store, QueryCachePolicy{MaxEntries: 0})
	require.NotNil(tb, exec)
	require.Nil(tb, exec.cache, "the query-result cache must be disabled for both arms")
	maxEntries, _ := exec.QueryCachePolicy()
	require.Zero(tb, maxEntries, "the executor must not admit cached results")
	return exec
}

// newNOR70BenchFixture seeds groups product groups and their relationships in a
// fresh store: ten reused customers, one node per product group, and ten REVIEWED
// relationships from a rotating customer to each group.
func newNOR70BenchFixture(tb testing.TB, groups int) *nor70BenchFixture {
	tb.Helper()
	require.Positive(tb, groups)

	store := storage.NewNamespacedEngine(newTestMemoryEngine(tb), "nor70_bench_"+strconv.Itoa(groups))
	ctx := tb.Context()

	for customer := 0; customer < nor70BenchCustomers; customer++ {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("nor70c%02d", customer)),
			Labels:     []string{"Customer"},
			Properties: map[string]any{"name": nor70BenchCustomerName(customer)},
		})
		require.NoError(tb, err)
	}
	for group := 0; group < groups; group++ {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("nor70p%04d", group)),
			Labels:     []string{"Product"},
			Properties: map[string]any{"name": nor70BenchProductName(group)},
		})
		require.NoError(tb, err)
	}
	for group := 0; group < groups; group++ {
		for edge := 0; edge < nor70BenchEdgesPerGroup; edge++ {
			var properties map[string]any
			if rating := nor70BenchRating(groups, group, edge); rating.present {
				properties = map[string]any{"rating": rating.property()}
			}
			err := store.CreateEdge(ctx, &storage.Edge{
				ID:         storage.EdgeID(fmt.Sprintf("nor70e%04d%02d", group, edge)),
				StartNode:  storage.NodeID(fmt.Sprintf("nor70c%02d", (group+edge)%nor70BenchCustomers)),
				EndNode:    storage.NodeID(fmt.Sprintf("nor70p%04d", group)),
				Type:       "REVIEWED",
				Properties: properties,
			})
			require.NoError(tb, err)
		}
	}
	return &nor70BenchFixture{groups: groups, store: store}
}

// nor70BenchObservedRow is one result row read through the measured column
// order: group name, average (nil when the group has no rated relationship),
// count.
type nor70BenchObservedRow struct {
	name  string
	avg   any
	count int64
}

// nor70BenchReadRows reads a result through the measured column order and
// asserts every row has the expected width and column types.
func nor70BenchReadRows(tb testing.TB, arm string, query nor70BenchQuery, result *ExecuteResult) []nor70BenchObservedRow {
	tb.Helper()
	require.NotNil(tb, result, "%s: result", arm)
	rows := make([]nor70BenchObservedRow, 0, len(result.Rows))
	for i, row := range result.Rows {
		require.Len(tb, row, len(query.columns), "%s: row %d must carry every emitted column", arm, i)
		name, ok := row[0].(string)
		require.True(tb, ok, "%s: row %d group value has type %T, want string", arm, i, row[0])
		average := row[1]
		if average != nil {
			_, isFloat := average.(float64)
			require.True(tb, isFloat, "%s: row %d average has type %T, want float64 or nil", arm, i, average)
		}
		count, ok := row[2].(int64)
		require.True(tb, ok, "%s: row %d count has type %T, want int64", arm, i, row[2])
		rows = append(rows, nor70BenchObservedRow{name: name, avg: average, count: count})
	}
	return rows
}

// nor70BenchSameAverage reports whether an observed average matches the fixture's
// expectation within nor70BenchAverageTolerance, treating nil as "no rated
// relationship" on both sides.
func nor70BenchSameAverage(want, got any) bool {
	if want == nil || got == nil {
		return want == nil && got == nil
	}
	wantValue, wantIsFloat := want.(float64)
	gotValue, gotIsFloat := got.(float64)
	if !wantIsFloat || !gotIsFloat {
		return false
	}
	return math.Abs(gotValue-wantValue) <= nor70BenchAverageTolerance*math.Max(1, math.Abs(wantValue))
}

// nor70BenchDescending reports whether average left may precede average right in
// descending order: null sorts above every value, so a null on the left is
// always in order, a null on the right after a value is not, and values must not
// increase.
func nor70BenchDescending(left, right any) bool {
	if left == nil {
		return true
	}
	if right == nil {
		return false
	}
	leftValue, leftIsFloat := left.(float64)
	rightValue, rightIsFloat := right.(float64)
	if !leftIsFloat || !rightIsFloat {
		return false
	}
	return leftValue >= rightValue
}

// nor70BenchExpectedRows builds the expected row sequence for the statement's
// order and LIMIT.
func nor70BenchExpectedRows(expected map[string]nor70BenchGroupAggregate, order nor70BenchOrder, limit int) []nor70BenchObservedRow {
	rows := make([]nor70BenchObservedRow, 0, len(expected))
	for name, aggregate := range expected {
		rows = append(rows, nor70BenchObservedRow{name: name, avg: aggregate.average(), count: aggregate.relationships})
	}
	switch order {
	case nor70BenchNameOrder:
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	case nor70BenchAverageDescOrder:
		// Cypher orders null above every value, so descending puts the
		// null-only group first and then the averages from high to low. Equal
		// averages keep a stable name order for a reproducible expectation;
		// the assertion below compares rank values, not group names, at equal
		// ranks.
		sort.Slice(rows, func(i, j int) bool {
			leftValue, leftRated := rows[i].avg.(float64)
			rightValue, rightRated := rows[j].avg.(float64)
			switch {
			case !leftRated && !rightRated:
				return rows[i].name < rows[j].name
			case !leftRated:
				return true
			case !rightRated:
				return false
			case leftValue != rightValue:
				return leftValue > rightValue
			default:
				return rows[i].name < rows[j].name
			}
		})
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

// nor70BenchAssertExpectedRows checks a read result against the fixture
// specification: the row population, one row per group, the exact group values,
// and the order and LIMIT the statement asks for.
func nor70BenchAssertExpectedRows(tb testing.TB, expected map[string]nor70BenchGroupAggregate, query nor70BenchQuery, groups int, arm string, rows []nor70BenchObservedRow) {
	tb.Helper()

	if query.limit > 0 {
		require.Len(tb, rows, query.limit, "%s: LIMIT %d rows", arm, query.limit)
	} else {
		require.Len(tb, rows, len(expected), "%s: every fixture group keeps a row, including the null-only group", arm)
	}

	seen := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		_, duplicate := seen[row.name]
		require.False(tb, duplicate, "%s: row %d repeats group %q", arm, i, row.name)
		seen[row.name] = struct{}{}

		aggregate, known := expected[row.name]
		require.True(tb, known, "%s: row %d names a group outside the fixture: %q", arm, i, row.name)
		require.Equal(tb, aggregate.relationships, row.count, "%s: row %d (%s) count(r)", arm, i, row.name)
		require.True(tb, nor70BenchSameAverage(aggregate.average(), row.avg),
			"%s: row %d (%s) avg(r.rating) is %v, want %v", arm, i, row.name, row.avg, aggregate.average())
	}

	if query.limit == 0 {
		nullGroup := nor70BenchProductName(groups - 1)
		_, present := seen[nullGroup]
		require.True(tb, present, "%s: the null-only group %q must keep its row", arm, nullGroup)
	}

	if query.order == nor70BenchUnordered {
		return
	}

	ranked := nor70BenchExpectedRows(expected, query.order, query.limit)
	require.Len(tb, ranked, len(rows), "%s: ranked population", arm)
	switch query.order {
	case nor70BenchNameOrder:
		for i, want := range ranked {
			require.Equal(tb, want.name, rows[i].name, "%s: row %d name order", arm, i)
		}
	case nor70BenchAverageDescOrder:
		for i := 1; i < len(rows); i++ {
			require.True(tb, nor70BenchDescending(rows[i-1].avg, rows[i].avg),
				"%s: row %d average %v must not rank above row %d average %v",
				arm, i-1, rows[i-1].avg, i, rows[i].avg)
		}
		for i, want := range ranked {
			require.True(tb, nor70BenchSameAverage(want.avg, rows[i].avg),
				"%s: rank %d holds average %v, want rank %d average %v", arm, i, rows[i].avg, i, want.avg)
		}
	}
}

// nor70BenchAssertSameRows compares validated result populations. Unordered
// rows are matched by name; average-only ordering permits different names at
// tied ranks. Floating averages use the fixture's documented rounding tolerance.
func nor70BenchAssertSameRows(tb testing.TB, query nor70BenchQuery, canonical, optimized []nor70BenchObservedRow) {
	tb.Helper()
	require.Len(tb, optimized, len(canonical))
	if query.order == nor70BenchUnordered {
		byName := nor70BenchRowsByName(optimized)
		for _, want := range canonical {
			got, ok := byName[want.name]
			require.True(tb, ok, "missing group %s", want.name)
			require.Equal(tb, want.count, got.count)
			require.True(tb, nor70BenchSameAverage(want.avg, got.avg))
		}
		return
	}
	for i, want := range canonical {
		got := optimized[i]
		if query.order == nor70BenchNameOrder {
			require.Equal(tb, want.name, got.name)
		}
		require.Equal(tb, want.count, got.count)
		require.True(tb, nor70BenchSameAverage(want.avg, got.avg))
	}
}

// nor70BenchRowsByName indexes already-validated rows by group name; duplicates
// were rejected by nor70BenchAssertExpectedRows before this runs.
func nor70BenchRowsByName(rows []nor70BenchObservedRow) map[string]nor70BenchObservedRow {
	byName := make(map[string]nor70BenchObservedRow, len(rows))
	for _, row := range rows {
		byName[row.name] = row
	}
	return byName
}

// nor70BenchDescribe renders an observed result compactly for a test log: its
// columns, its row count, its first rows and the null-only group's row. This is
// the capture the old-release overlay run prints.
func nor70BenchDescribe(result *ExecuteResult, nullGroupName string, maxRows int) string {
	if result == nil {
		return "<nil result>"
	}
	var described strings.Builder
	fmt.Fprintf(&described, "columns=%v rows=%d", result.Columns, len(result.Rows))
	shown := maxRows
	if len(result.Rows) < shown {
		shown = len(result.Rows)
	}
	for i := 0; i < shown; i++ {
		fmt.Fprintf(&described, "\n\t\trow[%d]=%v", i, result.Rows[i])
	}
	for i, row := range result.Rows {
		if len(row) == 0 {
			continue
		}
		if name, ok := row[0].(string); ok && name == nullGroupName {
			fmt.Fprintf(&described, "\n\t\tnull-group row[%d]=%v", i, row)
			return described.String()
		}
	}
	fmt.Fprintf(&described, "\n\t\tnull-only group %q: absent", nullGroupName)
	return described.String()
}

// nor70BenchPrepare detects the statement, runs both exported arms once over the
// same text and store, logs what each actually returned, and asserts both
// against the fixture specification and against each other before any timing
// starts. It returns the detector's own PatternInfo, which is the input
// ExecuteOptimized requires.
func nor70BenchPrepare(tb testing.TB, expected map[string]nor70BenchGroupAggregate, groups int, query nor70BenchQuery, canonicalExec, optimizedExec *StorageExecutor) PatternInfo {
	tb.Helper()
	ctx := tb.Context()

	info, err := DetectQueryPattern(ctx, query.text)
	require.NoError(tb, err)
	require.Equal(tb, PatternEdgePropertyAgg, info.Pattern, "the fixture's shape must stay in the detector population")
	require.True(tb, info.IsOptimizable())
	require.Equal(tb, "rating", info.AggProperty)
	require.Equal(tb, "r", info.RelVar)
	require.Equal(tb, []string{"avg"}, info.AggFunctions)
	require.Equal(tb, query.limit, info.Limit)

	optimized, handled := optimizedExec.ExecuteOptimized(ctx, query.text, info)
	require.True(tb, handled, "the detector's own PatternInfo must be handled")
	require.NotNil(tb, optimized)

	canonical, err := canonicalExec.Execute(ctx, query.text, nil)
	require.NoError(tb, err)
	require.NotNil(tb, canonical)

	nullGroup := nor70BenchProductName(groups - 1)
	tb.Logf("groups=%d query=%q\n\tExecuteOptimized actual: %s\n\tExecute actual: %s",
		groups, query.text,
		nor70BenchDescribe(optimized, nullGroup, 3),
		nor70BenchDescribe(canonical, nullGroup, 3))

	require.Equal(tb, query.columns, optimized.Columns, "ExecuteOptimized columns")
	require.Equal(tb, query.columns, canonical.Columns, "Execute columns")

	optimizedRows := nor70BenchReadRows(tb, "ExecuteOptimized", query, optimized)
	canonicalRows := nor70BenchReadRows(tb, "Execute", query, canonical)
	nor70BenchAssertExpectedRows(tb, expected, query, groups, "ExecuteOptimized", optimizedRows)
	nor70BenchAssertExpectedRows(tb, expected, query, groups, "Execute", canonicalRows)
	nor70BenchAssertSameRows(tb, query, canonicalRows, optimizedRows)

	return info
}

// TestNOR70EdgePropertyAggBenchFixtureParity validates the benchmark fixture and
// both measured arms before any timing: the detector's population, the exact
// columns, the row width, the group values, the row population the ORDER BY and
// LIMIT statements imply, and the agreement of ExecuteOptimized with the
// canonical Execute for the same text and data.
//
// Run this test through -overlay with the release version of
// optimized_executors.go to capture the pre-fix optimizer's actual output: the
// test then fails, and the logged "actual" lines plus the failing assertion are
// that output, which is an invalid correctness baseline (see the file header).
func TestNOR70EdgePropertyAggBenchFixtureParity(t *testing.T) {
	for _, groups := range []int{50, 500} {
		groups := groups
		t.Run(strconv.Itoa(groups)+"-groups", func(t *testing.T) {
			fixture := newNOR70BenchFixture(t, groups)
			expected := nor70BenchExpectedAggregates(groups)
			canonicalExec := fixture.executor(t)
			optimizedExec := fixture.executor(t)
			for _, query := range nor70BenchQueries() {
				query := query
				t.Run(query.label, func(t *testing.T) {
					nor70BenchPrepare(t, expected, groups, query, canonicalExec, optimizedExec)
				})
			}
		})
	}
}

// nor70BenchmarkSink keeps both arms' results reachable so the compiler cannot
// drop the measured work.
var nor70BenchmarkSink *ExecuteResult

// BenchmarkNOR70EdgePropertyAgg measures the detector-supported edge-property
// aggregate through ExecuteOptimized and through the canonical Execute for the
// same statement text and the same store, over the 50-group and 500-group
// fixtures.
//
// Both arms are validated (nor70BenchPrepare) before the timer starts, and the
// measured calls run on executors whose query-result cache is disabled, so no
// iteration is served from the result cache. Run it with a fixed, modest
// iteration count rather than the default calibration:
//
//	go test ./pkg/cypher -run '^$' -bench 'BenchmarkNOR70EdgePropertyAgg$' \
//	  -benchtime=10x -count=3 -benchmem \
//	  -cpuprofile=_nor70-bench-scratch/cpu.prof
//
// The CPU profile includes fixture setup and validation outside benchmark timers.
// A report must state the number of
// samples it actually collected rather than assume a representative profile.
func BenchmarkNOR70EdgePropertyAgg(b *testing.B) {
	for _, groups := range []int{50, 500} {
		fixture := newNOR70BenchFixture(b, groups)
		expected := nor70BenchExpectedAggregates(groups)
		canonicalExec := fixture.executor(b)
		optimizedExec := fixture.executor(b)

		for _, query := range nor70BenchQueries() {
			query := query
			info := nor70BenchPrepare(b, expected, groups, query, canonicalExec, optimizedExec)

			b.Run("groups="+strconv.Itoa(groups)+"/"+query.label+"/canonical-Execute", func(b *testing.B) {
				ctx := b.Context()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, err := canonicalExec.Execute(ctx, query.text, nil)
					if err != nil {
						b.Fatal(err)
					}
					nor70BenchmarkSink = result
				}
			})

			b.Run("groups="+strconv.Itoa(groups)+"/"+query.label+"/ExecuteOptimized", func(b *testing.B) {
				ctx := b.Context()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, handled := optimizedExec.ExecuteOptimized(ctx, query.text, info)
					if !handled || result == nil {
						b.Fatalf("ExecuteOptimized declined the detected pattern: handled=%v result=%v", handled, result)
					}
					nor70BenchmarkSink = result
				}
			})
		}
	}
}
