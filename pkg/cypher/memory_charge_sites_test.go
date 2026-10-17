package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// One accountant (#1343): only the accountant's files size a value or
// charge a statement's memory. Operators charge what they keep through its
// API (memoryRetention, rowGenerations, memorySpan, expressionBudget), so a
// new operator can't size values on its own, charge what another already
// charged, or size anything while no limit is configured.

// accountantFiles are the transaction memory accountant.
var accountantFiles = map[string]bool{
	"transaction_memory.go":           true,
	"transaction_memory_estimate.go":  true,
	"transaction_memory_retention.go": true,
}

// accountantFunctions and accountantMethods are what only the accountant
// calls: sizing a value or a row, and taking or giving back a scope's
// memory directly. A method name counts only as a method call (x.grow()),
// so a local closure of the same name is not one.
var accountantFunctions = map[string]bool{
	"estimateValueBytes":      true,
	"estimateValueBytesAt":    true,
	"estimateResultRowBytes":  true,
	"estimatePathResultBytes": true,
	"shallowValueBytes":       true,
	"sharedBindingBytes":      true,
}

var accountantMethods = map[string]bool{
	"grow":              true,
	"give":              true,
	"addCharge":         true,
	"growRows":          true,
	"growValue":         true,
	"refill":            true,
	"noteList":          true,
	"knownListBytes":    true,
	"valueBytes":        true,
	"sizedValue":        true,
	"reservePools":      true,
	"rowBytes":          true,
	"bindingBytes":      true,
	"columnBytes":       true,
	"holdBytes":         true,
	"ownRows":           true,
	"ownsRows":          true,
	"estimator":         true,
	"outerBindingBytes": true,
}

// accountantTypes are the accountant's own types, which operators reach
// only through its API.
var accountantTypes = map[string]bool{"memoryAccount": true, "rowEstimator": true}

// accountantViolations lists where a file's source uses the accountant's
// internals.
func accountantViolations(t *testing.T, name string, source []byte) []string {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, name, source, parser.SkipObjectResolution)
	require.NoError(t, err)
	var found []string
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			var called string
			switch function := typed.Fun.(type) {
			case *ast.Ident:
				if accountantFunctions[function.Name] {
					called = function.Name
				}
			case *ast.SelectorExpr:
				if accountantMethods[function.Sel.Name] || accountantFunctions[function.Sel.Name] {
					called = function.Sel.Name
				}
			}
			if called != "" {
				found = append(found, files.Position(typed.Pos()).String()+": "+called+"()")
			}
		case *ast.Ident:
			if accountantTypes[typed.Name] {
				found = append(found, files.Position(typed.Pos()).String()+": "+typed.Name)
			}
		}
		return true
	})
	return found
}

func TestMemoryChargeSitesUseTheAccountant(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	present := 0
	var violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if accountantFiles[name] {
			present++
			continue
		}
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		violations = append(violations, accountantViolations(t, name, source)...)
	}
	require.Equal(t, len(accountantFiles), present, "the accountant's files moved: update accountantFiles")
	require.Emptyf(t, violations, "sized or charged outside the accountant; use memoryRetention, rowGenerations, memorySpan or expressionBudget:\n%s", strings.Join(violations, "\n"))
}

// The check finds a site that sizes a value itself (even as the argument
// of an API call, sized before the API checks a limit is configured), one
// that charges a scope directly, and one that keeps its own account.
func TestMemoryChargeSiteCheckFindsPlantedSites(t *testing.T) {
	planted := `package cypher

func planted(ctx context.Context, value interface{}) {
	retention := newMemoryRetention(ctx)
	_ = retention.reserveBytes(estimateValueBytes(value))
	_ = memoryScopeFromContext(ctx).grow(8)
	account := memoryAccount{scope: memoryScopeFromContext(ctx)}
	_ = account
}
`
	violations := accountantViolations(t, "planted.go", []byte(planted))
	require.Len(t, violations, 3, "%v", violations)
	require.Contains(t, violations[0], "estimateValueBytes()")
	require.Contains(t, violations[1], "grow()")
	require.Contains(t, violations[2], "memoryAccount")
	require.Empty(t, accountantViolations(t, "clean.go", []byte(`package cypher

func clean(ctx context.Context, value interface{}) error {
	retention := newMemoryRetention(ctx)
	defer retention.close()
	var nodes []int32
	grow := func() { nodes = append(nodes, 0) }
	grow()
	return retention.reserveValue(value)
}
`)))
}

// With no limit configured nothing sizes a value or a row, in any operator:
// with a limit the same statements size what they keep and return the same
// rows.
func TestNoSizingWithoutLimitAcrossOperators(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	seedTopicGroupsGraph(t, executor, 40, 10)
	queries := map[string]string{
		"match":                 "MATCH (d:MemDoc)-[:HAS]->(t:MemThing) RETURN d.id AS d, t.id AS t ORDER BY d, t",
		"optional match":        "MATCH (d:MemDoc) OPTIONAL MATCH (d)-[:HAS]->(t:MemThing {id: 'thing-8'}) RETURN d.id AS d, t.id AS t ORDER BY d",
		"aggregate and order":   "MATCH (d:MemDoc)-[:HAS]->(t:MemThing) WITH t.id AS thing, collect(DISTINCT d.id) AS docs, count(*) AS n WITH DISTINCT thing, size(docs) AS s ORDER BY s DESC, thing RETURN thing, s LIMIT 20",
		"correlated call":       topicGroupsQuery,
		"independent call":      "MATCH (d:MemDoc) CALL { MATCH (t:MemThing) RETURN count(t) AS things } RETURN d.id AS d, things ORDER BY d",
		"procedure call":        "CALL db.labels() YIELD label RETURN label ORDER BY label",
		"expressions":           "UNWIND range(1, 5000) AS x WITH x, [y IN range(1, 20) WHERE y % 2 = 0 | y * x] AS l, reduce(s = 0, y IN range(1, 20) | s + y) AS r RETURN sum(size(l)) AS a, sum(r) AS b",
		"union":                 "MATCH (d:MemDoc) RETURN d.id AS id UNION MATCH (t:MemThing) RETURN t.id AS id",
		"percentile":            "MATCH (d:MemDoc)-[:HAS]->(t:MemThing) RETURN percentileCont(size(t.text), 0.5) AS p, percentileDisc(size(t.text), 0.9) AS q",
		"shortest path":         "MATCH (a:MemDoc {id: 'doc-1'}), (b:MemDoc {id: 'doc-2'}) MATCH p = shortestPath((a)-[:HAS*..4]-(b)) RETURN length(p) AS l",
		"quantified path":       "MATCH (a:MemDoc {id: 'doc-1'}) MATCH p = (a)(()-[:HAS]->())*(b) RETURN count(*) AS n",
		"pattern comprehension": "MATCH (d:MemDoc) RETURN d.id AS d, [(d)-[:HAS]->(t) | t.id] AS things ORDER BY d LIMIT 5",
		"variable length":       "MATCH (d:MemDoc {id: 'doc-1'})-[:HAS*1..2]-(x) RETURN count(*) AS n",
		"distinct aggregates":   "MATCH (d:MemDoc)-[:HAS]->(t:MemThing) RETURN count(DISTINCT t.id) AS n, size(collect(DISTINCT t.id)) AS c",
	}
	var sizings, trackedSizings atomic.Int64
	sizingObserver = func() { sizings.Add(1) }
	t.Cleanup(func() { sizingObserver = nil })
	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			ConfigureTransactionMemory(0, TransactionMemoryLimits{})
			sizings.Store(0)
			untracked, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Zero(t, sizings.Load(), "a value was sized with no limit configured")

			ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{TransactionMax: 1 << 40})
			tracked, err := executor.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			trackedSizings.Add(sizings.Load())
			require.Equal(t, untracked.Rows, tracked.Rows)
			requirePoolsEmpty(t, "nornic")
		})
	}
	require.Positive(t, trackedSizings.Load(), "with a limit what the statements keep is sized")
}

// collect(DISTINCT x) counts each value it keeps once: the list keeps the
// value its DISTINCT index saw.
func TestCollectDistinctCountsEachValueOnce(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	executor := newTransactionMemoryExecutor(t)
	spent := func(distinct bool) int64 {
		tracker := newTestMemoryTracker("distinct")
		defer tracker.end()
		state := pipelineAggregateState{name: "collect", expression: "x", distinct: distinct, memory: retentionIn(&tracker.root)}
		for i := 0; i < 1000; i++ {
			require.True(t, state.add(context.Background(), executor, pipelineRow{"x": fmt.Sprintf("distinct value %d", i)}))
		}
		return state.memory.account.spent
	}
	plain := spent(false)
	require.Positive(t, plain)
	require.Equal(t, plain, spent(true), "collect(DISTINCT x) counted its values twice")
}

// A list collect() built counts once: the aggregation's count goes back
// once the row that keeps the list counts it.
func TestCollectedListCountsOnceInTheRowThatKeepsIt(t *testing.T) {
	resetTransactionMemory(t)
	executor := newTransactionMemoryExecutor(t)
	const values = 200000
	list := make([]interface{}, values)
	for i := range list {
		list[i] = "value text with a longer tail " + strconv.Itoa(i+1)
	}
	size := estimateValueBytes(list)
	peak := countedPeak(t, executor, "UNWIND range(1, 200000) AS x WITH collect('value text with a longer tail ' + toString(x)) AS xs RETURN size(xs) AS n", nil)
	require.GreaterOrEqualf(t, peak, size, "the list (%d bytes) was undercounted: %d", size, peak)
	require.Lessf(t, peak, size*3/2, "the list (%d bytes) was counted twice: %d", size, peak)
}

// The rows a clause replaces give their charge back only once the rows that
// replace them are charged in full, carried values included: a value the
// new rows still hold stays counted.
func TestRowGenerationsKeepCarriedValuesCounted(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(1<<40, TransactionMemoryLimits{})
	tracker := newTestMemoryTracker("generations")
	t.Cleanup(tracker.end)
	scope := &tracker.root
	view := make([]interface{}, 20000)
	for i := range view {
		view[i] = "carried value " + strconv.Itoa(i)
	}
	viewBytes := estimateValueBytes(view)

	input := []pipelineRow{{}}
	generations := newRowGenerations(scope, input)
	generations.next(input)
	// WITH $view AS view: one row, charged in full.
	with := retentionIn(scope)
	withRows := []pipelineRow{{"view": view}}
	require.NoError(t, with.reserveRow(withRows[0]))
	with.owned(withRows)
	with.close()
	generations.next(withRows)
	require.GreaterOrEqual(t, scope.mark(), viewBytes)

	// CALL (view) { UNWIND range(1, 1000) AS i RETURN i }: 1,000 rows on the
	// one outer row, charged in full; the WITH row gives its charge back.
	call := retentionIn(scope)
	callRows := make([]pipelineRow, 0, 1000)
	for i := 0; i < 1000; i++ {
		require.NoError(t, call.reserveExtendedRow(withRows[0], []string{"i"}, []interface{}{int64(i)}))
		callRows = append(callRows, pipelineRow{"view": view, "i": int64(i)})
	}
	call.owned(callRows)
	call.close()
	generations.next(callRows)
	held := scope.mark()
	require.GreaterOrEqual(t, held, viewBytes, "the carried list went uncounted")
	require.Less(t, held, viewBytes+1000*256, "the carried list counted per row")

	// A clause whose rows are not charged in full (passed through, partly
	// built) keeps the rows they replace counted.
	partial := []pipelineRow{callRows[0]}
	generations.next(partial)
	require.Equal(t, held, scope.mark())
}
