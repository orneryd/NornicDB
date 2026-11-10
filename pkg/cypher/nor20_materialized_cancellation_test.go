package cypher

// NOR20 materialized cancellation.
//
// A canceled caller must end the materialized work of a large MATCH/RETURN:
// the candidate label scan, the RETURN projection, and the ORDER BY key
// evaluation (and the ordering itself, covered by the ordering checkpoint
// test below). The public cases drive Execute with a real context.CancelFunc
// called from this test's own goroutine and hold the real work sites with
// existing seams, then count the work the statement still drives:
//
//   - the storage ProjectedLabelNodeReader, whose StreamNodesByLabelProjected
//     has no context parameter: the visitor the executor gives it is the only
//     place the caller's cancellation can arrive;
//   - an admitted plugin function, evaluated once per row by the row
//     evaluator, which parks the first call until the caller cancels.
//
// Every case asserts the product outcome: context.Canceled, no result rows,
// the work site observed the caller's cancellation, the statement stops
// instead of finishing the row set, and the same statement returns its exact
// rows again afterwards.

// The cases hold public statements and the work they drive. Which private
// owner performs that work is a source/coverage diagnostic kept outside this
// regression: renaming an owner must not break a product-behavior test.

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nor20Rows = 4001

	// nor20HoldLimit bounds a held work site no caller cancellation reached;
	// a passing case always ends from the caller's cancellation.
	nor20HoldLimit = 30 * time.Second
	nor20WaitLimit = 60 * time.Second

	nor20ProbeFunction = "nor20.matprobe"

	nor20HealthyQuery         = "MATCH (n:Item) RETURN n.rank AS rank, n.payload AS payload ORDER BY rank"
	nor20TiesQuery            = "MATCH (n:Item) RETURN n.payload AS payload, n.grp AS grp ORDER BY grp"
	nor20TiesScanQuery        = "MATCH (n:Item) RETURN n.payload AS payload, n.grp AS grp"
	nor20ProjectionQuery      = "MATCH (n:Item) RETURN n.rank AS rank, " + nor20ProbeFunction + "(n.rank) AS probe ORDER BY rank"
	nor20OrderKeysQuery       = "MATCH (n:Item) RETURN n.rank AS rank, n.payload AS payload ORDER BY " + nor20ProbeFunction + "(n.rank)"
	nor20PlainProjectionQuery = "MATCH (n:Item) RETURN n.rank AS rank, " + nor20ProbeFunction + "(n.rank) AS probe"
)

type nor20Outcome struct {
	result *ExecuteResult
	err    error
}

func nor20Payload(index int) string {
	return fmt.Sprintf("%012d", index)
}

func nor20SeedItems(t *testing.T, store storage.Engine) {
	t.Helper()
	for index := 0; index < nor20Rows; index++ {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("nor20-item-%06d", index)),
			Labels: []string{"Item"},
			Properties: map[string]interface{}{
				"rank":    int64(index),
				"payload": nor20Payload(index),
				"tie1":    int64(0),
				"tie2":    int64(0),
				"tie3":    int64(0),
				"grp":     int64(index % 7),
			},
		})
		require.NoError(t, err)
	}
}

func nor20NewExecutor(t *testing.T, store storage.Engine) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

// nor20RequireHealthyRows is the control: the ordered statement returns every
// row, in key order, with its exact values.
func nor20RequireHealthyRows(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	result, err := exec.Execute(ctx, nor20HealthyQuery, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, nor20Rows)
	for index, row := range result.Rows {
		require.Equal(t, int64(index), row[0], "row %d rank", index)
		require.Equal(t, nor20Payload(index), row[1], "row %d payload", index)
	}
}

// nor20RequireStableTies is the control for equal ORDER BY keys: rows with
// the same key keep the order the scan produced, exactly as the unordered
// statement returns them.
func nor20RequireStableTies(t *testing.T, exec *StorageExecutor, ctx context.Context) {
	t.Helper()
	ordered, err := exec.Execute(ctx, nor20TiesQuery, nil)
	require.NoError(t, err)
	scanned, err := exec.Execute(ctx, nor20TiesScanQuery, nil)
	require.NoError(t, err)
	require.Len(t, ordered.Rows, nor20Rows)
	require.Len(t, scanned.Rows, nor20Rows)

	input := make(map[int64][]string, 7)
	var groups []int64
	for _, row := range scanned.Rows {
		group := row[1].(int64)
		if _, seen := input[group]; !seen {
			groups = append(groups, group)
		}
		input[group] = append(input[group], row[0].(string))
	}
	require.NotEmpty(t, groups)

	positions := make(map[int64]int, len(groups))
	lastGroup := int64(-1)
	for _, row := range ordered.Rows {
		group := row[1].(int64)
		require.GreaterOrEqual(t, group, lastGroup, "ORDER BY keys are in order")
		lastGroup = group
		position := positions[group]
		require.Less(t, position, len(input[group]), "group %d returned more rows than the scan", group)
		require.Equal(t, input[group][position], row[0], "equal ORDER BY keys keep their input order")
		positions[group] = position + 1
	}
	for _, group := range groups {
		require.Equal(t, len(input[group]), positions[group], "group %d lost rows", group)
	}
}

// nor20Probe is the admitted plugin function the held rows call: its first
// call parks until the caller cancels, then the remaining calls return at
// once, so the count tells how much of the row set the statement still drove.
type nor20Probe struct {
	hold    atomic.Bool
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newNor20Probe() *nor20Probe {
	return &nor20Probe{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (p *nor20Probe) summon() {
	p.once.Do(func() { close(p.release) })
}

func (p *nor20Probe) call(value interface{}) interface{} {
	if p.hold.Load() && p.calls.Add(1) == 1 {
		select {
		case p.entered <- struct{}{}:
		default:
		}
		select {
		case <-p.release:
		case <-time.After(nor20HoldLimit):
		}
	}
	return value
}

// nor20ProbeSlot publishes the probe a running case is holding. The function
// registry has no unregister and this package cannot remove an entry, so the
// dispatcher registered below keeps no probe: a case stores its probe here and
// clears the slot in cleanup, leaving the process-wide registration
// fixture-free.
var nor20ProbeSlot atomic.Pointer[nor20Probe]

func nor20InstallProbe(t *testing.T, probe *nor20Probe) {
	t.Helper()
	// The probe is a registered expression function. The row evaluator calls
	// it with the statement's context and the probe ignores it, so the hold is
	// the row loop's own work, not a callee that observes the context.
	cypherfn.Register(nor20ProbeFunction, func(_ context.Context, call cypherfn.Context, args []string) (interface{}, error) {
		active := nor20ProbeSlot.Load()
		value, err := call.Eval(args[0])
		if err != nil {
			return nil, err
		}
		if active == nil {
			return nil, fmt.Errorf("%s() has no active probe", nor20ProbeFunction)
		}
		return active.call(value), nil
	})
	// The unknown-function check reads the catalog's names; the probe is one
	// for this test, and only for this test.
	builtInCypherFunctions[nor20ProbeFunction] = struct{}{}
	nor20ProbeSlot.Store(probe)
	t.Cleanup(func() {
		nor20ProbeSlot.CompareAndSwap(probe, nil)
		delete(builtInCypherFunctions, nor20ProbeFunction)
	})
}

// nor20HeldProjectedReader is the storage seam: it holds the first node the
// projected label reader visits, then records how many nodes the scan visits
// after the caller canceled and the error the scan's visitor returned.
type nor20HeldProjectedReader struct {
	*storage.NamespacedEngine

	hold    atomic.Bool
	readers atomic.Int64
	visits  atomic.Int64
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	visitErr error
}

// Pin the optional capability: a signature drift must fail compilation.
var _ storage.ProjectedLabelNodeReader = (*nor20HeldProjectedReader)(nil)

func newNor20HeldProjectedReader(engine *storage.NamespacedEngine) *nor20HeldProjectedReader {
	return &nor20HeldProjectedReader{
		NamespacedEngine: engine,
		entered:          make(chan struct{}, 1),
		release:          make(chan struct{}),
	}
}

func (r *nor20HeldProjectedReader) summon() {
	r.once.Do(func() { close(r.release) })
}

func (r *nor20HeldProjectedReader) StreamNodesByLabelProjected(ctx context.Context, label string, properties []string, visit func(*storage.Node) error) error {
	r.readers.Add(1)
	return r.NamespacedEngine.StreamNodesByLabelProjected(ctx, label, properties, func(node *storage.Node) error {
		visited := r.visits.Add(1)
		if r.hold.Load() && visited == 1 {
			select {
			case r.entered <- struct{}{}:
			default:
			}
			select {
			case <-r.release:
			case <-time.After(nor20HoldLimit):
			}
		}
		err := visit(node)
		if err != nil {
			r.mu.Lock()
			if r.visitErr == nil {
				r.visitErr = err
			}
			r.mu.Unlock()
		}
		return err
	})
}

func (r *nor20HeldProjectedReader) visitorError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.visitErr
}

func (r *nor20HeldProjectedReader) reset() {
	r.readers.Store(0)
	r.visits.Store(0)
	r.mu.Lock()
	r.visitErr = nil
	r.mu.Unlock()
}

func nor20ExecuteAsync(exec *StorageExecutor, ctx context.Context, query string) <-chan nor20Outcome {
	outcome := make(chan nor20Outcome, 1)
	go func() {
		result, err := exec.Execute(ctx, query, nil)
		outcome <- nor20Outcome{result: result, err: err}
	}()
	return outcome
}

func nor20AwaitOutcome(t *testing.T, outcome <-chan nor20Outcome) nor20Outcome {
	t.Helper()
	select {
	case got := <-outcome:
		return got
	case <-time.After(nor20WaitLimit):
		t.Fatalf("the statement did not return within %s of the caller's cancellation", nor20WaitLimit)
		return nor20Outcome{}
	}
}

func TestNor20MaterializedCancellation(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []struct {
		name       string
		parserType string
	}{
		{"nornic_parser", config.ParserTypeNornic},
		{"antlr_parser", config.ParserTypeANTLR},
	} {
		t.Run(parser.name, func(t *testing.T) {
			config.SetParserType(parser.parserType)
			t.Run("candidate_scan", nor20CandidateScanCase)
			t.Run("return_projection", func(t *testing.T) { nor20ProbeCase(t, nor20ProjectionQuery, "the RETURN projection") })
			t.Run("return_projection_plain", func(t *testing.T) { nor20ProbeCase(t, nor20PlainProjectionQuery, "the plain RETURN projection") })
			t.Run("order_keys", func(t *testing.T) { nor20ProbeCase(t, nor20OrderKeysQuery, "the ORDER BY key evaluation") })
		})
	}
}

// nor20CandidateScanCase cancels while the label scan is reading its first
// node. The projected label reader has no context, so the only stop the scan
// can receive is the visitor's error.
func nor20CandidateScanCase(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20")
	nor20SeedItems(t, store)
	reader := newNor20HeldProjectedReader(store)
	t.Cleanup(reader.summon)
	exec := nor20NewExecutor(t, reader)

	nor20RequireHealthyRows(t, exec, context.Background())
	nor20RequireStableTies(t, exec, context.Background())

	reader.reset()
	reader.hold.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcome := nor20ExecuteAsync(exec, ctx, nor20HealthyQuery)
	joined := false
	t.Cleanup(func() {
		reader.summon()
		if joined {
			return
		}
		select {
		case <-outcome:
		case <-time.After(nor20WaitLimit):
			t.Errorf("the statement did not finish after its fixture was released")
		}
	})

	runtimeFixtureOwner.WaitForBarrier(t, reader.entered, nor20WaitLimit, nil, "the materialized label scan never started")
	cancel()
	reader.summon()

	got := nor20AwaitOutcome(t, outcome)
	joined = true
	require.Nil(t, got.result, "a canceled statement must not return rows")
	require.ErrorIs(t, got.err, context.Canceled)
	require.EqualValues(t, 1, reader.readers.Load(), "the projected label reader is the scan under test")
	require.EqualValues(t, 1, reader.visits.Load(), "the scan stops at the node in flight when the caller cancels")
	require.ErrorIs(t, reader.visitorError(), context.Canceled, "the scan's visitor receives the caller's cancellation")

	nor20RequireHealthyRows(t, exec, context.Background())
	nor20RequireStableTies(t, exec, context.Background())
}

// nor20ProbeCase cancels while the statement is evaluating the first row of
// the materialized row set. site names the work a failure message reports; it
// is a label, not an assertion about the private function that performs it.
func nor20ProbeCase(t *testing.T, query, site string) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20")
	nor20SeedItems(t, store)
	probe := newNor20Probe()
	t.Cleanup(probe.summon)
	nor20InstallProbe(t, probe)
	exec := nor20NewExecutor(t, store)

	nor20RequireProbeHealthy(t, exec, query)
	probe.hold.Store(true)
	probe.calls.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	outcome := nor20ExecuteAsync(exec, ctx, query)
	joined := false
	t.Cleanup(func() {
		probe.summon()
		if joined {
			return
		}
		select {
		case <-outcome:
		case <-time.After(nor20WaitLimit):
			t.Errorf("the statement did not finish after its fixture was released")
		}
	})

	runtimeFixtureOwner.WaitForBarrier(t, probe.entered, nor20WaitLimit, nil, site+" never started")
	cancel()
	probe.summon()

	got := nor20AwaitOutcome(t, outcome)
	joined = true
	require.Nil(t, got.result, "a canceled statement must not return rows")
	require.ErrorIs(t, got.err, context.Canceled)
	require.EqualValues(t, 1, probe.calls.Load(), "the statement stops evaluating rows when the caller cancels")

	probe.hold.Store(false)
	nor20RequireProbeHealthy(t, exec, query)
}

// nor20RequireProbeHealthy is the control for a statement that calls the
// probe: every row returns its exact values with no cancellation.
func nor20RequireProbeHealthy(t *testing.T, exec *StorageExecutor, query string) {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Rows, nor20Rows)
	probeColumn := strings.Contains(query, " AS probe")
	for index, row := range result.Rows {
		require.Equal(t, int64(index), row[0], "row %d rank", index)
		if probeColumn {
			require.Equal(t, int64(index), row[1], "row %d probe", index)
			continue
		}
		require.Equal(t, nor20Payload(index), row[1], "row %d payload", index)
	}
}

// nor20CheckpointContext counts the caller-cancellation observations an
// operator makes and reports context.Canceled from the one after after: an
// operator that only samples its caller now and then still stops between
// sampled observations.
type nor20CheckpointContext struct {
	context.Context
	after  int64
	checks atomic.Int64
	done   chan struct{}
	once   sync.Once
}

func newNor20CheckpointContext(base context.Context, after int64) *nor20CheckpointContext {
	return &nor20CheckpointContext{Context: base, after: after, done: make(chan struct{})}
}

func (c *nor20CheckpointContext) Err() error {
	if c.checks.Add(1) <= c.after {
		return nil
	}
	c.once.Do(func() { close(c.done) })
	return context.Canceled
}

func (c *nor20CheckpointContext) Done() <-chan struct{} {
	return c.done
}

func nor20OrderingInput() ([]pipelineRow, []pipelineRow) {
	rows := make([]pipelineRow, 4096)
	scopes := make([]pipelineRow, len(rows))
	for index := range rows {
		row := pipelineRow{"rank": int64(len(rows) - index)}
		rows[index] = row
		scopes[index] = row
	}
	return rows, scopes
}

func nor20OrderingFailureSlot() context.Context {
	return context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
}

// TestNor20MaterializedOrderingCheckpoints proves the ORDER BY owner stops at
// its caller-cancellation observations: before the keys, inside the ordering,
// and while the result rows are copied back. It never reports success once the
// caller's cancellation is observed.
func TestNor20MaterializedOrderingCheckpoints(t *testing.T) {
	exec := nor20NewExecutor(t, storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20-ordering"))
	terms := parseOrderByTerms(t.Context(), "ORDER BY rank")

	rows, scopes := nor20OrderingInput()
	dry := newNor20CheckpointContext(nor20OrderingFailureSlot(), math.MaxInt64)
	require.True(t, exec.orderPipelineRowsWithScopes(dry, rows, scopes, terms))
	for index, row := range rows {
		require.Equal(t, int64(index+1), row["rank"], "the healthy ordering is exact")
	}
	total := dry.checks.Load()
	t.Logf("caller observations for %d keys: %d", len(rows), total)
	require.Greater(t, total, int64(len(rows)), "the ordering observes its caller more than once per key")

	checkpoint := func(t *testing.T, cancelAt int64, wantUntouched bool) {
		t.Helper()
		rows, scopes := nor20OrderingInput()
		input := append([]pipelineRow(nil), rows...)
		ctx := newNor20CheckpointContext(nor20OrderingFailureSlot(), cancelAt)
		require.False(t, exec.orderPipelineRowsWithScopes(ctx, rows, scopes, terms), "cancel at check %d must not report success", cancelAt)
		require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled, "cancel at check %d must record the caller's error", cancelAt)
		require.GreaterOrEqual(t, ctx.checks.Load(), cancelAt+1, "check %d was observed", cancelAt)
		if wantUntouched {
			require.Equal(t, input, rows, "cancel at check %d must not reorder partially", cancelAt)
		}
	}

	checkpoint(t, 0, true)
	checkpoint(t, int64(len(rows)), true)
	checkpoint(t, int64(len(rows))+2, true)
	// The last observations are the result copy's: the copy stops part way
	// through it, never claiming a completed reordering.
	for cancelAt := total - 3; cancelAt < total; cancelAt++ {
		rows, scopes := nor20OrderingInput()
		input := append([]pipelineRow(nil), rows...)
		ctx := newNor20CheckpointContext(nor20OrderingFailureSlot(), cancelAt)
		require.False(t, exec.orderPipelineRowsWithScopes(ctx, rows, scopes, terms), "cancel at check %d must not report success", cancelAt)
		require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled, "cancel at check %d must record the caller's error", cancelAt)
		require.NotEqual(t, input, rows, "cancel at check %d stopped the result copy part way", cancelAt)
	}
}

// TestNor20MaterializedOrderingStableTies proves the context-aware stable
// ordering keeps the library's exact result: the terms are ordered, equal keys
// keep their input order, and every value survives.
func TestNor20MaterializedOrderingStableTies(t *testing.T) {
	exec := nor20NewExecutor(t, storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20-stable"))
	for _, clause := range []string{"ORDER BY tie1, tie2 DESC", "ORDER BY tie2, rank DESC"} {
		terms := parseOrderByTerms(t.Context(), clause)
		const size = 997
		rows := make([]pipelineRow, size)
		scopes := make([]pipelineRow, size)
		seen := make(map[int64]bool, size)
		for index := range rows {
			row := pipelineRow{
				"orig":   int64(index),
				"tie1":   int64((index * 7) % 5),
				"tie2":   int64(index % 3),
				"rank":   int64((index * 13) % 11),
				"values": nor20Payload(index),
			}
			rows[index] = row
			scopes[index] = row
		}

		require.True(t, exec.orderPipelineRowsWithScopes(context.Background(), rows, scopes, terms), clause)
		for index, row := range rows {
			seen[row["orig"].(int64)] = true
			require.Equal(t, nor20Payload(int(row["orig"].(int64))), row["values"], "value survives")
			if index == 0 {
				continue
			}
			require.LessOrEqual(t, nor20CompareTerms(terms, rows[index-1], row), 0, "%s: terms are ordered", clause)
			sameKeys := true
			for _, term := range terms {
				if compareValuesForSort(rows[index-1][term.column], row[term.column]) != 0 {
					sameKeys = false
					break
				}
			}
			if sameKeys {
				require.Less(t, rows[index-1]["orig"].(int64), row["orig"].(int64), "%s: equal keys keep their input order", clause)
			}
		}
		require.Len(t, seen, size, "every row survives")
	}
}

// nor20CompareTerms is the ORDER BY comparison the returned rows must respect:
// negative, zero or positive as the first differing term orders left against
// right.
func nor20CompareTerms(terms []orderByTerm, left, right pipelineRow) int {
	for _, term := range terms {
		comparison := compareValuesForSort(left[term.column], right[term.column])
		if comparison == 0 {
			continue
		}
		if term.descending {
			return -comparison
		}
		return comparison
	}
	return 0
}

// nor20ChargeInput is a deterministic reverse-ordered input of the ordering
// owner: ranks descend, so the owner has to reorder every row.
func nor20ChargeInput(size int) ([]pipelineRow, []pipelineRow) {
	rows := make([]pipelineRow, size)
	scopes := make([]pipelineRow, size)
	for index := range rows {
		row := pipelineRow{
			"n.tie1":  int64(0),
			"n.tie2":  int64(0),
			"n.tie3":  int64(0),
			"n.rank":  int64(size - 1 - index),
			"payload": nor20Payload(index),
		}
		rows[index] = row
		scopes[index] = row
	}
	return rows, scopes
}

// TestNor20MaterializedOrderingMemoryCharge proves the ordering charges the
// statement for its keys and for no second buffer of keyed entries: a limit of
// exactly the key estimate admits the ordering, one byte less fails it, so no
// n-sized scratch hides inside the keys' charge.
func TestNor20MaterializedOrderingMemoryCharge(t *testing.T) {
	resetTransactionMemory(t)
	exec := nor20NewExecutor(t, storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor20-charge"))
	terms := parseOrderByTerms(t.Context(), "ORDER BY n.tie1, n.tie2, n.tie3, n.rank")
	const size = 997
	// Each keyed entry owns both a slice header and its own row reference.
	entry := struct {
		values []interface{}
		row    pipelineRow
	}{}
	keyBytes := int64(size) * (int64(unsafe.Sizeof(entry)) + int64(len(terms))*valueSlotBytes)

	run := func(t *testing.T, limit int64) (bool, []pipelineRow) {
		t.Helper()
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
		tracker := &transactionMemoryTracker{
			limit:         limit,
			databaseLimit: limit,
			globalLimit:   limit,
			database:      &databaseMemory{},
		}
		// Nothing is reserved from the shared pools, so the tracker's used
		// bytes are exactly what the statement charged.
		tracker.reserved.Store(math.MaxInt64)
		ctx := withExpressionFailureSlot(context.WithValue(context.Background(), memoryScopeKey{}, &memoryScope{tracker: tracker}))
		rows, scopes := nor20ChargeInput(size)
		ok := exec.orderPipelineRowsWithScopes(ctx, rows, scopes, terms)
		return ok, rows
	}

	ok, rows := run(t, keyBytes)
	require.True(t, ok, "the keys' own charge admits the ordering")
	for index := 1; index < len(rows); index++ {
		require.Less(t, rows[index-1]["n.rank"].(int64), rows[index]["n.rank"].(int64), "the ordering is exact")
	}

	ok, _ = run(t, keyBytes-1)
	require.False(t, ok, "one byte less than the keys' charge fails the ordering")
}

// TestNor20InPlaceStableSortMatchesLibrary proves the in-place sort is the
// library's stable sort: deterministic layouts with heavy ties, at and around
// the insertion block size, produce exactly sort.SliceStable's order.
func TestNor20InPlaceStableSortMatchesLibrary(t *testing.T) {
	for _, size := range []int{0, 1, 2, 19, 20, 21, 23, 40, 41, 97, 997, 4096} {
		for _, layout := range []string{"random", "reverse", "ties"} {
			keys := make([]int64, size)
			state := uint64(1)
			for index := range keys {
				state = state*6364136223846793005 + 1442695040888963407
				switch layout {
				case "random":
					keys[index] = int64(state >> 33)
				case "reverse":
					keys[index] = int64(size - index)
				case "ties":
					keys[index] = int64(index % 7)
				}
			}
			want := make([]int, size)
			for index := range want {
				want[index] = index
			}
			sort.SliceStable(want, func(left, right int) bool { return keys[want[left]] < keys[want[right]] })

			got := make([]int, size)
			for index := range got {
				got[index] = index
			}
			sorter := inPlaceStableSort{
				less:   func(left, right int) bool { return keys[got[left]] < keys[got[right]] },
				swap:   func(left, right int) { got[left], got[right] = got[right], got[left] },
				check:  func() bool { return true },
				budget: pipelineMaterializedCancelCheck,
			}
			require.True(t, sorter.stable(size), "%s/%d completed", layout, size)
			require.Equal(t, want, got, "%s/%d matches the library's stable order", layout, size)
		}
	}
}

// TestNor20InPlaceStableSortStopsAtCaller proves the sort observes its caller
// at bounded intervals: once the check reports the caller stopped, no further
// comparison or move runs and stable reports false.
func TestNor20InPlaceStableSortStopsAtCaller(t *testing.T) {
	const size = 4096
	entries := make([]int, size)
	keys := make([]int64, size)
	state := uint64(1)
	for index := range entries {
		state = state*6364136223846793005 + 1442695040888963407
		entries[index] = index
		keys[index] = int64(state >> 33)
	}
	steps := 0
	checks := 0
	stopped := false
	sorter := inPlaceStableSort{
		less: func(left, right int) bool {
			require.False(t, stopped, "no comparison after the caller stopped the sort")
			return keys[entries[left]] < keys[entries[right]]
		},
		swap: func(left, right int) {
			require.False(t, stopped, "no move after the caller stopped the sort")
			steps++
			entries[left], entries[right] = entries[right], entries[left]
		},
		check: func() bool {
			checks++
			stopped = checks == 3
			return !stopped
		},
		budget: pipelineMaterializedCancelCheck,
	}
	require.False(t, sorter.stable(size), "a stopped sort must not report success")
	require.Equal(t, 3, checks, "the sort asks its caller once per budget")
	require.LessOrEqual(t, steps, 2*pipelineMaterializedCancelCheck, "the sort stops within the budget it was given")
}
