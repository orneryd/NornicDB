package cypher

// apoc_algorithm_cancel_test.go exercises caller cancellation of the private
// native algorithms behind apoc.algo.pageRank, apoc.algo.betweenness,
// apoc.algo.closeness, apoc.algo.labelPropagation and apoc.algo.wcc through the
// public StorageExecutor.Execute path (NOR-20).
//
// The cases per procedure are:
//   - caller cancellation fired from inside the procedure's own adjacency scan
//     (an independent CancelFunc this fixture owns): the remaining adjacency
//     reads, CPU work, aggregation and output must not run, no rows are
//     published, and the caller's context.Canceled comes back unchanged;
//   - a real caller deadline that expires inside the pass, held at one of the
//     procedure's own storage callbacks inside a testing/synctest bubble: the
//     same requirement, with context.DeadlineExceeded. The deadline is never
//     derived from a measured duration, so the case does not depend on how fast
//     this machine is;
//   - pageRank's CPU iteration phase, where no adjacency read is left to count:
//     an isolated child process ranks a prohibitively large iteration budget and
//     its caller cancels once the adjacency setup is complete. A pass that
//     observed the caller stops in milliseconds; one that ignored it would
//     iterate for hours, so the parent's watchdog bounds the case instead of a
//     comparison against a measured pass duration;
//   - healthy controls on the same statements: exact rows, scores and
//     communities, so the checkpoints are proven not to change live results.

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"os/exec"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// algorithmCancelPassNodes matches the procedure census population: every
	// whole-graph algorithm reads adjacency for all of these nodes.
	algorithmCancelPassNodes = 256
	// algorithmIterationNodes keeps the CPU-iteration cases' adjacency pass
	// short, so the case is about the iterations rather than the pass.
	algorithmIterationNodes = 128
	// algorithmCancelTriggerAt is the occurrence of the trigger read that fires
	// the caller's CancelFunc. Both parsers and every procedure reach it well
	// inside the pass.
	algorithmCancelTriggerAt = 8
	// algorithmDeadlineNodes is the deadline cases' population.
	algorithmDeadlineNodes = 512
	// algorithmDeadline is the caller's deadline in the deterministic cases. It
	// is a real context deadline on the bubble's clock: the pass cannot reach it
	// before its callback holds the deadline, so no case depends on how long the
	// work takes.
	algorithmDeadline = 100 * time.Millisecond
	// algorithmCPUIterations is a pageRank iteration budget no process could
	// finish. The case is whether the pass observes its caller, not whether the
	// budget completes, so the budget is far beyond the parent's watchdog.
	algorithmCPUIterations = 1_000_000_000
	// algorithmCPUWatchdog bounds the isolated child process. A pass that
	// ignored the caller's CancelFunc cannot finish this budget in hours, so the
	// watchdog distinguishes stopping from iterating without comparing against a
	// measured pass duration.
	algorithmCPUWatchdog = 30 * time.Second
	// algorithmCPUChildEnv marks the isolated child process of the CPU case.
	algorithmCPUChildEnv = "NORNICDB_APOC_ALGORITHM_ITERATION_CHILD"
	// algorithmIterationStopMarker is printed by the child once it has proven
	// that the canceled iteration pass stopped.
	algorithmIterationStopMarker = "apoc.algo.pageRank retired the canceled iteration pass"
)

// apocAlgorithmParser runs a case on the parser the statement is compiled with.
type apocAlgorithmParser struct {
	name  string
	setup func() func()
}

func apocAlgorithmParsers() []apocAlgorithmParser {
	return []apocAlgorithmParser{
		{"nornic", config.WithNornicParser},
		{"antlr", config.WithANTLRParser},
	}
}

// apocAlgorithmCase is one public statement and the adjacency read whose
// occurrence fires the caller's CancelFunc or holds the caller's deadline.
//
// The trigger read is the second read of a node's adjacency pair (incoming), so
// the pair already in flight completes and the next loop checkpoint stops the
// pass: no read starts after the cancellation.
type apocAlgorithmCase struct {
	name        string
	query       string
	triggerRead string
}

func apocAlgorithmCases() []apocAlgorithmCase {
	return []apocAlgorithmCase{
		{
			name:        "louvain",
			query:       "CALL apoc.algo.louvain(['P']) YIELD node, community RETURN node.id AS id, community",
			triggerRead: "GetOutgoingEdges",
		},
		{
			name:        "pageRank",
			query:       "CALL apoc.algo.pageRank(['P']) YIELD node, score RETURN node.id AS id, score",
			triggerRead: "GetOutgoingEdges",
		},
		{
			name:        "betweenness",
			query:       "CALL apoc.algo.betweenness(['P']) YIELD node, score RETURN node.id AS id, score",
			triggerRead: "GetOutgoingEdges",
		},
		{
			name:        "closeness",
			query:       "CALL apoc.algo.closeness(['P']) YIELD node, score RETURN node.id AS id, score",
			triggerRead: "GetIncomingEdges",
		},
		{
			name:        "labelPropagation",
			query:       "CALL apoc.algo.labelPropagation(['P']) YIELD node, community RETURN node.id AS id, community",
			triggerRead: "GetIncomingEdges",
		},
		{
			name:        "wcc",
			query:       "CALL apoc.algo.wcc(['P']) YIELD node, componentId RETURN node.id AS id, componentId",
			triggerRead: "GetIncomingEdges",
		},
	}
}

// apocAlgorithmDeadlineCases are the deadline statements. pageRank's deadline
// case ranks 200 iterations, so its deadline can be held at the last adjacency
// read and the iteration budget starts after the caller's context is done.
func apocAlgorithmDeadlineCases() []apocAlgorithmCase {
	cases := apocAlgorithmCases()
	for i := range cases {
		if cases[i].name == "pageRank" {
			cases[i].query = "CALL apoc.algo.pageRank(['P'], {iterations: 200}) YIELD node, score RETURN node.id AS id, score"
		}
	}
	return cases
}

// apocAlgorithmDeadlineCase finds a deadline case by procedure name.
func apocAlgorithmDeadlineCase(t *testing.T, name string) apocAlgorithmCase {
	t.Helper()
	for _, algorithm := range apocAlgorithmDeadlineCases() {
		if algorithm.name == name {
			return algorithm
		}
	}
	t.Fatalf("no deadline case named %s", name)
	return apocAlgorithmCase{}
}

// algorithmCancelProbe counts every adjacency/population read the executor makes
// through the storage it was built on, fires the caller's own CancelFunc
// immediately before a chosen read, holds a caller's deadline at a chosen read,
// and counts the reads that start while the caller's context is already done.
type algorithmCancelProbe struct {
	mu sync.Mutex

	cancel      context.CancelFunc
	triggerRead string
	triggerAt   int
	triggered   bool

	held      context.Context
	heldRead  string
	heldAt    int
	heldOnce  bool
	observed  context.Context
	calls     int
	postCalls int
	doneCalls int
	byRead    map[string]int
}

func newAlgorithmCancelProbe() *algorithmCancelProbe {
	return &algorithmCancelProbe{byRead: map[string]int{}}
}

// reset clears the counters earlier statements produced, so a measurement covers
// only the statement under test.
func (p *algorithmCancelProbe) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancel = nil
	p.triggerRead = ""
	p.triggerAt = 0
	p.triggered = false
	p.held = nil
	p.heldRead = ""
	p.heldAt = 0
	p.heldOnce = false
	p.observed = nil
	p.calls = 0
	p.postCalls = 0
	p.doneCalls = 0
	p.byRead = map[string]int{}
}

// armCancel makes the probe fire cancel immediately before the nth read named
// read, then count the reads that still follow it.
func (p *algorithmCancelProbe) armCancel(cancel context.CancelFunc, read string, nth int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancel = cancel
	p.triggerRead = read
	p.triggerAt = nth
}

// armDeadline makes the probe hold the nth read named read until wait is done.
// The read starts, the caller's deadline expires at the procedure's own storage
// callback, and every later read is counted as starting after the deadline.
func (p *algorithmCancelProbe) armDeadline(wait context.Context, read string, nth int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observed = wait
	p.held = wait
	p.heldRead = read
	p.heldAt = nth
}

// observe counts one read. It runs before the underlying read, so a trigger
// cancels the caller's statement before the work that read serves.
func (p *algorithmCancelProbe) observe(read string) {
	p.mu.Lock()
	p.calls++
	p.byRead[read]++
	if p.observed != nil && p.observed.Err() != nil {
		p.doneCalls++
	}
	var hold context.Context
	fire := false
	switch {
	case p.held != nil && !p.heldOnce && read == p.heldRead && p.byRead[read] >= p.heldAt:
		p.heldOnce = true
		hold = p.held
	case !p.triggered && p.triggerRead != "" && read == p.triggerRead && p.byRead[read] >= p.triggerAt:
		p.triggered = true
		fire = p.cancel != nil
	case p.triggered:
		p.postCalls++
	}
	p.mu.Unlock()
	if hold != nil {
		<-hold.Done()
	}
	if fire {
		p.cancel()
	}
}

// algorithmCancelSnapshot is a consistent copy of the probe's counters.
type algorithmCancelSnapshot struct {
	Calls      int
	PostCalls  int
	DoneCalls  int
	Triggered  bool
	Held       bool
	ByReadName map[string]int
}

func (p *algorithmCancelProbe) snapshot() algorithmCancelSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := algorithmCancelSnapshot{
		Calls:      p.calls,
		PostCalls:  p.postCalls,
		DoneCalls:  p.doneCalls,
		Triggered:  p.triggered,
		Held:       p.heldOnce,
		ByReadName: make(map[string]int, len(p.byRead)),
	}
	for name, count := range p.byRead {
		snapshot.ByReadName[name] = count
	}
	return snapshot
}

// algorithmCancelStorage is the storage the executor reads through. It embeds
// the concrete in-memory engine, so every other storage capability the executor
// uses stays visible through the wrapper, and counts only the reads the
// whole-graph algorithms perform.
type algorithmCancelStorage struct {
	*storage.MemoryEngine
	probe *algorithmCancelProbe
}

func (s *algorithmCancelStorage) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	s.probe.observe("GetNode")
	return s.MemoryEngine.GetNode(ctx, id)
}

func (s *algorithmCancelStorage) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	s.probe.observe("GetEdge")
	return s.MemoryEngine.GetEdge(ctx, id)
}

func (s *algorithmCancelStorage) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	s.probe.observe("GetNodesByLabel")
	return s.MemoryEngine.GetNodesByLabel(ctx, label)
}

func (s *algorithmCancelStorage) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	s.probe.observe("GetFirstNodeByLabel")
	return s.MemoryEngine.GetFirstNodeByLabel(ctx, label)
}

func (s *algorithmCancelStorage) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	s.probe.observe("GetOutgoingEdges")
	return s.MemoryEngine.GetOutgoingEdges(ctx, id)
}

func (s *algorithmCancelStorage) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	s.probe.observe("GetIncomingEdges")
	return s.MemoryEngine.GetIncomingEdges(ctx, id)
}

func (s *algorithmCancelStorage) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	s.probe.observe("AllNodes")
	return s.MemoryEngine.AllNodes(ctx)
}

func (s *algorithmCancelStorage) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	s.probe.observe("AllEdges")
	return s.MemoryEngine.AllEdges(ctx)
}

// algorithmCancelFixture is the executor under test plus its instrument.
type algorithmCancelFixture struct {
	exec   *StorageExecutor
	engine storage.Engine
	probe  *algorithmCancelProbe
}

func newAlgorithmCancelFixture(tb testing.TB, nodes int) algorithmCancelFixture {
	tb.Helper()
	probe := newAlgorithmCancelProbe()
	engine := storage.NewNamespacedEngine(&algorithmCancelStorage{MemoryEngine: newTestMemoryEngine(tb), probe: probe}, "algo_cancel")
	fixture := algorithmCancelFixture{
		exec:   NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0, TTL: 0}),
		engine: engine,
		probe:  probe,
	}
	if nodes > 0 {
		fixture.seedRing(tb, nodes)
	}
	probe.reset()
	return fixture
}

// algorithmClockFixture is a fixture whose engine lives and dies inside one
// synctest bubble. The clock cases cannot use the shared auto-closed fixture: a
// storage engine's in-flight counters belong to the goroutines that use them, and
// Go's WaitGroup cannot count from inside and outside a bubble at once, so the
// case opens, seeds, warms up and closes the engine in the bubble it ranks in.
// Fake time only advances when every goroutine of the bubble is blocked, so the
// warmup and the statement's compilation cost the deadline nothing.
type algorithmClockFixture struct {
	algorithmCancelFixture
	store *storage.MemoryEngine
	once  sync.Once
	err   error
}

// newAlgorithmCancelClockFixture builds a deadline case's fixture; its caller
// closes the store before the bubble exits.
func newAlgorithmCancelClockFixture(t *testing.T, nodes int) *algorithmClockFixture {
	t.Helper()
	probe := newAlgorithmCancelProbe()
	store := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	engine := storage.NewNamespacedEngine(&algorithmCancelStorage{MemoryEngine: store, probe: probe}, "algo_cancel")
	fixture := &algorithmClockFixture{
		algorithmCancelFixture: algorithmCancelFixture{
			exec:   NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0, TTL: 0}),
			engine: engine,
			probe:  probe,
		},
		// A namespaced engine never closes its inner engine, so the case owns the
		// store it created.
		store: store,
	}
	if nodes > 0 {
		fixture.seedRing(t, nodes)
	}
	probe.reset()
	return fixture
}

// close releases the fixture's store once.
func (f *algorithmClockFixture) close() error {
	f.once.Do(func() { f.err = f.store.Close() })
	return f.err
}

// seedRing creates a ring of P nodes over typed K relationships with a chord
// every eighth node, so every whole-graph algorithm reads adjacency for the
// whole population.
func (f algorithmCancelFixture) seedRing(tb testing.TB, nodes int) {
	tb.Helper()
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("p-%03d", i)
		_, err := f.engine.CreateNode(tb.Context(), &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"P"},
			Properties: map[string]interface{}{"id": id},
		})
		require.NoError(tb, err, "create node %s", id)
	}
	for i := 0; i < nodes; i++ {
		next := (i + 1) % nodes
		require.NoError(tb, f.createEdge(fmt.Sprintf("k-%03d", i), fmt.Sprintf("p-%03d", i), fmt.Sprintf("p-%03d", next)))
		if i%8 == 0 {
			require.NoError(tb, f.createEdge(fmt.Sprintf("c-%03d", i), fmt.Sprintf("p-%03d", i), fmt.Sprintf("p-%03d", (i+7)%nodes)))
		}
	}
}

func (f algorithmCancelFixture) createEdge(id, start, end string) error {
	return f.engine.CreateEdge(context.Background(), &storage.Edge{
		ID:        storage.EdgeID(id),
		Type:      "K",
		StartNode: storage.NodeID(start),
		EndNode:   storage.NodeID(end),
	})
}

// seedExactGraph creates the small graph the healthy exact controls rank: the
// cycle a->b->c->a, the cycle d->e->d, a bridge a->d, and an isolated f.
func (f algorithmCancelFixture) seedExactGraph(tb testing.TB) {
	tb.Helper()
	for _, id := range []string{"n-a", "n-b", "n-c", "n-d", "n-e", "n-f"} {
		_, err := f.engine.CreateNode(tb.Context(), &storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"P"},
			Properties: map[string]interface{}{"id": id},
		})
		require.NoError(tb, err, "create node %s", id)
	}
	for i, edge := range [][2]string{{"n-a", "n-b"}, {"n-b", "n-c"}, {"n-c", "n-a"}, {"n-d", "n-e"}, {"n-e", "n-d"}, {"n-a", "n-d"}} {
		require.NoError(tb, f.createEdge(fmt.Sprintf("x-%d", i), edge[0], edge[1]))
	}
}

// algorithmStatementRows renders a statement's rows as "id=value" strings, in
// the statement's own row order, so exact live results can be compared.
func algorithmStatementRows(t *testing.T, exec *StorageExecutor, query string) []string {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	rendered := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if score, ok := row[1].(float64); ok {
			rendered = append(rendered, fmt.Sprintf("%v=%0.12f", row[0], score))
			continue
		}
		rendered = append(rendered, fmt.Sprintf("%v=%v", row[0], row[1]))
	}
	return rendered
}

// TestApocAlgorithmCallerCancellationStopsRemainingWork cancels the caller's
// context from inside each procedure's adjacency scan. The procedure must stop
// at its next checkpoint: no read may start after the cancellation, the pass
// must not run to the end of the graph, no rows may be published, and the
// caller's context.Canceled must be returned unchanged.
func TestApocAlgorithmCallerCancellationStopsRemainingWork(t *testing.T) {
	for _, parser := range apocAlgorithmParsers() {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, algorithm := range apocAlgorithmCases() {
				t.Run(algorithm.name, func(t *testing.T) {
					fixture := newAlgorithmCancelFixture(t, algorithmCancelPassNodes)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					fixture.probe.armCancel(cancel, algorithm.triggerRead, algorithmCancelTriggerAt)

					result, err := fixture.exec.Execute(ctx, algorithm.query, nil)

					snapshot := fixture.probe.snapshot()
					t.Logf("reads=%d reads_after_cancel=%d by_read=%v", snapshot.Calls, snapshot.PostCalls, snapshot.ByReadName)
					require.True(t, snapshot.Triggered, "the pass reached its trigger read")
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, result, "a canceled algorithm publishes no rows")
					require.Zero(t, snapshot.PostCalls, "adjacency reads started after the caller canceled")
					require.LessOrEqual(t, snapshot.Calls, 2*algorithmCancelTriggerAt,
						"reads performed before the cancellation stopped the pass")
				})
			}
		})
	}
}

// TestApocAlgorithmCallerDeadlineStopsRemainingWork expires a real caller
// deadline inside each procedure's pass. The deadline is held at the procedure's
// own adjacency callback, which waits on the caller's Done channel inside a
// synctest bubble, so the case is deterministic: no deadline is derived from a
// measured duration, and the pass must stop at its next checkpoint instead of
// finishing. No read may start once the caller's context is done, nothing may be
// published, and the deadline error must come back unchanged.
func TestApocAlgorithmCallerDeadlineStopsRemainingWork(t *testing.T) {
	for _, parser := range apocAlgorithmParsers() {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, algorithm := range apocAlgorithmDeadlineCases() {
				t.Run(algorithm.name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						fixture := newAlgorithmCancelClockFixture(t, algorithmDeadlineNodes)
						defer func() { require.NoError(t, fixture.close()) }()
						// The healthy pass proves the procedure reads the whole population
						// before the caller's deadline exists. It costs the bubble no time:
						// fake time advances only while every goroutine is blocked.
						require.Len(t, algorithmStatementRows(t, fixture.exec, algorithm.query), algorithmDeadlineNodes)
						fixture.probe.reset()

						ctx, cancel := context.WithTimeout(context.Background(), algorithmDeadline)
						defer cancel()
						fixture.probe.armDeadline(ctx, algorithm.triggerRead, algorithmCancelTriggerAt)

						started := time.Now()
						result, err := fixture.exec.Execute(ctx, algorithm.query, nil)
						stopped := time.Since(started)

						snapshot := fixture.probe.snapshot()
						t.Logf("deadline=%v stopped=%v reads=%d reads_after_deadline=%d by_read=%v",
							algorithmDeadline, stopped, snapshot.Calls, snapshot.DoneCalls, snapshot.ByReadName)
						require.True(t, snapshot.Held, "the pass reached the adjacency read that holds the deadline")
						require.ErrorIs(t, err, context.DeadlineExceeded)
						require.Nil(t, result, "an expired algorithm publishes no rows")
						require.Zero(t, snapshot.DoneCalls, "adjacency reads started after the caller's deadline")
						require.Equal(t, algorithmDeadline, stopped,
							"the pass ended at the caller's deadline instead of running to the end of its work")
					})
				})
			}
		})
	}
}

// TestApocAlgorithmPageRankDeadlineStopsIterations covers pageRank's CPU
// iteration phase, where no adjacency read is left to count: the caller's
// deadline is held at the pass' last adjacency read, so the iteration budget
// starts only after the caller's context is done and the first iteration
// checkpoint has to retire it.
func TestApocAlgorithmPageRankDeadlineStopsIterations(t *testing.T) {
	algorithm := apocAlgorithmDeadlineCase(t, "pageRank")
	query := algorithm.query
	for _, parser := range apocAlgorithmParsers() {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())

			synctest.Test(t, func(t *testing.T) {
				fixture := newAlgorithmCancelClockFixture(t, algorithmIterationNodes)
				defer func() { require.NoError(t, fixture.close()) }()
				require.Len(t, algorithmStatementRows(t, fixture.exec, query), algorithmIterationNodes)
				fixture.probe.reset()

				ctx, cancel := context.WithTimeout(context.Background(), algorithmDeadline)
				defer cancel()
				fixture.probe.armDeadline(ctx, "GetOutgoingEdges", algorithmIterationNodes)

				started := time.Now()
				result, err := fixture.exec.Execute(ctx, query, nil)
				stopped := time.Since(started)

				snapshot := fixture.probe.snapshot()
				t.Logf("deadline=%v stopped=%v reads=%d reads_after_deadline=%d", algorithmDeadline, stopped, snapshot.Calls, snapshot.DoneCalls)
				require.True(t, snapshot.Held, "the pass reached the last adjacency read")
				require.Equal(t, algorithmIterationNodes, snapshot.Calls, "the adjacency pass completed before the deadline")
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, result, "an expired algorithm publishes no rows")
				require.Equal(t, algorithmDeadline, stopped,
					"the iteration budget was retired at the caller's deadline instead of being run")
			})
		})
	}
}

// TestApocAlgorithmPageRankCancelStopsIterationPass covers pageRank's CPU
// iteration phase with a real CancelFunc in an isolated process: the child ranks
// a budget no process could finish and its caller cancels once the adjacency
// setup is complete. A pass that observes the caller returns in milliseconds;
// one that ignored it would iterate for hours, so the parent's watchdog bounds
// the case instead of a comparison against a measured pass duration.
func TestApocAlgorithmPageRankCancelStopsIterationPass(t *testing.T) {
	binary, err := os.Executable()
	require.NoError(t, err)
	// The child's temporary files stay inside the parent's own directory, so a
	// killed child leaves nothing behind.
	childTemp := t.TempDir()
	watchdog, stopChild := context.WithTimeout(t.Context(), algorithmCPUWatchdog)
	defer stopChild()
	command := exec.CommandContext(watchdog, binary,
		"-test.run=^TestApocAlgorithmPageRankIterationChildProcess$",
		"-test.count=1", "-test.timeout=2m", "-test.v")
	command.Env = append(os.Environ(), algorithmCPUChildEnv+"=1", "TMPDIR="+childTemp)
	// CombinedOutput starts the child, waits for it to exit and kills it when the
	// watchdog expires: the parent owns the bound and the join.
	output, childErr := command.CombinedOutput()
	require.NoError(t, watchdog.Err(),
		"the iteration pass was still running after the %v watchdog instead of stopping when its caller canceled:\n%s",
		algorithmCPUWatchdog, output)
	require.NoError(t, childErr, "the isolated pageRank child process failed:\n%s", output)
	require.Contains(t, string(output), algorithmIterationStopMarker)
}

// TestApocAlgorithmPageRankIterationChildProcess is the isolated case above: it
// runs only in the child process the parent starts.
func TestApocAlgorithmPageRankIterationChildProcess(t *testing.T) {
	if os.Getenv(algorithmCPUChildEnv) != "1" {
		t.Skip("isolated pageRank iteration case: run through TestApocAlgorithmPageRankCancelStopsIterationPass")
	}
	query := fmt.Sprintf("CALL apoc.algo.pageRank(['P'], {iterations: %d}) YIELD node, score RETURN node.id AS id, score", algorithmCPUIterations)
	fixture := newAlgorithmCancelFixture(t, algorithmIterationNodes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The caller cancels on the last adjacency read: the adjacency setup is
	// complete and only the iteration budget is left.
	fixture.probe.armCancel(cancel, "GetOutgoingEdges", algorithmIterationNodes)

	started := time.Now()
	result, err := fixture.exec.Execute(ctx, query, nil)
	stopped := time.Since(started)

	snapshot := fixture.probe.snapshot()
	t.Logf("stopped=%v reads=%d reads_after_cancel=%d", stopped, snapshot.Calls, snapshot.PostCalls)
	require.True(t, snapshot.Triggered, "the pass reached the read that fires the caller's CancelFunc")
	require.Equal(t, algorithmIterationNodes, snapshot.Calls, "the adjacency pass read every node before the cancellation")
	require.Zero(t, snapshot.PostCalls, "adjacency reads started after the caller canceled")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result, "a canceled algorithm publishes no rows")
	fmt.Println(algorithmIterationStopMarker)
}

// TestApocAlgorithmHealthyExactResults pins the live results of all six
// procedures: the checkpoints must not change a score, a community number or a
// row's position, and both parsers must return the same rows.
func TestApocAlgorithmHealthyExactResults(t *testing.T) {
	golden := map[string][]string{
		// The triangle and reciprocal pair form separate communities; the
		// isolated node remains its own community. The single bridge does
		// not collapse those densely connected groups.
		"louvain": {"n-a=0", "n-b=0", "n-c=0", "n-d=1", "n-e=1", "n-f=2"},
		"pageRank": {
			"n-a=0.108202379413",
			"n-b=0.075113427588",
			"n-c=0.092979843514",
			"n-d=0.358873058025",
			"n-e=0.335705077867",
			"n-f=0.029126213592",
		},
		"betweenness": {
			"n-a=0.500000000000",
			"n-b=0.100000000000",
			"n-c=0.300000000000",
			"n-d=0.300000000000",
			"n-e=0.000000000000",
			"n-f=0.000000000000",
		},
		"closeness": {
			"n-a=0.640000000000",
			"n-b=0.457142857143",
			"n-c=0.457142857143",
			"n-d=0.533333333333",
			"n-e=0.355555555556",
			"n-f=0.000000000000",
		},
		"labelPropagation": {
			"n-a=0",
			"n-b=0",
			"n-c=0",
			"n-d=1",
			"n-e=1",
			"n-f=2",
		},
		"wcc": {
			"n-a=0",
			"n-b=0",
			"n-c=0",
			"n-d=0",
			"n-e=0",
			"n-f=1",
		},
	}
	for _, parser := range apocAlgorithmParsers() {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, algorithm := range apocAlgorithmCases() {
				t.Run(algorithm.name, func(t *testing.T) {
					fixture := newAlgorithmCancelFixture(t, 0)
					fixture.seedExactGraph(t)
					expected, exists := golden[algorithm.name]
					require.True(t, exists, "every procedure in the cancellation population needs an explicit healthy golden")
					require.Equal(t, expected, algorithmStatementRows(t, fixture.exec, algorithm.query))
				})
			}
		})
	}
}
