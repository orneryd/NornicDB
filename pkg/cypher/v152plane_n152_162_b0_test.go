package cypher

// NOR-75 defensive resource-cancellation: the served (indexed) relationship
// vector population owner.
//
// db.index.vector.queryRelationships has two population owners:
//
//   - storage's exact fallback scan, which procedure_scan_cancellation_test.go
//     already holds to an exact delivered prefix, a real CancelFunc and a real
//     deadline on both parsers; and
//   - the search service's indexed relationship-vector scoring,
//     search.(*Service).VectorQueryRelationships, which the procedure calls once
//     a schema relationship index is ONLINE and the service is ready. That owner
//     builds its candidate population from the service's in-memory relationship
//     vectors and polls the caller's context once per candidate while it ranks
//     them.
//
// Until now no test observed that served path, so a regression that dropped the
// per-candidate poll would only surface after the whole population had been
// scored, i.e. after the caller had already left.
//
// These cases drive the public CALL through StorageExecutor.Execute on both
// parsers against a real search service and a large deterministic population:
//
//   - the healthy control returns exactly the rows and score bits of storage's
//     own fallback over the same fixture, and the served statement streams no
//     edges. Zero edge streams is what identifies the indexed owner: had the
//     service not answered, the procedure would have taken the fallback and
//     streamed this population.
//   - the same statement is canceled, and separately given a deadline, while it
//     is working. The release budget is a quarter of a warm healthy run measured
//     immediately before on the same fixture. A query profile observer records
//     the instant the served owner was entered
//     (queryprofile "RelationshipVectorIndexQuery", written only by
//     search.(*Service).VectorQueryRelationships), so each ended case proves the
//     cancellation reached that owner and was not answered by the statement
//     gate. The response bound is measured from the actual cancel()/deadline
//     instant to Execute's return.
//
// The served owner's ranking loop reads in-memory vectors and exposes no
// storage or callback boundary at which a cancellation can be released at one
// exact arrival, unlike the fallback scan's edge stream. The mid-work point is
// therefore established by the warm healthy run being longer than the released
// budget and by the served owner having been entered before the release, not by
// an exact arrival. The healthy run hydrates exactly the requested neighbours;
// neither ended case hydrates any, which is the evidence the scoring pass did
// not run to completion and publish hits.
//
// Current-release behaviour this file measures (verified on this checkout): the
// caller's ending does reach the served owner's context. The observer below
// sees the caller's deadline on the very context
// search.(*Service).VectorQueryRelationships receives, and that context carries
// the running statement's own context, not a detached copy. What is not
// reliable is that owner's stop check. search.(*Service).beginRead re-wraps the
// statement context with context.WithCancel over the statement's lazily relayed
// Done channel (pkg/cypher's statementContext, the running statement's context
// in this checkout), and the served ranking loop polls that derived channel
// once per candidate. In repeated identical measurement the derived channel
// sometimes did not close with the caller: 6 of 12 deadline attempts scored the
// whole 60000-relationship population, hydrated all 30 hits 120-150ms after the
// deadline, and the statement then returned a bare context.DeadlineExceeded
// (no procedure wrapper) and discarded that completed work. Explicit CancelFunc
// runs showed the same non-close. When the derived channel did close, the same
// statement answered the procedure-wrapped deadline in 0.3-13ms with no
// hydration. Every attempt is therefore required to satisfy the contract below;
// the assertions are not relaxed to the current behaviour, so a run that
// reproduces the non-close reports the response it actually produced.
//
// gds.louvain.stream is not a registered procedure in this build; community
// detection is served only by apoc.algo.louvain, and
// apoc_algorithm_cancel_test.go already holds its cancellation. This file adds
// no procedure and no GDS case. gds.graph.project cancellation is already
// covered by gds_projection_cancellation_test.go.
//
// The preserved fixture names TestProcedureOutputPopulationsExecuteCancellation
// and TestRelationshipSearchOutputPopulationExecuteCancellation are not present
// on this checkout; their coverage is the current equivalent
// TestProcedureScanCancellationStopsWork, the TestGDSGraphProject* cases and this
// file.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// indexedRelationVectorIndex is the schema relationship vector index the
	// served procedure reads.
	indexedRelationVectorIndex = "rel_embeddings"
	// indexedRelationVectorPopulation is the deterministic relationship
	// population. It is large enough that the served scoring pass, not the
	// statement's setup, is the bulk of the healthy run.
	indexedRelationVectorPopulation = 60000
	// indexedRelationVectorLimit is the procedure's neighbour count.
	indexedRelationVectorLimit = 30
	// indexedRelationVectorType and indexedRelationVectorProperty are the
	// relationship type and property the index covers.
	indexedRelationVectorType     = "LINKS"
	indexedRelationVectorProperty = "emb"
	// indexedRelationVectorStopBound is the recorded cancellation-response
	// bound: a canceled or expired served population must stop within this
	// long after the caller actually ended it.
	indexedRelationVectorStopBound = 100 * time.Millisecond
	// indexedRelationVectorAttempts is how many separate ended served calls
	// each ending kind runs per parser. One attempt can satisfy the contract on
	// the current release even when the served owner was not stopped (see the
	// file comment), so every attempt is measured and reported.
	indexedRelationVectorAttempts = 5
	// indexedRelationVectorOwnerRead is the query profile kind written only by
	// search.(*Service).VectorQueryRelationships when it starts its served pass.
	indexedRelationVectorOwnerRead = "RelationshipVectorIndexQuery"
)

// indexedRelationVectorQuery is the public statement under test.
var indexedRelationVectorQuery = fmt.Sprintf(
	"CALL db.index.vector.queryRelationships('%s', %d, [1, 0])",
	indexedRelationVectorIndex, indexedRelationVectorLimit,
)

// indexedRelationVectorServability is the storage the served procedure reads
// through. StreamEdges is the fallback scan's entry point, so a served call
// that never enters it is the evidence the indexed owner answered. GetEdge is
// the served owner's hit hydration: a canceled pass must not reach it.
type indexedRelationVectorServability struct {
	*storage.NamespacedEngine
	streams       atomic.Int64
	getEdges      atomic.Int64
	lastHydration atomic.Int64
}

func (probe *indexedRelationVectorServability) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	probe.streams.Add(1)
	return probe.NamespacedEngine.StreamEdges(ctx, visit)
}

func (probe *indexedRelationVectorServability) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	probe.getEdges.Add(1)
	probe.lastHydration.Store(time.Now().UnixNano())
	return probe.NamespacedEngine.GetEdge(ctx, id)
}

// indexedRelationVectorEntry is the query profile observer the ended cases
// attach to their request context. It records the instant the served owner is
// entered, which the statement gate cannot supply, and delegates everything
// else to counters the statement does not read.
type indexedRelationVectorEntry struct {
	mu       sync.Mutex
	at       time.Time
	calls    int
	counters map[string]*queryprofile.ReadCounter
}

func (observer *indexedRelationVectorEntry) WithLifetime(lifetime context.Context, _ context.Context) context.Context {
	return queryprofile.WithObserver(lifetime, observer)
}

func (observer *indexedRelationVectorEntry) UnobservedRead(context.Context, string, string) {}

func (observer *indexedRelationVectorEntry) Access(_ context.Context, kind, detail string) *queryprofile.ReadCounter {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if kind == indexedRelationVectorOwnerRead {
		observer.calls++
		if observer.at.IsZero() {
			observer.at = time.Now()
		}
	}
	if observer.counters == nil {
		observer.counters = make(map[string]*queryprofile.ReadCounter)
	}
	key := kind + "\x00" + detail
	counter := observer.counters[key]
	if counter == nil {
		counter = &queryprofile.ReadCounter{}
		observer.counters[key] = counter
	}
	return counter
}

// entered reports the first served-owner entry instant and how many times the
// owner was entered.
func (observer *indexedRelationVectorEntry) entered() (time.Time, int) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.at, observer.calls
}

// indexedRelationVectorFixture stores the deterministic relationship
// population, publishes the schema relationship vector index, and builds the
// ready search service the procedure serves through.
func indexedRelationVectorFixture(t *testing.T) (*storage.NamespacedEngine, *search.Service) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor75-indexed-vector")
	for _, id := range []string{"anchor-a", "anchor-b"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:     storage.NodeID(id),
			Labels: []string{"Anchor"},
		})
		require.NoError(t, err)
	}
	for index := 0; index < indexedRelationVectorPopulation; index++ {
		x := float32(index+1) / float32(indexedRelationVectorPopulation+1)
		err := store.CreateEdge(t.Context(), &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("rel-%06d", index)),
			StartNode: storage.NodeID("anchor-a"),
			EndNode:   storage.NodeID("anchor-b"),
			Type:      indexedRelationVectorType,
			Properties: map[string]interface{}{
				indexedRelationVectorProperty: []float32{1, x},
			},
		})
		require.NoError(t, err)
	}
	require.NoError(t, store.GetSchema().AddVectorIndex(
		indexedRelationVectorIndex,
		indexedRelationVectorType,
		indexedRelationVectorProperty,
		2,
		"cosine",
		storage.ConstraintEntityRelationship,
	))

	service := search.NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	// BM25 is not part of the population under test; keep the build to the
	// relationship vectors the served query reads.
	service.SetIndexFlags(false, true)
	require.NoError(t, service.BuildIndexes(t.Context()))
	require.True(t, service.IsReady(), "the served query needs a ready service")
	require.True(t, service.HasRelationshipVectorEntries(indexedRelationVectorType, indexedRelationVectorProperty),
		"the build must index the relationship vectors the procedure reads")
	return store, service
}

// indexedRelationVectorServedExecutor wires the probe storage and the ready
// service the served procedure reads, with no result cache so every statement
// reaches its own population owner.
func indexedRelationVectorServedExecutor(store *storage.NamespacedEngine, service *search.Service, probe *indexedRelationVectorServability) *StorageExecutor {
	exec := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	exec.SetSearchService(service)
	return exec
}

// indexedRelationVectorScores indexes a relationship-vector result by stored
// edge id and holds its scores, so the served answer and the storage fallback
// answer can be compared independently of row order.
func indexedRelationVectorScores(t *testing.T, rows [][]interface{}) map[string]float64 {
	t.Helper()
	scores := make(map[string]float64, len(rows))
	for _, row := range rows {
		require.Len(t, row, 2)
		edge, ok := row[0].(*storage.Edge)
		require.True(t, ok, "relationship row entity is %T", row[0])
		score, ok := row[1].(float64)
		require.True(t, ok, "relationship row score is %T", row[1])
		scores[string(edge.ID)] = score
	}
	return scores
}

// indexedRelationVectorHealthy runs the uncanceled statement on the served
// executor and on the same storage without a search service, requires the same
// exact answer from both, and requires the served call to stream no edges.
// It returns how long the served call ran and how many hits it hydrated.
func indexedRelationVectorHealthy(t *testing.T, store *storage.NamespacedEngine, service *search.Service, probe *indexedRelationVectorServability) (time.Duration, int64) {
	t.Helper()
	fallback := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	want, err := fallback.Execute(t.Context(), indexedRelationVectorQuery, nil)
	require.NoError(t, err, "storage fallback healthy control")
	require.Equal(t, []string{"relationship", "score"}, want.Columns)
	require.Len(t, want.Rows, indexedRelationVectorLimit)

	served := indexedRelationVectorServedExecutor(store, service, probe)
	before := probe.getEdges.Load()
	started := time.Now()
	got, err := served.Execute(t.Context(), indexedRelationVectorQuery, nil)
	healthy := time.Since(started)
	hydrated := probe.getEdges.Load() - before
	require.NoError(t, err, "served healthy control")
	require.Equal(t, want.Columns, got.Columns)
	require.Len(t, got.Rows, indexedRelationVectorLimit)
	require.Zero(t, probe.streams.Load(),
		"a served call must not enter the storage fallback's edge stream; zero streams is what identifies the indexed owner")
	require.Equal(t, indexedRelationVectorScores(t, want.Rows), indexedRelationVectorScores(t, got.Rows),
		"the indexed answer and the storage fallback answer must agree exactly")
	require.Equal(t, int64(indexedRelationVectorLimit), hydrated,
		"the healthy served pass hydrates exactly the requested neighbours")
	return healthy, hydrated
}

// indexedRelationVectorRun is one ended served call as it actually happened.
type indexedRelationVectorRun struct {
	started       time.Time
	total         time.Duration
	afterEnd      time.Duration
	endedAt       time.Time
	released      bool
	err           error
	result        *ExecuteResult
	ownerEntered  time.Time
	ownerCalls    int
	hydrated      int64
	lastHydration int64
}

// indexedRelationVectorCancelAfter runs query on a caller context that a timer
// worker cancels after delay. The worker is joined before this function
// returns, so no goroutine outlives the call, and afterEnd is measured from the
// actual cancel() to Execute's return. A statement that returned before the
// timer fired is reported as not released: it exercised the healthy path and
// must not be read as a cancellation.
func indexedRelationVectorCancelAfter(t *testing.T, exec *StorageExecutor, query string, observer *indexedRelationVectorEntry, probe *indexedRelationVectorServability, delay time.Duration) indexedRelationVectorRun {
	t.Helper()
	base := context.Background()
	if observer != nil {
		base = queryprofile.WithObserver(base, observer)
	}
	ctx, cancel := context.WithCancel(base)
	defer cancel()

	var (
		canceledAt time.Time
		fired      bool
	)
	stop, workerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(workerDone)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			fired = true
			canceledAt = time.Now()
			cancel()
		case <-stop:
		}
	}()

	before := int64(0)
	if probe != nil {
		before = probe.getEdges.Load()
	}
	started := time.Now()
	result, err := exec.Execute(ctx, query, nil)
	returnedAt := time.Now()
	close(stop)
	<-workerDone

	run := indexedRelationVectorRun{started: started, total: returnedAt.Sub(started), err: err, result: result}
	if probe != nil {
		run.hydrated = probe.getEdges.Load() - before
		run.lastHydration = probe.lastHydration.Load()
	}
	if observer != nil {
		run.ownerEntered, run.ownerCalls = observer.entered()
	}
	if fired && !canceledAt.After(returnedAt) {
		run.released = true
		run.endedAt = canceledAt
		run.afterEnd = returnedAt.Sub(canceledAt)
	}
	return run
}

// indexedRelationVectorDeadlineAfter runs query on a caller context with the
// given budget and reports the response measured from the deadline instant.
func indexedRelationVectorDeadlineAfter(t *testing.T, exec *StorageExecutor, query string, observer *indexedRelationVectorEntry, probe *indexedRelationVectorServability, budget time.Duration) indexedRelationVectorRun {
	t.Helper()
	base := context.Background()
	if observer != nil {
		base = queryprofile.WithObserver(base, observer)
	}
	ctx, cancel := context.WithTimeout(base, budget)
	defer cancel()

	before := int64(0)
	if probe != nil {
		before = probe.getEdges.Load()
	}
	started := time.Now()
	result, err := exec.Execute(ctx, query, nil)
	returnedAt := time.Now()
	expired := ctx.Err() != nil

	run := indexedRelationVectorRun{started: started, total: returnedAt.Sub(started), err: err, result: result}
	if probe != nil {
		run.hydrated = probe.getEdges.Load() - before
		run.lastHydration = probe.lastHydration.Load()
	}
	if observer != nil {
		run.ownerEntered, run.ownerCalls = observer.entered()
	}
	if expired {
		run.released = true
		run.endedAt = started.Add(budget)
		if run.endedAt.Before(returnedAt) {
			run.afterEnd = returnedAt.Sub(run.endedAt)
		}
	}
	return run
}

// indexedRelationVectorContractViolations lists everything one ended served
// call did that the contract forbids: the caller must have ended it, the served
// owner must have been entered before that instant, the statement must answer
// with the caller's cancellation and publish no rows, the indexed owner (not the
// fallback) must be stopped before it hydrates hits, and it must stop within the
// recorded response bound. Every violated clause is reported, so one run shows
// the measured population instead of only its first failure.
func indexedRelationVectorContractViolations(run indexedRelationVectorRun, want error, fallbackStreams int64, label string) []string {
	var violations []string
	add := func(format string, args ...interface{}) {
		violations = append(violations, fmt.Sprintf(format, args...))
	}
	if !run.released {
		add("%s: the caller never ended the statement before Execute returned; the run exercised the healthy path (total %s)", label, run.total)
	} else {
		if run.endedAt.IsZero() {
			add("%s: the ending instant is unknown", label)
		}
		if run.ownerCalls <= 0 {
			add("%s: the cancellation never reached the served relationship-vector owner; it was answered by the statement gate", label)
		} else if !run.ownerEntered.Before(run.endedAt) {
			add("%s: the served owner was entered only after the caller ended the statement", label)
		}
	}
	if !errors.Is(run.err, want) {
		add("%s: the statement answered %v, want %v", label, run.err, want)
	}
	if run.result != nil {
		add("%s: an ended served procedure call published %d partial rows", label, len(run.result.Rows))
	}
	if run.hydrated != 0 {
		add("%s: the ended call hydrated %d of %d hits before the outer statement returned cancellation",
			label, run.hydrated, indexedRelationVectorLimit)
		add("%s: last hydration occurred %s relative to caller end", label, time.Unix(0, run.lastHydration).Sub(run.endedAt))
	}
	if run.afterEnd > indexedRelationVectorStopBound {
		add("%s: the indexed population kept working %s after the caller ended it, instead of stopping within %s",
			label, run.afterEnd, indexedRelationVectorStopBound)
	}
	if fallbackStreams != 0 {
		add("%s: the ended call entered the storage fallback's edge stream %d times instead of stopping the indexed owner",
			label, fallbackStreams)
	}
	return violations
}

// reportIndexedRelationVectorViolations fails the test with every contract
// clause the measured population violated.
func reportIndexedRelationVectorViolations(t *testing.T, violations []string) {
	t.Helper()
	for _, violation := range violations {
		t.Errorf("%s", violation)
	}
}

// TestIndexedRelationshipVectorQueryStopsOnCancellation is the served-path
// coverage on both parsers: a healthy control, a real caller CancelFunc
// released while the population is being scored, and a real caller deadline
// that expires while it is being scored. Each ending kind is repeated so the
// measured contract rests on the population of attempts, not on an attempt
// whose served owner happened to be stopped on time.
func TestIndexedRelationshipVectorQueryStopsOnCancellation(t *testing.T) {
	store, service := indexedRelationVectorFixture(t)

	for _, parser := range []struct {
		name  string
		setup func() func()
	}{
		{"nornic", config.WithNornicParser},
		{"antlr", config.WithANTLRParser},
	} {
		parser := parser
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())

			healthyProbe := &indexedRelationVectorServability{NamespacedEngine: store}
			healthy, healthyHydrated := indexedRelationVectorHealthy(t, store, service, healthyProbe)
			t.Logf("healthy served relationship-vector query over %d relationships took %s and hydrated %d hits",
				indexedRelationVectorPopulation, healthy, healthyHydrated)

			// Release the caller a quarter of the way into the work just
			// measured. The scoring pass, not statement setup, is the bulk of
			// that budget.
			delay := healthy / 4
			require.Greater(t, delay, time.Duration(0), "the measured workload must leave room for a mid-work release")

			for attempt := 1; attempt <= indexedRelationVectorAttempts; attempt++ {
				label := fmt.Sprintf("cancel attempt %d/%d", attempt, indexedRelationVectorAttempts)
				probe := &indexedRelationVectorServability{NamespacedEngine: store}
				exec := indexedRelationVectorServedExecutor(store, service, probe)
				observer := &indexedRelationVectorEntry{}
				canceled := indexedRelationVectorCancelAfter(t, exec, indexedRelationVectorQuery, observer, probe, delay)
				t.Logf("%s released %s into the healthy run: returned after %s, %s after the actual cancellation; owner entered %s into the run (%d reads); hydrated %d; err=%v",
					label, delay, canceled.total, canceled.afterEnd, canceled.ownerEntered.Sub(canceled.started), canceled.ownerCalls, canceled.hydrated, canceled.err)
				reportIndexedRelationVectorViolations(t,
					indexedRelationVectorContractViolations(canceled, context.Canceled, probe.streams.Load(), label))
			}

			// The same statement, ended by the caller's deadline instead of an
			// explicit CancelFunc.
			for attempt := 1; attempt <= indexedRelationVectorAttempts; attempt++ {
				label := fmt.Sprintf("deadline attempt %d/%d", attempt, indexedRelationVectorAttempts)
				probe := &indexedRelationVectorServability{NamespacedEngine: store}
				exec := indexedRelationVectorServedExecutor(store, service, probe)
				observer := &indexedRelationVectorEntry{}
				expired := indexedRelationVectorDeadlineAfter(t, exec, indexedRelationVectorQuery, observer, probe, delay)
				t.Logf("%s expired %s into the healthy run: returned after %s, %s after the deadline; owner entered %s into the run (%d reads); hydrated %d; err=%v",
					label, delay, expired.total, expired.afterEnd, expired.ownerEntered.Sub(expired.started), expired.ownerCalls, expired.hydrated, expired.err)
				reportIndexedRelationVectorViolations(t,
					indexedRelationVectorContractViolations(expired, context.DeadlineExceeded, probe.streams.Load(), label))
			}

			// The uncanceled statement is unchanged after every ended call.
			againProbe := &indexedRelationVectorServability{NamespacedEngine: store}
			again, againHydrated := indexedRelationVectorHealthy(t, store, service, againProbe)
			t.Logf("healthy served relationship-vector query after all ended calls took %s and hydrated %d hits", again, againHydrated)
		})
	}
}

// TestIndexedRelationshipVectorQueryCancellationTimer is the control for the
// cancel helper: a statement that returns before its timer fires is reported as
// not released and keeps its healthy outcome, never as a cancellation that
// returned late.
func TestIndexedRelationshipVectorQueryCancellationTimer(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	run := indexedRelationVectorCancelAfter(t, exec, "RETURN 1 AS one", nil, nil, time.Hour)
	require.False(t, run.released, "a statement that returned before the timer fired was not canceled")
	require.NoError(t, run.err, "the statement's healthy outcome is reported as it happened")
	require.NotNil(t, run.result)
	require.Less(t, run.total, time.Minute, "the timer worker is joined without waiting for the pending timer")
}
