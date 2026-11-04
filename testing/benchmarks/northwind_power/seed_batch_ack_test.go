package main

// AC7 acknowledgement-boundary tests for the benchmark write owner.
//
// The seeder's phase barrier claims that every batch of a phase is
// acknowledged before the next phase (or the completion timestamp) begins. With
// the pinned Neo4j Go driver (v5.28.5) that is only true once the terminal
// summary has been consumed: sessionWithContext.Run returns a lazily evaluated
// result, and sessionWithContext.Close discards a pending auto-commit result
// (autocommitTx.discard) while returning only explicit-transaction errors. A
// batch whose result is never consumed therefore loses a terminal error that
// arrives with the summary, and the seed reports success for rows the server
// rejected.
//
// These tests drive the real batchWrite / batchWriteParallel owners with the
// real seed-plan query text and row payloads (buildSeedPlan); only the
// driver/session/result boundary is faked, so no server or network is needed.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ackTestRecorder counts the driver calls the write owner makes and records the
// statements it sends. Every field is safe for the parallel workers' goroutines.
type ackTestRecorder struct {
	runs     atomic.Int64
	consumes atomic.Int64
	closes   atomic.Int64

	mu      sync.Mutex
	cyphers []string
	params  []map[string]any
}

func (r *ackTestRecorder) recordRun(cypher string, params map[string]any) {
	r.runs.Add(1)
	r.mu.Lock()
	r.cyphers = append(r.cyphers, cypher)
	r.params = append(r.params, params)
	r.mu.Unlock()
}

// recorded returns the statements sent so far in call order. Only serial tests
// assert on the order; parallel workers race for batches by design.
func (r *ackTestRecorder) recorded() ([]string, []map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.cyphers...), append([]map[string]any(nil), r.params...)
}

// ackTestSummary is a non-nil terminal summary placeholder. The write owner has
// to consume the summary to reach the acknowledgement boundary, but must not
// derive its ack decision from any summary field.
type ackTestSummary struct{ neo4j.ResultSummary }

// ackTestResult overrides only Consume. Embedding the driver interface supplies
// the rest of the (partly unexported) method set, so the fake never has to
// imitate a result stream.
type ackTestResult struct {
	neo4j.ResultWithContext
	rec        *ackTestRecorder
	consumeErr error
}

func (r *ackTestResult) Consume(context.Context) (neo4j.ResultSummary, error) {
	r.rec.consumes.Add(1)
	if r.consumeErr != nil {
		return nil, r.consumeErr
	}
	return ackTestSummary{}, nil
}

// ackTestSession overrides only Run and Close — the two session calls the write
// owner makes.
type ackTestSession struct {
	neo4j.SessionWithContext
	rec        *ackTestRecorder
	runErr     error
	consumeErr error
}

func (s *ackTestSession) Run(_ context.Context, cypher string, params map[string]any, _configurers ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	s.rec.recordRun(cypher, params)
	if s.runErr != nil {
		return nil, s.runErr
	}
	return &ackTestResult{rec: s.rec, consumeErr: s.consumeErr}, nil
}

func (s *ackTestSession) Close(context.Context) error {
	s.rec.closes.Add(1)
	return nil
}

// ackTestDriver overrides only NewSession; each worker gets its own session
// instance, mirroring the driver's "sessions are not goroutine-safe" rule.
type ackTestDriver struct {
	neo4j.DriverWithContext
	rec        *ackTestRecorder
	runErr     error
	consumeErr error

	mu       sync.Mutex
	sessions int
}

func (d *ackTestDriver) NewSession(context.Context, neo4j.SessionConfig) neo4j.SessionWithContext {
	d.mu.Lock()
	d.sessions++
	d.mu.Unlock()
	return &ackTestSession{rec: d.rec, runErr: d.runErr, consumeErr: d.consumeErr}
}

func (d *ackTestDriver) sessionCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions
}

// ackTestCategoriesPhase returns the real "categories" seed phase — the query
// text and row payloads produced by buildSeedPlan — sized to `categories` rows.
// No statement text or property shape is re-declared here, so the fixture
// cannot silently diverge from what the seeder sends in a real run.
func ackTestCategoriesPhase(t *testing.T, categories int) seedPhase {
	t.Helper()
	plan := buildSeedPlan(seedConfig{
		categories: categories,
		suppliers:  1,
		customers:  1,
		products:   1,
		seed:       42,
		label:      "ack-boundary",
	})
	for _, phase := range plan.phases {
		if phase.name == "categories" {
			return phase
		}
	}
	t.Fatalf("seed plan built with %d categories has no categories phase", categories)
	return seedPhase{}
}

// ackTestAssertStatements checks that the owner sent the fixture phase's own
// query text with the expected chunk slices of the fixture rows.
func ackTestAssertStatements(t *testing.T, rec *ackTestRecorder, phase seedPhase, want [][2]int) {
	t.Helper()
	cyphers, params := rec.recorded()
	if len(params) != len(want) {
		t.Fatalf("owner sent %d statements, want %d", len(params), len(want))
	}
	for i, bounds := range want {
		if cyphers[i] != phase.cypher {
			t.Fatalf("statement %d cypher = %q, want the phase cypher %q", i, cyphers[i], phase.cypher)
		}
		if len(params[i]) != 1 {
			t.Fatalf("statement %d sent %d parameters, want only \"rows\"", i, len(params[i]))
		}
		rows, ok := params[i]["rows"]
		if !ok {
			t.Fatalf("statement %d sent parameters %v, want a \"rows\" key", i, params[i])
		}
		if wantRows := phase.rows[bounds[0]:bounds[1]]; !reflect.DeepEqual(rows, wantRows) {
			t.Fatalf("statement %d rows payload = %v, want fixture rows %d..%d", i, rows, bounds[0], bounds[1])
		}
	}
}

// TestBatchWritePropagatesDeferredTerminalConsumeError covers the serial owner:
// the Run succeeds, the terminal summary reports a failure, and the batch must
// not be reported as written.
func TestBatchWritePropagatesDeferredTerminalConsumeError(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 2)
	deferred := errors.New("Neo.ClientError.Transaction.Terminal: statement failed after the run was accepted")
	rec := &ackTestRecorder{}
	session := &ackTestSession{rec: rec, consumeErr: deferred}

	err := batchWrite(context.Background(), session, phase.rows, 2, phase.cypher)
	if !errors.Is(err, deferred) {
		t.Fatalf("batchWrite() error = %v, want the deferred terminal error %v", err, deferred)
	}
	if got := rec.runs.Load(); got != 1 {
		t.Fatalf("batchWrite() issued %d statements for 1 batch, want 1", got)
	}
	if got := rec.consumes.Load(); got != 1 {
		t.Fatalf("batchWrite() consumed %d terminal results for 1 batch, want 1", got)
	}
	ackTestAssertStatements(t, rec, phase, [][2]int{{0, 2}})
}

// TestBatchWriteParallelPropagatesDeferredTerminalConsumeError covers the
// parallel owner: every worker's claimed batch must reach the acknowledgement
// boundary, so the same deferred terminal error must be reported by the phase.
func TestBatchWriteParallelPropagatesDeferredTerminalConsumeError(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 4)
	deferred := errors.New("Neo.ClientError.Transaction.Terminal: statement failed after the run was accepted")
	rec := &ackTestRecorder{}
	driver := &ackTestDriver{rec: rec, consumeErr: deferred}

	err := batchWriteParallel(context.Background(), driver, "nornic", phase.rows, 1, 2, phase.cypher)
	if !errors.Is(err, deferred) {
		t.Fatalf("batchWriteParallel() error = %v, want the deferred terminal error %v", err, deferred)
	}
	if got := rec.consumes.Load(); got == 0 {
		t.Fatalf("batchWriteParallel() returned %v without consuming any terminal result", err)
	}
	// Workers stop claiming batches once the first error is recorded, so the
	// number of completed batches is not fixed; the ack checks above are what
	// must hold.
	if runs, consumes := rec.runs.Load(), rec.consumes.Load(); runs < consumes {
		t.Fatalf("owner issued %d statements but consumed %d terminal results", runs, consumes)
	}
}

// TestBatchWriteConsumesEveryBatchOnSuccess pins the successful serial path: one
// terminal summary per batch, with the phase's own cypher and row chunks.
func TestBatchWriteConsumesEveryBatchOnSuccess(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 4)
	rec := &ackTestRecorder{}
	session := &ackTestSession{rec: rec}

	if err := batchWrite(context.Background(), session, phase.rows, 2, phase.cypher); err != nil {
		t.Fatalf("batchWrite() error = %v", err)
	}
	if got := rec.runs.Load(); got != 2 {
		t.Fatalf("batchWrite() issued %d statements for 2 batches, want 2", got)
	}
	if got := rec.consumes.Load(); got != 2 {
		t.Fatalf("batchWrite() consumed %d terminal results for 2 batches, want 2", got)
	}
	ackTestAssertStatements(t, rec, phase, [][2]int{{0, 2}, {2, 4}})
}

// TestBatchWriteParallelConsumesEveryBatchOnSuccess pins the same ack count on
// the parallel path across worker sessions, and that each worker session is
// still closed when its worker exits.
func TestBatchWriteParallelConsumesEveryBatchOnSuccess(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 4)
	rec := &ackTestRecorder{}
	driver := &ackTestDriver{rec: rec}

	if err := batchWriteParallel(context.Background(), driver, "nornic", phase.rows, 1, 2, phase.cypher); err != nil {
		t.Fatalf("batchWriteParallel() error = %v", err)
	}
	if got := rec.runs.Load(); got != 4 {
		t.Fatalf("batchWriteParallel() issued %d statements for 4 batches, want 4", got)
	}
	if got := rec.consumes.Load(); got != 4 {
		t.Fatalf("batchWriteParallel() consumed %d terminal results for 4 batches, want 4", got)
	}
	if got := driver.sessionCount(); got != 2 {
		t.Fatalf("batchWriteParallel() opened %d worker sessions, want 2", got)
	}
	if got := rec.closes.Load(); got != 2 {
		t.Fatalf("batchWriteParallel() closed %d worker sessions, want 2", got)
	}
}

// TestBatchWriteRunErrorUnchanged pins that a Run-level failure is still
// reported as before: same wrapped error, no summary consumption.
func TestBatchWriteRunErrorUnchanged(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 2)
	runErr := errors.New("Neo.ClientError.Statement.SyntaxError: invalid input 'X'")

	t.Run("serial", func(t *testing.T) {
		rec := &ackTestRecorder{}
		session := &ackTestSession{rec: rec, runErr: runErr}

		err := batchWrite(context.Background(), session, phase.rows, 2, phase.cypher)
		if !errors.Is(err, runErr) {
			t.Fatalf("batchWrite() error = %v, want the Run error %v", err, runErr)
		}
		if got := rec.runs.Load(); got != 1 {
			t.Fatalf("batchWrite() issued %d statements, want 1", got)
		}
		if got := rec.consumes.Load(); got != 0 {
			t.Fatalf("batchWrite() consumed %d terminal results after a failed Run, want 0", got)
		}
	})

	t.Run("parallel", func(t *testing.T) {
		rec := &ackTestRecorder{}
		driver := &ackTestDriver{rec: rec, runErr: runErr}

		err := batchWriteParallel(context.Background(), driver, "nornic", phase.rows, 1, 2, phase.cypher)
		if !errors.Is(err, runErr) {
			t.Fatalf("batchWriteParallel() error = %v, want the Run error %v", err, runErr)
		}
		if got := rec.consumes.Load(); got != 0 {
			t.Fatalf("batchWriteParallel() consumed %d terminal results after a failed Run, want 0", got)
		}
	})
}

// TestBatchWriteDoesNotRetryTerminalErrorsAfterContextCancellation pins the
// bounded-retry contract: a retryable-looking error is attempted once when the
// context is already cancelled, whether it arrives from Run or from the
// terminal summary. No wall-clock deadline is involved.
func TestBatchWriteDoesNotRetryTerminalErrorsAfterContextCancellation(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 2)
	transient := &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.DeadlockDetected", Msg: "conflict detected"}

	t.Run("run error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := &ackTestRecorder{}
		session := &ackTestSession{rec: rec, runErr: transient}

		err := batchWrite(ctx, session, phase.rows, 2, phase.cypher)
		if !errors.Is(err, transient) {
			t.Fatalf("batchWrite() error = %v, want the transient error %v", err, transient)
		}
		if got := rec.runs.Load(); got != 1 {
			t.Fatalf("batchWrite() attempted %d statements with a cancelled context, want 1", got)
		}
	})

	t.Run("consume error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := &ackTestRecorder{}
		session := &ackTestSession{rec: rec, consumeErr: transient}

		err := batchWrite(ctx, session, phase.rows, 2, phase.cypher)
		if !errors.Is(err, transient) {
			t.Fatalf("batchWrite() error = %v, want the terminal summary error %v", err, transient)
		}
		if got := rec.runs.Load(); got != 1 {
			t.Fatalf("batchWrite() attempted %d statements with a cancelled context, want 1", got)
		}
		if got := rec.consumes.Load(); got != 1 {
			t.Fatalf("batchWrite() consumed %d terminal results with a cancelled context, want 1", got)
		}
	})
}

func TestSeedRetryRequiresTypedTransientServerStatus(t *testing.T) {
	transient := &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.Outdated", Msg: "write conflict"}
	for _, check := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "typed transient", err: transient, want: true},
		{name: "wrapped transient", err: fmt.Errorf("batch: %w", transient), want: true},
		{name: "unknown commit mentions conflict", err: &neo4j.Neo4jError{Code: "Neo.DatabaseError.Transaction.CommitFailed", Msg: "outcome unknown after conflict detected"}},
		{name: "client error quotes transient text", err: &neo4j.Neo4jError{Code: "Neo.ClientError.Statement.TypeError", Msg: "invalid value TransientError: conflict detected"}},
		{name: "untyped message", err: errors.New("TransientError: conflict detected")},
		{name: "terminated is not retryable", err: &neo4j.Neo4jError{Code: "Neo.TransientError.Transaction.Terminated"}},
	} {
		t.Run(check.name, func(t *testing.T) {
			if got := isTransientConflict(check.err); got != check.want {
				t.Fatalf("isTransientConflict(%v) = %v, want %v", check.err, got, check.want)
			}
		})
	}
}

func TestBatchWriteDoesNotReplayUnknownCommitOutcome(t *testing.T) {
	phase := ackTestCategoriesPhase(t, 2)
	unknown := &neo4j.Neo4jError{Code: "Neo.DatabaseError.Transaction.CommitFailed", Msg: "outcome unknown after conflict detected"}
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			rec := &ackTestRecorder{}
			var err error
			if parallel {
				err = batchWriteParallel(t.Context(), &ackTestDriver{rec: rec, consumeErr: unknown}, "nornic", phase.rows, 2, 2, phase.cypher)
			} else {
				err = batchWrite(t.Context(), &ackTestSession{rec: rec, consumeErr: unknown}, phase.rows, 2, phase.cypher)
			}
			if !errors.Is(err, unknown) || rec.runs.Load() != 1 || rec.consumes.Load() != 1 {
				t.Fatalf("unknown outcome must be surfaced once: error=%v runs=%d consumes=%d", err, rec.runs.Load(), rec.consumes.Load())
			}
		})
	}
}
