// Author: Codex, NOR-180 producer-outward public regression fixtures, 2026-10-27.
package walterminal

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// FlowFamilies are the in-scope structural classes reached from the terminal
// producer: ordinary implicit return, native batch return, independent procedure
// batches (including their alias), and a registered callback's aggregate result.
// These are verification representatives, not the population extractor's roots.
var FlowFamilies = []string{
	"implicit", "transactional_call", "periodic_commit", "periodic_iterate",
	"periodic_alias", "callback_interface", "callback_nested",
}

var FlowOutcomes = []string{"commit", "abort_timeout", "abort_memory", "empty"}

var commitMarkerFailureMessage = regexp.MustCompile(`transaction [^[:space:]]+ committed, but failed to write WAL tx commit: wal: closed`)

// RequireCommitMarkerFailure checks native truth and the failed record together,
// including the transaction ID between "transaction" and "committed".
func RequireCommitMarkerFailure(t testing.TB, message string) {
	t.Helper()
	if !commitMarkerFailureMessage.MatchString(message) {
		t.Fatalf("missing committed transaction ID and failed commit-record diagnostic in %q", message)
	}
}

type OutcomeFlow struct {
	Query   string
	Primary error
	// PostCommit is a committed child's post-commit failure (commit_postcommit).
	PostCommit     error
	Closed         bool
	WantError      bool
	CommittedNodes int64
	mu             sync.Mutex
	observed       []error
}

func (f *OutcomeFlow) observe(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, err)
}

// Observed returns what an aggregation callback received before it replaced the
// outcome with a successful count. This also checks identity before the sink.
func (f *OutcomeFlow) Observed() []error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]error(nil), f.observed...)
}

type flowCallback interface {
	Run(context.Context) (*cypher.ExecuteResult, error)
}

type flowCallbackFunc func(context.Context) (*cypher.ExecuteResult, error)

func (f flowCallbackFunc) Run(ctx context.Context) (*cypher.ExecuteResult, error) {
	return f(ctx)
}

func registerFlowProcedure(t testing.TB, owns bool, handler cypher.ProcedureHandler) string {
	t.Helper()
	name := fmt.Sprintf("nor180.flow%d", procedureID.Add(1))
	if err := cypher.RegisterUserProcedure(cypher.ProcedureSpec{
		Name: name, Signature: name + "() :: (" + flowColumn + " :: INTEGER)",
		Mode: cypher.ProcedureModeWrite, OwnsTransactions: owns,
		Returns: []cypher.ProcedureColumn{{Name: flowColumn, Type: "INTEGER"}},
	}, handler); err != nil {
		t.Fatal(err)
	}
	return "CALL " + name + "() YIELD " + flowColumn + " RETURN " + flowColumn
}

// flowColumn names this fixture's own result column.
const flowColumn = "flowValue"

func flowValue(value int64) *cypher.ExecuteResult {
	return &cypher.ExecuteResult{Columns: []string{flowColumn}, Rows: [][]interface{}{{value}}, Stats: &cypher.QueryStats{}}
}

// NewOutcomeFlow injects only the logical WAL failure, after real native work
// has been staged. Public Execute and the production procedure/batch owners do
// every begin, commit and rollback. No private implementation symbol is used,
// so the same fixture can run fail-first on the pushed 04c87 baseline.
func NewOutcomeFlow(t testing.TB, engine *Engine, family, outcome string, closed bool) *OutcomeFlow {
	t.Helper()
	f := &OutcomeFlow{Closed: closed, WantError: closed}
	switch outcome {
	case "abort_timeout":
		f.Primary = Primary("timeout")
	case "abort_memory":
		f.Primary = Primary("memory")
	case "commit":
		f.CommittedNodes = 1
	case "commit_postcommit":
		// Author: Claude, NOR-180 one committed representation, 30 October 2026.
		// The native commit succeeds; its post-commit suppression step then
		// fails with a transient storage error. With a writable WAL its COMMIT
		// record is written; with a failed one the record fails too.
		f.CommittedNodes, f.PostCommit, f.WantError = 1, storage.ErrStorageRestoring, true
	case "empty":
	default:
		t.Fatalf("unknown logical terminal outcome %q", outcome)
	}
	if family == "implicit" || family == "transactional_call" {
		f.WantError = closed || f.Primary != nil || f.PostCommit != nil
	}
	var calls atomic.Int64
	step := registerFlowProcedure(t, false, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		// A healthy periodic.commit terminates after the first written batch.
		if calls.Add(1) > 1 {
			return flowValue(0), nil
		}
		result := flowValue(0)
		if outcome != "empty" {
			var err error
			result, err = exec.Execute(ctx, "CREATE (:WalWire) RETURN 1 AS value", nil)
			if err != nil {
				return nil, err
			}
		}
		if f.PostCommit != nil {
			if _, err := exec.Execute(ctx, "MATCH (n:WalWire) CALL kp.suppress(n)", nil); err != nil {
				return nil, err
			}
		}
		if closed {
			if err := engine.WAL.Close(); err != nil {
				return nil, err
			}
		}
		if f.Primary != nil {
			return nil, f.Primary
		}
		return result, nil
	})
	if f.Primary != nil {
		f.CommittedNodes = 0
	}
	switch family {
	case "implicit":
		f.Query = step
	case "transactional_call":
		f.Query = "UNWIND [1] AS x CALL { WITH x " + step + " } IN TRANSACTIONS OF 1 ROWS RETURN value"
	case "periodic_commit":
		f.Query = fmt.Sprintf("CALL apoc.periodic.commit('%s',{})", step)
	case "periodic_iterate", "periodic_alias":
		name := "apoc.periodic.iterate"
		if family == "periodic_alias" {
			name = "apoc.periodic.rock_n_roll"
		}
		f.Query = fmt.Sprintf("CALL %s('RETURN 1 AS input','%s',{batchSize:1,parallel:false})", name, step)
	case "callback_interface", "callback_nested":
		// The aggregate's name and body are unrelated to periodic procedures.
		// A function value, an interface dispatch and a callback all lie between
		// the public transaction return and the successful count replacement.
		var callback flowCallback = flowCallbackFunc(func(ctx context.Context) (*cypher.ExecuteResult, error) {
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
			return exec.Execute(ctx, step, nil)
		})
		aggregate := registerFlowProcedure(t, true, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
			_, err := callback.Run(ctx)
			f.observe(err)
			return flowValue(0), nil // Deliberately model a count-only consumer.
		})
		f.Query = aggregate
		if family == "callback_nested" {
			f.Query = registerFlowProcedure(t, true, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
				_, err := exec.Execute(ctx, aggregate, nil)
				f.observe(err)
				return flowValue(0), nil
			})
		}
	default:
		t.Fatalf("unknown terminal flow family %q", family)
	}
	return f
}

// NewMultipleOutcomeFlow models one invocation consuming several independent
// logical outcomes. Independent engines keep the second begin possible after
// the first logical WAL has failed; native work remains real in both stores.
// The options exercise joined parallel work and the matching healthy WAL control.
type MultipleOutcomeOptions struct {
	Parallel   bool
	HealthyWAL bool
	// Author: Codex,R5. Explicit child kinds and original primary identity.
	Kinds []string
	// Closed, when set, is each child's own WAL outcome (Claude, 30 Oct 2026);
	// otherwise every child's WAL is healthy or failed together (HealthyWAL).
	Closed []bool
	// ObservePrimary receives each child's reported primary, one per child in
	// order: its abort primary, its post-commit failure, or nil.
	ObservePrimary func(error)
}

// PostCommitFault fails the post-commit manual-suppression step with Cause.
type PostCommitFault struct {
	storage.Engine
	Cause error
}

func (e *PostCommitFault) GetInnerEngine() storage.Engine { return e.Engine }
func (e *PostCommitFault) SetManualSuppression(storage.StoredEntityKind, string, bool) error {
	return e.Cause
}

func NewMultipleOutcomeFlow(t testing.TB, engines []*Engine, options ...MultipleOutcomeOptions) *OutcomeFlow {
	t.Helper()
	var option MultipleOutcomeOptions
	if len(options) > 0 {
		option = options[0]
	}
	closed := !option.HealthyWAL
	f := &OutcomeFlow{Closed: closed, WantError: closed && len(option.Closed) == 0}
	var callbacks []flowCallback
	for i, engine := range engines {
		kind := "abort_timeout"
		if i%2 != 0 {
			kind = "abort_memory"
		}
		if len(option.Kinds) != 0 {
			kind = option.Kinds[i]
		}
		childClosed := closed
		if len(option.Closed) != 0 {
			childClosed = option.Closed[i]
		}
		child := NewOutcomeFlow(t, engine, "implicit", kind, childClosed)
		if option.ObservePrimary != nil {
			reported := child.Primary
			if child.PostCommit != nil {
				reported = child.PostCommit
			}
			option.ObservePrimary(reported)
		}
		if f.Primary == nil {
			f.Primary = child.Primary
		}
		// Only terminal child outcomes survive this count-only consumer, which
		// discards its children's returns: failed WAL records and committed
		// failures.
		if len(option.Closed) != 0 {
			f.WantError = f.WantError || childClosed || child.PostCommit != nil
		} else {
			f.WantError = f.WantError || child.PostCommit != nil
		}
		var store storage.Engine = storage.NewNamespacedEngine(engine, "nornic")
		if child.PostCommit != nil {
			store = &PostCommitFault{Engine: store, Cause: child.PostCommit}
		}
		exec := cypher.NewStorageExecutor(store)
		callbacks = append(callbacks, flowCallbackFunc(func(ctx context.Context) (*cypher.ExecuteResult, error) {
			return exec.Execute(ctx, child.Query, nil)
		}))
	}
	f.Query = registerFlowProcedure(t, true, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		var workers sync.WaitGroup
		for _, callback := range callbacks {
			if option.Parallel {
				workers.Add(1)
				go func() {
					defer workers.Done()
					_, err := callback.Run(ctx)
					f.observe(err)
				}()
			} else {
				_, err := callback.Run(ctx)
				f.observe(err)
			}
		}
		workers.Wait()
		return flowValue(int64(len(callbacks))), nil
	})
	return f
}

// EnclosedOutcomeFlow is the 5b9c review's construction through public
// consumers (Claude, 30 Oct 2026). An inner count-only aggregate has an
// error-free committed child and a transient aborted child whose WAL fails.
// Its retained outcome is the primary of an enclosing statement whose own
// abort record fails, and an outer count-only aggregate composes that with
// another transient aborted child whose WAL fails, in the given order.
type EnclosedOutcomeFlow struct {
	Query     string
	Committed *Engine   // the error-free committed child's store
	Empty     []*Engine // every other store: nothing stays stored
	Primaries []error   // both aborted children's primaries
}

func NewEnclosedOutcomeFlow(t testing.TB, enclosingFirst bool) *EnclosedOutcomeFlow {
	t.Helper()
	committed, aborted, enclosing, other := New(t), New(t), New(t), New(t)
	f := &EnclosedOutcomeFlow{Committed: committed, Empty: []*Engine{aborted, enclosing, other}}
	inner := NewMultipleOutcomeFlow(t, []*Engine{committed, aborted}, MultipleOutcomeOptions{
		Kinds: []string{"commit", "abort_memory"}, Closed: []bool{false, true},
		ObservePrimary: func(primary error) {
			if primary != nil {
				f.Primaries = append(f.Primaries, primary)
			}
		},
	})
	innerExec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(New(t), "nornic"))
	step := registerFlowProcedure(t, false, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		// The inner operation is an independent client operation: its returned
		// outcome, with its committed work, is what this statement wraps. No
		// live journal of the outer operation remembers the inner commit.
		_, err := innerExec.Execute(context.Background(), inner.Query, nil)
		if closeErr := enclosing.WAL.Close(); closeErr != nil {
			return nil, closeErr
		}
		if err == nil {
			// The construction needs a failed inner outcome; without one the
			// assertions fail (a procedure never returns a nil result).
			return flowValue(0), nil
		}
		return nil, err
	})
	sibling := NewOutcomeFlow(t, other, "implicit", "abort_memory", true)
	f.Primaries = append(f.Primaries, sibling.Primary)
	children := []flowCallback{
		flowCallbackFunc(func(ctx context.Context) (*cypher.ExecuteResult, error) {
			return cypher.NewStorageExecutor(storage.NewNamespacedEngine(other, "nornic")).Execute(ctx, sibling.Query, nil)
		}),
		flowCallbackFunc(func(ctx context.Context) (*cypher.ExecuteResult, error) {
			return cypher.NewStorageExecutor(storage.NewNamespacedEngine(enclosing, "nornic")).Execute(ctx, step, nil)
		}),
	}
	if enclosingFirst {
		children[0], children[1] = children[1], children[0]
	}
	f.Query = registerFlowProcedure(t, true, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
		for _, child := range children {
			_, _ = child.Run(ctx) // a count-only consumer discards child returns
		}
		return flowValue(int64(len(children))), nil
	})
	return f
}

// RequireNative checks native truth first: only the committed child stored work.
func (f *EnclosedOutcomeFlow) RequireNative(t testing.TB) {
	t.Helper()
	if got := Count(t, f.Committed); got != 1 {
		t.Fatalf("committed child stored %d nodes, want 1", got)
	}
	for i, engine := range f.Empty {
		if got := Count(t, engine); got != 0 {
			t.Fatalf("store %d stored %d nodes, want 0", i, got)
		}
	}
}
