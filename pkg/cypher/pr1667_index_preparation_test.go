package cypher

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This file owns the current-release applicability of
// origin/pr-1667:pkg/cypher/indexed_preparation_context_test.go for indexed
// MATCH and MERGE statement preparation.
//
// The historical file drove renamed preparation helpers
// (lookupLockedWithContext, copySliceWithContext,
// sortedKeysViewLockedWithContext, orderedIDsLockedWithContext,
// pendingValueMatchesLockedWithContext, MergePendingPropertyMatchesWithContext,
// MergePendingCompositeMatchesWithContext, trackWithContext) through a probe
// context and required the *helper itself* to observe the cancellation. #1615
// replaced those context-carrying helpers with the context-free
// storage.pendingWriteView, so none of those helper names exist any more and a
// probe keyed on them would observe nothing for reasons that are not a product
// defect.
//
// The observation that survived the refactor is the statement's own epilogue
// (pkg/cypher/executor_entry.go: statementErr := ctx.Err() in Execute's
// deferred completion, then result, retErr = nil, statementErr). These tests
// therefore drive the *public* statement contract at the caller's own engine
// seam instead of a helper name: the probe cancels the real caller context at
// the first observation of the statement's indexed preparation, and then the
// public statement must report that cancellation with no published result,
// within the approved post-cancellation latency budget, leaving no extra MERGE
// node behind, and must succeed once the caller is live again.
//
// The context-first index APIs now observe this cancellation before candidate
// materialization. The large-population regression additionally asserts that no
// candidates are read after cancellation; storage component tests exercise
// cancellation inside copying, sorting, pending merges and admission waits.

// pr1667PreparationCancellationBudget is the approved bound on how long a
// public statement may keep its caller waiting after the caller's context was
// actually canceled. Total request time is measured and reported separately
// and is not bounded by it.
const pr1667PreparationCancellationBudget = 100 * time.Millisecond

// pr1667StatementWaitBudget bounds a single public statement call in these
// tests so a lost cancellation is reported as a failure instead of hanging the
// suite.
const pr1667StatementWaitBudget = 10 * time.Second

// pr1667PreparationProbe is the caller's engine with one observation added:
// the first read the statement makes through the public Engine seam that
// carries its indexed preparation (the schema it prepares the seek from, or a
// candidate node it materializes) cancels the real caller context and records
// when that happened.
type pr1667PreparationProbe struct {
	*storage.NamespacedEngine
	cancel           context.CancelFunc
	seam             string
	canceledAt       time.Time
	candidateReads   int
	readsAfterCancel int
}

// fire cancels the caller once, at the first observed preparation seam.
func (p *pr1667PreparationProbe) fire(seam string) {
	if p.cancel == nil || !p.canceledAt.IsZero() {
		return
	}
	p.seam = seam
	p.cancel()
	p.canceledAt = time.Now()
}

// GetSchema is the seam indexed preparation begins at: both the MATCH seek
// (propertyIndexLookup/compositeIndexLookup) and the MERGE lookup
// (findMergeNodesScanned -> mergeNodeIndexedCandidateIDs) read the schema
// before they consult an index.
func (p *pr1667PreparationProbe) GetSchema() *storage.SchemaManager {
	p.fire("GetSchema")
	return p.NamespacedEngine.GetSchema()
}

// GetNode and GetQueryNode are the two candidate materialization seams the
// executor uses (StorageExecutor.getQueryNode prefers the storage.QueryNodeReader
// view and falls back to Engine.GetNode).
func (p *pr1667PreparationProbe) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	p.candidateReads++
	node, err := p.NamespacedEngine.GetNode(ctx, id)
	if !p.canceledAt.IsZero() {
		p.readsAfterCancel++
	}
	return node, err
}

func (p *pr1667PreparationProbe) GetQueryNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	p.candidateReads++
	node, err := p.NamespacedEngine.GetQueryNode(ctx, id)
	if !p.canceledAt.IsZero() {
		p.readsAfterCancel++
	}
	return node, err
}

// arm starts observing for exactly one statement and returns its caller
// context.
func (p *pr1667PreparationProbe) arm() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.seam = ""
	p.canceledAt = time.Time{}
	p.candidateReads = 0
	p.readsAfterCancel = 0
	return ctx, cancel
}

// disarm stops observing so the same store can run live control statements.
func (p *pr1667PreparationProbe) disarm() { p.cancel = nil }

func pr1667RowCount(result *ExecuteResult) int {
	if result == nil {
		return -1
	}
	return len(result.Rows)
}

// executeBounded runs one public statement and reports its result, error,
// duration, and whether the call overran pr1667StatementWaitBudget.
func executeBounded(e *StorageExecutor, ctx context.Context, query string) (*ExecuteResult, error, time.Duration, bool) {
	type outcome struct {
		result *ExecuteResult
		err    error
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		result, err := e.Execute(ctx, query, nil)
		done <- outcome{result: result, err: err}
	}()
	select {
	case got := <-done:
		return got.result, got.err, time.Since(start), false
	case <-time.After(pr1667StatementWaitBudget):
		return nil, nil, time.Since(start), true
	}
}

// requireCanonicalCase fails rather than masking an unexpected observation: the
// fixture must have canceled the actual caller inside the statement's indexed
// preparation.
func requireCanonicalCase(t *testing.T, canceledAt time.Time, seam string) {
	t.Helper()
	require.False(t, canceledAt.IsZero(),
		"fixture must cancel the actual caller inside the statement's indexed preparation; "+
			"a preparation seam that is never reached is a broken fixture, not a passing test")
	require.Equal(t, "GetSchema", seam,
		"indexed preparation must be observed at the caller's schema read")
}

// pr1667IndexedPreparationCase is one historical public case that still
// reaches indexed preparation in the current release.
type pr1667IndexedPreparationCase struct {
	name  string
	index string
	query string
	// expectedRetryRows is the row count the same public statement returns
	// once the caller's context is live again.
	expectedRetryRows int
	// creates reports whether a live run of the statement creates one node,
	// so the canceled run's "no extra creation" control applies.
	creates bool
}

const (
	pr1667IndexK  = "CREATE INDEX p_k FOR (n:P) ON (n.k)"
	pr1667IndexKL = "CREATE INDEX p_kl FOR (n:P) ON (n.k, n.l)"
)

func pr1667IndexedPreparationCases() []pr1667IndexedPreparationCase {
	return []pr1667IndexedPreparationCase{
		// Committed-index preparation, historically owned by
		// lookupLockedWithContext, copySliceWithContext,
		// sortedKeysViewLockedWithContext,
		// VisitPropertyIndexGroupsInRangeWithContext and
		// orderedIDsLockedWithContext.
		{name: "schema committed filtering", index: pr1667IndexK, query: "MATCH (n:P) WHERE n.k = 3 RETURN n.k", expectedRetryRows: 1},
		{name: "inline committed indexing", index: pr1667IndexK, query: "MATCH (n:P {k: 3}) RETURN n.k", expectedRetryRows: 1},
		{name: "composite committed copy", index: pr1667IndexKL, query: "MATCH (n:P {k: 3, l: 1}) RETURN n.k", expectedRetryRows: 1},
		{name: "ordered key preparation", index: pr1667IndexK, query: "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.k", expectedRetryRows: 12},
		{name: "ordered group snapshot", index: pr1667IndexK, query: "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.k ORDER BY n.k LIMIT 1", expectedRetryRows: 1},
		{name: "ordered candidate collection", index: pr1667IndexK, query: "MATCH (n:P) WHERE n.k >= 2 RETURN n.k", expectedRetryRows: 11},
		// MERGE preparation, historically owned by pendingValueMatchesLocked,
		// MergePendingPropertyMatches and MergePendingCompositeMatches, now
		// reached through StorageExecutor.findMergeNodesScanned.
		{name: "property merge matched", index: pr1667IndexK, query: "MERGE (n:P {k: 3, l: 1}) RETURN n.k", expectedRetryRows: 1},
		{name: "property merge creating", index: pr1667IndexK, query: "MERGE (n:P {k: 99}) RETURN n.k", expectedRetryRows: 1, creates: true},
		{name: "composite merge matched", index: pr1667IndexKL, query: "MERGE (n:P {k: 3, l: 1}) RETURN n.k", expectedRetryRows: 1},
		{name: "composite merge creating", index: pr1667IndexKL, query: "MERGE (n:P {k: 99, l: 1}) RETURN n.k", expectedRetryRows: 1, creates: true},
	}
}

// TestPR1667IndexedStatementPreparationCancellation drives every historical
// auto-commit public MATCH/MERGE case through both parsers: the caller's real
// context is canceled at the first observation of the statement's indexed
// preparation, and the public statement must report that cancellation with no
// published result inside the approved budget, then succeed once the caller is
// live.
func TestPR1667IndexedStatementPreparationCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range pr1667IndexedPreparationCases() {
			t.Run(tc.name, func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				probe := &pr1667PreparationProbe{NamespacedEngine: storage.NewNamespacedEngine(b, "pr1667")}
				e := NewStorageExecutor(probe)
				run := func(query string) *ExecuteResult {
					t.Helper()
					result, runErr := e.Execute(context.Background(), query, nil)
					require.NoError(t, runErr, query)
					return result
				}

				run(tc.index)
				run("UNWIND range(1, 12) AS i CREATE (:P {k: i, l: 1})")
				before := run("MATCH (n:P) RETURN count(n) AS c")

				ctx, cancel := probe.arm()
				defer cancel()
				statementStart := time.Now()
				result, execErr, totalRequestTime, overran := executeBounded(e, ctx, tc.query)
				canceledAt, seam := probe.canceledAt, probe.seam
				probe.disarm()

				require.False(t, overran, "statement must observe the cancellation and return")
				requireCanonicalCase(t, canceledAt, seam)

				// The product contract: a canceled prepared statement reports
				// the cancellation and publishes nothing.
				require.ErrorIs(t, execErr, context.Canceled)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.Nil(t, result, "a canceled statement must not publish a result")
				require.Equal(t, -1, pr1667RowCount(result))

				// Approved budget, measured from the actual cancellation.
				afterCancel := totalRequestTime - canceledAt.Sub(statementStart)
				t.Logf("seam=%s total=%v afterCancel=%v candidateReads=%d readsAfterCancel=%d",
					seam, totalRequestTime, afterCancel, probe.candidateReads, probe.readsAfterCancel)
				require.LessOrEqual(t, afterCancel, pr1667PreparationCancellationBudget,
					"public statement must observe the cancellation within the approved budget")

				// No extra creation: a canceled statement leaves the population alone.
				require.Equal(t, before.Rows, run("MATCH (n:P) RETURN count(n) AS c").Rows,
					"a canceled statement must not create a node")

				// Retry control: the same public statement succeeds live, and an
				// unmatched MERGE creates exactly one node.
				retried := run(tc.query)
				require.Len(t, retried.Rows, tc.expectedRetryRows, "retry must return the prepared rows")
				expected := int64(12)
				if tc.creates {
					expected = 13
				}
				require.Equal(t, [][]interface{}{{expected}}, run("MATCH (n:P) RETURN count(n) AS c").Rows,
					"retry must create exactly one node, once")
			})
		}
	})
}

// pr1667PendingPreparationCase is one historical public case whose indexed
// preparation must merge the open transaction's own pending node writes.
type pr1667PendingPreparationCase struct {
	name  string
	index string
	query string
}

func pr1667PendingPreparationCases() []pr1667PendingPreparationCase {
	return []pr1667PendingPreparationCase{
		{name: "where pending first build", index: pr1667IndexK, query: "MATCH (n:P) WHERE n.k = 'pending' RETURN n.k"},
		{name: "inline pending first build", index: pr1667IndexK, query: "MATCH (n:P {k: 'pending'}) RETURN n.k"},
		{name: "merge pending first build", index: pr1667IndexK, query: "MERGE (n:P {k: 'pending', l: 1}) RETURN n.k"},
		{name: "composite inline pending", index: pr1667IndexKL, query: "MATCH (n:P {k: 'pending', l: 1}) RETURN n.k"},
		{name: "composite merge pending", index: pr1667IndexKL, query: "MERGE (n:P {k: 'pending', l: 1}) RETURN n.k"},
	}
}

// TestPR1667PendingIndexedPreparationCancellation drives the historical
// explicit-transaction cases, whose indexed preparation has to merge the
// statement's own pending node writes (the pendingWriteView formed from
// BadgerTransaction pendingValueMatchesLocked / MergePendingPropertyMatches /
// MergePendingCompositeMatches). As in the historical contract, a failed
// public statement inside an explicit transaction marks it failed, so the
// retry control rolls back and re-prepares a fresh transaction.
func TestPR1667PendingIndexedPreparationCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range pr1667PendingPreparationCases() {
			t.Run(tc.name, func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				probe := &pr1667PreparationProbe{NamespacedEngine: storage.NewNamespacedEngine(b, "pr1667")}
				e := NewStorageExecutor(probe)
				run := func(query string) *ExecuteResult {
					t.Helper()
					result, runErr := e.Execute(context.Background(), query, nil)
					require.NoError(t, runErr, query)
					return result
				}
				// prepare opens a transaction and stages the pending population
				// the indexed preparation has to see. Other-label nodes force
				// the pending index to enumerate their records too.
				prepare := func() {
					t.Helper()
					run("BEGIN")
					run("UNWIND range(1, 12) AS i CREATE (:Other {v: i})")
					run("CREATE (:P {k: 'pending', l: 1})")
				}

				run(tc.index)
				run("CREATE (:P {k: 'committed', l: 1})")
				prepare()

				ctx, cancel := probe.arm()
				defer cancel()
				statementStart := time.Now()
				result, execErr, totalRequestTime, overran := executeBounded(e, ctx, tc.query)
				canceledAt, seam := probe.canceledAt, probe.seam
				probe.disarm()

				require.False(t, overran, "statement must observe the cancellation and return")
				requireCanonicalCase(t, canceledAt, seam)
				require.ErrorIs(t, execErr, context.Canceled)
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.Nil(t, result, "a canceled statement must not publish a result")
				require.Equal(t, -1, pr1667RowCount(result))

				afterCancel := totalRequestTime - canceledAt.Sub(statementStart)
				t.Logf("seam=%s total=%v afterCancel=%v candidateReads=%d readsAfterCancel=%d",
					seam, totalRequestTime, afterCancel, probe.candidateReads, probe.readsAfterCancel)
				require.LessOrEqual(t, afterCancel, pr1667PreparationCancellationBudget,
					"public statement must observe the cancellation within the approved budget")

				// Public query failure keeps the explicit transaction failed:
				// roll back, then repeat in a fresh transaction.
				run("ROLLBACK")
				prepare()
				retried := run(tc.query)
				require.Len(t, retried.Rows, 1, "retry must retain the indexed pending candidate")
				run("ROLLBACK")
			})
		}
	})
}
