// Regression controls for security finding F1: an accepted graph commit owns
// its completion. `kp.suppress` is deferred to the commit (afterCommit), and
// the native commit admits that lifetime through txphase.CommitContext. The
// queued completion actions must run under that same admitted lifetime, not
// under the cancelable statement context that admitted the commit; otherwise a
// client that goes away (or a transaction timeout) after acceptance loses the
// visibility change while the graph write stays durable, and COMMIT reports a
// terminal failure for work that committed.
package cypher

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

// admittedCommitCanceler ends the request lifetime at the managed commit
// boundary: the graph commit has been accepted and the request that asked for
// it goes away before the queued completion actions run.
type admittedCommitCanceler struct {
	*storage.NamespacedEngine
	cancel   context.CancelFunc
	attempts int
	applied  int
}

func (e *admittedCommitCanceler) GetInnerEngine() storage.Engine { return e.NamespacedEngine }

// SetManualSuppression records whether the deferred visibility change was
// attempted and whether storage applied it.
func (e *admittedCommitCanceler) SetManualSuppression(ctx context.Context, kind storage.StoredEntityKind, id string, suppressed bool) error {
	e.attempts++
	err := e.NamespacedEngine.SetManualSuppression(ctx, kind, id, suppressed)
	if err == nil {
		e.applied++
	}
	return err
}

// CommitTransaction is the native commit boundary: the write is accepted, then
// the request lifetime is cancelled exactly as a disconnect would.
func (e *admittedCommitCanceler) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	err := tx.CommitContext(ctx)
	if err == nil && e.cancel != nil {
		e.cancel()
	}
	return err
}

// newAdmittedCommitCanceler builds the public Execute path over an on-disk
// engine: reveal() and durable outcomes need a BadgerEngine.
func newAdmittedCommitCanceler(t *testing.T, namespace string) (*StorageExecutor, *admittedCommitCanceler, storage.Engine) {
	t.Helper()
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, namespace)
	engine := &admittedCommitCanceler{NamespacedEngine: store}
	exec := NewStorageExecutor(engine)
	t.Cleanup(func() {
		if exec.HasActiveTransaction() {
			_, _ = exec.Execute(context.Background(), "ROLLBACK", nil)
		}
	})
	return exec, engine, store
}

func countValue(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	require.Len(t, result.Rows[0], 1, query)
	value, ok := result.Rows[0][0].(int64)
	require.True(t, ok, query)
	return value
}

func rowCount(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	return int64(len(result.Rows))
}

// TestAcceptedCommitCompletionKeepsImplicitSuppression is the implicit
// autocommit control: a write that queues kp.suppress commits, the request dies
// at the managed commit boundary, and the accepted completion still applies the
// suppression exactly once.
func TestAcceptedCommitCompletionKeepsImplicitSuppression(t *testing.T) {
	for _, phaseOwned := range []bool{true, false} {
		t.Run(map[bool]string{true: "statement_phase", false: "plain_request"}[phaseOwned], func(t *testing.T) {
			exec, engine, store := newAdmittedCommitCanceler(t, "completion-implicit")
			_, err := exec.Execute(context.Background(), "CREATE (n:Document {id: 'implicit'})", nil)
			require.NoError(t, err)

			ctx, cancel := completionRequestLifetime(t, phaseOwned)
			defer cancel()
			engine.cancel = cancel

			_, err = exec.Execute(ctx, "MATCH (n:Document {id: 'implicit'}) SET n.touched = true WITH n CALL kp.suppress(n)", nil)
			require.NoError(t, err, "a cancelled statement cannot cancel the completion its admitted commit accepted")
			require.Equal(t, 1, engine.attempts, "the accepted commit runs its queued action once")
			require.Equal(t, 1, engine.applied, "the accepted commit applies its queued suppression")

			observer := NewStorageExecutor(store)
			require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'implicit'}) RETURN count(n)"),
				"the accepted suppression hides the node")
			require.EqualValues(t, 1, rowCount(t, observer, "MATCH (n:Document {id: 'implicit'}) RETURN reveal(n).touched"),
				"the graph write of the same transaction stays committed")
		})
	}
}

// TestAcceptedCommitCompletionKeepsExplicitSuppression is the explicit control
// the review asked for: BEGIN; CREATE; CALL kp.suppress; COMMIT with the request
// cancelled once the graph commit is accepted. A healthy retry afterwards is its
// own transaction and replays nothing from the accepted one.
func TestAcceptedCommitCompletionKeepsExplicitSuppression(t *testing.T) {
	for _, phaseOwned := range []bool{true, false} {
		t.Run(map[bool]string{true: "statement_phase", false: "plain_request"}[phaseOwned], func(t *testing.T) {
			exec, engine, store := newAdmittedCommitCanceler(t, "completion-explicit")
			ctx, cancel := completionRequestLifetime(t, phaseOwned)
			defer cancel()
			for _, statement := range []string{
				"BEGIN",
				"CREATE (n:Document {id: 'explicit'})",
				"CREATE (n:Document {id: 'retry'})",
				"MATCH (n:Document {id: 'explicit'}) SET n.touched = true WITH n CALL kp.suppress(n)",
			} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			engine.cancel = cancel

			_, err := exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err, "COMMIT reports the accepted outcome, not the vanished request")
			require.False(t, exec.HasActiveTransaction(), "COMMIT ends the transaction")
			require.Equal(t, 1, engine.attempts, "the accepted COMMIT runs its queued action once")
			require.Equal(t, 1, engine.applied, "the accepted COMMIT applies its queued suppression")

			observer := NewStorageExecutor(store)
			require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'explicit'}) RETURN count(n)"),
				"the accepted suppression hides the node")
			require.EqualValues(t, 1, rowCount(t, observer, "MATCH (n:Document {id: 'explicit'}) RETURN reveal(n).touched"),
				"the graph write of the same transaction stays committed")

			// A healthy retry is its own transaction: one more application, and no
			// replay of the accepted queue.
			engine.cancel = nil
			for _, statement := range []string{"BEGIN", "MATCH (n:Document {id: 'retry'}) CALL kp.suppress(n)", "COMMIT"} {
				_, err := exec.Execute(context.Background(), statement, nil)
				require.NoError(t, err, statement)
			}
			require.Equal(t, 2, engine.attempts)
			require.Equal(t, 2, engine.applied, "each accepted commit applies its own action exactly once")
			require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'retry'}) RETURN count(n)"))
			require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'explicit'}) RETURN count(n)"))
		})
	}
}

// TestAcceptedCommitCompletionIsDurable keeps the durable outcome in view: the
// suppression the accepted commit applied is in the store, so it survives a
// close and reopen of the same directory.
func TestAcceptedCommitCompletionIsDurable(t *testing.T) {
	for _, phaseOwned := range []bool{true, false} {
		t.Run(map[bool]string{true: "statement_phase", false: "plain_request"}[phaseOwned], func(t *testing.T) {
			dir := t.TempDir()
			base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "completion-durable")
			engine := &admittedCommitCanceler{NamespacedEngine: store}
			exec := NewStorageExecutor(engine)

			ctx, cancel := completionRequestLifetime(t, phaseOwned)
			defer cancel()
			for _, statement := range []string{"BEGIN", "CREATE (n:Document {id: 'durable'})", "MATCH (n:Document {id: 'durable'}) CALL kp.suppress(n)"} {
				_, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
			}
			engine.cancel = cancel
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			require.Equal(t, 1, engine.applied)
			require.NoError(t, base.Close())

			reopened, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			observer := NewStorageExecutor(storage.NewNamespacedEngine(reopened, "completion-durable"))
			require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'durable'}) RETURN count(n)"),
				"the accepted suppression is durable")
			require.EqualValues(t, 1, rowCount(t, observer, "MATCH (n:Document {id: 'durable'}) RETURN reveal(n).id"),
				"only the visibility changed; the committed data remains")
		})
	}
}

// TestCommitAdmissionRefusesCancellationBeforeAcceptance keeps the other half of
// the contract: a COMMIT whose request is already gone before admission is
// refused and commits nothing.
func TestCommitAdmissionRefusesCancellationBeforeAcceptance(t *testing.T) {
	exec, engine, store := newAdmittedCommitCanceler(t, "completion-refused")
	ctx, cancel := txphase.WithExecutionDeadline(context.Background(), time.Minute)
	defer cancel()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (n:Document {id: 'refused'})", nil)
	require.NoError(t, err)
	cancel()

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.ErrorIs(t, err, context.Canceled, "cancellation before acceptance is preserved")
	require.Zero(t, engine.applied, "nothing is completed for a refused commit")
	observer := NewStorageExecutor(store)
	require.Zero(t, countValue(t, observer, "MATCH (n:Document {id: 'refused'}) RETURN count(n)"),
		"a refused commit stores nothing")
}

// completionRequestLifetime covers both HTTP phase ownership and callers whose
// terminal commit receives a plain cancellable lifetime, including Bolt.
func completionRequestLifetime(t *testing.T, phaseOwned bool) (context.Context, context.CancelFunc) {
	t.Helper()
	if phaseOwned {
		return txphase.WithExecutionDeadline(t.Context(), time.Minute)
	}
	return context.WithCancel(t.Context())
}

// Admission can fail after Execute's initial check. Exercise that exact
// terminal boundary with real cancellation; the public Execute controls above
// independently cover routing and accepted completion.
func TestExplicitCommitAdmissionRefusalClosesNativeTransaction(t *testing.T) {
	exec, _, store := newAdmittedCommitCanceler(t, "completion-admission-cleanup")
	ctx, cancel := txphase.WithExecutionDeadline(t.Context(), time.Minute)
	defer cancel()
	for _, query := range []string{"BEGIN", "CREATE (n:Document {id:'refused'})"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok)
	t.Cleanup(func() { _ = tx.Rollback() })
	cancel()
	_, err := exec.handleCommit(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, exec.HasActiveTransaction())
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: "completion-admission-cleanup:late"})
	require.ErrorIs(t, err, storage.ErrTransactionClosed, "rejected admission must retire native state before dropping its handle")
	observer := NewStorageExecutor(store)
	require.Zero(t, countValue(t, observer, "MATCH (n:Document {id:'refused'}) RETURN count(n)"))
}
