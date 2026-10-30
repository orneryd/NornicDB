package storage_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pr1655ExecuteCommitReadMessage is the existing commit-path log record the
// fault arms on. Native PublishCommit emits it between the optimistic
// validation pass and the authoritative pass for every transaction carrying
// client metadata, which is how cypher.WithTransactionMetadata arms it.
const pr1655ExecuteCommitReadMessage = "transaction committing with metadata"

// pr1655CommitFailedStatus is the database-level status an infrastructure
// commit failure must map to: not a client retry code.
const pr1655CommitFailedStatus = "Neo.DatabaseError.Transaction.TransactionCommitFailed"

// pr1655ExecuteFault is the concurrency-safe outcome of the one-shot trigger.
// It runs inside the commit's slog handler, so it never touches *testing.T.
type pr1655ExecuteFault struct {
	once      sync.Once
	armed     atomic.Bool
	engine    *storage.BadgerEngine
	nodeID    storage.NodeID
	restoreFn func() error
	decodeErr error
	fired     atomic.Bool
	installed atomic.Bool
	setupErr  atomic.Value // error
}

func (f *pr1655ExecuteFault) setSetupError(err error) {
	if err != nil {
		f.setupErr.Store(err)
	}
}

func (f *pr1655ExecuteFault) setupError() error {
	if v, ok := f.setupErr.Load().(error); ok {
		return v
	}
	return nil
}

// pr1655ExecuteHandler is the custom slog.Handler handed to BadgerOptions at
// construction. It discards every record and acts once on the armed commit
// record for the target node.
type pr1655ExecuteHandler struct {
	fault *pr1655ExecuteFault
	next  slog.Handler
}

func (h *pr1655ExecuteHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *pr1655ExecuteHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == pr1655ExecuteCommitReadMessage && h.matches(record) {
		h.fault.once.Do(func() {
			h.fault.fired.Store(true)
			h.fault.inject()
		})
	}
	return h.next.Handle(ctx, record)
}

// matches selects only the armed transaction: the node the fixture targets
// must appear in the metadata map the commit record carries.
func (h *pr1655ExecuteHandler) matches(record slog.Record) bool {
	if !h.fault.armed.Load() {
		return false
	}
	matched := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "metadata" || attr.Value.Kind() != slog.KindAny {
			return true
		}
		metadata, ok := attr.Value.Any().(map[string]any)
		if !ok || metadata["fixture_node"] != string(h.fault.nodeID) {
			return true
		}
		matched = true
		return false
	})
	return matched
}

func (h *pr1655ExecuteHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *pr1655ExecuteHandler) WithGroup(string) slog.Handler      { return h }

// inject corrupts the target node's committed MVCC head once the commit path
// is between its optimistic and authoritative passes.
func (f *pr1655ExecuteFault) inject() {

	restore, decodeErr := f.engine.PR1655CorruptMVCCHead(f.nodeID)
	if decodeErr == nil {
		f.setSetupError(errors.New("the malformed MVCC head decoded without error"))
		return
	}
	if restore == nil {
		f.setSetupError(fmt.Errorf("corrupting the MVCC head of %q failed", f.nodeID))
		return
	}
	f.restoreFn = restore
	f.decodeErr = decodeErr
	f.installed.Store(true)
}

func (f *pr1655ExecuteFault) restore(t *testing.T) {
	if f.restoreFn == nil {
		return
	}
	if err := f.restoreFn(); err != nil {
		t.Errorf("restoring the MVCC head of %q: %v", f.nodeID, err)
	}
	f.restoreFn = nil
}

// pr1655ExecuteScenario is one Execute path the regression covers.
type pr1655ExecuteScenario struct {
	writeBehind bool
	explicit    bool
}

func (s pr1655ExecuteScenario) name() string {
	path := "implicit"
	if s.explicit {
		path = "explicit"
	}
	return fmt.Sprintf("%s/write_behind=%t", path, s.writeBehind)
}

// TestPR1655ExecuteCommitReadFailureIsTyped drives the real Cypher Execute
// paths: the statement stages every write before commit, the commit-time head
// read then fails, and the failure must surface as the shared typed commit
// failure over exactly TransactionCommitFailed while publishing nothing and
// leaking no transaction state.
func TestPR1655ExecuteCommitReadFailureIsTyped(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			scenario := pr1655ExecuteScenario{writeBehind: writeBehind, explicit: explicit}
			t.Run(scenario.name(), func(t *testing.T) { pr1655RunExecuteScenario(t, scenario) })
		}
	}
}

func pr1655RunExecuteScenario(t *testing.T, scenario pr1655ExecuteScenario) {
	fault := &pr1655ExecuteFault{}
	base, err := testfixture.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             t.TempDir(),
		LowMemory:           true,
		WriteBehind:         scenario.writeBehind,
		WriteBehindInterval: time.Hour,
		Logger:              slog.New(&pr1655ExecuteHandler{fault: fault, next: slog.DiscardHandler}),
	}))
	require.NoError(t, err)
	require.NotNil(t, base)
	t.Cleanup(func() { _ = base.Close() })
	fault.engine = base
	t.Cleanup(func() { fault.restore(t) })

	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
	ctx := context.Background()

	_, err = exec.Execute(ctx, "CREATE (:Doc {id: 'original', summary_input: 'before'})", nil)
	require.NoError(t, err)
	require.NoError(t, base.FlushWriteBehind(), "the seed must be stored before the statement pins its snapshot")
	nodeID, err := pr1655SingleDocID(base)
	require.NoError(t, err)
	fault.nodeID = nodeID
	before, err := exec.Execute(ctx, "MATCH (n:Doc {id: 'original'}) RETURN n.summary_input AS body", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"before"}}, before.Rows, "the seeded body must be committed and readable")

	metadata := cypher.WithTransactionMetadata(ctx, map[string]any{
		"fixture":      true,
		"fixture_node": string(fault.nodeID),
	})
	const update = "MATCH (n:Doc {id: 'original'}) SET n.summary_input = 'changed'"

	var commitErr error
	if scenario.explicit {
		_, err = exec.Execute(metadata, "BEGIN", nil)
		require.NoError(t, err, "BEGIN must succeed")
		_, err = exec.Execute(metadata, update, nil)
		require.NoError(t, err, "the statement must stage its write before the fault")
		require.False(t, fault.fired.Load(), "the fault fires only at COMMIT")

		fault.armed.Store(true)
		_, commitErr = exec.Execute(metadata, "COMMIT", nil)
	} else {
		fault.armed.Store(true)
		_, commitErr = exec.Execute(metadata, update, nil)
	}
	fault.armed.Store(false)

	require.True(t, fault.fired.Load(), "the commit read fault never fired")
	require.NoError(t, fault.setupError(), "the commit-read fault fixture failed to install")
	require.True(t, fault.installed.Load(), "the fault never installed the corrupted head")
	require.Error(t, commitErr, "the commit-time head read failed, so Execute must fail")
	require.Equal(t, fault.decodeErr.Error(), deepestPR1655Cause(commitErr).Error(), "the deepest cause is the decoder's own error")
	pr1655AssertTypedCommitFailure(t, commitErr)
	fault.restore(t)

	require.NoError(t, base.FlushWriteBehind(), "any acknowledged write must be stored before the body is read")
	after, err := exec.Execute(ctx, "MATCH (n:Doc {id: 'original'}) RETURN n.summary_input AS body", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{"before"}}, after.Rows, "the failed commit must not publish the staged body")
	require.False(t, exec.HasActiveTransaction(), "the failed Execute left an active transaction behind")
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.Error(t, err, "there is no transaction left to roll back")
}

// pr1655SingleDocID returns the stored ID of the only Doc in the test
// namespace, so the fixture targets the ID the executor actually minted.
func pr1655SingleDocID(base *storage.BadgerEngine) (storage.NodeID, error) {
	nodes, err := base.GetNodesByLabel("Doc")
	if err != nil {
		return "", err
	}
	if len(nodes) != 1 {
		return "", fmt.Errorf("expected exactly one Doc node, found %d", len(nodes))
	}
	return nodes[0].ID, nil
}

// deepestPR1655Cause unwraps err to its innermost cause.
func deepestPR1655Cause(err error) error {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}

// pr1655AssertTypedCommitFailure checks the failure is the shared typed commit
// failure and not a retryable conflict or a constraint violation.
func pr1655AssertTypedCommitFailure(t *testing.T, err error) {
	t.Helper()
	var failure *storage.CommitFailureError
	require.ErrorAs(t, err, &failure, "the Execute failure must be a typed commit failure: %v", err)
	require.NotNil(t, failure.Cause)
	require.False(t, errors.Is(err, storage.ErrConflict), "a commit read failure is not a retryable conflict: %v", err)
	var violation *storage.ConstraintViolationError
	require.False(t, errors.As(err, &violation), "a commit read failure is not a constraint violation: %v", err)

	code, message := nornicerrors.Neo4jStatus(err)
	require.Equal(t, pr1655CommitFailedStatus, code, "status: %q message: %q", code, message)
	commitCode, commitMessage := nornicerrors.Neo4jCommitStatus(err)
	require.Equal(t, pr1655CommitFailedStatus, commitCode, "commit status: %q message: %q", commitCode, commitMessage)
}
