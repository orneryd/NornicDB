package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mergeKeyWaitLog collects an engine's JSON log records.
type mergeKeyWaitLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *mergeKeyWaitLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *mergeKeyWaitLog) prolonged(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		if record["event_id"] == "storage.transaction.lock_wait_prolonged" {
			out = append(out, record)
		}
	}
	return out
}

// newMergeKeyWaitEngine is newMergeKeyTestEngine whose MERGE key waits are
// bounded and logged as options say, through the engine constructor.
func newMergeKeyWaitEngine(t *testing.T, options LockWaitOptions) (*BadgerEngine, *SchemaManager, *mergeKeyWaitLog) {
	t.Helper()
	log := &mergeKeyWaitLog{}
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory:      true,
		Logger:        slog.New(slog.NewJSONHandler(log, nil)),
		EngineOptions: EngineOptions{LockWait: options},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_k", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(context.Background(), NewNamespacedEngine(engine, "test"), schema))
	return engine, schema, log
}

func mergeKeyProps(k int64) map[string]interface{} { return map[string]interface{}{"k": k} }

// mergeLockKey is the registry key a MERGE of (:U {k}) locks, from the
// canonical projection MERGE admission and commit share.
func mergeLockKey(t *testing.T, schema *SchemaManager, k int64) uniqueConstraintLockKey {
	t.Helper()
	keys, err := schema.uniqueMergeKeys(t.Context(), []string{"U"}, mergeKeyProps(k))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	return keys[0].lock
}

// TestMergeKeyWaitEndsAtTransactionTimeout (NOR-284): a MERGE waiting for a
// key another transaction holds fails at the engine's transaction timeout
// with TransactionTimedOut, though its context has no deadline. Its wait
// leaves nothing registered, the key it took before waiting stays its own
// until its transaction ends, and the wait is logged once past the
// slow-query threshold, naming the holder.
func TestMergeKeyWaitEndsAtTransactionTimeout(t *testing.T) {
	const timeout, logAfter = 300 * time.Millisecond, 100 * time.Millisecond
	engine, schema, log := newMergeKeyWaitEngine(t, LockWaitOptions{Timeout: timeout, LogAfter: logAfter})
	ctx := context.Background()

	holder := beginMergeKeyTx(t, engine)
	require.NoError(t, holder.PrepareMergeNodeForWrite(ctx, []string{"U"}, mergeKeyProps(1)))
	waiter := beginMergeKeyTx(t, engine)
	require.NoError(t, waiter.PrepareMergeNodeForWrite(ctx, []string{"U"}, mergeKeyProps(2)))

	started := time.Now()
	err := waiter.PrepareMergeNodeForWrite(ctx, []string{"U"}, mergeKeyProps(1))
	elapsed := time.Since(started)
	require.ErrorIs(t, err, ErrLockWaitTimeout)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	var coded interface{ BoltErrorCode() string }
	require.True(t, errors.As(err, &coded))
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", coded.BoltErrorCode())
	require.Contains(t, err.Error(), "waited 300ms")
	require.GreaterOrEqual(t, elapsed, timeout)
	require.Less(t, elapsed, timeout+uniqueKeyLockWorkerTimeout)

	locks, waits := uniqueKeyLockRegistrySizes(schema)
	require.Equal(t, 2, locks, "the holder's key and the key the waiter took before waiting")
	require.Zero(t, waits)
	heldKey := mergeLockKey(t, schema, 1)
	schema.uniqueConstraintCommitLocksMu.Lock()
	held := schema.uniqueConstraintCommitLocks[heldKey]
	var refs, waitingWriters int
	var owner string
	if held != nil {
		refs, waitingWriters, owner = held.refs, held.waitingWriters, held.owner
	}
	schema.uniqueConstraintCommitLocksMu.Unlock()
	require.NotNil(t, held)
	require.Equal(t, 1, refs)
	require.Zero(t, waitingWriters)
	require.Equal(t, holder.ID, owner)

	records := log.prolonged(t)
	require.Len(t, records, 1)
	require.Equal(t, "WARN", records[0]["level"])
	require.Equal(t, "test", records[0]["database"])
	require.Equal(t, waiter.ID, records[0]["transaction_id"])
	require.Equal(t, holder.ID, records[0]["holder_transaction_id"])
	require.Equal(t, "U", records[0]["label"])
	require.Equal(t, "k", records[0]["property"])
	require.Equal(t, float64(timeout.Milliseconds()), records[0]["timeout_ms"])
	require.GreaterOrEqual(t, records[0]["waited_ms"], float64(logAfter.Milliseconds()))

	require.NoError(t, waiter.Rollback())
	locks, _ = uniqueKeyLockRegistrySizes(schema)
	require.Equal(t, 1, locks)
	require.NoError(t, holder.Rollback())
	locks, _ = uniqueKeyLockRegistrySizes(schema)
	require.Zero(t, locks)
}

// TestMergeKeyWaitOptions: each bound applies on its own. Without a timeout
// the wait still ends with its context and is logged; without a log
// threshold the timeout still applies and nothing is logged; with neither
// the wait is the unbounded one it was. A holder that ends lets a bounded
// waiter take the key.
func TestMergeKeyWaitOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		options LockWaitOptions
		logged  int
	}{
		{"log only", LockWaitOptions{LogAfter: 50 * time.Millisecond}, 1},
		{"timeout only", LockWaitOptions{Timeout: 150 * time.Millisecond}, 0},
		{"unbounded", LockWaitOptions{}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, _, log := newMergeKeyWaitEngine(t, test.options)
			holder := beginMergeKeyTx(t, engine)
			defer holder.Rollback()
			require.NoError(t, holder.PrepareMergeNodeForWrite(t.Context(), []string{"U"}, mergeKeyProps(1)))
			waiter := beginMergeKeyTx(t, engine)
			defer waiter.Rollback()
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			err := waiter.PrepareMergeNodeForWrite(ctx, []string{"U"}, mergeKeyProps(1))
			if test.options.Timeout > 0 {
				require.ErrorIs(t, err, ErrLockWaitTimeout)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Len(t, log.prolonged(t), test.logged)
		})
	}

	engine, _, _ := newMergeKeyWaitEngine(t, LockWaitOptions{Timeout: time.Minute, LogAfter: time.Minute})
	holder := beginMergeKeyTx(t, engine)
	require.NoError(t, holder.PrepareMergeNodeForWrite(t.Context(), []string{"U"}, mergeKeyProps(1)))
	waits := make(chan struct{}, 1)
	t.Cleanup(SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	waiter := beginMergeKeyTx(t, engine)
	defer waiter.Rollback()
	worker := newUniqueKeyLockWorker(func() error {
		return waiter.PrepareMergeNodeForWrite(t.Context(), []string{"U"}, mergeKeyProps(1))
	})
	<-waits
	require.NoError(t, holder.Rollback())
	worker.await(t, "the waiter after the holder rolled back")
}

// TestMergeKeyWaitTimeoutCountsFromItsStart: a wait woken while its key
// stays held keeps the timeout and the single log record of its first
// block. An exclusive MERGE waiting for a key two shared owners read wakes
// when one stops reading and blocks again on the other.
func TestMergeKeyWaitTimeoutCountsFromItsStart(t *testing.T) {
	const timeout = 600 * time.Millisecond
	sm := NewSchemaManager()
	log := &mergeKeyWaitLog{}
	key := uniqueConstraintLockKey{label: "L", property: "k", value: "a"}
	shared := key
	shared.shared = true
	releaseFirst, err := sm.acquireUniqueConstraintCommitLocks(t.Context(), "reader-1", []uniqueConstraintLockKey{shared})
	require.NoError(t, err)
	defer releaseFirst()
	releaseSecond, err := sm.acquireUniqueConstraintCommitLocks(t.Context(), "reader-2", []uniqueConstraintLockKey{shared})
	require.NoError(t, err)
	defer releaseSecond()

	blocks := make(chan struct{}, 8)
	t.Cleanup(SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case blocks <- struct{}{}:
		default:
		}
	}))
	wait := uniqueKeyLockWait{LockWaitOptions: LockWaitOptions{Timeout: timeout, LogAfter: 100 * time.Millisecond}, log: slog.New(slog.NewJSONHandler(log, nil)), database: "test"}
	started := time.Now()
	worker := newUniqueKeyLockWorker(func() error {
		release, err := sm.acquireUniqueConstraintLocks(t.Context(), "merge", []uniqueConstraintLockKey{key}, &wait)
		if err == nil {
			release()
			return errors.New("acquired a key two owners read")
		}
		if !errors.Is(err, ErrLockWaitTimeout) {
			return err
		}
		return nil
	})
	<-blocks
	time.Sleep(timeout * 2 / 3)
	releaseFirst()
	<-blocks // woken, and blocked again on the second reader
	worker.await(t, "the bounded wait")
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, timeout)
	require.Less(t, elapsed, timeout+timeout/2, "the timeout restarted when the wait woke")
	records := log.prolonged(t)
	require.Len(t, records, 1)
	require.Equal(t, "", records[0]["holder_transaction_id"], "only readers hold the key")
	locks, waits := uniqueKeyLockRegistrySizes(sm)
	require.Equal(t, 1, locks)
	require.Zero(t, waits)
}

// TestCommitKeyWaitIsNotBoundedByTheMergeTimeout: the transaction timeout
// bounds MERGE admission's wait only. A commit's wait for a key a MERGE
// holds ends with its context, as before, and is not logged.
func TestCommitKeyWaitIsNotBoundedByTheMergeTimeout(t *testing.T) {
	engine, schema, log := newMergeKeyWaitEngine(t, LockWaitOptions{Timeout: 100 * time.Millisecond, LogAfter: 50 * time.Millisecond})
	holder := beginMergeKeyTx(t, engine)
	defer holder.Rollback()
	require.NoError(t, holder.PrepareMergeNodeForWrite(t.Context(), []string{"U"}, mergeKeyProps(1)))
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := schema.acquireUniqueConstraintCommitLocks(ctx, "committer", []uniqueConstraintLockKey{mergeLockKey(t, schema, 1)})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrLockWaitTimeout)
	require.GreaterOrEqual(t, time.Since(started), 400*time.Millisecond)
	require.Empty(t, log.prolonged(t))
}
