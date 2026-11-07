package storage

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitPreparationBufferedConflictAdmissionCancellation(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		plain, create bool
		phase         string
	}{
		{name: "prepare-receipt", phase: ".observeBufferedCommitReceipt"},
		{name: "plain-preimage", plain: true, phase: ".validateMutationPreimagesLocked"},
		{name: "plain-conflict", plain: true, create: true, phase: ".commitsAfterBegin"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			plain := testCase.plain
			b := newWriteBehindTestEngine(t, time.Hour)
			_, err := b.CreateNode(t.Context(), &Node{ID: "test:target", Properties: map[string]any{"payload": "before"}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			if testCase.create {
				// A CREATE has no committed-node preimage. It reaches the
				// original buffered-conflict admission rather than the earlier
				// update-preimage read, so both public phases remain covered.
				_, err = tx.CreateNode(t.Context(), &Node{ID: "test:unaccepted", Properties: map[string]any{"payload": "unaccepted"}})
				require.NoError(t, err)
			} else {
				node, readErr := tx.GetNode(t.Context(), "test:target")
				require.NoError(t, readErr)
				before := CopyNode(node)
				node.Properties["payload"] = "unaccepted"
				require.NoError(t, tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}))
			}
			// A real later acknowledged native commit keeps the buffered conflict reader
			// active. Its unrelated identity cannot legitimately conflict with target.
			bufferedCreate(t, b, &Node{ID: "test:acknowledged", Properties: map[string]any{"payload": "peer"}})
			require.Equal(t, uint64(1), b.writeBehind.readEpoch.Load()&1)
			expectedPhase := testCase.phase
			ctx, cancel := context.WithCancel(t.Context())
			type admission struct {
				exact bool
				ctx   context.Context
			}
			entered := make(chan admission, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner != &b.writeBehind.mu {
					return
				}
				var pcs [32]uintptr
				n := runtime.Callers(2, pcs[:])
				frames := runtime.CallersFrames(pcs[:n])
				exact := false
				for {
					frame, more := frames.Next()
					if strings.HasSuffix(frame.Function, expectedPhase) {
						exact = true
					}
					if !more {
						break
					}
				}
				select {
				case entered <- admission{exact, seen}:
				default:
				}
			}))
			b.writeBehind.mu.Lock()
			var once sync.Once
			unlock := func() { once.Do(b.writeBehind.mu.Unlock) }
			var returnedPrepared atomic.Bool
			done := make(chan error, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("buffered conflict worker did not join")
				}
			})
			go func() {
				defer close(joined)
				var err error
				if plain {
					err = tx.CommitContext(ctx)
				} else {
					var p *PreparedEntityCommit
					p, err = tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "buffered-canceled"})
					if p != nil {
						returnedPrepared.Store(true)
						p.Abort()
					}
				}
				done <- err
			}()
			select {
			case event := <-entered:
				require.True(t, event.exact, "must reach the selected receipt, preimage or conflict admission owner")
			case <-time.After(5 * time.Second):
				t.Fatal("buffered conflict reader did not reach held admission")
			}
			cancel()
			var result error
			select {
			case result = <-done:
			case <-time.After(5 * time.Second):
				t.Error("buffered conflict preparation ignored cancellation while admission remained held")
				unlock()
				select {
				case result = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("buffered conflict reader did not finish after unlock")
				}
			}
			unlock()
			assert.ErrorIs(t, result, context.Canceled)
			assert.False(t, returnedPrepared.Load(), "canceled request must not return a prepared handoff")
			marker, markerErr := b.observeBufferedCommitReceipt(t.Context(), bufferedReceiptRead{namespace: "test", logicalID: "buffered-canceled"})
			require.NoError(t, markerErr)
			assert.False(t, marker.present, "canceled preparation must not publish its logical receipt")
			actual, err := b.GetNode(t.Context(), "test:target")
			require.NoError(t, err)
			assert.Equal(t, "before", actual.Properties["payload"])
			_, err = b.GetNode(t.Context(), "test:unaccepted")
			assert.ErrorIs(t, err, ErrNotFound, "canceled CREATE must not publish")
			peer, err := b.GetNode(t.Context(), "test:acknowledged")
			require.NoError(t, err)
			assert.Equal(t, "peer", peer.Properties["payload"])
			retry, err := b.BeginTransaction(t.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = retry.Rollback() })
			if testCase.create {
				_, err = retry.CreateNode(t.Context(), &Node{ID: "test:unaccepted", Properties: map[string]any{"payload": "healthy-buffered"}})
				require.NoError(t, err, "canceled identity remains available to a healthy retry")
			}
			retryNode, err := retry.GetNode(t.Context(), "test:target")
			require.NoError(t, err)
			retryBefore := CopyNode(retryNode)
			retryNode.Properties["payload"] = "healthy-buffered"
			require.NoError(t, retry.UpdateGraphNode(t.Context(), NodeUpdateRequest{Before: retryBefore, After: retryNode, Policy: ClientNodeEdits}))
			prepared, err := retry.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "buffered-canceled"})
			require.NoError(t, err)
			t.Cleanup(prepared.Abort)
			receipt, err := b.SubmitEntityCommit(t.Context(), prepared, nil)
			require.NoError(t, err)
			waitCtx, waitCancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer waitCancel()
			outcome := receipt.WaitOutcome(waitCtx)
			require.Equal(t, CommitApplied, outcome.State)
			require.NoError(t, outcome.Err)
			actual, err = b.GetNode(t.Context(), "test:target")
			require.NoError(t, err)
			require.Equal(t, "healthy-buffered", actual.Properties["payload"])
			if testCase.create {
				created, readErr := b.GetNode(t.Context(), "test:unaccepted")
				require.NoError(t, readErr)
				require.Equal(t, "healthy-buffered", created.Properties["payload"])
			}
		})
	}
}

func TestBufferedReceiptAndConflictScansObserveRealCancellation(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	begin, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = begin.Rollback() })
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		bufferedCreate(t, b, &Node{ID: id})
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	visits := 0
	err = b.writeBehind.commitsAfterBegin(ctx, begin.beginPresence, func(context.Context, MVCCVersion, []Operation) (bool, error) { visits++; cancel(); return true, nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, visits)
	visits = 0
	err = b.writeBehind.commitsAfterBegin(t.Context(), begin.beginPresence, func(context.Context, MVCCVersion, []Operation) (bool, error) { visits++; return false, nil })
	require.NoError(t, err)
	require.Equal(t, 1, visits, "visitor stop remains synchronous and honored")
	visits = 0
	err = b.writeBehind.commitsAfterBegin(t.Context(), begin.beginPresence, func(context.Context, MVCCVersion, []Operation) (bool, error) { visits++; return true, nil })
	require.NoError(t, err)
	require.Equal(t, 3, visits)
	poll := newIndexPollCancelContext(t, 2)
	b.writeBehind.mu.RLock()
	present, err := b.writeBehind.logicalReceiptPresentLocked(poll, "test", "absent")
	b.writeBehind.mu.RUnlock()
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, present)
	b.writeBehind.mu.RLock()
	present, err = b.writeBehind.logicalReceiptPresentLocked(t.Context(), "test", "absent")
	b.writeBehind.mu.RUnlock()
	require.NoError(t, err)
	require.False(t, present)
}

// Only polls from the staged-operation builder's canonical bulk-read owner
// count here. The first poll is the first record; cancellation at its second
// poll therefore occurs inside the owned loop, after256record visits, rather
// than at lock admission, a callback boundary, or the final exit check.
type bufferedOperationPollContext struct {
	context.Context
	cancel context.CancelFunc
	polls  atomic.Int64
}

func (c *bufferedOperationPollContext) Err() error {
	var pcs [48]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	builder, bulk := false, false
	for {
		f, more := frames.Next()
		builder = builder || strings.HasSuffix(f.Function, ".bufferedWriteConflictLocked.func1")
		bulk = bulk || (strings.Contains(f.Function, "bulkReadCancellation") && strings.HasSuffix(f.Function, ".check"))
		if !more {
			break
		}
	}
	if builder && bulk && c.polls.Add(1) == 2 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestBufferedConflictPreparationCancelsAtInnerBulkPoll(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for i := range bulkReadCancellationInterval*2 + 1 {
		_, err := tx.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("test:unaccepted%d", i))})
		require.NoError(t, err)
	}
	bufferedCreate(t, b, &Node{ID: "test:peer", Properties: map[string]any{"payload": "peer"}})
	real, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &bufferedOperationPollContext{Context: real, cancel: cancel}
	err = tx.CommitContext(ctx)
	assert.Equal(t, int64(2), ctx.polls.Load(), "must cancel at the actual second inner bulk poll")
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, real.Err(), context.Canceled)
	count, countErr := b.NodeCount(t.Context())
	require.NoError(t, countErr)
	assert.Equal(t, int64(1), count, "none of the staged operation population may publish")
	for _, id := range []NodeID{"test:unaccepted0", NodeID(fmt.Sprintf("test:unaccepted%d", bulkReadCancellationInterval*2))} {
		_, err = b.GetNode(t.Context(), id)
		assert.ErrorIs(t, err, ErrNotFound)
	}
	prepareContextRetry(t, b, "test:peer", "healthy-inner-poll")
}
