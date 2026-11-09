package storage

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// cascadeBoundaryWaitChain names the pkg/storage functions a goroutine is
// waiting in, innermost first, starting at the admission observer that reports
// the wait. A boundary test asserts the exact commit phase its window belongs
// to instead of ordering concurrent commits by timing.
func cascadeBoundaryWaitChain() string {
	var pcs [32]uintptr
	n := runtime.Callers(0, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var names []string
	for {
		frame, more := frames.Next()
		name := frame.Function
		if index := strings.LastIndex(name, "/pkg/storage."); index >= 0 {
			names = append(names, name[index+len("/pkg/storage."):])
		}
		if !more {
			break
		}
	}
	start := 0
	for i, name := range names {
		if strings.Contains(name, "observeWriteAdmissionWait") {
			start = i + 1
			break
		}
	}
	return strings.Join(names[start:], " < ")
}

// setupCascadeBoundaryGraph creates the release regression graph: test:n2 and
// test:n3 joined by test:r2, so deleting either endpoint cascades the same
// relationship. Under write-behind an unrelated buffered write keeps one
// generation unapplied, so a commit runs its buffered conflict scan.
func setupCascadeBoundaryGraph(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []NodeID{"test:n2", "test:n3"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:r2", StartNode: "test:n2", EndNode: "test:n3",
		Type: "LINK", Properties: map[string]any{"v": int64(1)}}))
	require.NoError(t, engine.FlushWriteBehind(ctx))
	if engine.writeBehind != nil {
		bufferedCreate(t, engine, &Node{ID: "test:unrelated", Properties: map[string]any{"v": int64(1)}})
		require.Equal(t, uint64(1), engine.writeBehind.readEpoch.Load()&1,
			"an unapplied generation must exist so the commit runs its buffered conflict scan")
	}
}

// requireCascadeBoundaryCounts asserts the derived relationship counters count
// exactly the relationships the two deletions left: one removal, counted once.
func requireCascadeBoundaryCounts(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, engine.FlushWriteBehind(ctx))
	count, err := engine.EdgeCountByType(ctx, "LINK")
	require.NoError(t, err)
	require.Equal(t, int64(0), count, "the relationship's removal is counted exactly once")
	total, err := engine.EdgeCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), total)
	_, err = engine.GetStoredEdge(ctx, "test:r2")
	require.ErrorIs(t, err, ErrNotFound)
}

// requireCascadeEdgeConflict asserts err is the conflict on the cascaded
// relationship itself, named by the identifier the validation reported.
func requireCascadeEdgeConflict(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrConflict, "a cascade over a removed relationship is an ordinary conflict")
	var localized *localization.LocalizedError
	require.True(t, errors.As(err, &localized), "conflict keeps its message identity, got %v", err)
	require.Equal(t, localization.MessageStorageTransactionEdgeChanged, localized.Message.ID)
	require.Equal(t, "test:r2", localized.Message.Data["EdgeID"],
		"the conflict names the relationship the cascade removes")
}

// TestNodeCascadeValidationWindowBlocksPeerAcknowledgement covers the residual
// claim on this correction: a peer could still be acknowledged inside a
// buffered commit's validation window, after the commit's buffered conflict
// scan and before its own commit enters the buffer, so both commits would
// count the same relationship removal.
//
// The window is reproduced deterministically: the buffer's write lock holds the
// primary cascade commit inside bufferedWriteConflictLocked, its own buffered
// conflict scan. The peer cascade then tries to commit there. It must wait for
// the exclusive entity authority the primary already holds on the relationship
// it cascades, so it cannot be acknowledged in the window; once the primary is
// acknowledged and releases that authority, the peer's own validation observes
// the acknowledged removal and rejects it as an ordinary conflict.
//
// That window belongs to the validation phase of a commit that runs its
// buffered conflict scan, so the unrelated generation the scan walks must still
// be unapplied when the commit validates. This test therefore owns the replay
// owner instead of running one: a live write-behind worker drains that
// generation whenever its queued flush signal reaches it, and a drained
// generation legitimately removes the scan - the commit then reaches its buffer
// append directly, so the first admission wait observed is the AppendCommit
// append and not the scan. That is which phase the commit is in, not whether
// the guarantee holds, so the prerequisite is fixed here rather than timed.
func TestNodeCascadeValidationWindowBlocksPeerAcknowledgement(t *testing.T) {
	ctx := context.Background()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	setupCascadeBoundaryGraph(t, engine)
	require.NotNil(t, engine.writeBehind)

	primary, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = primary.Rollback() })
	require.NoError(t, primary.SetImplicit(true))
	require.NoError(t, primary.DeleteGraphNode(ctx, "test:n3", CascadeEdges))

	peer, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Rollback() })
	require.NoError(t, peer.SetImplicit(true))
	require.NoError(t, peer.DeleteGraphNode(ctx, "test:n2", CascadeEdges))

	// The buffered conflict scan reads the buffer under its read lock, so
	// holding the write lock parks the primary commit inside that scan.
	engine.writeBehind.mu.Lock()
	var releaseOnce sync.Once
	releaseWindow := func() { releaseOnce.Do(engine.writeBehind.mu.Unlock) }
	t.Cleanup(releaseWindow)
	require.Equal(t, uint64(1), engine.writeBehind.readEpoch.Load()&1,
		"the window's commit must still find the unrelated generation unapplied, otherwise it skips its buffered conflict scan")

	parked := make(chan string, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner != &engine.writeBehind.mu {
			return
		}
		select {
		case parked <- cascadeBoundaryWaitChain():
		default:
		}
	}))

	primaryDone := make(chan error, 1)
	go func() { primaryDone <- primary.CommitContext(ctx) }()
	var waitChain string
	select {
	case waitChain = <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("primary commit did not reach its buffered conflict scan")
	}
	require.Contains(t, waitChain, "(*WriteBehindBuffer).commitsAfterBegin < (*BadgerTransaction).bufferedWriteConflictLocked",
		"the window under test is the buffered conflict scan of the primary commit, got %s", waitChain)

	peerWait := make(chan struct{}, 1)
	t.Cleanup(SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case peerWait <- struct{}{}:
		default:
		}
	}))

	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.CommitContext(ctx) }()
	select {
	case <-peerWait:
	case <-time.After(10 * time.Second):
		t.Fatal("peer commit did not wait for the cascaded relationship's entity authority")
	}
	// The wait is on the cascaded relationship's own entity key, held by the
	// primary commit: the relationship every derived count delta of the peer's
	// cascade would name.
	schema := engine.GetSchemaForNamespace("test")
	schema.uniqueConstraintCommitLocksMu.Lock()
	relationshipLock := schema.uniqueConstraintCommitLocks[uniqueConstraintLockKey{entityKind: 2, entityID: "test:r2"}]
	waiter := schema.uniqueConstraintLockWaits[peer.ID]
	holder := ""
	if relationshipLock != nil {
		holder = relationshipLock.owner
	}
	schema.uniqueConstraintCommitLocksMu.Unlock()
	require.NotNil(t, relationshipLock, "the cascaded relationship has entity authority")
	require.Equal(t, primary.ID, holder, "the primary cascade commit holds the relationship's entity authority")
	require.Same(t, relationshipLock, waiter, "the peer waits on that authority inside the validation window")
	select {
	case err := <-peerDone:
		t.Fatalf("peer commit returned %v inside the primary's validation window", err)
	default:
	}

	releaseWindow()
	select {
	case err := <-primaryDone:
		require.NoError(t, err, "the primary cascade commit owns the relationship and must be acknowledged")
	case <-time.After(10 * time.Second):
		t.Fatal("primary commit did not finish after the window closed")
	}
	select {
	case err := <-peerDone:
		requireCascadeEdgeConflict(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("peer commit did not finish after the window closed")
	}

	requireCascadeBoundaryCounts(t, engine)
}

// TestNodeCascadeValidationBoundaryOrdersAcceptedWrites covers both storage
// orders the window sits between, write-behind and direct, with the competing
// write issued through the public engine API:
//
//   - peer first: the peer removes the relationship while the cascade is staged
//     but uncommitted, so the cascade is an ordinary conflict on a relationship
//     it writes;
//   - cascade first: the acknowledged cascade removed the relationship, so the
//     peer's ordinary node deletion afterwards is not newly rejected - it reads
//     the acknowledged state and neither re-counts the removal nor rejects.
func TestNodeCascadeValidationBoundaryOrdersAcceptedWrites(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		writeBehind := writeBehind
		name := "writeBehind=false"
		if writeBehind {
			name = "writeBehind=true"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("peer first", func(t *testing.T) {
				ctx := context.Background()
				engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: !writeBehind})
				setupCascadeBoundaryGraph(t, engine)

				cascade, err := engine.BeginTransaction(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = cascade.Rollback() })
				require.NoError(t, cascade.SetImplicit(true))
				require.NoError(t, cascade.DeleteGraphNode(ctx, "test:n3", CascadeEdges))

				require.NoError(t, engine.DeleteNode(ctx, "test:n2"))
				require.NoError(t, engine.FlushWriteBehind(ctx))

				requireCascadeEdgeConflict(t, cascade.CommitContext(ctx))
				requireCascadeBoundaryCounts(t, engine)
			})

			t.Run("cascade first", func(t *testing.T) {
				ctx := context.Background()
				engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: !writeBehind})
				setupCascadeBoundaryGraph(t, engine)

				cascade, err := engine.BeginTransaction(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = cascade.Rollback() })
				require.NoError(t, cascade.SetImplicit(true))
				require.NoError(t, cascade.DeleteGraphNode(ctx, "test:n3", CascadeEdges))
				require.NoError(t, cascade.CommitContext(ctx))
				require.NoError(t, engine.FlushWriteBehind(ctx))

				require.NoError(t, engine.DeleteNode(ctx, "test:n2"),
					"a node deletion that reads the acknowledged removal is not a conflict")
				requireCascadeBoundaryCounts(t, engine)
			})
		})
	}
}
