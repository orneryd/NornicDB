package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Two edge commits with different identities may share an unchanged endpoint.
// Preparation retains all entity authority until commit or abort, so this
// exercises overlapping commit windows rather than merely concurrent staging.
func TestIndependentEdgeCommitsShareEndpointAuthority(t *testing.T) {
	b := newTestEngine(t)
	for _, id := range []NodeID{"test:hub", "test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	makeTxn := func(id EdgeID, end NodeID) *BadgerTransaction {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.CreateEdge(&Edge{ID: id, StartNode: "test:hub", EndNode: end, Type: "CONNECTS"}))
		return tx
	}
	first := makeTxn("test:first", "test:a")
	second := makeTxn("test:second", "test:b")
	p1, err := first.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "shared-endpoint-first"})
	require.NoError(t, err)
	defer p1.Abort()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		p   *PreparedEntityCommit
		err error
	}
	done := make(chan result, 1)
	go func() {
		p, e := second.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "shared-endpoint-second"})
		done <- result{p, e}
	}()
	got := <-done
	if got.err != nil {
		p1.Abort()
		t.Fatalf("independent edge prepare waited for shared endpoint: %v", got.err)
	}
	defer got.p.Abort()
	require.NoError(t, p1.Commit(nil))
	require.NoError(t, got.p.Commit(nil))
	_, err = b.GetEdge("test:first")
	require.NoError(t, err)
	_, err = b.GetEdge("test:second")
	require.NoError(t, err)
}

func TestSharedEndpointOwnershipExcludesWriterAndUnblocksReaders(t *testing.T) {
	sm := NewSchemaManager()
	read := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub", shared: true}
	write := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub"}
	acquire := func(ctx context.Context, owner string, key uniqueConstraintLockKey) (func(), error) {
		return sm.acquireUniqueConstraintCommitLocks(ctx, owner, []uniqueConstraintLockKey{key})
	}
	first, err := acquire(context.Background(), "one", read)
	require.NoError(t, err)
	defer first()
	second, err := acquire(context.Background(), "two", read)
	require.NoError(t, err)
	defer second()
	type result struct {
		release func()
		err     error
	}
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	defer cancelWriter()
	writerDone := make(chan result, 1)
	go func() { release, e := acquire(writerCtx, "writer", write); writerDone <- result{release, e} }()
	waiting := func(owner string) bool {
		sm.uniqueConstraintCommitLocksMu.Lock()
		defer sm.uniqueConstraintCommitLocksMu.Unlock()
		return sm.uniqueConstraintLockWaits[owner] != nil
	}
	require.Eventually(t, func() bool { return waiting("writer") }, time.Second, time.Millisecond)
	readerCtx, cancelReader := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelReader()
	readerDone := make(chan result, 1)
	go func() { release, e := acquire(readerCtx, "three", read); readerDone <- result{release, e} }()
	require.Eventually(t, func() bool { return waiting("three") }, time.Second, time.Millisecond)
	cancelWriter()
	resultWriter := <-writerDone
	require.ErrorIs(t, resultWriter.err, context.Canceled)
	resultReader := <-readerDone
	require.NoError(t, resultReader.err, "canceling the last queued writer must wake blocked readers")
	resultReader.release()
	first()
	second()
	exclusive, err := acquire(context.Background(), "writer", write)
	require.NoError(t, err)
	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancelBlocked()
	_, err = acquire(blockedCtx, "other", read)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	exclusive()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
	require.Empty(t, sm.uniqueConstraintLockWaitShared)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

func TestSharedEndpointReentrancyUpgradeAndRecovery(t *testing.T) {
	sm := NewSchemaManager()
	read := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub", shared: true}
	write := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub"}
	acquire := func(owner string, keys ...uniqueConstraintLockKey) (func(), error) {
		return sm.acquireUniqueConstraintCommitLocks(context.Background(), owner, keys)
	}
	first, err := acquire("a", read)
	require.NoError(t, err)
	upgrade, err := acquire("a", write)
	require.NoError(t, err)
	retained, err := acquire("a", read)
	require.NoError(t, err)
	upgrade()
	other, err := acquire("b", read)
	require.NoError(t, err)
	sm.markCommitOwnerRecoveryPending("b")
	_, err = acquire("c", read)
	require.ErrorIs(t, err, ErrConflict)
	again, err := acquire("a", read)
	require.NoError(t, err)
	again()
	other()
	retained()
	first()
	after, err := acquire("c", read)
	require.NoError(t, err)
	after()
	// A request containing both modes for one key must take exclusive mode.
	combined, err := acquire("mixed", read, write)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = sm.acquireUniqueConstraintCommitLocks(ctx, "not-mixed", []uniqueConstraintLockKey{read})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	combined()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

func TestSharedEndpointReleasesWakeEligibleUpgrades(t *testing.T) {
	sm := NewSchemaManager()
	read := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub", shared: true}
	write := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub"}
	first, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "first", []uniqueConstraintLockKey{read})
	require.NoError(t, err)
	defer first()
	second, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "second", []uniqueConstraintLockKey{read})
	require.NoError(t, err)
	defer second()
	type result struct {
		release func()
		err     error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan result, 1)
	go func() {
		release, err := sm.acquireUniqueConstraintCommitLocks(ctx, "first", []uniqueConstraintLockKey{write})
		done <- result{release, err}
	}()
	require.Eventually(t, func() bool {
		sm.uniqueConstraintCommitLocksMu.Lock()
		defer sm.uniqueConstraintCommitLocksMu.Unlock()
		return sm.uniqueConstraintLockWaits["first"] != nil
	}, time.Second, time.Millisecond)
	// The upgrader's own read remains; only the conflicting secondary read
	// leaves. Its release must wake the writer even though one reader remains.
	second()
	got := <-done
	require.NoError(t, got.err)
	got.release()
	first()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

func TestSharedEndpointBatchDeduplicationAcrossAcquisitions(t *testing.T) {
	sm := NewSchemaManager()
	keys := make([]uniqueConstraintLockKey, 0, 82)
	for i := 0; i < 80; i++ {
		keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: fmt.Sprintf("test:n%d", i), shared: true})
	}
	// The later exclusive request must dominate the earlier shared one.
	keys = append(keys, uniqueConstraintLockKey{entityKind: 1, entityID: "test:n0"})
	for i := 0; i < 3; i++ {
		release, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "batch", keys)
		require.NoError(t, err)
		wait, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err = sm.acquireUniqueConstraintCommitLocks(wait, "reader", []uniqueConstraintLockKey{keys[0]})
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		release()
	}
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

func TestSharedEndpointDeadlockFollowsAllReaders(t *testing.T) {
	sm := NewSchemaManager()
	a := uniqueConstraintLockKey{entityKind: 1, entityID: "test:a", shared: true}
	b := uniqueConstraintLockKey{entityKind: 1, entityID: "test:b", shared: true}
	releaseA, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "a", []uniqueConstraintLockKey{a})
	require.NoError(t, err)
	defer releaseA()
	releaseB, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "b", []uniqueConstraintLockKey{b})
	require.NoError(t, err)
	defer releaseB()
	b.shared = false
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		release, e := sm.acquireUniqueConstraintCommitLocks(ctx, "a", []uniqueConstraintLockKey{b})
		if e == nil {
			release()
		}
		done <- e
	}()
	require.Eventually(t, func() bool {
		sm.uniqueConstraintCommitLocksMu.Lock()
		defer sm.uniqueConstraintCommitLocksMu.Unlock()
		return sm.uniqueConstraintLockWaits["a"] != nil
	}, time.Second, time.Millisecond)
	a.shared = false
	_, err = sm.acquireUniqueConstraintCommitLocks(ctx, "b", []uniqueConstraintLockKey{a})
	require.ErrorIs(t, err, ErrDeadlock)
	releaseB()
	require.NoError(t, <-done)
	releaseA()
	sm.uniqueConstraintCommitLocksMu.Lock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
	sm.uniqueConstraintCommitLocksMu.Unlock()
}

func TestSharedEndpointExcludesNodeMutations(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete=%v", deletion), func(t *testing.T) {
			b := newTestEngine(t)
			for _, id := range []NodeID{"test:hub", "test:end"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Before"}})
				require.NoError(t, err)
			}
			edgeTx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, edgeTx.SetNamespace("test"))
			require.NoError(t, edgeTx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:hub", EndNode: "test:end", Type: "LINK"}))
			nodeTx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, nodeTx.SetNamespace("test"))
			if deletion {
				require.NoError(t, nodeTx.DeleteGraphNode("test:hub", CascadeEdges))
			} else {
				require.NoError(t, nodeTx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:hub", Labels: []string{"After"}}, Policy: ClientNodeEdits}))
			}
			prepared, err := edgeTx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: "edge-held"})
			require.NoError(t, err)
			defer prepared.Abort()
			retain, err := prepared.RetainAcceptedAuthority()
			require.NoError(t, err)
			defer retain()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				p, e := nodeTx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "node-waiting"})
				if p != nil {
					p.Abort()
				}
				done <- e
			}()
			schema := b.GetSchemaForNamespace("test")
			require.Eventually(t, func() bool {
				schema.uniqueConstraintCommitLocksMu.Lock()
				defer schema.uniqueConstraintCommitLocksMu.Unlock()
				return schema.uniqueConstraintLockWaits[nodeTx.ID] != nil
			}, time.Second, time.Millisecond, "node mutation must wait for endpoint authority")
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
			require.NoError(t, prepared.Commit(nil))
			if deletion {
				require.NoError(t, b.DeleteNode("test:hub"))
				_, err = b.GetEdge("test:e")
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:hub", Labels: []string{"After"}}}))
				n, err := b.EdgeCountByStartLabelInNamespace("test", "Before", "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(0), n)
				n, err = b.EdgeCountByStartLabelInNamespace("test", "After", "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(1), n)
			}
		})
	}
}

func TestSharedEndpointConcurrentBufferedCommitCounts(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{wal: true, manualDrain: true})
	for _, id := range []NodeID{"test:hub", "test:left", "test:right"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Endpoint"}})
		require.NoError(t, err)
	}
	for i := 0; i < 8; i++ {
		prepared := make([]*PreparedEntityCommit, 0, 2)
		for j, end := range []NodeID{"test:left", "test:right"} {
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.SetImplicit(true))
			edgeID := EdgeID(fmt.Sprintf("test:e-%d-%d", i, j))
			require.NoError(t, tx.CreateEdge(&Edge{ID: edgeID, StartNode: "test:hub", EndNode: end, Type: "LINK"}))
			p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: fmt.Sprintf("shared-commit-%d-%d", i, j)})
			require.NoError(t, err)
			prepared = append(prepared, p)
		}
		start := make(chan struct{})
		done := make(chan error, 2)
		for _, p := range prepared {
			go func(p *PreparedEntityCommit) { <-start; done <- p.Commit(nil) }(p)
		}
		close(start)
		require.NoError(t, <-done)
		require.NoError(t, <-done)
	}
	require.NoError(t, b.FlushWriteBehind())
	for i := 0; i < 8; i++ {
		for j := 0; j < 2; j++ {
			_, err := b.GetEdge(EdgeID(fmt.Sprintf("test:e-%d-%d", i, j)))
			require.NoError(t, err)
		}
	}
	total, err := b.EdgeCountByTypeInNamespace("test", "LINK")
	require.NoError(t, err)
	require.Equal(t, int64(16), total)
	startCount, err := b.EdgeCountByStartLabelInNamespace("test", "Endpoint", "LINK")
	require.NoError(t, err)
	require.Equal(t, int64(16), startCount)
	endCount, err := b.EdgeCountByEndLabelInNamespace("test", "Endpoint", "LINK")
	require.NoError(t, err)
	require.Equal(t, int64(16), endCount)
}

func TestSharedEndpointSecondaryReaderReentersAfterPrimaryLeaves(t *testing.T) {
	sm := NewSchemaManager()
	read := uniqueConstraintLockKey{entityKind: 1, entityID: "test:hub", shared: true}
	write := read
	write.shared = false
	first, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "first", []uniqueConstraintLockKey{read})
	require.NoError(t, err)
	second, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "second", []uniqueConstraintLockKey{read})
	require.NoError(t, err)
	first()
	retained, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "second", []uniqueConstraintLockKey{read})
	require.NoError(t, err)
	second()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = sm.acquireUniqueConstraintCommitLocks(ctx, "writer", []uniqueConstraintLockKey{write})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	retained()
	exclusive, err := sm.acquireUniqueConstraintCommitLocks(context.Background(), "writer", []uniqueConstraintLockKey{write})
	require.NoError(t, err)
	exclusive()
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	require.Empty(t, sm.uniqueConstraintCommitLocks)
	require.Empty(t, sm.uniqueConstraintLockWaits)
}
