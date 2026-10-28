package multidb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The durable storage owner controls actual Sync and purge boundaries. It has
// no transparent-unwrapper method that could bypass those storage boundaries.
type catalogDropBarrierNative struct {
	*storage.BadgerEngine
	closeEntered     chan struct{}
	closing          atomic.Bool
	postCloseCapture atomic.Int32
	syncResults      chan error
	syncCalls        atomic.Int32
	purgeCalls       atomic.Int32
	blockSync        int32
	failSync         int32
	syncEntered      chan struct{}
	syncPermit       chan struct{}
	purgeEntered     chan int32
	purgePermits     []chan struct{}
}

func (e *catalogDropBarrierNative) SetChangeCapture(namespace string, mode storage.ChangeCaptureMode) {
	if e.closing.Load() && mode != storage.ChangeCaptureOff {
		e.postCloseCapture.Add(1)
		return
	}
	e.BadgerEngine.SetChangeCapture(namespace, mode)
}
func (e *catalogDropBarrierNative) Close() error {
	e.closing.Store(true)
	if e.closeEntered != nil {
		close(e.closeEntered)
		e.closeEntered = nil
	}
	return e.BadgerEngine.Close()
}
func (e *catalogDropBarrierNative) Sync() (err error) {
	defer func() {
		if e.syncResults != nil {
			e.syncResults <- err
		}
	}()
	call := e.syncCalls.Add(1)
	if call == e.blockSync {
		close(e.syncEntered)
		<-e.syncPermit
	}
	if call == e.failSync {
		return storage.ErrConflict
	}
	return e.BadgerEngine.Sync()
}
func (e *catalogDropBarrierNative) DeleteByPrefix(prefix string) (int64, int64, error) {
	call := e.purgeCalls.Add(1)
	if e.purgeEntered != nil {
		e.purgeEntered <- call
		if int(call) <= len(e.purgePermits) {
			<-e.purgePermits[call-1]
		}
	}
	return e.BadgerEngine.DeleteByPrefix(prefix)
}
func newCatalogDropBarrier(t *testing.T) (*DatabaseManager, *f25CatalogReceiptProbe, *catalogDropBarrierNative, string) {
	t.Helper()
	dir := t.TempDir()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
	require.NoError(t, err)
	fixtureowner.Own(t, base)
	native := &catalogDropBarrierNative{BadgerEngine: base}
	probe := &f25CatalogReceiptProbe{Engine: native, native: base, ownerContext: t.Context()}
	manager, err := NewDatabaseManager(t.Context(), probe, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
	view, err := manager.GetStorage("target")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "old"})
	require.NoError(t, err)
	return manager, probe, native, dir
}

func TestCatalogDropReceiptOwnsCompletionSync(t *testing.T) {
	manager, _, native, _ := newCatalogDropBarrier(t)
	native.blockCompletionSync()
	result := make(chan error, 1)
	go func() { result <- manager.DropDatabase(t.Context(), "target") }()
	<-native.syncEntered
	assert.Zero(t, len(manager.catalogGate), "physical receipt must retain catalog serialization through completion Sync")
	close(native.syncPermit)
	require.NoError(t, <-result)
	assert.False(t, manager.Exists("target"))
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
	fresh, err := manager.GetStorage("target")
	require.NoError(t, err)
	_, err = fresh.CreateNode(&storage.Node{ID: "fresh"})
	require.NoError(t, err)
	_, err = fresh.GetNode("fresh")
	require.NoError(t, err)
}

func TestCatalogDropDurabilityFollowsPhysicalOutcome(t *testing.T) {
	for _, test := range []struct {
		phase      string
		faultAfter int
		state      storage.CommitOutcomeState
		wantSync   int32
	}{
		{"rejected", 1, storage.CommitDiscarded, 0}, {"pending-discarded", 1, storage.CommitDiscarded, 0},
		{"applied-ack-error", 1, storage.CommitApplied, 1}, {"applied-ack-error", 2, storage.CommitApplied, 2},
		{"accepted-wait", 2, storage.CommitApplied, 2},
	} {
		t.Run(fmt.Sprintf("%s/%d", test.phase, test.faultAfter), func(t *testing.T) {
			manager, probe, native, _ := newCatalogDropBarrier(t)
			probe.mode = test.phase
			probe.faultAfter = test.faultAfter
			if test.phase == "accepted-wait" {
				probe.submitted = make(chan struct{})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- manager.DropDatabase(ctx, "target") }()
				<-probe.submitted
				cancel()
				var accepted *storage.AcceptedCommitError
				require.ErrorAs(t, <-result, &accepted)
				require.ErrorIs(t, accepted, context.Canceled)
				require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: test.state, Err: storage.ErrConflict}))
			} else {
				require.Error(t, manager.DropDatabase(t.Context(), "target"))
				if test.phase == "pending-discarded" {
					require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: test.state, Err: storage.ErrConflict}))
				}
			}
			assert.Equal(t, test.wantSync, native.syncCalls.Load(), "only physical Applied DROP receipts Sync, including acknowledgement error and canceled waiter")
		})
	}
}

func TestCatalogDropSyncFailureRetainsAbsentNameFence(t *testing.T) {
	for _, recovery := range []string{"retry", "reopen"} {
		t.Run(recovery, func(t *testing.T) {
			manager, _, native, dir := newCatalogDropBarrier(t)
			native.failSync = 2
			require.ErrorIs(t, manager.DropDatabase(t.Context(), "target"), storage.ErrConflict)
			assert.False(t, manager.Exists("target"), "Applied catalog removal must not resurrect metadata")
			assert.True(t, manager.pendingDrops["target"], "failed durability acknowledgement retains unavailable-name fence")
			purges := native.purgeCalls.Load()
			if !assert.ErrorIs(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}), ErrDatabaseExists, "pending name cannot be reused before durable completion") {
				return
			}
			assert.ErrorIs(t, manager.CreateCompositeDatabase(t.Context(), "target", []ConstituentRef{{Alias: "a", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"}}), ErrDatabaseExists)
			native.failSync = 3
			require.ErrorIs(t, manager.DropDatabase(t.Context(), "target"), storage.ErrConflict, "renewed completion Sync failure retains the same fence")
			assert.False(t, manager.Exists("target"))
			assert.True(t, manager.pendingDrops["target"])
			assert.Equal(t, purges, native.purgeCalls.Load(), "missing-record retries never purge")
			native.failSync = 0
			if recovery == "retry" {
				require.NoError(t, manager.DropDatabase(t.Context(), "target"))
				assert.Equal(t, purges, native.purgeCalls.Load(), "missing-record pending completion never purges another namespace")
			} else {
				// An unrelated accepted catalog write persists the same canonical pending
				// marker. Writable startup must finish it even though the record is absent.
				require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "peer"}))
				before, err := native.GetNode("system:databases:metadata")
				require.NoError(t, err)
				syncs := native.syncCalls.Load()
				standby, err := NewDatabaseManager(t.Context(), &identityReadOnlyEngine{Engine: native}, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = standby.Close() })
				assert.True(t, standby.pendingDrops["target"])
				assert.False(t, standby.Exists("target"))
				after, err := native.GetNode("system:databases:metadata")
				require.NoError(t, err)
				assert.Equal(t, before.Properties, after.Properties, "read-only startup never rewrites the absent-record fence")
				assert.Equal(t, syncs, native.syncCalls.Load())
				assert.Equal(t, purges, native.purgeCalls.Load())
				require.NoError(t, manager.Close())
				reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				fixtureowner.Own(t, reopened)
				recovered := &catalogDropBarrierNative{BadgerEngine: reopened}
				manager, err = NewDatabaseManager(t.Context(), recovered, nil)
				require.NoError(t, err)
				assert.Zero(t, recovered.purgeCalls.Load(), "startup completes absent-record pending marker without a namespace purge")
				t.Cleanup(func() { _ = manager.Close() })
				assert.False(t, manager.pendingDrops["target"])
				assert.True(t, manager.Exists("peer"))
			}
			assert.False(t, manager.Exists("target"))
			assert.False(t, manager.pendingDrops["target"])
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
			view, err := manager.GetStorage("target")
			require.NoError(t, err)
			_, err = view.CreateNode(&storage.Node{ID: "fresh"})
			require.NoError(t, err)
			_, err = view.GetNode("fresh")
			require.NoError(t, err)
		})
	}
}

type catalogDropAttemptContext struct {
	context.Context
	attempted chan struct{}
	once      sync.Once
}

func (ctx *catalogDropAttemptContext) Err() error {
	ctx.once.Do(func() { close(ctx.attempted) })
	return ctx.Context.Err()
}

func TestCatalogDropInvocationSerializesOldGenerationPurges(t *testing.T) {
	for _, cancelSecond := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelSecond), func(t *testing.T) {
			manager, _, native, _ := newCatalogDropBarrier(t)
			native.purgeEntered = make(chan int32, 2)
			native.purgePermits = []chan struct{}{make(chan struct{}), make(chan struct{})}
			close(native.purgePermits[1])
			first := make(chan error, 1)
			go func() { first <- manager.DropDatabase(t.Context(), "target") }()
			require.EqualValues(t, 1, <-native.purgeEntered)
			acquired := manager.dropAdmission.gate.TryAcquire(writeAdmissionCapacity)
			assert.False(t, acquired, "invocation owner must retain exclusive purge admission")
			if acquired {
				manager.dropAdmission.Unlock()
			}
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			secondCtx := &catalogDropAttemptContext{Context: parent, attempted: make(chan struct{})}
			second := make(chan error, 1)
			go func() { second <- manager.DropDatabase(secondCtx, "target") }()
			<-secondCtx.attempted
			if cancelSecond {
				cancel()
				require.ErrorIs(t, <-second, context.Canceled)
				assert.EqualValues(t, 1, native.purgeCalls.Load(), "canceled waiter never owns an old-generation purge")
			}
			close(native.purgePermits[0])
			require.NoError(t, <-first)
			if !cancelSecond {
				require.ErrorIs(t, <-second, ErrDatabaseNotFound, "second invocation resolves after the first completed generation")
			}
			assert.EqualValues(t, 1, native.purgeCalls.Load())
			require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
			view, err := manager.GetStorage("target")
			require.NoError(t, err)
			_, err = view.CreateNode(&storage.Node{ID: "fresh"})
			require.NoError(t, err)
			_, err = view.GetNode("fresh")
			require.NoError(t, err)
			assert.EqualValues(t, 1, native.purgeCalls.Load(), "no old DROP continuation survives the new CREATE")
		})
	}
}

func TestCatalogCanceledWaitDuringCompletionSyncRetainsReceiptLease(t *testing.T) {
	manager, probe, native, _ := newCatalogDropBarrier(t)
	native.blockCompletionSync()
	probe.mode = "accepted-wait"
	probe.faultAfter = 2
	probe.submitted = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dropped := make(chan error, 1)
	go func() { dropped <- manager.DropDatabase(ctx, "target") }()
	<-probe.submitted
	finished := make(chan error, 1)
	go func() {
		finished <- storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict})
	}()
	<-native.syncEntered
	cancel()
	var err error
	select {
	case err = <-dropped:
	case <-time.After(10 * time.Second):
		close(native.syncPermit)
		t.Fatal("canceled waiter did not detach from accepted Sync owner")
	}
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, storage.CommitRecoveryPending, accepted.State)
	assert.Zero(t, len(manager.catalogGate), "physical receipt retains catalog gate while Sync is unfinished")
	assert.False(t, manager.Exists("target"), "Applied metadata is not rolled back by caller cancellation")
	close(native.syncPermit)
	require.NoError(t, <-finished)
	assert.Equal(t, 1, len(manager.catalogGate))
	assert.EqualValues(t, 2, native.syncCalls.Load())
	probe.mode = ""
	probe.submitted = nil
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
	view, err := manager.GetStorage("target")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "fresh"})
	require.NoError(t, err)
	_, err = view.GetNode("fresh")
	require.NoError(t, err)
	assert.EqualValues(t, 1, native.purgeCalls.Load())
}

func TestCatalogDurableDropPublicationDuringClose(t *testing.T) {
	manager, probe, native, _ := newCatalogDropBarrier(t)
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "peer"}))
	require.NoError(t, manager.SetDatabaseOption(t.Context(), "peer", OptionTxLogEnrichment, "FULL"))
	require.Equal(t, storage.ChangeCaptureFull, native.ChangeCapture("peer"))
	scope, err := manager.StatementCacheScope("target")
	require.NoError(t, err)
	native.syncResults = make(chan error, 2)
	probe.mode = "accepted-wait"
	probe.faultAfter = 2
	probe.submitted = make(chan struct{})
	dropped := make(chan error, 1)
	go func() { dropped <- manager.DropDatabase(t.Context(), "target") }()
	<-probe.submitted
	probe.closeStarted = make(chan struct{})
	native.closeEntered = make(chan struct{})
	closeEntered := native.closeEntered
	closed := make(chan error, 1)
	go func() { closed <- manager.Close() }()
	<-probe.closeStarted
	unlocked := manager.mu.TryRLock()
	require.True(t, unlocked)
	manager.mu.RUnlock()
	// Close has reached the real native owner; it waits for the outstanding frame.
	<-closeEntered
	finished := make(chan error, 1)
	go func() {
		finished <- storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied, Err: storage.ErrConflict})
	}()
	select {
	case err = <-finished:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("native Close and receipt Sync deadlocked")
	}
	require.NoError(t, <-closed)
	probe.closeStarted = nil
	err = <-dropped
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	assert.Equal(t, storage.CommitApplied, accepted.State)
	require.NoError(t, <-native.syncResults)
	syncErr := <-native.syncResults
	if syncErr != nil {
		assert.ErrorIs(t, err, syncErr)
	}
	assert.Equal(t, syncErr != nil, manager.pendingDrops["target"])
	assert.False(t, manager.Exists("target"))
	assert.False(t, scope.Valid())
	assert.True(t, manager.statementCachesClosed)
	assert.Empty(t, manager.limitCheckers)
	assert.Zero(t, native.postCloseCapture.Load(), "Applied publication must not restart a closed FULL peer")
	assert.Equal(t, 1, len(manager.catalogGate))
	assert.EqualValues(t, 2, native.syncCalls.Load())
}

func (e *catalogDropBarrierNative) blockCompletionSync() {
	e.blockSync = 2
	e.syncEntered = make(chan struct{})
	e.syncPermit = make(chan struct{})
}
