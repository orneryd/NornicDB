package multidb

// NOR-183: a held (pending) DROP fence and a reentering catalog listener.
//
// nor183_listener_reentry_test.go stated that FinishPendingDrop is reachable
// only in its no-fence form, because "a held fence only exists while an
// accepted DROP is unfinished, which no public call produces". That is not
// accurate: catalog_drop_durability_test.go
// TestCatalogDropSyncFailureRetainsAbsentNameFence produces a held fence with
// public calls alone - a DropDatabase whose completion Sync fails returns the
// accepted outcome of an applied removal and retains the unavailable-name
// fence. This file covers that public shape with a listener that reenters
// DatabaseManager from every catalog publication, which is the last
// listener/DROP-lifecycle intersection NOR-183 names:
//
//	failed DropDatabase   the intent publication carries the held fence; the
//	                      applied removal whose Sync failed is reported as the
//	                      AcceptedCommitError it is, and its published image
//	                      leaves the name absent while the fence keeps it
//	                      unusable
//	FinishPendingDrop     the recovery call completes the retained fence,
//	                      publishes the cleared catalog, purges nothing, and
//	                      frees the name for reuse
//	reuse                 the name creates and stores data again, and the
//	                      no-fence recovery call cannot reach that new database
//
// The scenario runs in an isolated child process. A regression blocks the
// calling goroutine inside the manager's own lock, so an in-process test would
// hang the package run instead of reporting one. The child's watchdog prints
// every stack and exits with nor183PendingBlockedExit, which the parent
// reports as the reproduction.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	nor183PendingScenarioEnv = "NOR183_PENDING_LISTENER_SCENARIO"
	nor183PendingScenario    = "failed-drop-finish-recovery"
	nor183PendingChildRun    = "^TestNOR183PendingListenerChild$"

	nor183PendingCompletedMarker = "NOR183_PENDING_LISTENER_SCENARIO_COMPLETED"
	nor183PendingBlockedMarker   = "NOR183_PENDING_LISTENER_SCENARIO_BLOCKED"

	nor183PendingBlockedExit = 87

	// nor183PendingDatabase is the fixture's database: newCatalogDropBarrier
	// creates it with one stored node, so the DROP purges real data.
	nor183PendingDatabase = "target"

	// nor183PendingBudget bounds one scenario. A manager lock held across
	// listener delivery never returns, so the child reports a reproduction
	// instead of waiting for the package timeout.
	nor183PendingBudget = 20 * time.Second
	// nor183PendingProcessBudget bounds the child process, including its own
	// start-up, above the child's -test.timeout.
	nor183PendingProcessBudget = 120 * time.Second
)

// TestNOR183PendingListenerFailedDropAndFinishRecovery runs the held-fence
// listener scenario and turns a blocked child into the reproduction.
func TestNOR183PendingListenerFailedDropAndFinishRecovery(t *testing.T) {
	output := nor183PendingRunChild(t)
	require.Contains(t, output, nor183PendingCompletedMarker+" scenario="+nor183PendingScenario,
		"the NOR-183 pending-drop listener scenario must reach its completion marker: %s", output)
}

// nor183PendingRunChild runs the scenario in a child process and reports a
// blocked child as the reproduction. The child is the test binary itself, so
// the scenario runs through the real public manager path.
func nor183PendingRunChild(t *testing.T) string {
	t.Helper()
	binaryPath, err := os.Executable()
	require.NoError(t, err)
	deadline, cancel := context.WithTimeout(t.Context(), nor183PendingProcessBudget)
	t.Cleanup(cancel)
	child := exec.CommandContext(deadline, binaryPath,
		"-test.run="+nor183PendingChildRun, "-test.count=1", "-test.timeout=90s")
	child.Env = append(os.Environ(), nor183PendingScenarioEnv+"="+nor183PendingScenario)
	output, runErr := child.CombinedOutput()
	require.NoError(t, deadline.Err(),
		"the NOR-183 pending listener child did not exit within %s; output:\n%s", nor183PendingProcessBudget, output)
	var exit *exec.ExitError
	if errors.As(runErr, &exit) && exit.ExitCode() == nor183PendingBlockedExit {
		t.Fatalf("NOR-183 reproduced: the pending-drop listener scenario blocked while a storage listener reentered DatabaseManager\n%s",
			output)
	}
	require.NoError(t, runErr, "the NOR-183 pending listener child failed:\n%s", output)
	require.NotContains(t, string(output), nor183PendingBlockedMarker,
		"the NOR-183 pending listener child reported a blocked scenario: %s", output)
	return string(output)
}

// TestNOR183PendingListenerChild runs the scenario in the isolated process the
// parent started. The fixture is built on the test goroutine; only the manager
// operations run on the watched goroutine, so a blocked operation is reported
// instead of being left behind.
func TestNOR183PendingListenerChild(t *testing.T) {
	scenario := os.Getenv(nor183PendingScenarioEnv)
	if scenario == "" {
		t.Skip("the NOR-183 pending listener child scenario requires " + nor183PendingScenarioEnv)
	}
	require.Equal(t, nor183PendingScenario, scenario)
	fixture := newNor183PendingFixture(t)

	outcome := make(chan error, 1)
	go func() { outcome <- fixture.run() }()

	select {
	case runErr := <-outcome:
		require.NoError(t, runErr, "the NOR-183 pending listener scenario failed")
		fixture.verify(t)
		events, _ := fixture.log.snapshot()
		fmt.Printf("%s scenario=%s drop=%v finish=%v events=%d reentrant-reads=%d catalog=%q\n",
			nor183PendingCompletedMarker, scenario,
			fixture.result.dropErr, fixture.result.finishErr,
			len(events), fixture.log.completedReentries(), events)
	case <-time.After(nor183PendingBudget):
		// The scenario goroutine is blocked inside the manager (or the storage
		// delivery it waits for), so no progress is possible in this process.
		fmt.Printf("%s scenario=%s\n%s", nor183PendingBlockedMarker, scenario, nor183ListenerStacks())
		os.Exit(nor183PendingBlockedExit)
	}
}

// nor183PendingFixture holds the durable store, the manager over it and the
// reentering catalog listener, plus the raw outcomes the scenario observed.
type nor183PendingFixture struct {
	ctx     context.Context
	manager *DatabaseManager
	native  *catalogDropBarrierNative
	log     *nor183ListenerLog
	result  nor183PendingResult
}

// nor183PendingResult is what the scenario observed, recorded on the watched
// goroutine and asserted afterwards so a wrong outcome is reported rather than
// turned into a reproduction.
type nor183PendingResult struct {
	dropErr              error
	fencedAfterDrop      bool
	presentAfterDrop     bool
	createWhileFencedErr error
	purgesBeforeFinish   int32
	syncsBeforeFinish    int32
	finishErr            error
	fencedAfterFinish    bool
	presentAfterFinish   bool
	purgesAfterFinish    int32
	syncsAfterFinish     int32
	imageAfterFinish     string
	imageAfterReuse      string
	finishWithoutFence   error
}

func newNor183PendingFixture(t *testing.T) *nor183PendingFixture {
	t.Helper()
	manager, _, native, _ := newCatalogDropBarrier(t)
	log := &nor183ListenerLog{}
	// The catalog publication events are emitted by the durable store, so the
	// listener subscribes there rather than on the receipt probe.
	require.NoError(t, nor183ListenerWatchCatalog(native, log, nor183ListenerReentryTarget{
		ctx: t.Context(), log: log, manager: manager,
		database: nor183PendingDatabase, sizeDatabase: manager.DefaultDatabaseName(),
	}), "the catalog listener must subscribe to the store the manager publishes over")
	return &nor183PendingFixture{ctx: t.Context(), manager: manager, native: native, log: log}
}

// run drives the public lifecycle the issue names: a DROP whose completion
// acknowledgement fails, the recovery entry point that finishes its fence, and
// the reuse of the freed name.
func (f *nor183PendingFixture) run() error {
	result := &f.result
	// The fixture's catalog and data writes precede the listener, so their
	// publications are not part of this scenario. The DROP publishes its intent
	// with the first Sync and its completion with the second; the second one
	// fails, which is the public shape that retains the fence.
	f.native.failSync = 2
	result.dropErr = f.manager.DropDatabase(f.ctx, nor183PendingDatabase)
	result.fencedAfterDrop = f.manager.dropFenceHeld(nor183PendingDatabase)
	result.presentAfterDrop = f.manager.Exists(nor183PendingDatabase)
	result.createWhileFencedErr = f.manager.CreateDatabase(f.ctx, DatabaseCreateRequest{Name: nor183PendingDatabase})

	result.purgesBeforeFinish = f.native.purgeCalls.Load()
	result.syncsBeforeFinish = f.native.syncCalls.Load()
	f.native.failSync = 0
	result.finishErr = f.manager.FinishPendingDrop(f.ctx, nor183PendingDatabase)
	result.fencedAfterFinish = f.manager.dropFenceHeld(nor183PendingDatabase)
	result.presentAfterFinish = f.manager.Exists(nor183PendingDatabase)
	result.purgesAfterFinish = f.native.purgeCalls.Load()
	result.syncsAfterFinish = f.native.syncCalls.Load()
	result.imageAfterFinish = nor183ListenerPublicImage(f.manager)

	if err := f.manager.CreateDatabase(f.ctx, DatabaseCreateRequest{Name: nor183PendingDatabase}); err != nil {
		return fmt.Errorf("create %s after its fence was finished: %w (fenced=%t present=%t finish=%v drop=%v syncs=%d->%d purges=%d->%d createWhile=%v)",
			nor183PendingDatabase, err, result.fencedAfterFinish, result.presentAfterFinish, result.finishErr, result.dropErr,
			result.syncsBeforeFinish, result.syncsAfterFinish, result.purgesBeforeFinish, result.purgesAfterFinish, result.createWhileFencedErr)
	}
	result.imageAfterReuse = nor183ListenerPublicImage(f.manager)
	fresh, err := f.manager.GetStorage(nor183PendingDatabase)
	if err != nil {
		return fmt.Errorf("storage for the recovered database: %w", err)
	}
	if _, err := fresh.CreateNode(f.ctx, &storage.Node{ID: "fresh"}); err != nil {
		return fmt.Errorf("create node in the recovered database: %w", err)
	}
	if _, err := fresh.GetNode(f.ctx, "fresh"); err != nil {
		return fmt.Errorf("read node from the recovered database: %w", err)
	}
	// The recovery entry point may only finish a drop its own owner accepted.
	result.finishWithoutFence = f.manager.FinishPendingDrop(f.ctx, nor183PendingDatabase)
	if _, err := fresh.GetNode(f.ctx, "fresh"); err != nil {
		return fmt.Errorf("read node after the no-fence recovery call: %w", err)
	}
	return nil
}

// verify asserts the exact error identity, the durable catalog outcome and the
// listener reentry of every publication.
func (f *nor183PendingFixture) verify(t *testing.T) {
	t.Helper()
	result := f.result

	// The failed Drop is an accepted outcome of an applied removal, never a
	// retryable rejection a name-only retry could carry to a newer database.
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, result.dropErr, &accepted,
		"a failed completion Sync must report the accepted outcome")
	require.Equal(t, storage.CommitApplied, accepted.State,
		"the applied catalog removal keeps its applied state")
	require.NotEmpty(t, accepted.LogicalID,
		"the accepted outcome names the catalog commit it applied")
	require.NotErrorIs(t, result.dropErr, storage.ErrConflict,
		"an applied removal is never reported as a retryable conflict")
	requireCatalogAcceptedDiagnostic(t, accepted, storage.ErrConflict)
	var localized *localization.LocalizedError
	require.ErrorAs(t, accepted.Err, &localized,
		"the accepted cause keeps its localized identity")
	require.Equal(t, string(localization.MessageMultidbCompositePersistAfterDropFailed), localized.Code,
		"the failed completion is reported as the durable-metadata failure it is")
	require.Contains(t, localized.Error(), "failed to persist metadata after drop")

	// The retained fence leaves the name absent and unavailable.
	require.True(t, result.fencedAfterDrop,
		"the failed completion acknowledgement must retain the unavailable-name fence")
	require.False(t, result.presentAfterDrop,
		"the applied catalog removal must not resurrect the dropped metadata")
	require.ErrorIs(t, result.createWhileFencedErr, ErrDatabaseExists,
		"the pending name cannot be reused before durable completion")

	// The recovery call finishes exactly that fence.
	require.NoError(t, result.finishErr,
		"FinishPendingDrop must finish the fence the failed Drop retained")
	require.False(t, result.fencedAfterFinish,
		"the finished drop must clear the unavailable-name fence")
	require.False(t, result.presentAfterFinish,
		"finishing an absent-record fence must not recreate the name")
	require.Equal(t, result.purgesBeforeFinish, result.purgesAfterFinish,
		"finishing an absent-record fence must not purge another namespace")
	require.Equal(t, result.syncsBeforeFinish+1, result.syncsAfterFinish,
		"the recovery makes exactly one durable catalog boundary")

	// Every catalog publication is delivered once, in order, and each one
	// reentered the manager from the listener and returned.
	events := f.log.requireSettled(t, 4, 4)
	require.Len(t, events, 4,
		"the intent, the failed removal, the finished fence and the reuse are the catalog publications")
	require.Contains(t, events[0], nor183PendingDatabase+":standard:offline",
		"the DROP intent must publish the name offline")
	require.Contains(t, events[0], "pending="+nor183PendingDatabase,
		"the DROP intent must publish the held fence with it")
	require.Equal(t, result.imageAfterFinish, events[1],
		"the failed completion still publishes the catalog it applied, without the fence")
	require.Equal(t, result.imageAfterFinish, events[2],
		"finishing the fence publishes the same catalog with the fence cleared")
	require.Equal(t, result.imageAfterReuse, events[3],
		"the freed name is published as a new database")
	require.Equal(t, len(events), f.log.completedReentries(),
		"every publication's reentrant listener must return")
	f.log.requireNoOpenCalls(t)

	// The freed name is a real, writable database, and the no-fence recovery
	// form cannot reach it.
	require.ErrorIs(t, result.finishWithoutFence, ErrDatabaseNotFound,
		"the recovery entry point without a held fence reports the absent name")
}
