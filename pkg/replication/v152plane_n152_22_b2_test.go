// NOR-288's accepted settlement notification must not run under accepted work
// that the observer's own Close or Shutdown joins: the notification runs the
// caller's settlement callback, and that callback may close the owner that is
// executing it. The WAL consumer of this unit is covered in pkg/storage
// (wal_settlement_callback_close_test.go). These are the remaining consumers of
// the same accepted join/notification unit:
//
//   - Standalone (and HA standby) SubmitAcceptedCommand joins the native owner
//     inline on its accepted apply goroutine, whose replicator work Shutdown
//     joins; a settlement callback that shuts the replicator down would wait
//     for its own goroutine.
//   - Raft hands every recorded application to the same owner through Settle,
//     whose join runs as an asynchronously launched unit of the engine's
//     accepted work that the engine's Close joins; a settlement callback that
//     closes the engine would wait for that unit.
//
// Both cases run in isolated child processes with a watchdog, so a
// self-deadlock is reported against its own scenario instead of hanging this
// suite. The accepted outcome is asserted too: an applied command must stay
// applied and keep its image when the callback closes its own owner.
package replication

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// acceptedSettlementCloseChildEnv marks the isolated child process.
	acceptedSettlementCloseChildEnv = "NORNICDB_REPLICATION_SETTLEMENT_CLOSE_CHILD"
	// acceptedSettlementClosePassedMarker is printed by a child only after its
	// scenario proved the settlement callback completed and preserved the
	// accepted outcome.
	acceptedSettlementClosePassedMarker = "replication accepted settlement closed its own owner without self-deadlock and preserved the accepted outcome"
	// acceptedSettlementCloseBound bounds one observable step (callback
	// outcome, callback Close/Shutdown). Healthy work on these paths completes
	// in well under a second; the bound only turns the deterministic
	// self-deadlock into a reported failure.
	acceptedSettlementCloseBound = 20 * time.Second
	// acceptedSettlementCloseWatchdog bounds the whole isolated child process,
	// and acceptedSettlementCloseChildTimeout makes the child report a stuck
	// goroutine before the watchdog kills it.
	acceptedSettlementCloseWatchdog     = 120 * time.Second
	acceptedSettlementCloseChildTimeout = "90s"
)

// TestAcceptedSettlementCallbackShutdownStandalone covers the inline join of
// StandaloneReplicator.SubmitAcceptedCommand: the settlement callback shuts its
// own replicator down.
func TestAcceptedSettlementCallbackShutdownStandalone(t *testing.T) {
	runAcceptedSettlementCloseChild(t, "TestAcceptedSettlementCallbackShutdownStandaloneChild")
}

// TestAcceptedSettlementCallbackEngineCloseRaft covers the asynchronously
// launched join of a recorded Raft application: the settlement callback closes
// the native engine whose accepted work runs that join.
func TestAcceptedSettlementCallbackEngineCloseRaft(t *testing.T) {
	runAcceptedSettlementCloseChild(t, "TestAcceptedSettlementCallbackEngineCloseRaftChild")
}

// runAcceptedSettlementCloseChild runs one scenario in this test binary as an
// isolated child process. The watchdog owns the bound and the join: a child
// that self-deadlocks is killed or stopped by its own test timeout, and the
// parent reports the child's output.
func runAcceptedSettlementCloseChild(t *testing.T, child string) {
	t.Helper()
	require.Empty(t, os.Getenv(acceptedSettlementCloseChildEnv), "the isolated settlement Close child driver runs only in the parent test process")
	binary, err := os.Executable()
	require.NoError(t, err)
	watchdog, stopChild := context.WithTimeout(t.Context(), acceptedSettlementCloseWatchdog)
	defer stopChild()
	command := exec.CommandContext(watchdog, binary,
		"-test.run=^"+child+"$", "-test.count=1", "-test.timeout="+acceptedSettlementCloseChildTimeout, "-test.v")
	// The child's temporary files stay inside the parent's own directory, so a
	// killed child leaves nothing outside the fixture.
	command.Env = append(os.Environ(), acceptedSettlementCloseChildEnv+"=1", "TMPDIR="+t.TempDir())
	output, childErr := command.CombinedOutput()
	require.NoError(t, watchdog.Err(),
		"the isolated %s child was still running after the %v watchdog; settlement of an accepted command waited on the work that runs it:\n%s",
		child, acceptedSettlementCloseWatchdog, output)
	require.NoError(t, childErr, "the isolated %s child reported the settlement Close regression:\n%s", child, output)
	require.Contains(t, string(output), acceptedSettlementClosePassedMarker)
}

// requireAcceptedSettlementCloseChild keeps a child case out of an ordinary run
// of this package; only its parent driver starts it.
func requireAcceptedSettlementCloseChild(t *testing.T) {
	t.Helper()
	if os.Getenv(acceptedSettlementCloseChildEnv) != "1" {
		t.Skip("isolated replication settlement Close case: run through its parent test")
	}
}

// acceptedSettlementCloseRequire waits for one observable step under the case
// bound and reports the whole goroutine set when it does not complete: a
// settlement callback that cannot close its own owner is the reproduced
// regression.
func acceptedSettlementCloseRequire[T any](t *testing.T, step string, done <-chan T) T {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-time.After(acceptedSettlementCloseBound):
		t.Fatalf("%s did not complete within %v; the accepted settlement callback waits on its own completing work:\n%s",
			step, acceptedSettlementCloseBound, acceptedSettlementCloseStacks())
	}
	var zero T
	return zero
}

func acceptedSettlementCloseStacks() string {
	buffer := make([]byte, 1<<18)
	return string(buffer[:runtime.Stack(buffer, true)])
}

// acceptedSettlementClosePrepare stages one entity command and takes it to the
// prephysical boundary, exactly as an accepted command is prepared before an
// outer transport submits it.
func acceptedSettlementClosePrepare(t *testing.T, base *storage.BadgerEngine, id storage.NodeID) *storage.PreparedEntityCommit {
	t.Helper()
	tx, err := base.BeginTransaction(t.Context(), storage.OrdinaryBegin)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	return prepared
}

// acceptedSettlementCloseStored proves the applied accepted image is readable
// from a reopened native data directory.
func acceptedSettlementCloseStored(t *testing.T, dir string, id storage.NodeID) {
	t.Helper()
	reopened, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	node, err := reopened.GetStoredNode(t.Context(), id)
	require.NoError(t, err, "the accepted image must survive the settlement callback's Close")
	require.Equal(t, "accepted", node.Properties["v"])
}

// TestAcceptedSettlementCallbackShutdownStandaloneChild is the isolated
// standalone case: the accepted apply goroutine settles a recorded command and
// its settlement callback shuts that same replicator down.
func TestAcceptedSettlementCallbackShutdownStandaloneChild(t *testing.T) {
	requireAcceptedSettlementCloseChild(t)
	dir := t.TempDir()
	base, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	config := DefaultConfig()
	config.Mode = ModeStandalone
	config.NodeID = "settlement-close-node"
	replicator, err := NewReplicator(config, adapter)
	require.NoError(t, err)
	require.NoError(t, replicator.Start(context.Background()))
	engine := NewReplicatedEngine(base, replicator, 5*time.Second)
	id := storage.NodeID("test:standalone-settlement-close")
	prepared := acceptedSettlementClosePrepare(t, base, id)
	settled := make(chan storage.CommitOutcome, 1)
	closed := make(chan error, 1)
	receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, func(outcome storage.CommitOutcome) {
		settled <- outcome
		closed <- replicator.Shutdown()
	})
	require.NoError(t, err)
	outcome := acceptedSettlementCloseRequire(t, "the standalone settlement callback outcome", settled)
	closeErr := acceptedSettlementCloseRequire(t, "StandaloneReplicator.Shutdown from the accepted settlement callback", closed)
	require.NoError(t, closeErr, "the settlement callback's own Shutdown must complete")
	waitCtx, cancel := context.WithTimeout(context.Background(), acceptedSettlementCloseBound)
	defer cancel()
	require.NoError(t, receipt.Wait(waitCtx), "the receipt must keep the applied accepted outcome")
	require.Equal(t, storage.CommitApplied, outcome.State)
	require.NoError(t, outcome.Err)
	node, err := base.GetStoredNode(t.Context(), id)
	require.NoError(t, err, "the accepted image must survive the settlement callback's Shutdown")
	require.Equal(t, "accepted", node.Properties["v"])
	require.NoError(t, adapter.Close())
	require.NoError(t, base.Close())
	fmt.Println(acceptedSettlementClosePassedMarker)
}

// TestAcceptedSettlementCallbackEngineCloseRaftChild is the isolated Raft case:
// the recorded application is settled by the asynchronously launched join, and
// its settlement callback closes the native engine that owns that accepted work.
func TestAcceptedSettlementCallbackEngineCloseRaftChild(t *testing.T) {
	requireAcceptedSettlementCloseChild(t)
	dir := t.TempDir()
	base, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	raft, _ := newCoverageRaftReplicator(t)
	raft.storage = adapter
	raft.started.Store(true)
	raft.mu.Lock()
	raft.state, raft.currentTerm = StateLeader, 1
	raft.mu.Unlock()
	engine := NewReplicatedEngine(base, raft, 5*time.Second)
	id := storage.NodeID("test:raft-settlement-close")
	prepared := acceptedSettlementClosePrepare(t, base, id)
	settled := make(chan storage.CommitOutcome, 1)
	closed := make(chan error, 1)
	receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, func(outcome storage.CommitOutcome) {
		settled <- outcome
		closed <- base.Close()
	})
	require.NoError(t, err)
	// The single-node application owner records this entry in its own queue;
	// this scenario drives that owner exactly as its apply loop does.
	future := acceptedSettlementCloseRequire(t, "the accepted command reaching the raft apply queue", raft.applyCh)
	require.NoError(t, raft.processApply(context.Background(), future))
	outcome := acceptedSettlementCloseRequire(t, "the raft settlement callback outcome", settled)
	closeErr := acceptedSettlementCloseRequire(t, "BadgerEngine.Close from the accepted settlement callback", closed)
	require.NoError(t, closeErr, "the settlement callback's own Close must complete")
	waitCtx, cancel := context.WithTimeout(context.Background(), acceptedSettlementCloseBound)
	defer cancel()
	require.NoError(t, receipt.Wait(waitCtx), "the receipt must keep the applied accepted outcome")
	require.Equal(t, storage.CommitApplied, outcome.State)
	require.NoError(t, outcome.Err)
	require.NoError(t, adapter.Close())
	acceptedSettlementCloseStored(t, dir, id)
	fmt.Println(acceptedSettlementClosePassedMarker)
}
