// NOR-288 bounded follow-up evidence (delegated worker lane; author/date not
// asserted). Task: establish or refute two reported accepted-settlement
// notification paths for the NOR-288 proposal head, without changing the
// proposal's production files.
//
// Bounded unresolved-scope evidence for NOR-288's accepted settlement
// notification. Consumers of the accepted owner deliver a settlement outcome
// without the release-before-notify step the proposal added to the WAL,
// Standalone and launched-join units:
//
//   - raft.go's log-truncation discard (handleAppendEntriesRequest, reached from
//     the exported HandleRaftAppendEntries transport entry) runs
//     `r.goWork(func() { future.discardBeforeCommit(ErrNotLeader) })`.
//     discardBeforeCommit calls AcceptedEntityCommitHandle.Reject, whose
//     not-started branch resolves the outer receipt inline
//     (pkg/storage/accepted_entity_handle.go, Reject: `_ = outer.Resolve(...)`).
//     CommitReceiptOwner.Resolve runs the settlement chain synchronously
//     (pkg/storage/commit_receipt_prototype.go), so a caller's settlement
//     callback executes while the goWork unit that runs it is still enrolled
//     and cannot return. RaftReplicator.Shutdown joins exactly that work
//     (r.work.JoinStopped), so a settlement callback that shuts its own
//     replicator down waits on itself.
//   - raft.go's retire() discard (`future.discardBeforeCommit(ErrClosed)`) takes
//     the same Reject path inside WorkLifetime's dispose hook, which
//     JoinStopped also waits for: retire itself is what dispose must return.
//
// Both cases run in isolated child processes with a watchdog, exactly like
// pkg/replication/accepted_settlement_close_test.go, so a self-deadlock cannot
// hang this package.
//
// Status on this proposal head: the reentrant cases are reproduced, still-open
// gaps and are therefore opt-in (NORNICDB_NOR288_RAFT_DISCARD_REPRO=1) so they
// do not change this package's default result before the owner chooses a
// correction. The control case is enabled: it shows the same truncation discard
// route, fixture and delivery are healthy when the settlement callback does not
// shut its own replicator down, so the failure is the notification order.
package replication

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// raftDiscardSettlementChildEnv_r9port marks the isolated child process.
	raftDiscardSettlementChildEnv_r9port = "NORNICDB_RAFT_DISCARD_SETTLEMENT_CHILD"
	// raftDiscardSettlementReproEnv opts into the reproduced reentrant cases.
	raftDiscardSettlementReproEnv = "NORNICDB_NOR288_RAFT_DISCARD_REPRO"
	// raftDiscardSettlementPassedMarker_r9port is printed by a child only after its
	// scenario completed every step inside the case bound.
	raftDiscardSettlementPassedMarker_r9port = "raft discard settlement scenario completed without self-wait"
	// raftDiscardSettlementBound_r9port bounds one observable step (settlement
	// outcome, settlement callback's Shutdown, control Shutdown).
	raftDiscardSettlementBound_r9port = 20 * time.Second
	// raftDiscardSettlementWatchdog_r9port bounds the whole isolated child process,
	// and raftDiscardSettlementChildTimeout_r9port makes the child report its own
	// stuck goroutines before the watchdog kills it.
	raftDiscardSettlementWatchdog_r9port     = 120 * time.Second
	raftDiscardSettlementChildTimeout_r9port = "90s"
)

// TestRaftDiscardSettlementCallbackShutdown_r9port reproduces the open truncation case:
// the Raft uncommitted-truncation discard delivers its settlement while the
// accepted work unit that runs it is still enrolled, so a settlement callback
// that shuts its own replicator down waits on itself.
func TestRaftDiscardSettlementCallbackShutdown_r9port(t *testing.T) {
	requireRaftDiscardSettlementRepro(t)
	runRaftDiscardSettlementChild_r9port(t, "TestRaftDiscardSettlementCallbackShutdownChild_r9port")
}

// TestRaftRetireDiscardSettlementCallbackShutdown_r9port reproduces the open
// shutdown-drain case: the queued command is discarded by retire inside the
// lifetime's dispose hook, and a settlement callback that shuts its own
// replicator down waits for that dispose to return.
func TestRaftRetireDiscardSettlementCallbackShutdown_r9port(t *testing.T) {
	requireRaftDiscardSettlementRepro(t)
	runRaftDiscardSettlementChild_r9port(t, "TestRaftRetireDiscardSettlementCallbackShutdownChild_r9port")
}

// TestRaftDiscardSettlementCallbackControl_r9port is the non-reentrant control for the
// truncation discard: the settlement callback only records its outcome, the
// discard is delivered exactly once, and Shutdown from the test completes.
func TestRaftDiscardSettlementCallbackControl_r9port(t *testing.T) {
	runRaftDiscardSettlementChild_r9port(t, "TestRaftDiscardSettlementCallbackControlChild_r9port")
}

// requireRaftDiscardSettlementRepro keeps a reproduced open case out of the
// package's default result until the owner chooses a correction.
func requireRaftDiscardSettlementRepro(t *testing.T) {
	t.Helper()
	if os.Getenv(raftDiscardSettlementReproEnv) != "1" {
		t.Skip("NOR-288 reproduced open case: a Raft discard settlement callback that shuts its own replicator down waits on the accepted work that delivers it. Run with " + raftDiscardSettlementReproEnv + "=1 to reproduce.")
	}
}

// runRaftDiscardSettlementChild_r9port runs one scenario in this test binary as an
// isolated child process. The watchdog owns the bound and the join: a child
// that self-deadlocks is killed or stopped by its own test timeout, and the
// parent reports the child's output.
func runRaftDiscardSettlementChild_r9port(t *testing.T, child string) {
	t.Helper()
	require.Empty(t, os.Getenv(raftDiscardSettlementChildEnv_r9port), "the isolated raft discard settlement child driver runs only in the parent test process")
	binary, err := os.Executable()
	require.NoError(t, err)
	watchdog, stopChild := context.WithTimeout(t.Context(), raftDiscardSettlementWatchdog_r9port)
	defer stopChild()
	command := exec.CommandContext(watchdog, binary,
		"-test.run=^"+child+"$", "-test.count=1", "-test.timeout="+raftDiscardSettlementChildTimeout_r9port, "-test.v")
	// The child's temporary files stay inside the parent's own directory, so a
	// killed child leaves nothing outside the fixture.
	command.Env = append(os.Environ(), raftDiscardSettlementChildEnv_r9port+"=1", "TMPDIR="+t.TempDir())
	output, childErr := command.CombinedOutput()
	require.NoError(t, watchdog.Err(),
		"the isolated %s child was still running after the %v watchdog; a raft discard settlement callback waited on its own accepted work:\n%s",
		child, raftDiscardSettlementWatchdog_r9port, output)
	require.NoError(t, childErr, "the isolated %s child reported the raft discard settlement regression:\n%s", child, output)
	require.Contains(t, string(output), raftDiscardSettlementPassedMarker_r9port)
}

// requireRaftDiscardSettlementChild_r9port keeps a child case out of an ordinary run of
// this package; only its parent driver starts it.
func requireRaftDiscardSettlementChild_r9port(t *testing.T) {
	t.Helper()
	if os.Getenv(raftDiscardSettlementChildEnv_r9port) != "1" {
		t.Skip("isolated raft discard settlement case: run through its parent test")
	}
}

func raftDiscardSettlementStacks_r9port() string {
	buffer := make([]byte, 1<<18)
	return string(buffer[:runtime.Stack(buffer, true)])
}

// requireRaftDiscardSettlementStep_r9port waits for one observable step under the case
// bound and reports the whole goroutine set when it does not complete.
func requireRaftDiscardSettlementStep_r9port[T any](t *testing.T, step string, done <-chan T) T {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-time.After(raftDiscardSettlementBound_r9port):
		t.Fatalf("%s did not complete within %v:\n%s", step, raftDiscardSettlementBound_r9port, raftDiscardSettlementStacks_r9port())
	}
	var zero T
	return zero
}

// raftDiscardSettlementFixture_r9port is one Raft leader with an accepted command that
// is appended but not committed: the state both discard owners act on.
type raftDiscardSettlementFixture_r9port struct {
	base  *storage.BadgerEngine
	raft  *RaftReplicator
	adptr *StorageAdapter
}

// newRaftDiscardSettlementLeader_r9port builds a single-peer Raft leader so an accepted
// command is appended to the log and waits for a quorum that never arrives,
// which is the ordinary precondition for a later conflicting AppendEntries.
func newRaftDiscardSettlementLeader_r9port(t *testing.T) raftDiscardSettlementFixture_r9port {
	t.Helper()
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
	// The commit observer must not add its own pending report while the case
	// inspects the discard delivery.
	raft.config.Raft.CommitTimeout = 30 * time.Second
	raft.peerMu.Lock()
	raft.config.Raft.Peers = []PeerConfig{{ID: "peer"}}
	raft.peerMu.Unlock()
	return raftDiscardSettlementFixture_r9port{base: base, raft: raft, adptr: adapter}
}

// submit prepares one entity command in this fixture's native engine and submits
// it through the public accepted API. The future stays in the replicator's apply
// queue, exactly as a queue-transferring producer leaves it.
func (f raftDiscardSettlementFixture_r9port) submit(t *testing.T, id storage.NodeID, settle func(storage.CommitOutcome)) *storage.CommitReceipt {
	t.Helper()
	tx, err := f.base.BeginTransaction(t.Context(), storage.OrdinaryBegin)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	engine := NewReplicatedEngine(f.base, f.raft, 5*time.Second)
	receipt, err := engine.SubmitEntityCommit(context.Background(), prepared, settle)
	require.NoError(t, err)
	return receipt
}

// appendUncommitted drives the replicator's apply owner exactly as its apply
// loop does: the entry is appended and registered, then waits for a quorum.
func (f raftDiscardSettlementFixture_r9port) appendUncommitted(t *testing.T) {
	t.Helper()
	future := requireRaftDiscardSettlementStep_r9port(t, "the accepted command reaching the raft apply queue", f.raft.applyCh)
	require.NoError(t, f.raft.processApply(context.Background(), future))
}

// conflict replaces the uncommitted entry through the exported AppendEntries
// transport entry, which is the reachable public owner of the truncation
// discard.
func (f raftDiscardSettlementFixture_r9port) conflict(t *testing.T) {
	t.Helper()
	resp, err := f.raft.HandleRaftAppendEntries(&RaftAppendEntriesRequest{
		Term:     2,
		LeaderID: "replacement",
		Entries:  []*RaftLogEntry{{Index: 1, Term: 2, Command: &Command{Type: CmdUnknown}}},
	})
	require.NoError(t, err)
	require.True(t, resp.Success, "the replacement leader's entry replaces the uncommitted entry")
}

// requireDiscarded_r9port asserts the definite-discard outcome of an unrecorded entry.
func requireDiscarded_r9port(t *testing.T, outcome storage.CommitOutcome, cause error) {
	t.Helper()
	require.Equal(t, storage.CommitDiscarded, outcome.State)
	require.ErrorIs(t, outcome.Err, cause)
}

// TestRaftDiscardSettlementCallbackShutdownChild_r9port is the isolated reentrant case:
// the truncation discard's settlement callback shuts the replicator down from
// inside the accepted work unit that delivers the discard.
func TestRaftDiscardSettlementCallbackShutdownChild_r9port(t *testing.T) {
	requireRaftDiscardSettlementChild_r9port(t)
	fixture := newRaftDiscardSettlementLeader_r9port(t)
	settled := make(chan storage.CommitOutcome, 1)
	closed := make(chan error, 1)
	fixture.submit(t, storage.NodeID("test:raft-discard-settlement-close"), func(outcome storage.CommitOutcome) {
		settled <- outcome
		closed <- fixture.raft.Shutdown()
	})
	fixture.appendUncommitted(t)
	fixture.conflict(t)
	requireDiscarded_r9port(t, requireRaftDiscardSettlementStep_r9port(t, "the raft discard settlement callback outcome", settled), ErrNotLeader)
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr, "the settlement callback's own Shutdown must complete")
	case <-time.After(raftDiscardSettlementBound_r9port):
		// The reproduced open case. Capture the cycle before releasing this
		// disposable child's fixture, so the run leaves no store open.
		stacks := raftDiscardSettlementStacks_r9port()
		_ = fixture.adptr.Close()
		_ = fixture.base.Close()
		t.Fatalf("RaftReplicator.Shutdown from the truncation discard settlement callback did not return within %v; the discard notification runs inside the accepted work unit that Shutdown joins:\n%s",
			raftDiscardSettlementBound_r9port, stacks)
	}
	require.NoError(t, fixture.adptr.Close())
	require.NoError(t, fixture.base.Close())
	fmt.Println(raftDiscardSettlementPassedMarker_r9port)
}

// TestRaftRetireDiscardSettlementCallbackShutdownChild_r9port is the isolated reentrant
// case for the shutdown drain: the queued command is discarded by retire inside
// the lifetime's dispose hook, and the settlement callback shuts the replicator
// down again from there.
func TestRaftRetireDiscardSettlementCallbackShutdownChild_r9port(t *testing.T) {
	requireRaftDiscardSettlementChild_r9port(t)
	fixture := newRaftDiscardSettlementLeader_r9port(t)
	settled := make(chan storage.CommitOutcome, 1)
	closed := make(chan error, 1)
	fixture.submit(t, storage.NodeID("test:raft-retire-discard-settlement-close"), func(outcome storage.CommitOutcome) {
		settled <- outcome
		closed <- fixture.raft.Shutdown()
	})
	// The future stays queued, so Shutdown's retire drain discards it.
	shutdown := make(chan error, 1)
	go func() { shutdown <- fixture.raft.Shutdown() }()
	requireDiscarded_r9port(t, requireRaftDiscardSettlementStep_r9port(t, "the retire discard settlement callback outcome", settled), ErrClosed)
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr, "the settlement callback's own Shutdown must complete")
	case <-time.After(raftDiscardSettlementBound_r9port):
		stacks := raftDiscardSettlementStacks_r9port()
		_ = fixture.adptr.Close()
		_ = fixture.base.Close()
		t.Fatalf("RaftReplicator.Shutdown from the retire discard settlement callback did not return within %v; retire is the lifetime dispose hook that JoinStopped waits for:\n%s",
			raftDiscardSettlementBound_r9port, stacks)
	}
	require.NoError(t, requireRaftDiscardSettlementStep_r9port(t, "the shutdown that drained the queue", shutdown))
	require.NoError(t, fixture.adptr.Close())
	require.NoError(t, fixture.base.Close())
	fmt.Println(raftDiscardSettlementPassedMarker_r9port)
}

// TestRaftDiscardSettlementCallbackControlChild_r9port is the same truncation discard
// route with a non-reentrant settlement callback: the discard is delivered
// exactly once and Shutdown from outside the callback completes.
func TestRaftDiscardSettlementCallbackControlChild_r9port(t *testing.T) {
	requireRaftDiscardSettlementChild_r9port(t)
	fixture := newRaftDiscardSettlementLeader_r9port(t)
	settled := make(chan storage.CommitOutcome, 1)
	receipt := fixture.submit(t, storage.NodeID("test:raft-discard-settlement-control"), func(outcome storage.CommitOutcome) { settled <- outcome })
	fixture.appendUncommitted(t)
	fixture.conflict(t)
	requireDiscarded_r9port(t, requireRaftDiscardSettlementStep_r9port(t, "the control discard settlement callback outcome", settled), ErrNotLeader)
	select {
	case <-receipt.Done():
		require.Equal(t, storage.CommitDiscarded, receipt.Outcome().State)
	case <-time.After(raftDiscardSettlementBound_r9port):
		t.Fatalf("the discarded receipt did not reach its terminal state:\n%s", raftDiscardSettlementStacks_r9port())
	}
	select {
	case duplicate := <-settled:
		t.Fatalf("the settlement callback ran twice; second outcome %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- fixture.raft.Shutdown() }()
	require.NoError(t, requireRaftDiscardSettlementStep_r9port(t, "RaftReplicator.Shutdown outside the settlement callback", closed))
	require.NoError(t, fixture.adptr.Close())
	require.NoError(t, fixture.base.Close())
	fmt.Println(raftDiscardSettlementPassedMarker_r9port)
}
