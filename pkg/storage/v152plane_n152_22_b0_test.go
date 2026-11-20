// Author: NOR-288 unit-test lane, 2026-11-07.
//
// NOR-288 final remaining regression (7 November 2026 triage row): a Close
// invoked from a WALEngine.SubmitEntityCommit settlement callback reportedly
// waits for its own completion.
//
// Contract: an accepted command settles exactly once and its client outcome
// reflects acceptance. Settlement (and any observer it runs, including a
// caller's Close) must not run under accepted work the engine's own Close
// joins; otherwise Close waits on the worker that is executing Close.
//
// These are public-API reproductions: NewWALEngine, BadgerEngine,
// BeginTransaction/PrepareEntityCommit/SubmitEntityCommit and the exported
// CommitOutcome settlement callback. The cases run in isolated child processes
// with a watchdog, so a self-deadlock cannot hang this suite and every hang is
// reported against its own scenario. Covered:
//   - accepted success: applied outcome with no error;
//   - accepted failure: the native application committed but its
//     acknowledgement failed, so the applied outcome retains an accepted error;
//   - accepted recovery-pending failure: no terminal outcome exists, so the
//     approved contract does not run the settlement callback at all;
//   - healthy non-reentrant control: a settlement callback that does not Close,
//     followed by a write to the same entity and Close from the test, proving
//     the path and the released accepted locks are healthy.
//
// The child deliberately does not register its first engine for
// close-at-cleanup: on the reproduced regression the engine's Close cannot
// finish (that is the defect), a cleanup Close would only move the hang into
// cleanup, and the child is a disposable process the parent bounds. Passing
// cases close explicitly.

package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

const (
	// walSettlementCloseChildEnv_r9port marks the isolated child process.
	walSettlementCloseChildEnv_r9port = "NORNICDB_WAL_SETTLEMENT_CLOSE_CHILD"
	// walSettlementClosePassedMarker_r9port is printed by a child only after its
	// scenario proved the settlement callback completed and preserved the
	// accepted outcome.
	walSettlementClosePassedMarker_r9port = "WAL settlement callback Close returned without self-deadlock and preserved the accepted outcome"
	// walSettlementCloseReturnBound_r9port bounds one observable step (callback
	// outcome, callback Close, Close outside a healthy callback). Healthy work
	// on the same path completes in well under a second; the bound only turns
	// the deterministic self-deadlock into a reported failure.
	walSettlementCloseReturnBound_r9port = 15 * time.Second
	// walSettlementCloseWatchdog_r9port bounds the whole isolated child process, and
	// walSettlementCloseChildTimeout_r9port makes the child itself report a stuck
	// goroutine before the watchdog kills it.
	walSettlementCloseWatchdog_r9port     = 90 * time.Second
	walSettlementCloseChildTimeout_r9port = "60s"
)

// TestWALSubmitEntityCommitSettlementCallbackCloseApplied_r9port reproduces the
// accepted-success settlement: the callback receives CommitApplied and closes
// its WAL engine.
func TestWALSubmitEntityCommitSettlementCallbackCloseApplied_r9port(t *testing.T) {
	runWALSubmitEntityCommitSettlementCloseChild_r9port(t, "TestWALSubmitEntityCommitSettlementCallbackCloseAppliedChild_r9port")
}

// TestWALSubmitEntityCommitSettlementCallbackCloseFailure_r9port reproduces the
// accepted-failure settlement: the storage image committed, the acknowledgement
// failed, so the callback receives CommitApplied with an accepted error and
// closes its WAL engine.
func TestWALSubmitEntityCommitSettlementCallbackCloseFailure_r9port(t *testing.T) {
	runWALSubmitEntityCommitSettlementCloseChild_r9port(t, "TestWALSubmitEntityCommitSettlementCallbackCloseFailureChild_r9port")
}

// TestWALSubmitEntityCommitSettlementCallbackClosePending_r9port covers the other
// accepted failure class: a recovery-pending outcome is not terminal, so the
// approved contract runs no settlement callback. Close from outside then
// completes, and nothing unaccepted was applied.
func TestWALSubmitEntityCommitSettlementCallbackClosePending_r9port(t *testing.T) {
	runWALSubmitEntityCommitSettlementCloseChild_r9port(t, "TestWALSubmitEntityCommitSettlementCallbackClosePendingChild_r9port")
}

// TestWALSubmitEntityCommitSettlementCallbackCloseControl_r9port is the healthy
// non-reentrant control: the same accepted path settles without a Close in the
// callback, the entity is writable afterwards (released accepted locks), and
// Close outside the callback completes.
func TestWALSubmitEntityCommitSettlementCallbackCloseControl_r9port(t *testing.T) {
	runWALSubmitEntityCommitSettlementCloseChild_r9port(t, "TestWALSubmitEntityCommitSettlementCallbackCloseControlChild_r9port")
}

// runWALSubmitEntityCommitSettlementCloseChild_r9port runs one scenario in this test
// binary as an isolated child process. The watchdog owns the bound and the
// join: a child that self-deadlocks is killed or stopped by its own test
// timeout, and the parent reports the child's output. The regression is
// deterministic, so only the reported evidence waits on the bound.
func runWALSubmitEntityCommitSettlementCloseChild_r9port(t *testing.T, child string) {
	t.Helper()
	require.Empty(t, os.Getenv(walSettlementCloseChildEnv_r9port), "the isolated settlement Close child driver runs only in the parent test process")
	binary, err := os.Executable()
	require.NoError(t, err)
	watchdog, stopChild := context.WithTimeout(t.Context(), walSettlementCloseWatchdog_r9port)
	defer stopChild()
	command := exec.CommandContext(watchdog, binary,
		"-test.run=^"+child+"$", "-test.count=1", "-test.timeout="+walSettlementCloseChildTimeout_r9port, "-test.v")
	// The child's temporary files stay inside the parent's own directory, so a
	// killed child leaves nothing outside the fixture.
	command.Env = append(os.Environ(), walSettlementCloseChildEnv_r9port+"=1", "TMPDIR="+t.TempDir())
	// CombinedOutput starts the child, waits for it to exit, and kills it when
	// the watchdog expires: the parent owns the bound and the join.
	output, childErr := command.CombinedOutput()
	require.NoError(t, watchdog.Err(),
		"the isolated %s child was still running after the %v watchdog; WALEngine.Close from an accepted settlement callback waited on the work that runs it:\n%s",
		child, walSettlementCloseWatchdog_r9port, output)
	require.NoError(t, childErr, "the isolated %s child reported the settlement Close regression:\n%s", child, output)
	require.Contains(t, string(output), walSettlementClosePassedMarker_r9port)
}

// requireWALSubmitEntityCommitSettlementCloseChild_r9port keeps a child case out of an
// ordinary run of this package; only its parent driver starts it.
func requireWALSubmitEntityCommitSettlementCloseChild_r9port(t *testing.T) {
	t.Helper()
	if os.Getenv(walSettlementCloseChildEnv_r9port) != "1" {
		t.Skip("isolated WAL settlement Close case: run through its parent test")
	}
}

// walSettlementCloseEngine_r9port is one accepted-command owner under test: a native
// Badger engine (optionally behind a public accepted-command submitter), its
// journal and the WALEngine that accepts the command.
type walSettlementCloseEngine_r9port struct {
	dir   string
	base  *BadgerEngine
	owner *WALEngine
}

func newWALSubmitEntityCommitSettlementCloseEngine_r9port(t *testing.T, wrap func(*BadgerEngine) Engine) walSettlementCloseEngine_r9port {
	t.Helper()
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	walDir := t.TempDir()
	wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	var engine Engine = base
	if wrap != nil {
		engine = wrap(base)
	}
	return walSettlementCloseEngine_r9port{dir: dir, base: base, owner: NewWALEngine(engine, wal)}
}

// walSettlementClosePrepare_r9port stages one entity command and takes it to the
// prephysical boundary, exactly as an accepted command is prepared before an
// outer transport submits it.
func walSettlementClosePrepare_r9port(t *testing.T, base *BadgerEngine, id NodeID) *PreparedEntityCommit {
	t.Helper()
	tx, err := base.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: id, Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	return prepared
}

// walSettlementCloseRequire_r9port waits for one observable step under the case bound
// and reports the whole goroutine set when it does not complete: a settlement
// callback that cannot Close is the reproduced regression.
func walSettlementCloseRequire_r9port[T any](t *testing.T, step string, done <-chan T) T {
	t.Helper()
	select {
	case value := <-done:
		return value
	case <-time.After(walSettlementCloseReturnBound_r9port):
		t.Fatalf("%s did not complete within %v; the accepted settlement callback waits on its own completing work:\n%s",
			step, walSettlementCloseReturnBound_r9port, walSettlementCloseStacks_r9port())
	}
	var zero T
	return zero
}

// walSettlementCloseWithin_r9port runs one Close under the same bound, for the cases
// whose Close is issued by the test rather than by a settlement callback.
func walSettlementCloseWithin_r9port(t *testing.T, step string, close func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- close() }()
	return walSettlementCloseRequire_r9port(t, step, done)
}

func walSettlementCloseStacks_r9port() string {
	buffer := make([]byte, 1<<18)
	return string(buffer[:runtime.Stack(buffer, true)])
}

// walSettlementCloseRequireStored_r9port reopens the native data directory after a
// Close and proves the accepted image survived it with value.
func walSettlementCloseRequireStored_r9port(t *testing.T, dir string, id NodeID, value string) {
	t.Helper()
	reopened, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	node, err := reopened.GetStoredNode(t.Context(), id)
	require.NoError(t, err, "the accepted image must survive the settlement callback's Close")
	require.Equal(t, value, node.Properties["v"])
}

// walSettlementCloseRequireAbsent_r9port reopens the native data directory after a
// recovery-pending outcome and proves nothing was applied there.
func walSettlementCloseRequireAbsent_r9port(t *testing.T, dir string, id NodeID) {
	t.Helper()
	reopened, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	_, err = reopened.GetStoredNode(t.Context(), id)
	require.ErrorIs(t, err, ErrNotFound, "a recovery-pending accepted command applies nothing")
}

// walSettlementCloseAcknowledgedFailure_r9port is a public accepted-command transport
// whose native application commits and then fails to acknowledge it: the
// accepted outcome is CommitApplied with an error. The command really applied,
// so the durable image is the one the native engine committed.
type walSettlementCloseAcknowledgedFailure_r9port struct {
	*BadgerEngine
	failure error
}

func (s *walSettlementCloseAcknowledgedFailure_r9port) GetInnerEngine() Engine { return s.BadgerEngine }

func (s *walSettlementCloseAcknowledgedFailure_r9port) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	native, err := s.BadgerEngine.SubmitEntityCommit(ctx, prepared, nil)
	if err != nil {
		return nil, err
	}
	outcome := native.WaitOutcome(ctx)
	if outcome.State == CommitApplied {
		outcome.Err = s.failure
	}
	receipt, owner := NewCommitReceipt(settle)
	owner.PublishPhysical(outcome)
	if outcome.State != CommitRecoveryPending {
		_ = owner.Resolve(outcome)
	}
	return receipt, nil
}

// walSettlementCloseUnsubmitted_r9port is a public accepted-command transport whose
// native submission fails after acceptance: the command is recovery-pending and
// has no terminal outcome, so no settlement callback runs.
type walSettlementCloseUnsubmitted_r9port struct {
	*BadgerEngine
	failure error
}

func (s *walSettlementCloseUnsubmitted_r9port) GetInnerEngine() Engine { return s.BadgerEngine }

func (s *walSettlementCloseUnsubmitted_r9port) SubmitEntityCommit(context.Context, *PreparedEntityCommit, func(CommitOutcome)) (*CommitReceipt, error) {
	return nil, s.failure
}

// TestWALSubmitEntityCommitSettlementCallbackCloseAppliedChild_r9port is the isolated
// accepted-success case: the settlement callback closes its WAL engine.
func TestWALSubmitEntityCommitSettlementCallbackCloseAppliedChild_r9port(t *testing.T) {
	requireWALSubmitEntityCommitSettlementCloseChild_r9port(t)
	t.Cleanup(config.WithWALEnabled())
	fixture := newWALSubmitEntityCommitSettlementCloseEngine_r9port(t, nil)
	id := NodeID("test:wal-settlement-applied")
	prepared := walSettlementClosePrepare_r9port(t, fixture.base, id)
	settled := make(chan CommitOutcome, 1)
	closed := make(chan error, 1)
	receipt, err := fixture.owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		settled <- outcome
		closed <- fixture.owner.Close()
	})
	require.NoError(t, err)
	outcome := walSettlementCloseRequire_r9port(t, "the accepted-success settlement callback outcome", settled)
	closeErr := walSettlementCloseRequire_r9port(t, "WALEngine.Close from the accepted-success settlement callback", closed)
	require.NoError(t, closeErr, "the settlement callback's own Close must complete")
	waitCtx, cancel := context.WithTimeout(context.Background(), walSettlementCloseReturnBound_r9port)
	defer cancel()
	require.NoError(t, receipt.Wait(waitCtx), "the receipt must keep the applied accepted outcome")
	require.Equal(t, CommitApplied, outcome.State)
	require.NoError(t, outcome.Err)
	walSettlementCloseRequireStored_r9port(t, fixture.dir, id, "accepted")
	fmt.Println(walSettlementClosePassedMarker_r9port)
}

// TestWALSubmitEntityCommitSettlementCallbackCloseFailureChild_r9port is the isolated
// accepted-failure case: storage committed, the acknowledgement failed, and the
// settlement callback closes its WAL engine while preserving that outcome.
func TestWALSubmitEntityCommitSettlementCallbackCloseFailureChild_r9port(t *testing.T) {
	requireWALSubmitEntityCommitSettlementCloseChild_r9port(t)
	t.Cleanup(config.WithWALEnabled())
	failure := errors.New("storage committed but its acknowledgement failed")
	fixture := newWALSubmitEntityCommitSettlementCloseEngine_r9port(t, func(base *BadgerEngine) Engine {
		return &walSettlementCloseAcknowledgedFailure_r9port{BadgerEngine: base, failure: failure}
	})
	id := NodeID("test:wal-settlement-failure")
	prepared := walSettlementClosePrepare_r9port(t, fixture.base, id)
	settled := make(chan CommitOutcome, 1)
	closed := make(chan error, 1)
	receipt, err := fixture.owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		settled <- outcome
		closed <- fixture.owner.Close()
	})
	require.NoError(t, err)
	outcome := walSettlementCloseRequire_r9port(t, "the accepted-failure settlement callback outcome", settled)
	closeErr := walSettlementCloseRequire_r9port(t, "WALEngine.Close from the accepted-failure settlement callback", closed)
	require.NoError(t, closeErr, "the settlement callback's own Close must complete")
	waitCtx, cancel := context.WithTimeout(context.Background(), walSettlementCloseReturnBound_r9port)
	defer cancel()
	waitErr := receipt.Wait(waitCtx)
	require.Error(t, waitErr, "a failed acknowledgement after commit stays an accepted error")
	var reported *AcceptedCommitError
	require.ErrorAs(t, waitErr, &reported)
	require.Equal(t, CommitApplied, reported.State)
	require.ErrorIs(t, reported.Err, failure)
	require.Equal(t, CommitApplied, outcome.State)
	var settledAccepted *AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &settledAccepted)
	require.Equal(t, CommitApplied, settledAccepted.State)
	require.ErrorIs(t, settledAccepted.Err, failure)
	walSettlementCloseRequireStored_r9port(t, fixture.dir, id, "accepted")
	fmt.Println(walSettlementClosePassedMarker_r9port)
}

// TestWALSubmitEntityCommitSettlementCallbackClosePendingChild_r9port is the isolated
// recovery-pending failure case: acceptance is durable but nothing is terminal,
// so the approved contract runs no settlement callback; Close from outside
// completes and applies nothing.
func TestWALSubmitEntityCommitSettlementCallbackClosePendingChild_r9port(t *testing.T) {
	requireWALSubmitEntityCommitSettlementCloseChild_r9port(t)
	t.Cleanup(config.WithWALEnabled())
	failure := errors.New("native accepted application was not submitted")
	fixture := newWALSubmitEntityCommitSettlementCloseEngine_r9port(t, func(base *BadgerEngine) Engine {
		return &walSettlementCloseUnsubmitted_r9port{BadgerEngine: base, failure: failure}
	})
	id := NodeID("test:wal-settlement-pending")
	prepared := walSettlementClosePrepare_r9port(t, fixture.base, id)
	settled := make(chan CommitOutcome, 1)
	receipt, err := fixture.owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		settled <- outcome
	})
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(context.Background(), walSettlementCloseReturnBound_r9port)
	defer cancel()
	outcome := receipt.WaitOutcome(waitCtx)
	require.Equal(t, CommitRecoveryPending, outcome.State)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.Equal(t, CommitRecoveryPending, accepted.State)
	require.ErrorIs(t, accepted.Err, failure)
	require.NoError(t, walSettlementCloseWithin_r9port(t, "WALEngine.Close after a recovery-pending accepted outcome", fixture.owner.Close))
	select {
	case ran := <-settled:
		t.Fatalf("the approved contract runs no settlement callback for a recovery-pending outcome, got %+v", ran)
	default:
	}
	walSettlementCloseRequireAbsent_r9port(t, fixture.dir, id)
	fmt.Println(walSettlementClosePassedMarker_r9port)
}

// TestWALSubmitEntityCommitSettlementCallbackCloseControlChild_r9port is the isolated
// healthy non-reentrant control: the callback does not Close, the accepted
// entity is writable afterwards, and Close outside the callback completes.
func TestWALSubmitEntityCommitSettlementCallbackCloseControlChild_r9port(t *testing.T) {
	requireWALSubmitEntityCommitSettlementCloseChild_r9port(t)
	t.Cleanup(config.WithWALEnabled())
	fixture := newWALSubmitEntityCommitSettlementCloseEngine_r9port(t, nil)
	id := NodeID("test:wal-settlement-control")
	prepared := walSettlementClosePrepare_r9port(t, fixture.base, id)
	settled := make(chan CommitOutcome, 1)
	receipt, err := fixture.owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		settled <- outcome
	})
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(context.Background(), walSettlementCloseReturnBound_r9port)
	defer cancel()
	require.NoError(t, receipt.Wait(waitCtx))
	outcome := walSettlementCloseRequire_r9port(t, "the non-reentrant settlement callback outcome", settled)
	require.Equal(t, CommitApplied, outcome.State)
	require.NoError(t, outcome.Err)
	require.NoError(t, fixture.base.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{ID: id, Properties: map[string]any{"v": "after-control"}}}),
		"settlement releases the accepted entity's locks")
	require.NoError(t, walSettlementCloseWithin_r9port(t, "WALEngine.Close outside a healthy settlement callback", fixture.owner.Close))
	walSettlementCloseRequireStored_r9port(t, fixture.dir, id, "after-control")
	fmt.Println(walSettlementClosePassedMarker_r9port)
}
