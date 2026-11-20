// NOR-288 bounded follow-up evidence (delegated worker lane; author/date not
// asserted). Task: establish or refute the reported duplicate-frame immediate
// settlement delivery, without changing the proposal's production files.
//
// Reported shape:
//
//	pkg/storage/accepted_entity_application.go, registerAcceptedEntityApplication:
//	    if application != nil && prepared != application.prepared {
//	        prepared.Abort()
//	        application.receipt.OnSettled(func(outcome CommitOutcome) { ... })
//	    }
//
//	CommitReceipt.OnSettled delivers at once when the receipt is not
//	CommitRecoveryPending (it waits for Done, then runs the callback inline).
//	The report is that this immediate delivery can run during park under a
//	caller's accepted enrollment.
//
// Two source facts bear on it: parkAcceptedEntityApplication registers with a
// nil settle, so the callback it can deliver is only the duplicate frame's own
// release set (drainReleaseCallbacks) - a native cleanup owner, never a caller's
// settlement observer - and the one terminal resolution of a registered receipt
// is preceded by retireRegistry and by the accepted work Leave.
//
// The two tests below replace a registry-absent control with owner-level
// evidence: the first records the ordering at terminal delivery, the second
// uses a pointer retained across retirement - exactly what the register path
// holds after it unlocks - and shows what the retained pointer delivers.
package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestAcceptedRegistryRetiredBeforeTerminalSettlement records ordering at
// terminal delivery through the public accepted path.
func TestAcceptedRegistryRetiredBeforeTerminalSettlement(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	base, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	walDir := t.TempDir()
	wal, err := NewWAL(walDir, &WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	owner := NewWALEngine(base, wal)
	t.Cleanup(func() { _ = owner.Close() })
	tx, err := base.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &Node{ID: NodeID("test:registry-terminal-order"), Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	key := acceptedApplicationKey{namespace: prepared.Namespace(), logicalID: prepared.LogicalID()}

	var mu sync.Mutex
	registeredAtSettlement := make([]bool, 0, 1)
	states := make([]CommitOutcomeState, 0, 1)
	settled := make(chan struct{})
	receipt, err := owner.SubmitEntityCommit(context.Background(), prepared, func(outcome CommitOutcome) {
		base.mu.Lock()
		registered := base.acceptedApplications[key] != nil
		base.mu.Unlock()
		mu.Lock()
		registeredAtSettlement = append(registeredAtSettlement, registered)
		states = append(states, outcome.State)
		mu.Unlock()
		close(settled)
	})
	require.NoError(t, err)
	select {
	case <-settled:
	case <-time.After(30 * time.Second):
		t.Fatal("the accepted command never delivered its terminal settlement")
	}
	mu.Lock()
	observed, state := registeredAtSettlement, states
	mu.Unlock()
	require.Len(t, observed, 1, "the accepted command settles exactly once")
	require.Equal(t, CommitApplied, state[0])
	require.False(t, observed[0],
		"the registry no longer owns the application at this terminal delivery; a concurrent previously retained lookup is outside this control")
	require.NoError(t, receipt.Wait(t.Context()))
	base.mu.Lock()
	remaining := base.acceptedApplications[key]
	base.mu.Unlock()
	require.Nil(t, remaining)
	require.NoError(t, owner.Close())
	require.NoError(t, base.Close())
}

// TestAcceptedRegistryRetainedPointerJoinsDeliveredSettlement establishes what a
// registry pointer retained across retirement delivers. The register path holds
// exactly such a pointer: it reads the application under the engine mutex,
// unlocks, aborts the duplicate frame and only then registers its settle.
//
// The owner barriers below are deterministic. The accepted owner's recorded
// decision and its delivered settlement are separated on purpose: the decision
// is terminal while Done is still open, which is one of the two states a
// duplicate registration can find. In that state the retained pointer still
// names the same application after its registry removal, its OnSettled callback
// cannot deliver before the accepted owner's own observer returns, and it then
// delivers that one recorded terminal outcome exactly once.
//
// Limit: reproducing the production interleaving itself (registry read, unlock,
// retirement, OnSettled) requires a pause inside registerAcceptedEntityApplication
// that this package does not expose, so the interleaving remains unverified.
// What is established here is its consequence: the retained pointer cannot
// fabricate or duplicate an outcome, and it joins the delivered settlement
// rather than only the recorded decision.
func TestAcceptedRegistryRetainedPointerJoinsDeliveredSettlement(t *testing.T) {
	b := newTestEngine(t)
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: NodeID("test:registry-retained-pointer")})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	key := acceptedApplicationKey{namespace: prepared.Namespace(), logicalID: prepared.LogicalID()}

	application := b.registerAcceptedEntityApplication(prepared, nil)
	// The pointer a duplicate registration reads before it unlocks.
	b.mu.Lock()
	retained := b.acceptedApplications[key]
	b.mu.Unlock()
	require.Same(t, application, retained, "the register path holds the registered application pointer")

	entered := make(chan struct{})
	permit := make(chan struct{})
	application.receipt.OnSettled(func(CommitOutcome) {
		close(entered)
		select {
		case <-permit:
		case <-time.After(30 * time.Second):
		}
	})
	t.Cleanup(func() {
		select {
		case <-permit:
		default:
			close(permit)
		}
	})
	// The accepted owner's applied retirement order: registry removal, then the
	// one terminal resolution.
	application.retireRegistry()
	discarded := CommitOutcome{State: CommitDiscarded, Err: ErrStorageClosed}
	settle, decided, err := application.owner.claim(discarded)
	require.NoError(t, err)
	require.True(t, decided)
	// Delivery from a frame of the receipt's own, so this test can hold the
	// observer open while it inspects the recorded decision.
	go application.owner.deliverTerminal(settle, discarded)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the accepted owner's settlement observer did not start")
	}
	outcome, _ := retained.receipt.Decision()
	require.Equal(t, CommitDiscarded, outcome.State, "the decision is recorded before the observer returns")
	select {
	case <-retained.receipt.Done():
		t.Fatal("Done closed before the accepted owner's observer returned")
	default:
	}
	b.mu.Lock()
	require.Nil(t, b.acceptedApplications[key], "the application is retired from the registry")
	b.mu.Unlock()
	require.Same(t, application, retained, "retirement does not invalidate an earlier retained pointer")

	// Exactly what the register path then does with the retained pointer.
	settles := make(chan CommitOutcome, 2)
	delivered := make(chan struct{})
	go func() {
		retained.receipt.OnSettled(func(final CommitOutcome) { settles <- final })
		close(delivered)
	}()
	select {
	case <-delivered:
		t.Fatal("the retained pointer delivered before the accepted owner's observer returned")
	case <-time.After(100 * time.Millisecond):
	}
	close(permit)
	select {
	case <-delivered:
	case <-time.After(10 * time.Second):
		t.Fatal("the retained pointer never delivered after the accepted owner's observer returned")
	}
	select {
	case final := <-settles:
		require.Equal(t, CommitDiscarded, final.State, "the retained pointer sees the one recorded outcome")
		require.ErrorIs(t, final.Err, ErrStorageClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("the retained pointer never invoked its settle callback")
	}
	select {
	case duplicate := <-settles:
		t.Fatalf("the retained pointer delivered twice; second outcome %+v", duplicate)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-retained.receipt.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the accepted owner's observer returned")
	}
}
