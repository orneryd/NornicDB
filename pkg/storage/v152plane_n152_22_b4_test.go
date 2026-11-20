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
// Two source facts bear on it, and this file asserts the second one:
//
//  1. parkAcceptedEntityApplication registers with a nil settle
//     (accepted_entity_application.go:100), so the callback it can deliver is
//     only the duplicate frame's own release set (drainReleaseCallbacks) — a
//     native cleanup owner, never a caller's settlement observer.
//  2. The immediate branch additionally needs the registered application's
//     receipt to be terminal. The one terminal resolution of that receipt
//     (accepted_entity_application.go:266) is preceded by retireRegistry
//     (line 263) and by the accepted work Leave (line 265), so a registry hit
//     always finds a CommitRecoveryPending receipt.
//
// This control records retirement ordering through the public accepted path: at the
// moment a terminal outcome is delivered, the engine registry no longer owns
// the application. This does not rule out another goroutine retaining an earlier
// registry lookup across retirement and later entering OnSettled.
package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestAcceptedRegistryRetiredBeforeTerminalSettlement_r9port records ordering at
// terminal delivery; it does not exercise a concurrent retained registry lookup.
func TestAcceptedRegistryRetiredBeforeTerminalSettlement_r9port(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
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
