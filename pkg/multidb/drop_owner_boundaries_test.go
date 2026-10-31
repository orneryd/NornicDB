package multidb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// receiveWithin bounds a test wait, so a missing step fails instead of hanging.
func receiveWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not happen within 10s", what)
	}
	var zero T
	return zero
}

// requireAcceptedDropOutcome accepts a finished DROP (nil) or the accepted
// outcome of one whose request stopped waiting; never a plain rejection.
func requireAcceptedDropOutcome(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted, "a request that stops after acceptance gets the accepted outcome")
	require.NotEqual(t, storage.CommitDiscarded, accepted.State)
}

// Author: NOR-288 lane (Claude), 2026-10-31. Once a DROP's intent is
// accepted, its completion belongs to the DROP owner, not to the request.
// The request is cancelled at each point where it can end after acceptance:
// while the intent settles, during the data purge, while the final catalog
// change waits for the catalog gate, just before that change is submitted,
// and while its outcome is awaited. Each time the caller gets the accepted
// outcome, and the owner still purges the data, removes the record and clears
// the fence, so CREATE of the name succeeds with no further DROP and no
// restart.
func TestDropOwnerFinishesAcceptedDropAtEveryRequestBoundary(t *testing.T) {
	for _, boundary := range []string{"intent-wait", "purge", "catalog-gate", "final-submit", "final-wait"} {
		t.Run(boundary, func(t *testing.T) {
			manager, probe, native, _ := newCatalogDropBarrier(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch boundary {
			case "intent-wait":
				probe.mode, probe.faultAfter, probe.submitted = "accepted-wait", 1, make(chan struct{})
			case "purge", "catalog-gate":
				native.purgeEntered = make(chan int32, 1)
				native.purgePermits = []chan struct{}{make(chan struct{})}
			case "final-submit":
				// The request ends just before the final change is submitted.
				probe.mode, probe.faultAfter, probe.submitted = "accepted-wait", 2, make(chan struct{})
				probe.beforeSubmit = cancel
			case "final-wait":
				probe.mode, probe.faultAfter, probe.submitted = "accepted-wait", 2, make(chan struct{})
			}
			result := make(chan error, 1)
			go func() { result <- manager.DropDatabase(ctx, "target") }()
			switch boundary {
			case "intent-wait":
				receiveWithin(t, probe.submitted, "intent submission")
				// Later catalog transitions (the owner's completion) commit natively.
				probe.mode = ""
				cancel()
				requireAcceptedDropOutcome(t, receiveWithin(t, result, "DROP returning after cancellation"))
				require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied}))
			case "purge":
				receiveWithin(t, native.purgeEntered, "data purge")
				cancel()
				close(native.purgePermits[0])
				requireAcceptedDropOutcome(t, receiveWithin(t, result, "DROP returning after cancellation"))
			case "catalog-gate":
				receiveWithin(t, native.purgeEntered, "data purge")
				// Hold the catalog gate, so the final change waits for it.
				receiveWithin(t, manager.catalogGate, "catalog gate")
				close(native.purgePermits[0])
				cancel()
				requireAcceptedDropOutcome(t, receiveWithin(t, result, "DROP returning after cancellation"))
				manager.catalogGate <- struct{}{}
			case "final-submit", "final-wait":
				receiveWithin(t, probe.submitted, "final catalog submission")
				if boundary == "final-wait" {
					cancel()
				}
				requireAcceptedDropOutcome(t, receiveWithin(t, result, "DROP returning after cancellation"))
				probe.mode = ""
				require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied}))
			}
			require.Eventually(t, func() bool {
				_, readErr := native.GetNode("target:old")
				manager.mu.RLock()
				fenced := manager.pendingDrops["target"]
				manager.mu.RUnlock()
				return !manager.Exists("target") && !fenced && errors.Is(readErr, storage.ErrNotFound)
			}, 10*time.Second, 10*time.Millisecond, "the DROP owner finishes the accepted drop")
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
