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

// Author: NOR-288 lane (Claude), 2026-10-30. Release-buildable. A DROP whose caller stops
// waiting while its intent is in flight (driver timeout, disconnect, a
// replication wait timeout) gets a non-retryable accepted outcome. When the
// intent applies later, the DROP owner itself finishes the drop: data purged,
// fence cleared, the name reusable, with no further DROP and no restart.
// "at-apply" cancels while the intent settles, so the caller observes either
// a detached wait or an applied intent followed by its own cancellation.
func TestDropOwnerFinishesIntentAfterCallerStoppedWaiting(t *testing.T) {
	for _, timing := range []string{"before-apply", "at-apply"} {
		rounds := 1
		if timing == "at-apply" {
			rounds = 6
		}
		for round := 0; round < rounds; round++ {
			t.Run(timing, func(t *testing.T) {
				manager, probe, native, _ := newCatalogDropBarrier(t)
				probe.mode = "accepted-wait"
				probe.faultAfter = 1
				probe.submitted = make(chan struct{})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- manager.DropDatabase(ctx, "target") }()
				<-probe.submitted
				// Later catalog transitions (the owner's completion) commit natively.
				probe.mode = ""
				var err error
				if timing == "before-apply" {
					cancel()
					err = <-result
					require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied}))
				} else {
					go cancel()
					require.NoError(t, storagefault.FinishPreparedReceipt(probe.prepared, probe.receiptOwner, storage.CommitOutcome{State: storage.CommitApplied}))
					err = <-result
				}
				if err != nil {
					var accepted *storage.AcceptedCommitError
					require.ErrorAs(t, err, &accepted, "a stopped wait reports the accepted outcome")
				}
				require.Eventually(t, func() bool {
					_, readErr := native.GetNode("target:old")
					manager.mu.RLock()
					fenced := manager.pendingDrops["target"]
					manager.mu.RUnlock()
					return !manager.Exists("target") && !fenced && errors.Is(readErr, storage.ErrNotFound)
				}, 10*time.Second, 10*time.Millisecond, "the DROP owner finishes the applied intent")
				require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: "target"}))
			})
		}
	}
}
