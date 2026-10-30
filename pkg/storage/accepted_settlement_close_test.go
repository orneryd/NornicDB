package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-31. A commit's settlement runs after
// its worker left the lifecycle owner, and it can call Sync. Close retires the
// Badger handle, so a Sync that passed its open check must be joined. Without
// that, this Sync reads the released handle after Close returned.
func TestCloseJoinsSyncInProgress(t *testing.T) {
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	entered, permit := make(chan struct{}), make(chan struct{})
	base.nativeSyncHook = func() {
		close(entered)
		<-permit
	}
	synced := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				synced <- fmt.Errorf("Sync used the retired handle: %v", r)
			}
		}()
		synced <- base.Sync()
	}()
	requireWithin(t, "Sync reaching its native call", entered)
	closed := make(chan error, 1)
	go func() { closed <- base.Close() }()
	require.Eventually(t, base.work.Closing, 10*time.Second, time.Millisecond, "Close seals admission")
	select {
	case <-closed:
		t.Error("Close returned while a Sync was still using the engine")
	case <-time.After(200 * time.Millisecond):
	}
	close(permit)
	assert.NoError(t, requireWithin(t, "the joined Sync", synced))
	require.NoError(t, requireWithin(t, "Close after the Sync", closed))
	base.nativeSyncHook = nil
	assert.ErrorIs(t, base.Sync(), ErrStorageClosed, "a Sync after Close is refused")
}

// Author: NOR-288 lane (Claude), 2026-10-31. Sync's admission must not undo the
// constraint on settlement: a callback that syncs and then closes its own engine
// finishes, because nothing it holds is what Close waits for.
func TestAcceptedSettlementCanSyncAndCloseItsEngine(t *testing.T) {
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	p, payload := acceptedClosePhaseFrame(t, base, "test:settled")
	p.Abort()
	envelope, operations, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	synced, closed := make(chan error, 1), make(chan error, 1)
	receipt, err := SubmitAcceptedEntityTransaction(context.Background(), base, envelope, operations, func(CommitOutcome) {
		synced <- base.Sync()
		closed <- base.Close()
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := receipt.Wait(ctx); err != nil {
		// The broken owner waits for itself inside Close. A second Close in
		// cleanup would conceal this bounded failure behind a package timeout.
		t.Errorf("settlement could not sync and close its engine: %v", err)
		return
	}
	assert.NoError(t, <-synced)
	assert.NoError(t, <-closed)
}
