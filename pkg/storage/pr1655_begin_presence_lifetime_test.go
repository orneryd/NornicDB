package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func pr1655PresenceLifetimeEngine(t *testing.T) (*BadgerEngine, *memory.Policy) {
	t.Helper()
	clock := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	policy := memory.NewPolicy(1<<20, 0, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: 1, Ceiling: 1 << 30}, nil
	}, func() time.Time { return clock })
	require.NoError(t, policy.Refresh())
	engine, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir: t.TempDir(), LowMemory: true, WriteBehind: true,
		WriteBehindInterval: time.Hour, MemoryPolicy: policy,
	}))
	require.NoError(t, err)
	return engine, policy
}

func pr1655PresenceLifetimeCreate(t *testing.T, engine *BadgerEngine, id NodeID) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: id, Properties: map[string]interface{}{"body": "buffered presence"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func pr1655PresenceLifetimeReader(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() {
		if tx.IsActive() {
			require.NoError(t, tx.Rollback())
		}
	})
	require.NoError(t, tx.SetNamespace("test"))
	require.NotNil(t, tx.beginPresence)
	return tx
}

func requirePR1655PresenceReleased(t *testing.T, presence *transactionBeginPresence) {
	t.Helper()
	presence.mu.RLock()
	defer presence.mu.RUnlock()
	require.True(t, presence.released)
	require.False(t, presence.listed)
	require.Nil(t, presence.active)
	require.Nil(t, presence.buffer)
	require.Nil(t, presence.commits)
	require.Nil(t, presence.draining)
	require.Nil(t, presence.prev)
	require.Nil(t, presence.next)
	for _, generation := range presence.drainingBuf {
		require.Nil(t, generation)
	}
}

// Retired buffer memory belongs to the final reader, not to the flusher or
// the first frame that finishes. Repeating either frame's close is harmless.
func TestPR1655BeginPresenceSharedGenerationLastPin(t *testing.T) {
	for _, firstExit := range []string{"commit", "rollback"} {
		t.Run(firstExit, func(t *testing.T) {
			engine, policy := pr1655PresenceLifetimeEngine(t)
			pr1655PresenceLifetimeCreate(t, engine, "test:shared")
			first := pr1655PresenceLifetimeReader(t, engine)
			second := pr1655PresenceLifetimeReader(t, engine)
			firstPresence, secondPresence := first.beginPresence, second.beginPresence
			generation := firstPresence.active
			require.Same(t, generation, secondPresence.active)
			require.Equal(t, int64(2), generation.beginPins.Load())
			reserved := policy.Reserved()
			require.Positive(t, reserved)
			require.NoError(t, engine.FlushWriteBehind())
			require.True(t, generation.queueRetired.Load())
			require.False(t, generation.leasesReleased.Load())
			require.Equal(t, reserved, policy.Reserved())
			if firstExit == "commit" {
				require.NoError(t, first.Commit())
			} else {
				require.NoError(t, first.Rollback())
			}
			require.Nil(t, first.beginPresence)
			requirePR1655PresenceReleased(t, firstPresence)
			require.Equal(t, int64(1), generation.beginPins.Load())
			require.False(t, generation.leasesReleased.Load())
			require.Equal(t, reserved, policy.Reserved())
			require.ErrorIs(t, first.Rollback(), ErrTransactionClosed)
			require.Equal(t, int64(1), generation.beginPins.Load())
			require.NoError(t, second.Rollback())
			require.Nil(t, second.beginPresence)
			requirePR1655PresenceReleased(t, secondPresence)
			require.Zero(t, generation.beginPins.Load())
			require.True(t, generation.leasesReleased.Load())
			require.Zero(t, policy.Reserved())
			engine.writeBehind.beginPresenceMu.Lock()
			remaining := engine.writeBehind.beginPresences
			engine.writeBehind.beginPresenceMu.Unlock()
			require.Nil(t, remaining)
		})
	}
}

// Close must return pinned memory even when the client never closes its frame.
// Retaining the released presence shell is deliberate proof that heavy buffer
// references are cleared rather than merely left for garbage collection.
func TestPR1655BeginPresenceCloseOpenAndAbandoned(t *testing.T) {
	for _, abandoned := range []bool{false, true} {
		t.Run(fmt.Sprintf("abandoned=%t", abandoned), func(t *testing.T) {
			engine, policy := pr1655PresenceLifetimeEngine(t)
			pr1655PresenceLifetimeCreate(t, engine, "test:close")
			reader, err := engine.BeginTransaction()
			require.NoError(t, err)
			presence := reader.beginPresence
			require.NotNil(t, presence)
			generation := presence.active
			require.Equal(t, int64(1), generation.beginPins.Load())
			require.Positive(t, policy.Reserved())
			if abandoned {
				reader = nil
			}
			// No frame completion happens before Close returns.
			require.NoError(t, engine.Close())
			requirePR1655PresenceReleased(t, presence)
			require.Zero(t, generation.beginPins.Load())
			require.True(t, generation.queueRetired.Load())
			require.True(t, generation.leasesReleased.Load())
			require.Zero(t, policy.Reserved())
			lease, err := policy.Reserve((1 << 20) - 2)
			require.NoError(t, err, "the next budget consumer can reuse all released memory")
			lease.Release()
			if reader != nil {
				require.NoError(t, reader.Rollback())
				require.Nil(t, reader.beginPresence)
			}
			require.NoError(t, engine.Close())
			require.Zero(t, generation.beginPins.Load())
			require.Zero(t, policy.Reserved())
		})
	}
}

func TestPR1655BeginPresenceImplicitAdvanceReplacesPins(t *testing.T) {
	engine, policy := pr1655PresenceLifetimeEngine(t)
	pr1655PresenceLifetimeCreate(t, engine, "test:old")
	reader := pr1655PresenceLifetimeReader(t, engine)
	require.NoError(t, reader.SetImplicit(true))
	oldPresence := reader.beginPresence
	oldGeneration := oldPresence.active
	require.NoError(t, engine.FlushWriteBehind())
	pr1655PresenceLifetimeCreate(t, engine, "test:next")
	engine.writeBehind.mu.RLock()
	newGeneration := engine.writeBehind.active
	engine.writeBehind.mu.RUnlock()
	require.NotSame(t, oldGeneration, newGeneration)
	require.NoError(t, reader.ResumeAfterIndependentCommit())
	requirePR1655PresenceReleased(t, oldPresence)
	require.Zero(t, oldGeneration.beginPins.Load())
	require.True(t, oldGeneration.leasesReleased.Load())
	require.NotNil(t, reader.beginPresence)
	require.NotSame(t, oldPresence, reader.beginPresence)
	require.Same(t, newGeneration, reader.beginPresence.active)
	require.Equal(t, int64(1), newGeneration.beginPins.Load())
	require.NoError(t, engine.FlushWriteBehind())
	require.False(t, newGeneration.leasesReleased.Load())
	require.Positive(t, policy.Reserved())
	require.NoError(t, reader.Rollback())
	require.Zero(t, newGeneration.beginPins.Load())
	require.True(t, newGeneration.leasesReleased.Load())
	require.Zero(t, policy.Reserved())
}

func TestPR1655BeginPresenceDoesNotRetainFutureGenerations(t *testing.T) {
	engine, policy := pr1655PresenceLifetimeEngine(t)
	pr1655PresenceLifetimeCreate(t, engine, "test:original")
	reader := pr1655PresenceLifetimeReader(t, engine)
	presence := reader.beginPresence
	initial := presence.active
	initialPrefix := len(presence.commits)
	require.Positive(t, initialPrefix)
	require.NoError(t, engine.FlushWriteBehind())
	reserved := policy.Reserved()
	require.Positive(t, reserved)
	for i := 0; i < 16; i++ {
		pr1655PresenceLifetimeCreate(t, engine, NodeID(fmt.Sprintf("test:future-%02d", i)))
		engine.writeBehind.mu.RLock()
		future := engine.writeBehind.active
		engine.writeBehind.mu.RUnlock()
		require.NotSame(t, initial, future)
		require.Zero(t, future.beginPins.Load(), "an old frame never pins a future generation")
		require.NoError(t, engine.FlushWriteBehind())
		require.True(t, future.queueRetired.Load())
		require.True(t, future.leasesReleased.Load())
		require.Equal(t, reserved, policy.Reserved(), "only the initial generation remains charged")
		require.Same(t, initial, presence.active)
		require.Empty(t, presence.draining)
		require.Len(t, presence.commits, initialPrefix)
		require.Equal(t, int64(1), initial.beginPins.Load())
	}
	require.NoError(t, reader.Rollback())
	requirePR1655PresenceReleased(t, presence)
	require.Zero(t, initial.beginPins.Load())
	require.True(t, initial.leasesReleased.Load())
	require.Zero(t, policy.Reserved())
}

func TestPR1655BeginPresenceCanceledCommitKeepsPinsUntilRollback(t *testing.T) {
	engine, policy := pr1655PresenceLifetimeEngine(t)
	pr1655PresenceLifetimeCreate(t, engine, "test:cancel")
	reader := pr1655PresenceLifetimeReader(t, engine)
	presence := reader.beginPresence
	generation := presence.active
	require.NoError(t, engine.FlushWriteBehind())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, reader.CommitContext(ctx), context.Canceled)
	require.True(t, reader.IsActive(), "pre-canceled commit has not started")
	require.Same(t, presence, reader.beginPresence)
	require.Equal(t, int64(1), generation.beginPins.Load())
	require.Positive(t, policy.Reserved())
	require.NoError(t, reader.Rollback())
	requirePR1655PresenceReleased(t, presence)
	require.Zero(t, generation.beginPins.Load())
	require.Zero(t, policy.Reserved())
}
