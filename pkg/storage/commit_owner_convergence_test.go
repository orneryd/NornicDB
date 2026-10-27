package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Both real release owners must detach once and invoke outside their actual
// mutex, including when a callback reenters that same owner.
func TestReleaseCallbackOwnersDetachBeforeReentry(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "prepared"}[prepared], func(t *testing.T) {
			// Concurrent empty drains may briefly acquire the mutex while the
			// callback runs. Prove reentry without that unrelated contention,
			// then prove exactly-once release on fresh owners under contention.
			for _, concurrent := range []bool{false, true} {
				t.Run(map[bool]string{false: "reentry", true: "concurrent"}[concurrent], func(t *testing.T) {
					var calls atomic.Int32
					var unlocked atomic.Bool
					var release func()
					if prepared {
						p := &PreparedEntityCommit{}
						release = func() { drainReleaseCallbacks(&p.hooksMu, &p.appliedHooks) }
						p.appliedHooks = []func(){func() {
							calls.Add(1)
							if !concurrent && p.hooksMu.TryLock() {
								unlocked.Store(true)
								p.hooksMu.Unlock()
								release()
							}
						}}
					} else {
						tx := &BadgerTransaction{}
						release = tx.ReleaseMergeKeys
						tx.mergeKeyReleases = map[uniqueConstraintLockKey]func(){{label: "Doc", property: "id", value: "one"}: func() {
							calls.Add(1)
							if !concurrent && tx.mu.TryLock() {
								unlocked.Store(true)
								tx.mu.Unlock()
								release()
							}
						}}
					}
					if !concurrent {
						release()
						require.EqualValues(t, 1, calls.Load())
						require.True(t, unlocked.Load(), "callbacks execute outside their own mutex and can reenter")
						return
					}
					var wg sync.WaitGroup
					for i := 0; i < 20; i++ {
						wg.Add(1)
						go func() { defer wg.Done(); release() }()
					}
					wg.Wait()
					require.EqualValues(t, 1, calls.Load())
				})
			}
		})
	}
}

type invalidEntityPolicyStarter struct {
	Engine
	calls int
}

func (s *invalidEntityPolicyStarter) BeginTransaction() (*BadgerTransaction, error) {
	s.calls++
	return nil, ErrInvalidData
}
func TestEntityTransactionRejectsPolicyBeforeNativeAcquisition(t *testing.T) {
	starter := &invalidEntityPolicyStarter{}
	tx, err := BeginEntityTransaction(starter, EntityCommitMode(255))
	require.ErrorIs(t, err, ErrInvalidData)
	require.Nil(t, tx)
	require.Zero(t, starter.calls)

	frame := &BadgerTransaction{Status: TxStatusActive}
	require.ErrorIs(t, frame.UpdateGraphNode(&Node{ID: "test:n"}, EntityImagePolicy(255)), ErrInvalidData)
	require.Empty(t, frame.namespace, "invalid policy must not pin a namespace")
	require.Empty(t, frame.pendingNodes)
	// Stored images still obey the original client-detachment boundary.
	frame.clientDetached.Store(true)
	require.ErrorIs(t, frame.UpdateGraphNode(&Node{ID: "test:n"}, StoredNodeImages), ErrTransactionClosed)
}
