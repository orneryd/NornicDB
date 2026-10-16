package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tlfCurrentLockKeys(sm *SchemaManager) map[uniqueConstraintLockKey]bool {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	keys := make(map[uniqueConstraintLockKey]bool, len(sm.uniqueConstraintCommitLocks))
	for key := range sm.uniqueConstraintCommitLocks {
		keys[key] = true
	}
	return keys
}

func tlfCurrentLockState(sm *SchemaManager, waiter string) (waiting bool, held, waits int) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	if waiter == "" {
		waiting = len(sm.uniqueConstraintLockWaits) != 0
	} else {
		waiting = sm.uniqueConstraintLockWaits[waiter] != nil
	}
	return waiting, len(sm.uniqueConstraintCommitLocks), len(sm.uniqueConstraintLockWaits)
}

// TestTemporalListFamilyTemporalCompositeListCommitLocksDirectFirst covers the
// two acquisition orders where a direct write takes the lock first. A direct
// node create is held after its Badger commit while it still owns the complete
// composite/list-bearing temporal lock; a transaction or a second direct
// writer must wait, then recheck and reject the overlapping equivalent key.
func TestTemporalListFamilyTemporalCompositeListCommitLocksDirectFirst(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			for _, contender := range []string{"transaction", "direct"} {
				t.Run(fmt.Sprintf("position=%d/order=%d/%s", position, order, contender), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c := tlfMixedConstraint(position, false)
					tlfRegister(t, inner, engine, c)
					pair := tlfMixedPair(order, "mixed")
					spec, firstKey, secondKey := tlfKeys(t, c, pair, true, true)
					require.Equal(t, spec.keyHash(firstKey), spec.keyHash(secondKey))
					expectedLocks := tlfLockSnapshot(t, c, pair[0])

					var contenderTx *BadgerTransaction
					waiterID := ""
					waitCtx, cancelWait := context.WithCancel(context.Background())
					if contender == "transaction" {
						contenderTx = tlfStageMixedTx(t, inner, c, "second", pair[1])
						waiterID = contenderTx.ID
					}

					entered := make(chan struct{})
					releaseTail := make(chan struct{})
					var held atomic.Bool
					var releaseOnce sync.Once
					release := func() { releaseOnce.Do(func() { close(releaseTail) }) }
					restore := setCommitTailHook(func() {
						// NamespacedEngine may publish new label/property tokens before
						// BadgerEngine.CreateNode takes the constraint lock. Ignore that
						// earlier helper-write tail and hold only the entity write whose
						// live registry contains the expected temporal key.
						if !reflect.DeepEqual(expectedLocks, tlfCurrentLockKeys(engine.GetSchema())) {
							return
						}
						if held.CompareAndSwap(false, true) {
							close(entered)
							<-releaseTail
						}
					})
					var workers sync.WaitGroup
					startWorker := func(run func()) {
						workers.Add(1)
						go func() {
							defer workers.Done()
							run()
						}()
					}
					var cleanupOnce sync.Once
					cleanup := func() {
						cleanupOnce.Do(func() {
							cancelWait()
							release()
							workers.Wait()
							if contenderTx != nil {
								_ = contenderTx.Rollback()
							}
							restore()
						})
					}
					t.Cleanup(cleanup)

					firstDone := make(chan error, 1)
					startWorker(func() {
						_, err := engine.CreateNode(&Node{ID: "first", Labels: []string{c.Label}, Properties: pair[0]})
						firstDone <- err
					})
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("direct first writer did not reach its post-commit hold")
					}
					require.True(t, held.Load())
					require.Equal(t, expectedLocks, tlfCurrentLockKeys(engine.GetSchema()), "direct first writer owns the canonical temporal key")

					other := tlfMixedPair(0, "mixed")[0]
					other["scalar"], other["list"] = int64(9), []int64{9}
					unrelated := tlfStageMixedTx(t, inner, c, "unrelated", other)
					require.NoError(t, unrelated.Commit(), "an unrelated temporal key progresses while the direct key is held")

					contenderDone := make(chan error, 1)
					if contender == "transaction" {
						startWorker(func() { contenderDone <- contenderTx.CommitContext(waitCtx) })
					} else {
						startWorker(func() {
							_, err := engine.CreateNode(&Node{ID: "second", Labels: []string{c.Label}, Properties: pair[1]})
							contenderDone <- err
						})
					}
					require.Eventually(t, func() bool {
						waiting, heldLocks, waits := tlfCurrentLockState(engine.GetSchema(), waiterID)
						return waiting && heldLocks == 1 && waits == 1
					}, 5*time.Second, time.Millisecond, "equivalent writer must wait on the direct writer's temporal key")
					select {
					case err := <-contenderDone:
						t.Fatalf("contender completed before the direct key was released: %v", err)
					default:
					}

					release()
					select {
					case err := <-firstDone:
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Fatal("direct first writer did not finish after release")
					}
					var contenderErr error
					select {
					case contenderErr = <-contenderDone:
					case <-time.After(5 * time.Second):
						t.Fatal("contender did not finish after direct writer released the key")
					}
					require.Error(t, contenderErr, "the waiting writer rechecks and rejects the committed overlap")
					var violation *ConstraintViolationError
					require.True(t, errors.As(contenderErr, &violation), contenderErr)
					require.Equal(t, ConstraintTemporal, violation.Type)
					_, err := engine.GetNode("second")
					require.ErrorIs(t, err, ErrNotFound)

					adjacent := tlfMixedPair(order, "adjacent")[1]
					_, err = engine.CreateNode(&Node{ID: "adjacent", Labels: []string{c.Label}, Properties: adjacent})
					require.NoError(t, err, "an equal but non-overlapping interval remains valid")

					cleanup()
					waiting, heldLocks, waits := tlfCurrentLockState(engine.GetSchema(), "")
					require.False(t, waiting)
					require.Zero(t, heldLocks)
					require.Zero(t, waits)
					t.Logf("TLF_DIRECT_FIRST position=%d order=%d contender=%s held=true waited=true rechecked=true unrelated=true adjacent=true cleanup=true", position, order, contender)
				})
			}
		}
	}
}
