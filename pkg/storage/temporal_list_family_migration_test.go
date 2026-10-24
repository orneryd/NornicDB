package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// tlfFormat3Hash reproduces the last private derived format: canonical list
// tokens were present, but scalar signed zero inside a composite/list-bearing
// key was not normalized at the whole-key boundary.
func tlfFormat3Hash(spec temporalKeySpec, keyValue interface{}) string {
	if values, ok := keyValue.([]interface{}); ok && spec.composite() {
		normalized := make([]interface{}, len(values))
		for i, value := range values {
			normalized[i] = temporalKeyHashValue(value)
		}
		return stableTemporalKeyDigest(normalized...)
	}
	return stableTemporalKeyDigest(temporalKeyHashValue(keyValue))
}

func tlfWriteFormat3Indexes(t *testing.T, engine *BadgerEngine, c Constraint, properties [2]map[string]interface{}) [2]temporalIndexDescriptor {
	t.Helper()
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixTemporalIndex))
	require.NoError(t, engine.clearBadgerPrefix(context.Background(), prefixTemporalHead))
	spec, ok := splitTemporalKeySpec(c.Properties)
	require.True(t, ok)
	var old [2]temporalIndexDescriptor
	require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
		for i, id := range []NodeID{"test:first", "test:second"} {
			keyValue, missing := spec.keyValue(properties[i])
			require.Empty(t, missing)
			old[i] = makeTemporalDescriptor("test", c, keyValue)
			old[i].keyHash = tlfFormat3Hash(spec, keyValue)
			if err := txn.Set(temporalHistoryKey(old[i], properties[i]["from"].(time.Time), id), nil); err != nil {
				return err
			}
			if err := txn.Set(temporalCurrentKey(old[i]), []byte(id)); err != nil {
				return err
			}
		}
		return txn.Set(temporalIndexFormatKey, []byte{3})
	}))
	return old
}

func TestTemporalListFamilyTemporalFormat4Migration(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			t.Run(fmt.Sprintf("position=%d/order=%d", position, order), func(t *testing.T) {
				inner, dir := createTestBadgerEngineOnDisk(t)
				engine := NewNamespacedEngine(inner, "test")
				c := tlfMixedConstraint(position, false)
				pair := tlfMixedPair(order, "mixed")
				base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
				pair[0]["from"], pair[0]["to"] = base, base.Add(10*time.Hour)
				pair[1]["from"], pair[1]["to"] = base.Add(time.Hour), base.Add(2*time.Hour)
				for i, id := range []string{"first", "second"} {
					tlfSeed(t, engine, c, id, pair[i])
					pair[i] = tlfColdProperties(t, inner, c, id, pair[i])
				}
				spec, firstKey, secondKey := tlfKeys(t, c, pair, true, true)
				require.NotEqual(t, tlfFormat3Hash(spec, firstKey), tlfFormat3Hash(spec, secondKey), "format 3 split equal whole keys")
				require.Equal(t, spec.keyHash(firstKey), spec.keyHash(secondKey), "format 4 joins equal whole keys")
				require.NoError(t, inner.GetSchemaForNamespace("test").AddConstraint(c))
				old := tlfWriteFormat3Indexes(t, inner, c, pair)
				require.NoError(t, inner.MarkCleanShutdown(context.Background()))
				require.NoError(t, inner.Close())

				reopened, err := NewBadgerEngine(dir)
				require.NoError(t, err)
				defer reopened.Close()
				clean, err := reopened.ConsumeCleanShutdownMarker(context.Background())
				require.NoError(t, err)
				require.True(t, clean)
				current, err := reopened.temporalIndexFormatIsCurrent()
				require.NoError(t, err)
				require.True(t, current)
				require.True(t, reopened.temporalIndexReady.Load())
				currentDesc := makeTemporalDescriptor("test", c, firstKey)
				canonical, divergent := 0, 0
				require.NoError(t, reopened.withView(func(txn *badger.Txn) error {
					for i, id := range []NodeID{"test:first", "test:second"} {
						_, err := txn.Get(temporalHistoryKey(old[i], pair[i]["from"].(time.Time), id))
						if old[i].keyHash == currentDesc.keyHash {
							canonical++
							require.Equal(t, currentDesc, old[i], "matching old descriptor is the canonical format-4 descriptor")
							require.NoError(t, err, "canonical format-4 history rebuilt at its reused key")
							continue
						}
						divergent++
						require.ErrorIs(t, err, badger.ErrKeyNotFound, "divergent format-3 history removed")
					}
					return nil
				}))
				require.Equal(t, 1, divergent, "exactly one obsolete format-3 descriptor diverges from format 4")
				require.Equal(t, 1, canonical, "exactly one format-3 descriptor is reused by format 4")

				first, err := reopened.GetNode("test:first")
				require.NoError(t, err)
				second, err := reopened.GetNode("test:second")
				require.NoError(t, err)
				history, complete, err := reopened.temporalKeyHistoryNodesLatest(context.Background(), "test", c, first)
				require.NoError(t, err)
				require.True(t, complete)
				require.Len(t, history, 2, "both retained holders share the rebuilt history")
				asOf := base.Add(3 * time.Hour)
				currentFirst, err := reopened.IsCurrentTemporalNodeInNamespace("test", first, asOf)
				require.NoError(t, err)
				require.True(t, currentFirst, "descending history finds the earlier covering holder")
				currentSecond, err := reopened.IsCurrentTemporalNodeInNamespace("test", second, asOf)
				require.NoError(t, err)
				require.False(t, currentSecond)

				probe := map[string]interface{}{"scalar": pair[1]["scalar"], "list": pair[1]["list"], "from": base.Add(3 * time.Hour), "to": base.Add(4 * time.Hour)}
				reopenedNS := NewNamespacedEngine(reopened, "test")
				_, err = reopenedNS.CreateNode(&Node{ID: "direct", Labels: []string{c.Label}, Properties: probe})
				require.Error(t, err, "full retained history rejects an overlap hidden behind a later closed holder")
				tx, err := reopened.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				require.NoError(t, tx.SetDeferredConstraintValidation(true))
				_, err = tx.CreateNode(&Node{ID: "test:transaction", Labels: []string{c.Label}, Properties: probe})
				require.NoError(t, err)
				require.Error(t, tx.Commit())

				tlfForceScan(t, reopened, reopenedNS, c)
				_, err = reopenedNS.CreateNode(&Node{ID: "scan", Labels: []string{c.Label}, Properties: probe})
				require.Error(t, err, "untrusted fallback scan preserves the same result")
				t.Log("TLF_FORMAT4 format3-rebuilt history=2 direct=reject tx=reject scan=reject current=first")
			})
		}
	}
}

func TestTemporalListFamilyTemporalFormat4Trust(t *testing.T) {
	for _, mode := range []string{"cancel", "partial-cancel"} {
		t.Run(mode, func(t *testing.T) {
			inner, engine := tlfEngine(t, false)
			c := tlfMixedConstraint(0, false)
			tlfRegister(t, inner, engine, c)
			pair := tlfMixedPair(0, "mixed")
			tlfSeed(t, engine, c, "first", pair[0])
			require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
			ctx := context.Context(context.Background())
			if mode == "cancel" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			} else {
				ctx = &cancelAfterInitialCheckContext{Context: context.Background()}
			}
			err := inner.RebuildTemporalIndexes(ctx)
			require.Error(t, err)
			require.True(t, errors.Is(err, context.Canceled))
			require.False(t, inner.temporalIndexReady.Load())
			current, err := inner.temporalIndexFormatIsCurrent()
			require.NoError(t, err)
			require.False(t, current, "failed rebuild leaves format marker absent")
			if partial, ok := ctx.(*cancelAfterInitialCheckContext); ok {
				require.GreaterOrEqual(t, partial.checks, 2)
			}
			require.NoError(t, inner.RebuildTemporalIndexes(context.Background()))
			current, err = inner.temporalIndexFormatIsCurrent()
			require.NoError(t, err)
			require.True(t, current)
			require.True(t, inner.temporalIndexReady.Load())
			_, err = engine.CreateNode(&Node{ID: "overlap", Labels: []string{c.Label}, Properties: pair[1]})
			require.Error(t, err)
			t.Logf("TLF_FORMAT4_TRUST mode=%s failed-untrusted retry-trusted", mode)
		})
	}
}

func tlfStageMixedTx(t *testing.T, inner *BadgerEngine, c Constraint, id string, properties map[string]interface{}) *BadgerTransaction {
	t.Helper()
	tx, err := inner.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	_, err = tx.CreateNode(&Node{ID: NodeID("test:" + id), Labels: []string{c.Label}, Properties: properties})
	require.NoError(t, err)
	return tx
}

func TestTemporalListFamilyTemporalCompositeListCommitLocks(t *testing.T) {
	for position := 0; position < 2; position++ {
		for order := 0; order < 2; order++ {
			t.Run(fmt.Sprintf("snapshot/position=%d/order=%d", position, order), func(t *testing.T) {
				c := tlfMixedConstraint(position, false)
				pair := tlfMixedPair(order, "mixed")
				first := tlfLockSnapshot(t, c, pair[0])
				second := tlfLockSnapshot(t, c, pair[1])
				require.NotEmpty(t, first)
				require.Equal(t, first, second, "equal complete composite/list-bearing keys share one commit lock")
				spec, a, b := tlfKeys(t, c, pair, true, true)
				require.Equal(t, spec.keyHash(a), spec.keyHash(b))
				for key := range first {
					require.Equal(t, spec.keyHash(a), key.value)
				}
			})

			for _, contender := range []string{"transaction", "direct"} {
				t.Run(fmt.Sprintf("concurrent/position=%d/order=%d/%s", position, order, contender), func(t *testing.T) {
					inner, engine := tlfEngine(t, false)
					c := tlfMixedConstraint(position, false)
					tlfRegister(t, inner, engine, c)
					pair := tlfMixedPair(order, "mixed")
					firstTx := tlfStageMixedTx(t, inner, c, "first", pair[0])
					entered, releaseTail := make(chan struct{}), make(chan struct{})
					var blocked atomic.Bool
					var releaseOnce sync.Once
					release := func() { releaseOnce.Do(func() { close(releaseTail) }) }
					restore := setCommitTailHook(func() {
						if blocked.CompareAndSwap(false, true) {
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
							release()
							workers.Wait()
							restore()
						})
					}
					t.Cleanup(cleanup)
					firstDone := make(chan error, 1)
					startWorker(func() { firstDone <- firstTx.Commit() })
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("first commit did not reach its pre-tail hold")
					}

					other := tlfMixedPair(0, "mixed")[0]
					other["scalar"], other["list"] = int64(9), []int64{9}
					unrelated := tlfStageMixedTx(t, inner, c, "unrelated", other)
					require.NoError(t, unrelated.Commit(), "an unrelated temporal key progresses while the first key is held")

					contenderDone := make(chan error, 1)
					waiterID := ""
					if contender == "transaction" {
						tx := tlfStageMixedTx(t, inner, c, "second", pair[1])
						waiterID = tx.ID
						startWorker(func() { contenderDone <- tx.Commit() })
					} else {
						startWorker(func() {
							_, err := engine.CreateNode(&Node{ID: "second", Labels: []string{c.Label}, Properties: pair[1]})
							contenderDone <- err
						})
					}
					require.Eventually(t, func() bool {
						inner.GetSchemaForNamespace("test").uniqueConstraintCommitLocksMu.Lock()
						defer inner.GetSchemaForNamespace("test").uniqueConstraintCommitLocksMu.Unlock()
						waits := inner.GetSchemaForNamespace("test").uniqueConstraintLockWaits
						if waiterID != "" {
							return waits[waiterID] != nil
						}
						return len(waits) != 0
					}, 5*time.Second, time.Millisecond, "equivalent writer must wait on the same temporal key")
					select {
					case err := <-contenderDone:
						t.Fatalf("contender completed before the first key was released: %v", err)
					default:
					}
					release()
					require.NoError(t, <-firstDone)
					err := <-contenderDone
					require.Error(t, err, "the waiting writer rechecks and rejects the committed overlap")
					var violation *ConstraintViolationError
					require.True(t, errors.As(err, &violation), err)
					require.Equal(t, ConstraintTemporal, violation.Type)
					cleanup()
					require.Eventually(t, func() bool {
						inner.GetSchemaForNamespace("test").uniqueConstraintCommitLocksMu.Lock()
						defer inner.GetSchemaForNamespace("test").uniqueConstraintCommitLocksMu.Unlock()
						return len(inner.GetSchemaForNamespace("test").uniqueConstraintCommitLocks) == 0 && len(inner.GetSchemaForNamespace("test").uniqueConstraintLockWaits) == 0
					}, 5*time.Second, time.Millisecond)
					t.Logf("TLF_LOCK position=%d order=%d contender=%s waited=true rechecked=true unrelated=true", position, order, contender)
				})
			}
		}
	}
}

func TestTemporalListFamilyTemporalFormat4Anchors(t *testing.T) {
	negative := math.Copysign(0, -1)
	composite, ok := splitTemporalKeySpec([]string{"scalar", "plain", "from", "to"})
	require.True(t, ok)
	require.Equal(t, stableTemporalKeyDigest(negative, "plain"), composite.keyHash([]interface{}{negative, "plain"}), "pure-scalar composite digest stays byte-identical")
	single, ok := splitTemporalKeySpec([]string{"list", "from", "to"})
	require.True(t, ok)
	require.Equal(t, single.keyHash([]int64{1}), single.keyHash([]float64{1}), "single-list history uses the canonical list token")
	mixed, ok := splitTemporalKeySpec([]string{"scalar", "list", "from", "to"})
	require.True(t, ok)
	require.Equal(t, mixed.keyHash([]interface{}{int64(0), []int64{1}}), mixed.keyHash([]interface{}{negative, []float64{1}}))
	require.Equal(t, []byte{4}, temporalIndexFormatCurrent)
}
