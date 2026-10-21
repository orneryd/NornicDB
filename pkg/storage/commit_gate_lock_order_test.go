package storage

import (
	"fmt"
	"go/ast"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reuse the publication boundary's type-resolved source inventory. Every gate
// acquisition must have a reviewed path class, and its statically resolved
// callees must acquire no count lock. The large writer keeps its gate across
// calls, so the runtime batch tests separately pin that lifetime and its
// callbacks/property-token writes.
func checkCommitGateLockOrder(t *testing.T, src *packageSource) {
	t.Helper()
	classes := map[string]string{
		"commitShared":         "ordinary data, count publication, nested metadata; TestCommitCountLockOrderPaths",
		"beginLargeCommit":     "large synchronous/replay batches and tokens; TestCommitCountLockOrderPaths and TestReview1406R3LargeReplayCountLockOrder",
		"commitBufferedLocked": "ACK validation/WAL/buffer, no count publication; TestWriteBehindBarrierAdmission",
		"DropPrefix":           "offline prefix drop; TestManagedBadgerDropPrefix",
		"DropPrefixContext":    "context-aware offline prefix drop; namespace lifetime tests",
		"Load":                 "offline restore; backup/restore tests",
	}
	countLocks := []string{"BadgerEngine.labelCountWriteMu", "BadgerEngine.edgeTypeCountWriteMu", "BadgerEngine.totalCountWriteMu"}
	owners := 0
	for fn, decl := range src.decls {
		takesGate := false
		ast.Inspect(decl.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if ok && (sel.Sel.Name == "Lock" || sel.Sel.Name == "RLock") && src.lockName(sel.X) == "managedBadgerDB.commitGate" {
				takesGate = true
			}
			return true
		})
		if !takesGate {
			continue
		}
		owners++
		classification, known := classes[fn.Name()]
		assert.True(t, known, "%s acquires the gate without lock-order/path coverage", fn.FullName())
		_, locks, dynamic, _ := src.reach([]ast.Node{decl.Body})
		for _, lock := range countLocks {
			assert.Empty(t, locks[lock], "%s can acquire %s under the gate", fn.FullName(), lock)
		}
		t.Logf("%s: %s; function-value calls checked by path tests: %v", fn.FullName(), classification, dynamic)
	}
	assert.Equal(t, 5, owners, "update the gate-acquisition inventory and its path tests")
}

// Ordinary and large synchronous commits retain their positional count lock
// through the metadata tail. Ordinary and large replay hold all count tiers.
// In both cases the gate is released before that tail can commit metadata.
func TestCommitCountLockOrderPaths(t *testing.T) {
	for _, replay := range []bool{false, true} {
		for _, size := range []int{1, 3000} {
			t.Run(fmt.Sprintf("replay=%t/nodes=%d", replay, size), func(t *testing.T) {
				b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Identity"}})
					require.NoError(t, err)
				}
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(replay))
				for i := 0; i < size; i++ {
					_, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"fresh_key": strings.Repeat("x", 700)}})
					require.NoError(t, err)
				}
				require.NoError(t, tx.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))
				checkCounts := func() error {
					if b.edgeTypeCountWriteMu.TryLock() {
						b.edgeTypeCountWriteMu.Unlock()
						return fmt.Errorf("publisher has no positional count lock")
					}
					if replay {
						if b.labelCountWriteMu.TryLock() {
							b.labelCountWriteMu.Unlock()
							return fmt.Errorf("replay has no label count lock")
						}
						if b.totalCountWriteMu.TryLock() {
							b.totalCountWriteMu.Unlock()
							return fmt.Errorf("replay has no total count lock")
						}
					}
					return nil
				}
				checkTail := func() {
					assert.NoError(t, checkCounts())
					if assert.True(t, b.db.commitGate.TryLock(), "metadata tail must not retain the exclusive gate") {
						b.db.commitGate.Unlock()
					}
				}
				sawBatch := false
				restoreBatch := setLargeCommitBatchHook(func(int) error {
					sawBatch = true
					if err := checkCounts(); err != nil {
						return err
					}
					if b.db.commitGate.TryRLock() {
						b.db.commitGate.RUnlock()
						return fmt.Errorf("large batch has no exclusive gate")
					}
					return nil
				})
				defer restoreBatch()
				if replay {
					// Claim the generation before ACK so the real background flusher
					// cannot race this direct replay-owner assertion.
					b.writeBehind.mu.RLock()
					buf := b.writeBehind.active
					require.True(t, buf.claimDrain())
					b.writeBehind.mu.RUnlock()
					defer buf.releaseDrain()
					require.NoError(t, tx.Commit())
					require.True(t, tx.AcknowledgedBuffered())
					require.NoError(t, b.applyBufferedCommitsWithCountPublication(buf.Commits(), func() {
						checkTail()
						buf.countsApplied.Store(true)
					}, buf.markApplied))
					buf.releaseDrain()
					require.NoError(t, b.FlushWriteBehind())
				} else {
					sawTail := false
					restoreTail := setCommitTailHook(func() { sawTail = true; checkTail() })
					defer restoreTail()
					require.NoError(t, tx.Commit())
					require.True(t, sawTail)
				}
				require.Equal(t, size > 1, sawBatch, "exercise ordinary and large commits, including fresh property tokens")
				count, err := b.EdgeCountByStartLabelInNamespace("test", "Identity", "LINK")
				require.NoError(t, err)
				require.Equal(t, int64(1), count)
			})
		}
	}
}
