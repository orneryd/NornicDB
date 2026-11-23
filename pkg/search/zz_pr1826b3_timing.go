package search

// DIAGNOSTIC ONLY, not for merge. Author: pr1826-base3 test lane (Claude Opus
// 5.5), 23 Nov 2026. Times what a committing writer does in
// applyReservedNodeIndex while a vector index build catches up: the read lock,
// its own index work, and the catch-up inserts it pays (waits for the graph
// lock, time holding it, waits for publication). PR1826B3_SKIP_PAY=1 is the
// bypass: writers pay nothing and the construction job drains every change.

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type pr1826b3Timing struct {
	lockWait, held, stepWait time.Duration
	locks, steps, pubWaits   int
}

func (tm *pr1826b3Timing) lock(mu *sync.RWMutex) time.Time {
	start := time.Now()
	mu.Lock()
	now := time.Now()
	if tm != nil {
		tm.lockWait += now.Sub(start)
		tm.locks++
	}
	return now
}

func (tm *pr1826b3Timing) release(held time.Time) {
	if tm != nil {
		tm.held += time.Since(held)
	}
}

func (tm *pr1826b3Timing) publishingSeen() {
	if tm != nil {
		tm.pubWaits++
	}
}

func (tm *pr1826b3Timing) waited(start time.Time) {
	if tm != nil {
		tm.stepWait += time.Since(start)
	}
}

func (tm *pr1826b3Timing) stepped() {
	if tm != nil {
		tm.steps++
	}
}

var (
	pr1826b3SkipPay = os.Getenv("PR1826B3_SKIP_PAY") == "1"
	pr1826b3LogMin  = func() time.Duration {
		if v, err := strconv.Atoi(os.Getenv("PR1826B3_LOG_MS")); err == nil && v >= 0 {
			return time.Duration(v) * time.Millisecond
		}
		return 500 * time.Millisecond
	}()
	pr1826b3Refreshes, pr1826b3Paying, pr1826b3Over1s, pr1826b3Over3s atomic.Int64
	pr1826b3PayNanos, pr1826b3HeldNanos, pr1826b3WaitNanos, pr1826b3Inserts atomic.Int64
)

func pr1826b3LogRefresh(node *storage.Node, owed uint64, start, readLocked, indexed time.Time, tm *pr1826b3Timing) {
	end := time.Now()
	total := end.Sub(start)
	pay := end.Sub(indexed)
	pr1826b3Refreshes.Add(1)
	if owed > 0 {
		pr1826b3Paying.Add(1)
		pr1826b3PayNanos.Add(int64(pay))
		pr1826b3HeldNanos.Add(int64(tm.held))
		pr1826b3WaitNanos.Add(int64(tm.lockWait + tm.stepWait))
		pr1826b3Inserts.Add(int64(tm.locks))
	}
	if total >= time.Second {
		pr1826b3Over1s.Add(1)
	}
	if total >= 3*time.Second {
		pr1826b3Over3s.Add(1)
	}
	if total < pr1826b3LogMin {
		return
	}
	id := ""
	if node != nil {
		id = string(node.ID)
	}
	fmt.Fprintf(os.Stderr, "PR1826B3REFRESH id=%s skipPay=%t owed=%d total=%v readLock=%v index=%v pay=%v steps=%d graphLocks=%d graphLockWait=%v graphLockHeld=%v stepWait=%v publishingWaits=%d\n",
		id, pr1826b3SkipPay, owed, total, readLocked.Sub(start), indexed.Sub(readLocked), pay, tm.steps, tm.locks, tm.lockWait, tm.held, tm.stepWait, tm.pubWaits)
}

func pr1826b3LogPublish(finished bool, pendingBefore int, quiesceStart, lockStart, locked time.Time) {
	inserts := pr1826b3Inserts.Load()
	perInsert := time.Duration(0)
	if inserts > 0 {
		perInsert = time.Duration(pr1826b3HeldNanos.Load() / inserts)
	}
	fmt.Fprintf(os.Stderr, "PR1826B3PUBLISH finished=%t pendingUnderLocks=%d quiesce=%v takeLocks=%v skipPay=%t refreshes=%d paying=%d over1s=%d over3s=%d payTotal=%v graphLockHeldTotal=%v waitTotal=%v payerInserts=%d meanHeldPerInsert=%v\n",
		finished, pendingBefore, lockStart.Sub(quiesceStart), locked.Sub(lockStart), pr1826b3SkipPay,
		pr1826b3Refreshes.Load(), pr1826b3Paying.Load(), pr1826b3Over1s.Load(), pr1826b3Over3s.Load(),
		time.Duration(pr1826b3PayNanos.Load()), time.Duration(pr1826b3HeldNanos.Load()), time.Duration(pr1826b3WaitNanos.Load()), inserts, perInsert)
}

// updateTimed is Update with the diagnostic timing (nil: none).
func (h *HNSWIndex) updateTimed(id string, vec []float32, tm *pr1826b3Timing) error {
	h.mu.RLock()
	internalID, exists := h.idToInternal[id]
	exists = exists && validHNSWIndex(internalID, len(h.deleted)) && !h.deleted[internalID]
	h.mu.RUnlock()
	if exists {
		h.removeTimed(id, tm)
	}
	if len(vec) != h.dimensions {
		return ErrDimensionMismatch
	}
	return h.addLockedTimed(id, vec, nil, tm)
}
