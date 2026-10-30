package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// lockHoldTracker counts, per goroutine, the write-order stripes and commit
// gate holds (trackedRWMutex) and records every delivery to listeners on a
// goroutine that holds one. It is installed for every test of the package,
// not for benchmarks, which measure production code: a recorded delivery
// fails the run (TestMain).
type lockHoldTracker struct {
	mu         sync.Mutex
	holds      map[uint64]int
	violations []string
}

var lockHolds = &lockHoldTracker{holds: map[uint64]int{}}

func checkStorageLockHolds(code int) int {
	if violations := lockHolds.since(0); len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: %d deliveries to listeners ran while their goroutine held a write-order stripe or the commit gate:\n%s\n",
			len(violations), strings.Join(violations, "\n"))
		code = 1
	}
	return code
}

func (t *lockHoldTracker) held() {
	id := fixtureowner.CurrentGoroutineID()
	t.mu.Lock()
	t.holds[id]++
	t.mu.Unlock()
}

func (t *lockHoldTracker) released() {
	id := fixtureowner.CurrentGoroutineID()
	t.mu.Lock()
	if t.holds[id] > 1 {
		t.holds[id]--
	} else {
		delete(t.holds, id)
	}
	t.mu.Unlock()
}

func (t *lockHoldTracker) delivering() {
	id := fixtureowner.CurrentGoroutineID()
	t.mu.Lock()
	defer t.mu.Unlock()
	if n := t.holds[id]; n > 0 {
		t.violations = append(t.violations, fmt.Sprintf("goroutine %d holds %d write-order stripe(s) or commit gate holds:\n%s", id, n, debug.Stack()))
	}
}

// mark is the number of deliveries recorded so far.
func (t *lockHoldTracker) mark() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.violations)
}

func (t *lockHoldTracker) since(mark int) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.violations[mark:]...)
}

// forget drops the deliveries recorded since mark (a test that provoked them).
func (t *lockHoldTracker) forget(mark int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.violations = t.violations[:mark]
}

// The tracker records a delivery to listeners on a goroutine that holds a
// write-order stripe, and none once it released it.
func TestLockHoldTrackerRecordsDeliveryUnderAStripe(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	t.Cleanup(func() { _ = engine.Close() })
	delivered := 0
	engine.OnNodeUpdated(func(*Node) { delivered++ })
	mark := lockHolds.mark()
	var stripes orderStripeSet
	stripes.add("test:n")
	engine.writeBehind.order.lock(&stripes, true)
	deliverNodeUpdate(engine, listenerOrderNode("held"))
	engine.writeBehind.order.unlock(&stripes, true)
	recorded := lockHolds.since(mark)
	lockHolds.forget(mark)
	require.Len(t, recorded, 1)
	require.Contains(t, recorded[0], "holds 1 write-order stripe(s)")

	deliverNodeUpdate(engine, listenerOrderNode("released"))
	require.Empty(t, lockHolds.since(mark))
	require.Equal(t, 2, delivered)
}

// deliverNodeUpdate publishes an update of node and delivers it on the
// calling goroutine.
func deliverNodeUpdate(engine *BadgerEngine, node *Node) {
	operation := Operation{Type: OpUpdateNode, Node: node}
	engine.queueCommitOperations(nil, &operation, notificationImmediate)
}
