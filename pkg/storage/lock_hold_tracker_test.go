package storage

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"

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

func TestMain(m *testing.M) {
	flag.Parse()
	if bench := flag.Lookup("test.bench"); bench == nil || bench.Value.String() == "" {
		lockHoldHooks = lockHolds
	}
	code := m.Run()
	if violations := lockHolds.since(0); len(violations) > 0 {
		fmt.Fprintf(os.Stderr, "FAIL: %d deliveries to listeners ran while their goroutine held a write-order stripe or the commit gate:\n%s\n",
			len(violations), strings.Join(violations, "\n"))
		code = 1
	}
	os.Exit(code)
}

func currentGoroutineID() uint64 {
	var buf [64]byte
	fields := bytes.Fields(buf[:runtime.Stack(buf[:], false)])
	id, _ := strconv.ParseUint(string(fields[1]), 10, 64)
	return id
}

func (t *lockHoldTracker) held() {
	id := currentGoroutineID()
	t.mu.Lock()
	t.holds[id]++
	t.mu.Unlock()
}

func (t *lockHoldTracker) released() {
	id := currentGoroutineID()
	t.mu.Lock()
	if t.holds[id] > 1 {
		t.holds[id]--
	} else {
		delete(t.holds, id)
	}
	t.mu.Unlock()
}

func (t *lockHoldTracker) delivering() {
	id := currentGoroutineID()
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
	engine := listenerOrderEngine(t, true)
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
	engine.enqueueCommitOperation(Operation{Type: OpUpdateNode, Node: node})
}
