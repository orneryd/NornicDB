package observability

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureT is an in-package CardinalityT that records Errorf calls and
// terminates the calling goroutine on FailNow (matching *testing.T.FailNow
// semantics so testify/require's "stop now" contract holds). Because the
// helper drives across an errgroup, captureT is the only object the
// helper's failure path touches — Go's t.Run-propagates-failure-to-parent
// rule never fires (testenv.go's CardinalityT signature note).
type captureT struct {
	failed int32
	mu     sync.Mutex
	msgs   []string
}

func (c *captureT) Helper() {}
func (c *captureT) Errorf(format string, args ...interface{}) {
	atomic.StoreInt32(&c.failed, 1)
	c.mu.Lock()
	c.msgs = append(c.msgs, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}
func (c *captureT) FailNow() {
	atomic.StoreInt32(&c.failed, 1)
	runtime.Goexit()
}
func (c *captureT) Failed() bool { return atomic.LoadInt32(&c.failed) == 1 }
func (c *captureT) Messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.msgs))
	copy(out, c.msgs)
	return out
}

// runHelperCapturing invokes fn in a fresh goroutine so that the helper's
// FailNow → runtime.Goexit terminates only the helper's goroutine, leaving
// the test goroutine free to inspect captureT after the helper returns.
// Mirrors testify/require's standard isolation idiom.
func runHelperCapturing(fn func(ct *captureT)) *captureT {
	ct := &captureT{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(ct)
	}()
	<-done
	return ct
}

// TestAssertCardinalityCeiling_Helper exercises the TEST-02 helper across
// three modes:
//
//  1. bounded vec passes — 1k tenants × 1 op_type = 1000 series, ceiling
//     1001 ⇒ green;
//  2. unbounded vec — drive injects ~2k op_type variations under each
//     tenant; ceiling=100 ⇒ helper must call subT.Fatalf (TEST-02
//     falsifiability gate);
//  3. race-clean across two parallel TestEnvs — proves no shared state.
//
// Per RESEARCH §4 the correct testutil call is GatherAndCount (takes a
// Gatherer; *prometheus.Registry implements). The Collector-typed
// counterpart would not compile against a Registry.
func TestAssertCardinalityCeiling_Helper(t *testing.T) {
	t.Run("bounded vec passes", func(t *testing.T) {
		te := NewTestEnv(t)
		cv := NewCounterVec(te.Registry,
			MetricOpts{Subsystem: "cypher", Name: "test_total", Help: "h"},
			[]string{"database", "op_type"})
		// drive: 1k tenants × 1 op_type = 1000 series; ceiling 1001 ⇒ green.
		te.AssertCardinalityCeiling(t, "nornicdb_cypher_test_total", 1001, func(tenant string) {
			cv.WithLabelValues(tenant, "read").Inc()
		})
	})

	t.Run("unbounded vec — helper marks t.Failed (TEST-02 falsifiability)", func(t *testing.T) {
		te := NewTestEnv(t)
		cv := NewCounterVec(te.Registry,
			MetricOpts{Subsystem: "cypher", Name: "unbounded_total", Help: "h"},
			[]string{"database", "op_type"})

		// Pre-drive ~2k op variations under each helper-supplied tenant so the
		// unbounded *Vec produces thousands of series; ceiling=100 ⇒ helper
		// must call FailNow on the supplied CardinalityT. RESEARCH §4:
		// GatherAndCount takes a Gatherer (Registry implements); the
		// Collector-typed counterpart would not compile here.
		//
		// Capture pattern (testenv.go CardinalityT signature note): the
		// helper runs against a captureT in a separate goroutine so its
		// FailNow → runtime.Goexit terminates only that goroutine. The
		// parent *testing.T never sees a propagated failure.
		ct := runHelperCapturing(func(ct *captureT) {
			te.AssertCardinalityCeiling(ct, "nornicdb_cypher_unbounded_total", 100, func(tenant string) {
				cv.WithLabelValues(tenant, fmt.Sprintf("op_%d", rand.Intn(2000))).Inc()
			})
		})
		require.True(t, ct.Failed(),
			"TEST-02 falsifiability: AssertCardinalityCeiling must mark t.Failed when GatherAndCount > ceiling")
		require.NotEmpty(t, ct.Messages(),
			"helper must emit a diagnostic via Errorf before FailNow (so operators know which family + count tripped)")

		// Sanity: confirm the underlying *Vec did exceed the ceiling
		// (precondition diagnostic).
		got, err := testutil.GatherAndCount(te.Registry, "nornicdb_cypher_unbounded_total")
		require.NoError(t, err)
		assert.Greaterf(t, got, 100,
			"precondition: unbounded drive must exceed ceiling 100 (got %d) so helper observes the breach", got)
	})

	t.Run("race-clean across two parallel TestEnvs", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				te := NewTestEnv(t)
				cv := NewCounterVec(te.Registry,
					MetricOpts{Subsystem: "cypher", Name: "race_total", Help: "h"},
					[]string{"database"})
				te.AssertCardinalityCeiling(t, "nornicdb_cypher_race_total", 1001, func(tenant string) {
					cv.WithLabelValues(tenant).Inc()
				})
			}()
		}
		wg.Wait()
	})
}
