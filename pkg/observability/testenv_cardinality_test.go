package observability

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		// must call subT.Fatalf. RESEARCH §4: GatherAndCount takes a Gatherer
		// (Registry implements); the Collector-typed counterpart would not
		// compile here.
		helperPassed := t.Run("helper-failure-capture", func(subT *testing.T) {
			te.AssertCardinalityCeiling(subT, "nornicdb_cypher_unbounded_total", 100, func(tenant string) {
				cv.WithLabelValues(tenant, fmt.Sprintf("op_%d", rand.Intn(2000))).Inc()
			})
		})
		require.False(t, helperPassed,
			"TEST-02 falsifiability: AssertCardinalityCeiling must mark t.Failed when GatherAndCount > ceiling — t.Run returns false on subtest failure")

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
