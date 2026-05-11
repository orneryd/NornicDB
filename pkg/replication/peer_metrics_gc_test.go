package replication

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gcFixture builds a fresh registry + ReplicationMetrics + PeerMetricsGC
// with test-tuned interval and staleness windows.
func gcFixture(t *testing.T, mode string, interval, staleness time.Duration) (*observability.ReplicationMetrics, *PeerMetricsGC, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	bag := observability.NewReplicationMetrics(reg, mode, false)
	gc := NewPeerMetricsGC(bag, interval, staleness)
	return bag, gc, reg
}

// TestPeerGC_Evicts asserts that a peer marked then left idle past the
// staleness threshold gets DeleteLabelValues'd from every per-peer family.
func TestPeerGC_Evicts(t *testing.T) {
	bag, gc, reg := gcFixture(t, "raft", 10*time.Millisecond, 50*time.Millisecond)

	// Mark + observe peer "p1" so the series exists.
	gc.Tracker().Mark("p1")
	bag.LagBytes.WithLabelValues("p1").Set(1024)
	bag.LagEntries.WithLabelValues("p1").Set(10)
	bag.RTTSeconds.WithLabelValues("p1").Observe(0.001)
	bag.LastContactSeconds.WithLabelValues("p1").Set(0.5)

	// Series should be present in registry.
	require.Equal(t, 1, testutil.CollectAndCount(bag.LagBytes, "nornicdb_replication_lag_bytes"))

	// Wait past staleness then sweep.
	time.Sleep(100 * time.Millisecond)
	gc.sweep()

	// Series should be gone.
	got, err := testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
	require.NoError(t, err)
	assert.Equal(t, 0, got, "D-05b: stale peer must be evicted from lag_bytes")

	// All four per-peer families should be cleared.
	for _, name := range []string{
		"nornicdb_replication_lag_entries",
		"nornicdb_replication_rtt_seconds",
		"nornicdb_replication_last_contact_seconds",
	} {
		got, err := testutil.GatherAndCount(reg, name)
		require.NoError(t, err)
		assert.Equal(t, 0, got, "D-05b: %q must be cleared for stale peer", name)
	}
	assert.Equal(t, 0, gc.Tracker().Len(), "tracker must forget evicted peer")
}

// TestPeerGC_DoesNotEvictRecent asserts that a peer continuously marked
// (live) is NOT evicted by sweep.
func TestPeerGC_DoesNotEvictRecent(t *testing.T) {
	bag, gc, reg := gcFixture(t, "raft", 10*time.Millisecond, 50*time.Millisecond)

	gc.Tracker().Mark("p2")
	bag.LagBytes.WithLabelValues("p2").Set(1)

	// Mark the peer continuously while sweeps run.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				gc.Tracker().Mark("p2")
			}
		}
	}()

	// Run several sweeps; the live peer must survive.
	for i := 0; i < 5; i++ {
		time.Sleep(20 * time.Millisecond)
		gc.sweep()
	}
	got, err := testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
	require.NoError(t, err)
	assert.Equal(t, 1, got, "D-05b: live peer must NOT be evicted")
}

// TestPeerGC_RaceSafe asserts -race -count=10 cleanliness across concurrent
// Mark + sweep — Pitfall 3 / T-04-05 mitigation.
func TestPeerGC_RaceSafe(t *testing.T) {
	bag, gc, _ := gcFixture(t, "raft", 5*time.Millisecond, 1*time.Millisecond)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Many concurrent markers.
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					peer := "peer-" + strconv.Itoa(w) + "-" + strconv.Itoa(i%4)
					gc.Tracker().Mark(peer)
					bag.LagBytes.WithLabelValues(peer).Set(float64(i))
					i++
				}
			}
		}()
	}
	// Sweeper alongside.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				gc.sweep()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestPeerCardinality_ModeAware asserts cardinality stays bounded by the
// D-05a mode ceiling under churn — the scenario where a misbehaving
// component generates 1k synthetic peer IDs but the GC keeps cardinality
// at the ceiling.
func TestPeerCardinality_ModeAware(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		ceiling int
	}{
		{"ha_standby", 8},
		{"raft", 16},
		{"multi_region", 64},
	} {
		tc := tc
		t.Run(tc.mode, func(t *testing.T) {
			bag, gc, reg := gcFixture(t, tc.mode, 1*time.Millisecond, 50*time.Millisecond)
			require.Equal(t, tc.ceiling, bag.Ceiling())

			// Drive churn: 1k synthetic peer IDs cycle through the
			// observation site. The replicator contract is Mark-on-observe,
			// so every observation tracks. The first 1000-ceiling peers
			// disconnect and never re-Mark; after the staleness window
			// they are evicted. The last `ceiling` peers are "live"
			// (re-Marked just before the sweep) and survive.
			for i := 0; i < 1000; i++ {
				peer := "synthetic-peer-" + strconv.Itoa(i)
				gc.Tracker().Mark(peer) // contract: Mark-on-observe
				bag.LagBytes.WithLabelValues(peer).Set(1)
			}

			// Wait past staleness so all 1000 are now stale.
			time.Sleep(60 * time.Millisecond)

			// "Live" peers re-Mark right before sweep — these survive.
			for i := 1000 - tc.ceiling; i < 1000; i++ {
				peer := "synthetic-peer-" + strconv.Itoa(i)
				gc.Tracker().Mark(peer)
			}

			gc.sweep()

			got, err := testutil.GatherAndCount(reg, "nornicdb_replication_lag_bytes")
			require.NoError(t, err)
			assert.LessOrEqual(t, got, tc.ceiling,
				"D-05a/D-05b: mode %q must enforce cardinality ceiling %d (got %d)",
				tc.mode, tc.ceiling, got)
		})
	}
}

// TestPeerGC_ShutdownStops asserts ctx cancel exits Start and Shutdown
// is idempotent.
func TestPeerGC_ShutdownStops(t *testing.T) {
	_, gc, _ := gcFixture(t, "raft", 5*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() { doneCh <- gc.Start(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-doneCh:
		assert.NoError(t, err, "Start must return nil on ctx cancel")
	case <-time.After(time.Second):
		t.Fatal("Start did not exit after ctx cancel")
	}

	// Shutdown is idempotent.
	require.NoError(t, gc.Shutdown(context.Background()))
	require.NoError(t, gc.Shutdown(context.Background()))
}

// TestPeerGC_StartViaShutdown asserts that calling Shutdown while Start
// is running causes Start to exit cleanly.
func TestPeerGC_StartViaShutdown(t *testing.T) {
	_, gc, _ := gcFixture(t, "raft", 5*time.Millisecond, 50*time.Millisecond)

	doneCh := make(chan error, 1)
	go func() { doneCh <- gc.Start(context.Background()) }()

	time.Sleep(15 * time.Millisecond)
	require.NoError(t, gc.Shutdown(context.Background()))

	select {
	case err := <-doneCh:
		assert.NoError(t, err, "Start must return nil after Shutdown")
	case <-time.After(time.Second):
		t.Fatal("Start did not exit after Shutdown")
	}
}

// TestPeerGC_NilMetrics asserts that nil metrics is a tolerated partial-init
// state — Start blocks on ctx without panicking.
func TestPeerGC_NilMetrics(t *testing.T) {
	gc := NewPeerMetricsGC(nil, 5*time.Millisecond, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() { doneCh <- gc.Start(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-doneCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("nil-metrics Start did not exit after ctx cancel")
	}
}

// TestPeerTracker_RaceSafe asserts the tracker is race-clean under many
// concurrent Mark / StaleSince / Forget calls.
func TestPeerTracker_RaceSafe(t *testing.T) {
	tr := NewPeerTracker()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					tr.Mark("peer-" + strconv.Itoa(w%4))
					if i%10 == 0 {
						_ = tr.StaleSince(time.Now())
					}
					if i%20 == 0 {
						tr.Forget("peer-" + strconv.Itoa(w%4))
					}
					i++
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	_ = tr.Len()
}
