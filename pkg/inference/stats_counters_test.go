package inference

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// liveSnapshots calls snapshot repeatedly until work returns, and reports
// every snapshot that breaks its invariants (snapshot returns "" when it
// holds). Snapshots taken while workers update the statistics are what a
// separately-loaded set of counters gets wrong.
func liveSnapshots(work func(), snapshot func() string) (taken int, broken []string) {
	var done atomic.Bool
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for !done.Load() {
			taken++
			if problem := snapshot(); problem != "" && len(broken) < 5 {
				broken = append(broken, problem)
			}
		}
	}()
	work()
	done.Store(true)
	<-finished
	return taken, broken
}

// Cooldown checks run concurrently under the table's read lock. Every check
// is counted exactly once, as allowed or blocked, with no lost update, and
// every Stats snapshot taken while checks run is one consistent set:
// TotalAllowed+TotalBlocked == TotalChecks and BlockRate in [0, 1].
func TestCooldownTableCountsConcurrentChecksExactly(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.RecordMaterialization("a", "b", "relates_to") // a cooling-down pair: blocked
	const goroutines, checks = 8, 2000
	taken, broken := liveSnapshots(func() {
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < checks; i++ {
					if i%2 == 0 {
						ct.CanMaterialize("a", "b", "relates_to")
					} else {
						ct.CanMaterialize("a", fmt.Sprintf("free-%d-%d", g, i), "relates_to")
					}
				}
			}(g)
		}
		wg.Wait()
	}, func() string {
		stats := ct.Stats()
		if stats.TotalAllowed+stats.TotalBlocked != stats.TotalChecks || stats.BlockRate < 0 || stats.BlockRate > 1 {
			return fmt.Sprintf("%+v", stats)
		}
		return ""
	})
	require.Positive(t, taken)
	require.Empty(t, broken, "inconsistent snapshots while checks ran (%d taken)", taken)

	stats := ct.Stats()
	require.Equal(t, int64(goroutines*checks), stats.TotalChecks)
	require.Equal(t, int64(goroutines*checks/2), stats.TotalBlocked)
	require.Equal(t, int64(goroutines*checks/2), stats.TotalAllowed)

	ct.Clear()
	stats = ct.Stats()
	require.Zero(t, stats.TotalChecks)
	require.Zero(t, stats.TotalBlocked)
	require.Zero(t, stats.TotalAllowed)
}

// Cluster searches run concurrently under the integration's read lock. Both
// search counters are counted exactly: the fallback searches before
// clustering add to the total only, the clustered searches after it add to
// both. Every Stats snapshot taken while clustered searches run is one
// consistent set: clustered <= total and total-clustered == the fallback
// searches.
func TestClusterIntegrationCountsConcurrentSearchesExactly(t *testing.T) {
	ci := NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(5), testEmbConfig())
	for i := 0; i < 20; i++ {
		require.NoError(t, ci.AddEmbedding(fmt.Sprintf("node-%d", i), generateRandomEmbedding(64)))
	}
	search := func(goroutines, searches int) {
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < searches; i++ {
					_, err := ci.Search(context.Background(), generateRandomEmbedding(64), 5)
					assert.NoError(t, err)
				}
			}()
		}
		wg.Wait()
	}

	// Fallback: not clustered yet.
	require.False(t, ci.IsClustered())
	const fallback = 4 * 10
	search(4, 10)
	stats := ci.Stats()
	require.Equal(t, int64(fallback), stats.SearchesTotal)
	require.Zero(t, stats.SearchesClustered)

	// Clustered.
	require.NoError(t, ci.OnIndexComplete())
	require.True(t, ci.IsClustered())
	const goroutines, searches = 8, 100
	taken, broken := liveSnapshots(func() { search(goroutines, searches) }, func() string {
		stats := ci.Stats()
		if stats.SearchesClustered > stats.SearchesTotal || stats.SearchesTotal-stats.SearchesClustered != fallback {
			return fmt.Sprintf("total=%d clustered=%d", stats.SearchesTotal, stats.SearchesClustered)
		}
		return ""
	})
	require.Positive(t, taken)
	require.Empty(t, broken, "inconsistent snapshots while searches ran (%d taken)", taken)

	stats = ci.Stats()
	require.Equal(t, int64(fallback+goroutines*searches), stats.SearchesTotal)
	require.Equal(t, int64(goroutines*searches), stats.SearchesClustered)
}

// Graph rebuilds write the build statistics under the graph's write lock
// while Stats reads them; Stats must read them as one set under that lock
// (a data race under -race otherwise). Every snapshot taken while rebuilds
// run is consistent: no build time without a completed build, and the last
// build no longer than the total.
func TestTopologyStatsReadsBuildStatsDuringRebuilds(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "test")
	setupTestGraph(t, engine)
	cfg := DefaultTopologyConfig()
	cfg.Enabled = true
	cfg.GraphRefreshInterval = 1 // every prediction after the first rebuilds the graph
	topo := NewTopologyIntegration(engine, cfg)

	const goroutines, predictions = 4, 25
	taken, broken := liveSnapshots(func() {
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < predictions; i++ {
					_, err := topo.SuggestTopological(context.Background(), "alice")
					assert.NoError(t, err)
				}
			}()
		}
		wg.Wait()
	}, func() string {
		stats := topo.Stats()
		if (stats.BuildsCompleted == 0 && (stats.TotalBuildTime != 0 || stats.LastBuildTime != 0)) || stats.LastBuildTime > stats.TotalBuildTime {
			return fmt.Sprintf("builds=%d last=%v total=%v", stats.BuildsCompleted, stats.LastBuildTime, stats.TotalBuildTime)
		}
		return ""
	})
	require.Positive(t, taken)
	require.Empty(t, broken, "inconsistent build stats while rebuilds ran (%d taken)", taken)

	stats := topo.Stats()
	require.Positive(t, stats.BuildsCompleted)
	require.LessOrEqual(t, stats.LastBuildTime, stats.TotalBuildTime)
}
