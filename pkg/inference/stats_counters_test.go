package inference

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cooldown checks run concurrently under the table's read lock. Every check
// is counted exactly once, as allowed or blocked, with no lost update; under
// -race this also proves the counters are not plain fields written under a
// read lock.
func TestCooldownTableCountsConcurrentChecksExactly(t *testing.T) {
	cleanup := config.WithCooldownEnabled()
	defer cleanup()

	ct := NewCooldownTable()
	ct.RecordMaterialization("a", "b", "relates_to") // a cooling-down pair: blocked
	const goroutines, checks = 8, 250
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
				_ = ct.Stats() // concurrent reader
			}
		}(g)
	}
	wg.Wait()

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

// Cluster searches run concurrently under the integration's read lock; every
// search is counted exactly once.
func TestClusterIntegrationCountsConcurrentSearchesExactly(t *testing.T) {
	ci := NewClusterIntegration(testGPUManager(), testClusterConfig(), testKMeansConfig(5), testEmbConfig())
	for i := 0; i < 20; i++ {
		require.NoError(t, ci.AddEmbedding(fmt.Sprintf("node-%d", i), generateRandomEmbedding(64)))
	}
	const goroutines, searches = 8, 25
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < searches; i++ {
				_, err := ci.Search(context.Background(), generateRandomEmbedding(64), 5)
				assert.NoError(t, err)
				_ = ci.Stats() // concurrent reader
			}
		}()
	}
	wg.Wait()

	stats := ci.Stats()
	require.Equal(t, int64(goroutines*searches), stats.SearchesTotal)
	require.LessOrEqual(t, stats.SearchesClustered, stats.SearchesTotal)
}
