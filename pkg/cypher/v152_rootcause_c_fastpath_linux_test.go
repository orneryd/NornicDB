//go:build linux

package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func v152RCCCPUSeconds() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func v152RCCThrottle() string {
	raw, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return "n/a"
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "nr_throttled") || strings.HasPrefix(line, "throttled_usec") || strings.HasPrefix(line, "usage_usec") {
			out = append(out, strings.ReplaceAll(line, " ", "="))
		}
	}
	return strings.Join(out, ",")
}

// TestV152RCCFastPathRateVariance repeats, in one process and on one
// executor, the exact LDBC workload of fastpath_test.go:63-108 in 12 batches
// of 1,000 and logs each batch's wall rate next to its process CPU time per
// operation, the load average and the cgroup throttle counters. The test
// thresholds (5,000 and 10,000 ops/s) are compared with the wall rate only.
func TestV152RCCFastPathRateVariance(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for i := 1; i <= 10; i++ {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("person%d", i)), Labels: []string{"Person"},
			Properties: map[string]interface{}{"id": int64(i), "name": fmt.Sprintf("Person_%d", i)}})
		require.NoError(t, err)
	}
	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	query := "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r"
	below, above := 0, 0
	for batch := 0; batch < 12; batch++ {
		load, _ := os.ReadFile("/proc/loadavg")
		throttleBefore := v152RCCThrottle()
		cpu0, start := v152RCCCPUSeconds(), time.Now()
		for i := 0; i < 1000; i++ {
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, 1, result.Stats.RelationshipsCreated)
		}
		wall := time.Since(start).Seconds()
		cpu := v152RCCCPUSeconds() - cpu0
		rate := 1000 / wall
		if rate < 5000 {
			below++
		} else {
			above++
		}
		t.Logf("RCC_FASTPATH batch=%d wall_ops_per_s=%.0f cpu_us_per_op=%.1f cpu_over_wall=%.2f compound_fastpath=%v loadavg=%q throttle_before=%s throttle_after=%s",
			batch, rate, cpu*1e6/1000, cpu/wall, executor.LastHotPathTrace().CompoundQueryFastPath, strings.TrimSpace(string(load)), throttleBefore, v152RCCThrottle())
	}
	t.Logf("RCC_FASTPATH_SUMMARY threshold=5000 batches_below=%d batches_above=%d", below, above)
}

// TestV152RCCFastPathWithLimitRateVariance does the same for the WITH LIMIT
// workload of fastpath_test.go:14-61 (threshold 10,000 ops/s).
func TestV152RCCFastPathWithLimitRateVariance(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	for i := 0; i < 10; i++ {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("actor%d", i)), Labels: []string{"Actor"}, Properties: map[string]interface{}{"name": fmt.Sprintf("Actor_%d", i)}})
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("movie%d", i)), Labels: []string{"Movie"}, Properties: map[string]interface{}{"title": fmt.Sprintf("Movie_%d", i)}})
		require.NoError(t, err)
	}
	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	query := "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r"
	below, above := 0, 0
	for batch := 0; batch < 12; batch++ {
		load, _ := os.ReadFile("/proc/loadavg")
		throttleBefore := v152RCCThrottle()
		cpu0, start := v152RCCCPUSeconds(), time.Now()
		for i := 0; i < 1000; i++ {
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, 1, result.Stats.RelationshipsCreated)
		}
		wall := time.Since(start).Seconds()
		cpu := v152RCCCPUSeconds() - cpu0
		rate := 1000 / wall
		if rate < 10000 {
			below++
		} else {
			above++
		}
		t.Logf("RCC_FASTPATH_WITHLIMIT batch=%d wall_ops_per_s=%.0f cpu_us_per_op=%.1f cpu_over_wall=%.2f compound_fastpath=%v loadavg=%q throttle_before=%s throttle_after=%s",
			batch, rate, cpu*1e6/1000, cpu/wall, executor.LastHotPathTrace().CompoundQueryFastPath, strings.TrimSpace(string(load)), throttleBefore, v152RCCThrottle())
	}
	t.Logf("RCC_FASTPATH_WITHLIMIT_SUMMARY threshold=10000 batches_below=%d batches_above=%d", below, above)
}
