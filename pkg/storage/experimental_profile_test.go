package storage

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/experiments/property-storage/corpus"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestExperimentalStorageProfile runs only when explicitly requested. Invoke the
// compiled test binary once per flag setting to isolate RSS high-water marks.
func TestExperimentalStorageProfile(t *testing.T) {
	root, out := os.Getenv("NORNIC_STORAGE_DOCS_ROOT"), os.Getenv("NORNIC_STORAGE_PROFILE_RESULTS")
	if root == "" || out == "" {
		t.Skip("set documentation root and profile results path")
	}
	enabled, err := strconv.ParseBool(os.Getenv("NORNIC_STORAGE_PROFILE_ENABLED"))
	require.NoError(t, err)
	syncWrites := true
	if setting := os.Getenv("NORNIC_STORAGE_PROFILE_SYNC"); setting != "" {
		syncWrites, err = strconv.ParseBool(setting)
		require.NoError(t, err)
	}
	experimentalStorageFlag(t, enabled)
	base, digest, err := corpus.Load(root)
	require.NoError(t, err)
	var nodes []*Node
	for _, record := range corpus.Replicate(base, 16) {
		nodes = append(nodes, &Node{ID: NodeID(record.ID), Labels: []string{"Documentation"}, Properties: record.Properties})
	}
	dir := t.TempDir()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, SyncWrites: syncWrites})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ctx := context.Background()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	begin := time.Now()
	writes := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		start := time.Now()
		_, err = engine.CreateNode(ctx, node)
		writes = append(writes, time.Since(start).Nanoseconds())
		require.NoError(t, err)
	}
	seed := time.Since(begin)
	sort.Slice(writes, func(i, j int) bool { return writes[i] < writes[j] })
	// Close/reopen forces reads through persisted tables rather than memtables.
	flushStart := time.Now()
	require.NoError(t, engine.Close())
	flush := time.Since(flushStart)
	engine, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, LowMemory: true, SyncWrites: syncWrites})
	require.NoError(t, err)
	read := func(projected bool, measured bool) []int64 {
		samples := make([]int64, 0, 3*len(nodes))
		rounds := 1
		if measured {
			rounds = 3
		}
		for round := 0; round < rounds; round++ {
			for _, want := range nodes {
				start := time.Now()
				var got *Node
				var e error
				if projected {
					got, e = engine.GetNodeProjected(ctx, want.ID, []string{"path"})
				} else {
					got, e = engine.GetNode(ctx, want.ID)
				}
				elapsed := time.Since(start).Nanoseconds()
				require.NoError(t, e)
				if projected {
					require.Equal(t, want.Properties["path"], got.Properties["path"])
				} else {
					require.Equal(t, want.Properties, got.Properties)
				}
				if measured {
					samples = append(samples, elapsed)
				}
			}
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples
	}
	read(true, false)
	projected := read(true, true)
	read(false, false)
	full := read(false, true)
	runtime.ReadMemStats(&after)
	runtime.GC()
	var live runtime.MemStats
	runtime.ReadMemStats(&live)
	require.NoError(t, engine.Close())
	var size int64
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		info, e := entry.Info()
		if e == nil {
			size += info.Size()
		}
		return e
	}))
	var peakRSSKiB uint64
	if status, e := os.ReadFile("/proc/self/status"); e == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				fields := strings.Fields(line)
				peakRSSKiB, err = strconv.ParseUint(fields[1], 10, 64)
				require.NoError(t, err)
			}
		}
	}
	result := map[string]any{"sync_writes": syncWrites, "flush_ms": float64(flush.Nanoseconds()) / 1e6, "write_p50_ns": writes[len(writes)/2], "write_p95_ns": writes[len(writes)*95/100], "enabled": enabled, "corpus_sha256": digest, "nodes": len(nodes), "file_bytes": size, "seed_ms": float64(seed.Nanoseconds()) / 1e6, "projected_p50_ns": projected[len(projected)/2], "projected_p95_ns": projected[len(projected)*95/100], "full_p50_ns": full[len(full)/2], "full_p95_ns": full[len(full)*95/100], "peak_rss_kib": peakRSSKiB, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "live_heap_bytes": live.HeapAlloc, "gc_cycles": after.NumGC - before.NumGC}
	data, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(out, data, 0600))
}
