// Author: Codex, NOR-160 implementation sub-agent, 2026-10-26.
package bolt

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sort"
	"testing"
	"time"

	profile "github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
)

// Profiles start after fixture creation, connection setup and transaction
// priming. Raw gzip pprof bytes travel through the durable tnrun output, avoiding
// worker-local files that a later checkout would overwrite. The allocation
// profile is the difference of cumulative allocs snapshots, not live heap size.
func nor160Profile(t *testing.T, shape, mode string, run func(int)) {
	t.Helper()
	previousRate := runtime.MemProfileRate
	runtime.MemProfileRate = 64 << 10
	defer func() { runtime.MemProfileRate = previousRate }()
	allocationSnapshot := func() *profile.Profile {
		runtime.GC()
		var data bytes.Buffer
		require.NoError(t, pprof.Lookup("allocs").WriteTo(&data, 0))
		p, err := profile.ParseData(data.Bytes())
		require.NoError(t, err)
		return p
	}
	before := allocationSnapshot()
	var cpu bytes.Buffer
	require.NoError(t, pprof.StartCPUProfile(&cpu))
	// Keep profiling bounded while collecting more than one CPU tick for fast
	// routes. Each invocation must produce the full expected row multiset.
	start := time.Now()
	iterations := 0
	func() {
		defer pprof.StopCPUProfile()
		for iterations < 3 {
			run(iterations)
			iterations++
			if time.Since(start) >= time.Second {
				break
			}
		}
	}()
	after := allocationSnapshot()
	before.Scale(-1)
	delta, err := profile.Merge([]*profile.Profile{after, before})
	require.NoError(t, err)
	var allocations bytes.Buffer
	require.NoError(t, delta.Write(&allocations))
	for _, item := range []struct {
		kind   string
		data   []byte
		sample string
	}{
		{"cpu", cpu.Bytes(), "cpu"}, {"alloc", allocations.Bytes(), "alloc_space"},
	} {
		p, err := profile.ParseData(item.data)
		require.NoError(t, err)
		flat, cumulative, total := nor160ProfileTop(p, item.sample)
		nor160Log(t, "PROFILE_SUMMARY", map[string]interface{}{"shape": shape, "mode": mode, "kind": item.kind, "sample": item.sample, "iterations": iterations, "total": total, "flat": flat, "cumulative": cumulative})
		// The marker is intentionally machine-readable and includes every byte.
		t.Logf("NOR160_PROFILE_DATA %s %s %s %s", shape, mode, item.kind, base64.StdEncoding.EncodeToString(item.data))
	}
}

type nor160ProfileEntry struct {
	Function string `json:"function"`
	Value    int64  `json:"value"`
}

func nor160ProfileTop(p *profile.Profile, sampleType string) (flat, cumulative []nor160ProfileEntry, total int64) {
	index := -1
	for i, kind := range p.SampleType {
		if kind.Type == sampleType {
			index = i
			break
		}
	}
	if index < 0 {
		panic(fmt.Sprintf("missing profile sample %s", sampleType))
	}
	flatValues, cumulativeValues := map[string]int64{}, map[string]int64{}
	for _, sample := range p.Sample {
		value := sample.Value[index]
		total += value
		seen := map[string]bool{}
		leaf := true
		for _, location := range sample.Location {
			for _, line := range location.Line {
				if line.Function == nil {
					continue
				}
				name := line.Function.Name
				if leaf {
					flatValues[name] += value
					leaf = false
				}
				if !seen[name] {
					cumulativeValues[name] += value
					seen[name] = true
				}
			}
		}
	}
	top := func(values map[string]int64) []nor160ProfileEntry {
		entries := make([]nor160ProfileEntry, 0, len(values))
		for name, value := range values {
			entries = append(entries, nor160ProfileEntry{name, value})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Value == entries[j].Value {
				return entries[i].Function < entries[j].Function
			}
			return entries[i].Value > entries[j].Value
		})
		return entries[:min(len(entries), 20)]
	}
	return top(flatValues), top(cumulativeValues), total
}
