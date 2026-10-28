//go:build linux || darwin

package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math/rand"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"syscall"
	"testing"
)

type snapshotPressureObject struct {
	a, b *snapshotPressureObject
	s    string
	v    any
}

type snapshotByteCounter int64

func (w *snapshotByteCounter) Write(data []byte) (int, error) {
	*w += snapshotByteCounter(len(data))
	return len(data), nil
}

func snapshotProcessCPU() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

// BenchmarkSnapshotStoredGraph reproduces the investigation's actual Badger
// graph and pointer-dense heap, rather than an encoder-only fixture. One op
// writes the complete 30k-node/300k-edge snapshot. Run -benchtime=1x; tnrun ab
// alternates ten fresh processes per side on the same CPUs.
func BenchmarkSnapshotStoredGraph(b *testing.B) {
	const nodes, edges = 30000, 300000
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	rng := rand.New(rand.NewSource(1))
	desc := strings.Repeat("calls helper to resolve the symbol table entry for the given package path; ", 3)
	nodeID := func(i int) NodeID { return NodeID(fmt.Sprintf("probe:%08x-1111-4000-8000-%012x", i, i)) }
	for i := 0; i < nodes; i += 5000 {
		batch := make([]*Node, 0, 5000)
		for j := i; j < i+5000 && j < nodes; j++ {
			batch = append(batch, &Node{ID: nodeID(j), Labels: []string{"CodeIntel", "CodeIntelFunction"}, Properties: map[string]any{
				"_ci_key": fmt.Sprintf("github.com/orneryd/nornicdb/pkg/storage.func%07d", j), "id": fmt.Sprintf("f%07d", j),
				"label": fmt.Sprintf("func%07d()", j), "source_file": "pkg/storage/badger_stats.go", "source_location": "L877",
				"repo": "nornicdb", "gen": int64(7), "props_hash": fmt.Sprintf("%064x", j),
			}})
		}
		if err := engine.BulkCreateNodes(batch); err != nil {
			b.Fatal(err)
		}
	}
	types := []string{"CALLS", "CALLS_GO", "CONTAINS", "IMPORTS"}
	for i := 0; i < edges; i += 5000 {
		batch := make([]*Edge, 0, 5000)
		for j := i; j < i+5000 && j < edges; j++ {
			batch = append(batch, &Edge{
				ID:        EdgeID(fmt.Sprintf("probe:%08x-2222-4000-8000-%012x", j, j)),
				StartNode: nodeID(rng.Intn(nodes)), EndNode: nodeID(rng.Intn(nodes)), Type: types[j%len(types)],
				VisibilitySuppressed: j%17 == 0,
				Properties: map[string]any{
					"relation": "calls", "confidence": "EXTRACTED", "confidence_score": 1.0,
					"source_file": fmt.Sprintf("pkg/storage/file_%04d.go", j%3000), "source_location": fmt.Sprintf("L%d", j%2000),
					"description": desc, "site": fmt.Sprintf("pkg/storage/file_%04d.go:%d:%d", j%3000, j%2000, j%80),
					"algo": "vta", "props_hash": fmt.Sprintf("%064x", j), "gen": int64(7), "repo": "nornicdb", "weight": 1.0,
				},
			})
		}
		if err := engine.BulkCreateEdges(batch); err != nil {
			b.Fatal(err)
		}
	}
	for _, pressure := range []bool{false, true} {
		name := "normal"
		if pressure {
			name = "pressure"
		}
		b.Run(name, func(b *testing.B) {
			var ballast []*snapshotPressureObject
			if pressure {
				ballast = make([]*snapshotPressureObject, 24000000)
				for i := range ballast {
					ballast[i] = &snapshotPressureObject{s: desc}
				}
				for i := range ballast {
					ballast[i].a = ballast[rng.Intn(len(ballast))]
					ballast[i].b = ballast[rng.Intn(len(ballast))]
				}
			}
			runtime.GC()
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			if pressure {
				old := debug.SetMemoryLimit(int64(memory.Sys-memory.HeapReleased) + int64(memory.HeapAlloc*6/100))
				defer debug.SetMemoryLimit(old)
			}
			samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
			metrics.Read(samples)
			gcBefore, cpuBefore := samples[0].Value.Float64(), snapshotProcessCPU()
			var output snapshotByteCounter
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := WriteSnapshot(context.Background(), engine, &output, SnapshotOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			cpu := snapshotProcessCPU() - cpuBefore
			metrics.Read(samples)
			b.ReportMetric(cpu*1e9/float64(b.N*edges), "CPU-ns/edge")
			b.ReportMetric((samples[0].Value.Float64()-gcBefore)*1e9/float64(b.N*edges), "GC-ns/edge")
			b.ReportMetric(float64(output)/float64(b.N*edges), "output-B/edge")
			b.ReportMetric(float64(memory.HeapAlloc)/(1<<20), "live-MiB")
			runtime.KeepAlive(ballast)
		})
	}
}
