package antlr

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Prediction-cache benchmarks. The cold ones measure a process's first
// parses, so run each alone with -benchtime 1x: BenchmarkANTLRColdParse and
// BenchmarkANTLRConcurrentColdParse. Each reports the prediction DFA states
// and the heap they retain (benchPredictionStates).

func benchLargeStatement(b *testing.B) string {
	b.Helper()
	data, err := os.ReadFile("testdata/prediction_large_statement.cypher")
	if err != nil {
		b.Fatal(err)
	}
	return string(data)
}

func benchExpression(r *rand.Rand, depth int) string {
	if depth == 0 {
		return []string{"n.p", "1", "'s'", "$p", "x"}[r.Intn(5)]
	}
	a, c := benchExpression(r, depth-1), benchExpression(r, depth-1)
	switch r.Intn(6) {
	case 0:
		return "[x IN " + a + " WHERE x = " + c + " | x]"
	case 1:
		return "CASE WHEN " + a + " THEN " + c + " END"
	case 2:
		return "{k: " + a + ", j: " + c + "}"
	case 3:
		return "coalesce(" + a + ", " + c + ")"
	case 4:
		return "(" + a + " + " + c + ")"
	default:
		return a + " IN [" + c + "]"
	}
}

func benchSmallStatement(r *rand.Rand) string {
	return fmt.Sprintf("MATCH (n) WITH n, 1 AS x, %s AS v0, %s AS v1 RETURN v0, v1", benchExpression(r, 2), benchExpression(r, 1+r.Intn(2)))
}

func benchHeapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func reportPrediction(b *testing.B, heapBefore uint64) {
	b.ReportMetric(float64(benchPredictionStates()), "dfa-states")
	b.ReportMetric(float64(int64(benchHeapInUse())-int64(heapBefore))/(1<<20), "retained-MiB")
}

// The first parse of a 99 KB statement in a fresh process.
func BenchmarkANTLRColdParse(b *testing.B) {
	query := benchLargeStatement(b)
	heap := benchHeapInUse()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Validate(query)
	}
	b.StopTimer()
	reportPrediction(b, heap)
}

// A fresh process's first parse of the 99 KB statement while 8 workers
// validate distinct small statements: the long parse's time and the small
// parses' latency percentiles.
func BenchmarkANTLRConcurrentColdParse(b *testing.B) {
	query := benchLargeStatement(b)
	r0 := rand.New(rand.NewSource(99))
	for i := 0; i < 20; i++ {
		_ = Validate(benchSmallStatement(r0))
	}
	heap := benchHeapInUse()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var longTotal time.Duration
	var lat []time.Duration
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var longDone atomic.Bool
		var wgLong sync.WaitGroup
		wgLong.Add(1)
		go func() {
			defer wgLong.Done()
			started := time.Now()
			_ = Validate(query)
			longTotal += time.Since(started)
			longDone.Store(true)
		}()
		time.Sleep(50 * time.Millisecond)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(n*8 + w + 1)))
				for i := 0; !longDone.Load() || i < 20; i++ {
					statement := benchSmallStatement(r)
					started := time.Now()
					_ = Validate(statement)
					d := time.Since(started)
					mu.Lock()
					lat = append(lat, d)
					mu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		wgLong.Wait()
	}
	b.StopTimer()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) float64 { return float64(lat[int(p*float64(len(lat)-1))]) / float64(time.Microsecond) }
	b.ReportMetric(float64(longTotal)/float64(b.N)/float64(time.Millisecond), "long-ms")
	b.ReportMetric(pct(0.50), "small-p50-us")
	b.ReportMetric(pct(0.99), "small-p99-us")
	b.ReportMetric(float64(len(lat))/float64(b.N), "small-parses")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N)/(1<<20), "alloc-MiB")
	reportPrediction(b, heap)
}

// The 99 KB statement again, warm.
func BenchmarkANTLRWarmParse(b *testing.B) {
	query := benchLargeStatement(b)
	_ = Validate(query)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Validate(query)
	}
}

// Small statements of 64 shapes, warm: the per-parse cost of taking and
// giving back a generation.
func BenchmarkANTLRWarmSmall(b *testing.B) {
	r := rand.New(rand.NewSource(5))
	statements := make([]string, 64)
	for i := range statements {
		statements[i] = benchSmallStatement(r)
		_ = Validate(statements[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Validate(statements[i%len(statements)])
	}
}

// The same, from 4 goroutines at once.
func BenchmarkANTLRWarmSmallParallel(b *testing.B) {
	r := rand.New(rand.NewSource(5))
	statements := make([]string, 64)
	for i := range statements {
		statements[i] = benchSmallStatement(r)
		_ = Validate(statements[i])
	}
	var next atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = Validate(statements[next.Add(1)%int64(len(statements))])
		}
	})
}
