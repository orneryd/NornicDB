package server

// Batch-3b probe (research only, untracked; same text on the final and the
// previous build): why the same HTTP auto-commit DELETE of 60,000 nodes takes
// between ~8 and ~60 s with the timeout at 10 minutes. The condition of the
// row-72 runs is kept: one server, the timeout at 10 minutes, seed over Bolt,
// then POST /db/nornic/tx/commit with the DELETE, repeated on the same server.
// For each run: wall time, this process's CPU time (getrusage), GC count and
// pause, and the machine's load (/proc/loadavg, /proc/stat busy share).
// From 10 s into a run, every 5 s: the statement's phase and the machine's
// busy share; at 15, 30 and 50 s a grouped goroutine profile of the
// goroutines in nornicdb code, so a slow run shows what it is waiting on.

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func zz3bCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// zz3bProcStat returns busy and total jiffies of the whole machine.
func zz3bProcStat() (uint64, uint64) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	if !sc.Scan() {
		return 0, 0
	}
	fields := strings.Fields(sc.Text())
	var total, idle uint64
	for i, v := range fields[1:] {
		n, _ := strconv.ParseUint(v, 10, 64)
		total += n
		if i == 3 || i == 4 { // idle, iowait
			idle += n
		}
	}
	return total - idle, total
}

func zz3bLoadavg() string {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "n/a"
	}
	return strings.TrimSpace(string(b))
}

// zz3bGroupedStacks is the debug=1 goroutine profile (identical stacks
// grouped with their count), keeping the groups that run nornicdb code.
func zz3bGroupedStacks() string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
	var keep []string
	for _, g := range strings.Split(b.String(), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/") || strings.Contains(g, "zz_3b_slowdelete") {
			continue
		}
		lines := strings.Split(g, "\n")
		if len(lines) > 70 {
			lines = append(lines[:70], "#\t... (truncated)")
		}
		keep = append(keep, strings.Join(lines, "\n"))
	}
	return strings.Join(keep, "\n\n")
}

func TestZZ3BSlowDelete60k(t *testing.T) {
	const nodes, runs = 60000, 10
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	env := zz3bStart(t)
	t.Logf("ZZ3B SLOW start: GOMAXPROCS=%d NumCPU=%d loadavg=%s", runtime.GOMAXPROCS(0), runtime.NumCPU(), zz3bLoadavg())
	for run := 1; run <= runs; run++ {
		seedStart := time.Now()
		out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
		require.Empty(t, out.Err)
		seedTook := time.Since(seedStart)

		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)
		cpu0 := zz3bCPU()
		busy0, total0 := zz3bProcStat()
		start := time.Now()
		stop := make(chan struct{})
		var mu sync.Mutex
		var notes []string
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			dumps := map[int]bool{15: true, 30: true, 50: true}
			lastBusy, lastTotal := busy0, total0
			lastCPU := cpu0
			for sec := 10; ; sec += 5 {
				select {
				case <-stop:
					return
				case <-time.After(time.Until(start.Add(time.Duration(sec) * time.Second))):
				}
				b, tt := zz3bProcStat()
				c := zz3bCPU()
				machine := 0.0
				if tt > lastTotal {
					machine = float64(b-lastBusy) / float64(tt-lastTotal)
				}
				proc := float64(c-lastCPU) / float64(5*time.Second)
				lastBusy, lastTotal, lastCPU = b, tt, c
				ph, top := zz3bPhase()
				note := fmt.Sprintf("t=%ds phase=%s [%s] process CPU last 5s=%.2f cores machine busy=%.0f%% loadavg=%s goroutines=%d", sec, ph, top, proc, machine*100, zz3bLoadavg(), runtime.NumGoroutine())
				if dumps[sec] {
					note += "\n--- grouped goroutines in nornicdb code at t=" + strconv.Itoa(sec) + "s:\n" + zz3bGroupedStacks()
				}
				mu.Lock()
				notes = append(notes, note)
				mu.Unlock()
			}
		}()
		status, resp, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
		wall := time.Since(start)
		close(stop)
		wg.Wait()
		cpu := zz3bCPU() - cpu0
		busy1, total1 := zz3bProcStat()
		runtime.ReadMemStats(&ms1)
		machine := 0.0
		if total1 > total0 {
			machine = float64(busy1-busy0) / float64(total1-total0)
		}
		left := env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		t.Logf("ZZ3B SLOW run=%d seed=%v delete wall=%v status=%d response=%s left=%s process CPU=%v (%.2f cores avg) GC=%d pause=%v machine busy=%.0f%% loadavg=%s",
			run, seedTook.Round(time.Millisecond), wall.Round(time.Millisecond), status, zz3bHTTPOutcome(status, resp, err), left,
			cpu.Round(time.Millisecond), float64(cpu)/float64(wall), ms1.NumGC-ms0.NumGC, time.Duration(ms1.PauseTotalNs-ms0.PauseTotalNs).Round(time.Millisecond),
			machine*100, zz3bLoadavg())
		mu.Lock()
		for _, n := range notes {
			t.Logf("ZZ3B SLOW run=%d %s", run, n)
		}
		mu.Unlock()
	}
}
