package cypher

// v1.5.2 close-out lane L4, H-79 (NOR-20): a statement whose deadline expires
// stops materialising rows: it returns the deadline error promptly and the
// process does no further work for it afterwards (CPU time measured with
// getrusage over the following second).

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func l4CPUTime() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func TestCloseoutL4H79DeadlineStopsWork(t *testing.T) {
	shapes := map[string]string{
		"cartesian-count":   "MATCH (a:N79), (b:N79) WHERE a.v + b.v = -1 RETURN count(*) AS n",
		"collect-sorted":    "MATCH (a:N79), (b:N79) WITH a.v * b.v AS p ORDER BY p RETURN collect(p)[0] AS first",
		"unwind-match-scan": "UNWIND range(1, 30000) AS i MATCH (n:N79) WHERE n.v = i * -1 RETURN count(n) AS n",
		"optional-in-scan":  "UNWIND range(1, 30000) AS i OPTIONAL MATCH (n:N79) WHERE n.w = i RETURN count(n) AS n",
	}
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(l4MemStore(t, "l4"))
		l4Batch(t, exec, 30000, "UNWIND range($start, $end - 1) AS k CREATE (:N79 {v: k, w: -k})")
		for name, query := range shapes {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			start := time.Now()
			_, err := exec.Execute(ctx, query, nil)
			returned := time.Since(start)
			cancel()
			runtime.GC()
			cpuBefore := l4CPUTime()
			time.Sleep(time.Second)
			cpuAfter := l4CPUTime() - cpuBefore
			deadline := err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
			l4Report(t, "H-79", name, deadline && returned < 3*time.Second && cpuAfter < 300*time.Millisecond, map[string]any{
				"returnedMs": returned.Milliseconds(), "error": fmt.Sprint(err), "cpuMsInFollowingSecond": cpuAfter.Milliseconds(),
			})
		}
	})
}
