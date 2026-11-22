// Close-out lane L8, round 4 (2026-11-22): owner run for L-H2-010. The
// statements are run as the lead runs them (Execute, 500,000 parameter
// items, 20 ms deadline) after one warm-up execution; the CPU profile of
// those runs is dominated by the work done after the deadline. The reduce
// form, which stops within the budget, is profiled the same way as the
// control. Test-only, not for merge.
package cypher

import (
	"bytes"
	"context"
	"runtime/pprof"
	"testing"
	"time"
)

func TestL8Owner_H2_010_DeadlineProfile(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := l8Exec(t)
		params := map[string]interface{}{"items": l8Items(500000)}
		for shape, q := range map[string]string{
			"comprehension-projection": "RETURN [x IN $items | size(randomUUID())] AS v",
			"any":                      "RETURN any(x IN $items WHERE size(randomUUID()) < 0) AS v",
			"reduce-control":           "RETURN reduce(a = 0, x IN $items | a + size(randomUUID())) AS v",
		} {
			warm, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			_, _ = exec.Execute(warm, q, params)
			cancel()
			var buf bytes.Buffer
			if err := pprof.StartCPUProfile(&buf); err != nil {
				t.Logf("profile: %v", err)
				continue
			}
			start := time.Now()
			runs := 0
			var overrun time.Duration
			for runs < 4 {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				began := time.Now()
				_, _ = exec.Execute(ctx, q, params)
				overrun += time.Since(began) - 20*time.Millisecond
				cancel()
				runs++
			}
			pprof.StopCPUProfile()
			l8Owner(t, "L-H2-010", "mean overrun "+shape, map[string]any{"runs": runs, "mean_overrun_ms": float64(overrun.Milliseconds()) / float64(runs)})
			l8ProfileTop(t, "L-H2-010", "deadline-20ms "+shape, runs, time.Since(start), buf.Bytes())
		}
	})
}
