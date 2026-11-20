package cypher

// Part 50.16 follow-up (neutral): what the executor does with a 7,955-byte
// statement of about 720 string concatenations when the caller's context has
// a 100 ms deadline. In-memory store, namespace "probe", cache off.
//   W = CREATE (:Probe {run: $run, k: 'secret' + 'secret' + ... + 'x'})
//   R = RETURN 'secret' + 'secret' + ... + 'x' AS v   (same length)
// For each statement, in a fresh executor: call 1 with a 100 ms deadline (in
// a goroutine; the goroutine stacks are sampled 1 s and 5 s after the call
// starts); then calls 2 and 3 with no deadline. After each call of W the
// :Probe nodes of that run are counted.

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func nrpConcat(prefix, suffix string) string {
	var b strings.Builder
	b.WriteString(prefix)
	for b.Len() < 7950-len(suffix)+len("'x'") {
		b.WriteString("'secret' + ")
	}
	b.WriteString("'x'")
	b.WriteString(suffix)
	return b.String()
}

func nrpRepeat(prefix, unit, suffix string) string {
	var b strings.Builder
	b.WriteString(prefix)
	for b.Len()+len(unit)+len(suffix) < 7970 {
		b.WriteString(unit)
	}
	b.WriteString(suffix)
	return b.String()
}

func nrpStackSample(t *testing.T, label string) {
	buf := make([]byte, 1<<22)
	n := runtime.Stack(buf, true)
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/cypher") || strings.Contains(g, "nrpStackSample") {
			continue
		}
		lines := strings.Split(g, "\n")
		var frames []string
		for _, l := range lines {
			if strings.HasPrefix(l, "\t") || strings.HasPrefix(l, "goroutine") {
				continue
			}
			frames = append(frames, l)
			if len(frames) == 12 {
				break
			}
		}
		t.Logf("NRP stack %s: %s", label, strings.Join(frames, " <- "))
	}
}

func TestNRPConcatDeadline(t *testing.T) {
	stmts := map[string]string{
		"write": nrpConcat("CREATE (:Probe {run: $run, k: ", "})"),
		"read":  nrpConcat("RETURN ", " AS v"),
		"or":    nrpRepeat("MATCH (n:Probe) WHERE ", "n.k = 'secret' OR ", "false RETURN count(n) AS c"),
		"list":  nrpRepeat("RETURN size([", "'secret', ", "'x']) AS v"),
	}
	names := []string{"write", "read"}
	if os.Getenv("NRP_CONCAT_SHAPES") != "" {
		names = strings.Split(os.Getenv("NRP_CONCAT_SHAPES"), ",")
	}
	for _, name := range names {
		q := stmts[name]
		s := nrpStore(t, false, "probe")
		e := nrpExecutor(t, s, false)
		t.Logf("NRP %s bytes=%d", name, len(q))
		for call := 1; call <= 3; call++ {
			run := fmt.Sprintf("%s-%d", name, call)
			ctx := nrpCtx()
			cancel := context.CancelFunc(func() {})
			if call == 1 {
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
			}
			done := make(chan error, 1)
			start := time.Now()
			go func() { _, err := e.Execute(ctx, q, map[string]any{"run": run}); done <- err }()
			var err error
			sampled := map[int]bool{}
		wait:
			for {
				select {
				case err = <-done:
					break wait
				case <-time.After(250 * time.Millisecond):
					el := time.Since(start)
					if el > time.Second && !sampled[1] {
						sampled[1] = true
						nrpStackSample(t, fmt.Sprintf("%s call%d +1s", name, call))
					}
					if el > 5*time.Second && !sampled[5] {
						sampled[5] = true
						nrpStackSample(t, fmt.Sprintf("%s call%d +5s", name, call))
					}
				}
			}
			elapsed := time.Since(start)
			cancel()
			stored := "-"
			if name == "write" {
				r := nrpExec(t, e, "MATCH (p:Probe {run: $run}) RETURN count(p)", map[string]any{"run": run})
				stored = fmt.Sprint(r.Rows[0][0])
			}
			msg := "none"
			if err != nil {
				msg = err.Error()
				if len(msg) > 140 {
					msg = msg[:140]
				}
			}
			t.Logf("NRPSUM concat %s call=%d deadline=%v ms=%.1f err=%q stored=%s", name, call, call == 1, float64(elapsed.Microseconds())/1000, msg, stored)
		}
	}
}
