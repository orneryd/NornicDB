package cypher

// Batch 4b, 48.18 (untracked; never committed): the committed test
// TestExecutionDeterminismFabricEmbeddedDeadline alone, eight copies of this
// race-built test binary at once (each running only that test), GOMAXPROCS=2,
// for six rounds (48 copies); then, as a reference, eight copies one after
// another. Each copy's PASS or FAIL and its failure line are counted.

import (
	"fmt"
	"os"
	osexec "os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

func b4bDeadlineCopy() (string, time.Duration) {
	start := time.Now()
	cmd := osexec.Command(os.Args[0], "-test.run=^TestExecutionDeterminismFabricEmbeddedDeadline$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	out, _ := cmd.CombinedOutput()
	return string(out), time.Since(start)
}

func b4bDeadlineVerdict(out string) string {
	switch {
	case strings.Contains(out, "--- FAIL: TestExecutionDeterminismFabricEmbeddedDeadline"):
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "execution_determinism_test.go:") {
				return "FAIL " + strings.TrimSpace(line)
			}
		}
		return "FAIL"
	case strings.Contains(out, "--- PASS: TestExecutionDeterminismFabricEmbeddedDeadline"):
		return "PASS"
	default:
		return "NOVERDICT"
	}
}

func TestL9DeadlineEightColdProcesses(t *testing.T) {
	if os.Getenv("B4B_CHILD") != "" {
		t.Skip("parent only")
	}
	totals := map[string]int{}
	for round := 0; round < 6; round++ {
		var wg sync.WaitGroup
		results := make([]string, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				out, d := b4bDeadlineCopy()
				results[i] = fmt.Sprintf("%s (%s)", b4bDeadlineVerdict(out), d.Round(time.Millisecond))
			}(i)
		}
		wg.Wait()
		for _, r := range results {
			totals["together "+strings.SplitN(r, " ", 2)[0]]++
		}
		t.Logf("B4B 48.18 together round=%d: %s", round, strings.Join(results, " | "))
	}
	for i := 0; i < 8; i++ {
		out, d := b4bDeadlineCopy()
		v := b4bDeadlineVerdict(out)
		totals["sequential "+strings.SplitN(v, " ", 2)[0]]++
		t.Logf("B4B 48.18 sequential copy=%d: %s (%s)", i, v, d.Round(time.Millisecond))
	}
	t.Logf("L9 E54.18 TOTAL %v", totals)
	if totals["together PASS"] != 48 || totals["sequential PASS"] != 8 {
		t.Errorf("expected every cold copy to pass, got %v", totals)
	}
}
