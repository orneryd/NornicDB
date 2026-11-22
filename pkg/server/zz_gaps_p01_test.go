package server

// Report gap P01 (v1.5.2 verification campaign, test-side only): what the
// polluter TestServerStopR4LogOutcomes leaves in process-wide state before the
// victim TestHTTPTerminateEndsAnIdleHolderAtOnce runs (L-H7-008 pair).
// Same file on the pin and on v1.5.1.
//
// TestGapP01R4Pair runs, as subtests of one process: a probe, the polluter,
// a probe, the victim, a probe. A probe lists every transaction SHOW
// TRANSACTIONS reports (the process-wide registry the victim reads, all
// default columns) and the goroutines whose stacks mention a transaction or
// session. Select one kind of the polluter with -run, e.g.
//   -run '^TestGapP01R4Pair$/./^terminal-observer$'
// (subtests named probe-* and victim have no third level and always run).
// Diagnostic: the victim's own failure is reported by its subtest.

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func gapP01Probe(t *testing.T, label string) {
	store := storage.NewMemoryEngine()
	defer func() { _ = store.Close() }()
	res, err := cypher.NewStorageExecutor(store).Execute(context.Background(), "SHOW TRANSACTIONS", nil)
	if err != nil {
		t.Logf("GAPP01 %s SHOW TRANSACTIONS error: %v", label, err)
	} else {
		t.Logf("GAPP01 %s SHOW TRANSACTIONS rows=%d columns=%v", label, len(res.Rows), res.Columns)
		for i, row := range res.Rows {
			parts := make([]string, 0, len(row))
			for j, v := range row {
				if j < len(res.Columns) {
					parts = append(parts, fmt.Sprintf("%s=%v", res.Columns[j], v))
				}
			}
			t.Logf("GAPP01 %s tx#%d %s", label, i+1, strings.Join(parts, " "))
		}
	}
	buf := make([]byte, 8<<20)
	all := string(buf[:runtime.Stack(buf, true)])
	counts := map[string]int{}
	total := 0
	for _, g := range strings.Split(all, "\n\n") {
		total++
		lg := strings.ToLower(g)
		if !(strings.Contains(lg, "transaction") || strings.Contains(lg, "session")) || strings.Contains(g, "gapP01Probe") {
			continue
		}
		lines := strings.Split(g, "\n")
		top := ""
		for i := 1; i < len(lines); i += 2 {
			fn := strings.TrimSpace(lines[i])
			if strings.Contains(fn, "nornicdb/pkg/") {
				if j := strings.Index(fn, "("); j > 0 && !strings.HasPrefix(fn[j:], "(*") {
					fn = fn[:j]
				}
				top = fn
				break
			}
		}
		state := strings.TrimSpace(lines[0])
		if k := strings.Index(state, "["); k >= 0 {
			state = state[k:]
		}
		counts[state+" "+top]++
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("GAPP01 %s goroutines=%d mentioning transaction/session=%d", label, total, len(keys))
	for _, k := range keys {
		t.Logf("GAPP01 %s goroutine x%d %s", label, counts[k], k)
	}
}

func TestGapP01R4Pair(t *testing.T) {
	t.Run("probe-before", func(t *testing.T) { gapP01Probe(t, "before") })
	t.Run("polluter", TestServerStopR4LogOutcomes)
	t.Run("probe-after-polluter", func(t *testing.T) { gapP01Probe(t, "after-polluter") })
	t.Run("victim", TestHTTPTerminateEndsAnIdleHolderAtOnce)
	t.Run("probe-after-victim", func(t *testing.T) { gapP01Probe(t, "after-victim") })
}

func TestGapP01VictimAlone(t *testing.T) {
	t.Run("probe-before", func(t *testing.T) { gapP01Probe(t, "before") })
	t.Run("victim", TestHTTPTerminateEndsAnIdleHolderAtOnce)
	t.Run("probe-after-victim", func(t *testing.T) { gapP01Probe(t, "after-victim") })
}
