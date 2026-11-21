package cypher

// Verifier of hunt lead L-H10-002, submission 2 (test-side only): which step
// takes the time on the TCK movie-graph statement. Each step runs alone, on a
// fresh text variant (trailing spaces differ, so no validated-text cache entry
// is reused), with no deadline; its goroutine's stack is sampled every 50 ms
// and the frames are counted. VERIFY_H10_OWNER lines: per step the elapsed
// time, the innermost nornicdb frames (self) and the nornicdb frames anywhere
// in the stack (inclusive), top 10 each, as function file:line.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func verifyH10OwnerStatement(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.FromSlash("../../testing/cypher/tck/testdata/opencypher/features/clauses/create/Create4.feature"))
	if err != nil {
		t.Fatalf("read feature: %v", err)
	}
	var lines []string
	state := 0
	for _, line := range strings.Split(string(data), "\n") {
		tr := strings.TrimSpace(line)
		switch {
		case state == 0 && strings.HasPrefix(tr, "Scenario: [1] Generate the movie graph"):
			state = 1
		case state == 1 && tr == `"""`:
			state = 2
		case state == 2 && tr == `"""`:
			return strings.Join(lines, "\n")
		case state == 2:
			lines = append(lines, tr)
		}
	}
	t.Fatalf("scenario not found")
	return ""
}

var verifyH10FrameRE = regexp.MustCompile(`(?m)^(github\.com/orneryd/nornicdb/[^\n(]+)\([^\n]*\)\n\t([^\n ]+)`)

type verifyH10Count struct {
	Frame string `json:"frame"`
	N     int    `json:"n"`
}

func verifyH10Top(m map[string]int, k int) []verifyH10Count {
	out := make([]verifyH10Count, 0, len(m))
	for f, n := range m {
		out = append(out, verifyH10Count{f, n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].N > out[j].N || out[i].N == out[j].N && out[i].Frame < out[j].Frame })
	if len(out) > k {
		out = out[:k]
	}
	return out
}

func verifyH10Profile(t *testing.T, step, marker string, call func() error) {
	t.Helper()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- call() }()
	self, incl := map[string]int{}, map[string]int{}
	samples := 0
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	limit := time.After(90 * time.Second)
	var err error
	returned := false
loop:
	for {
		select {
		case err = <-done:
			returned = true
			break loop
		case <-limit:
			break loop
		case <-tick.C:
			buf := make([]byte, 4<<20)
			n := runtime.Stack(buf, true)
			for _, g := range strings.Split(string(buf[:n]), "\n\n") {
				if !strings.Contains(g, marker) {
					continue
				}
				samples++
				seen := map[string]bool{}
				for i, m := range verifyH10FrameRE.FindAllStringSubmatch(g, -1) {
					frame := strings.TrimPrefix(m[1], "github.com/orneryd/nornicdb/") + " " + filepath.Base(m[2])
					if i == 0 {
						self[frame]++
					}
					fn := strings.TrimPrefix(m[1], "github.com/orneryd/nornicdb/")
					if !seen[fn] {
						seen[fn] = true
						incl[fn]++
					}
				}
			}
		}
	}
	rec := map[string]any{"step": step, "returned": returned, "elapsed_s": time.Since(start).Seconds(), "samples": samples,
		"self_top": verifyH10Top(self, 10), "inclusive_top": verifyH10Top(incl, 12), "parser_config": config.GetParserType()}
	if err != nil {
		rec["error"] = err.Error()
	}
	data, _ := json.Marshal(rec)
	t.Logf("VERIFY_H10_OWNER %s", data)
}

func verifyH10OwnerValidate(ctx context.Context, q string) error { return validateStaticMapKeys(ctx, q) }
func verifyH10OwnerTokenize(ctx context.Context, q string) error {
	_, err := tokenize(ctx, q)
	return err
}
func verifyH10OwnerParse(ctx context.Context, q string) error {
	_, err := NewParser().Parse(ctx, q)
	return err
}

func TestVerifyH10LongStatementOwner(t *testing.T) {
	full := verifyH10OwnerStatement(t)
	variant := func(i int) string { return full + strings.Repeat(" ", i) }
	ctx := context.Background()
	verifyH10Profile(t, "validateStaticMapKeys", "verifyH10OwnerValidate", func() error { return verifyH10OwnerValidate(ctx, variant(1)) })
	verifyH10Profile(t, "tokenize", "verifyH10OwnerTokenize", func() error { return verifyH10OwnerTokenize(ctx, variant(2)) })
	verifyH10Profile(t, "Parser.Parse", "verifyH10OwnerParse", func() error { return verifyH10OwnerParse(ctx, variant(3)) })
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	verifyH10Profile(t, "StorageExecutor.Execute", "verifyH10OwnerExecute", func() error { return verifyH10OwnerExecute(ctx, exec, variant(4)) })
	t.Logf("VERIFY_H10_OWNER_INPUT %s", fmt.Sprint(len(full)))
}

func verifyH10OwnerExecute(ctx context.Context, exec *StorageExecutor, q string) error {
	_, err := exec.Execute(ctx, q, nil)
	return err
}
