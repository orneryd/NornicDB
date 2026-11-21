package cypher_test

// Verifier of hunt lead L-H10-002 (v1.5.2 verification campaign, test-side
// only). The input is the openCypher TCK scenario Create4 "[1] Generate the
// movie graph" (one statement of CREATE clauses), read from the repository's
// TCK feature file. No fuzz instrumentation: a plain `go test` run.
//
// Every measurement is printed as a VERIFY_H10 line. A call that has not
// returned after 10 s gets a stack sample of its goroutine (VERIFY_H10_STACK),
// repeated at 30 s and 90 s, so the step that is running is visible.

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const verifyH10Feature = "../../testing/cypher/tck/testdata/opencypher/features/clauses/create/Create4.feature"

// verifyH10MovieClauses returns the scenario's statement as its lines (one
// CREATE clause per line in the feature file), indentation removed.
func verifyH10MovieClauses(t *testing.T) []string {
	t.Helper()
	f, err := os.Open(filepath.FromSlash(verifyH10Feature))
	if err != nil {
		t.Fatalf("open feature: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	state := 0 // 0 before scenario, 1 in scenario before docstring, 2 inside docstring
	var lines []string
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		switch state {
		case 0:
			if strings.HasPrefix(trimmed, "Scenario: [1] Generate the movie graph") {
				state = 1
			}
		case 1:
			if trimmed == `"""` {
				state = 2
			}
		case 2:
			if trimmed == `"""` {
				return lines
			}
			lines = append(lines, trimmed)
		}
	}
	t.Fatalf("scenario docstring not found (state %d)", state)
	return nil
}

// verifyH10Prefix keeps the first n lines that start a CREATE clause together
// with their continuation lines, so the prefix is itself a valid statement.
func verifyH10Prefix(lines []string, clauses int) string {
	var out []string
	seen := 0
	for _, l := range lines {
		if strings.HasPrefix(strings.ToUpper(l), "CREATE") {
			seen++
			if seen > clauses {
				break
			}
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

func verifyH10CountCreate(q string) int {
	n := 0
	for _, l := range strings.Split(q, "\n") {
		if strings.HasPrefix(strings.ToUpper(l), "CREATE") {
			n++
		}
	}
	return n
}

func verifyH10Emit(t *testing.T, fields map[string]any) {
	t.Helper()
	fields["parser_config"] = config.GetParserType()
	data, _ := json.Marshal(fields)
	t.Logf("VERIFY_H10 %s", data)
}

// verifyH10StackOf returns the stack of every goroutine whose stack contains
// marker (the function name of the call under test).
func verifyH10StackOf(marker string) string {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	var keep []string
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, marker) {
			if len(g) > 6000 {
				g = g[:6000] + "\n...(truncated)"
			}
			keep = append(keep, g)
		}
	}
	return strings.Join(keep, "\n--\n")
}

type verifyH10Outcome struct {
	returned bool
	elapsed  time.Duration
	err      error
}

// verifyH10Time runs call in a goroutine and waits up to limit for it,
// sampling the call's stack at 10 s, 30 s and 90 s while it runs.
func verifyH10Time(t *testing.T, label, marker string, limit time.Duration, call func() error) verifyH10Outcome {
	t.Helper()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- call() }()
	samples := []time.Duration{10 * time.Second, 30 * time.Second, 90 * time.Second}
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	for {
		var sample <-chan time.Time
		if len(samples) > 0 {
			sample = time.After(time.Until(start.Add(samples[0])))
		}
		select {
		case err := <-done:
			return verifyH10Outcome{returned: true, elapsed: time.Since(start), err: err}
		case <-sample:
			t.Logf("VERIFY_H10_STACK %s at %s:\n%s", label, samples[0], verifyH10StackOf(marker))
			samples = samples[1:]
		case <-deadline.C:
			t.Logf("VERIFY_H10_STACK %s at limit %s:\n%s", label, limit, verifyH10StackOf(marker))
			return verifyH10Outcome{returned: false, elapsed: time.Since(start)}
		}
	}
}

func verifyH10Fields(o verifyH10Outcome) map[string]any {
	m := map[string]any{"returned": o.returned, "elapsed_s": o.elapsed.Seconds()}
	if o.err != nil {
		m["error"] = o.err.Error()
	}
	return m
}

// verifyH10Parse is a named function so its stack frames can be found.
func verifyH10Parse(ctx context.Context, q string) error {
	_, err := cypher.NewParser().Parse(ctx, q)
	return err
}

func verifyH10Execute(ctx context.Context, q string) error {
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"))
	_, err := exec.Execute(ctx, q, nil)
	return err
}

func verifyH10ANTLRParse(q string) error {
	_, err := cypherantlr.Parse(q)
	return err
}

// TestVerifyH10LongStatementParse: default parser's Parse on the full
// statement, with the lead's 2 s deadline; the time until it returns is the
// real time (limit 12 min). Controls: prefixes of the statement, and the
// ANTLR parser on the full statement.
func TestVerifyH10LongStatementParse(t *testing.T) {
	lines := verifyH10MovieClauses(t)
	full := strings.Join(lines, "\n")
	total := verifyH10CountCreate(full)
	verifyH10Emit(t, map[string]any{"case": "input", "chars": len(full), "create_lines": total, "head": full[:80]})

	// Control and scaling: prefixes, no deadline, limit 2 min each.
	for _, n := range []int{8, 27, 53, 106} {
		q := verifyH10Prefix(lines, n)
		o := verifyH10Time(t, "parse-prefix", "verifyH10Parse", 2*time.Minute, func() error { return verifyH10Parse(context.Background(), q) })
		f := verifyH10Fields(o)
		f["case"], f["role"], f["clauses"], f["chars"] = "parse-prefix", "control", n, len(q)
		verifyH10Emit(t, f)
		if n == 8 && (!o.returned || o.elapsed > time.Second || o.err != nil) {
			t.Errorf("control: 8-clause prefix did not parse within 1 s: %+v", f)
		}
		if !o.returned {
			break
		}
	}

	// ANTLR parser on the full statement (control for the parser).
	o := verifyH10Time(t, "antlr-parse-full", "verifyH10ANTLRParse", 3*time.Minute, func() error { return verifyH10ANTLRParse(full) })
	f := verifyH10Fields(o)
	f["case"], f["role"], f["chars"] = "antlr-parse-full", "control", len(full)
	verifyH10Emit(t, f)
	if !o.returned || o.elapsed > 10*time.Second || o.err != nil {
		t.Errorf("control: ANTLR parse of the full statement did not succeed within 10 s: %+v", f)
	}

	// Default parser, full statement, 2 s deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	o = verifyH10Time(t, "parse-full-2s", "verifyH10Parse", 12*time.Minute, func() error { return verifyH10Parse(ctx, full) })
	f = verifyH10Fields(o)
	f["case"], f["role"], f["chars"], f["deadline_s"] = "parse-full-2s-deadline", "fail", len(full), 2
	verifyH10Emit(t, f)
	if !o.returned || o.elapsed > 10*time.Second {
		t.Errorf("DISAGREE default Parse with a 2 s deadline did not return within 10 s: %+v", f)
	}
	if o.returned && o.err != nil && o.elapsed <= 10*time.Second {
		// Deadline honoured; measure the real parse time without a deadline.
		o2 := verifyH10Time(t, "parse-full-nodeadline", "verifyH10Parse", 12*time.Minute, func() error { return verifyH10Parse(context.Background(), full) })
		f2 := verifyH10Fields(o2)
		f2["case"], f2["role"], f2["chars"] = "parse-full-no-deadline", "measure", len(full)
		verifyH10Emit(t, f2)
		if !o2.returned || o2.elapsed > 10*time.Second {
			t.Errorf("DISAGREE default Parse of the full statement takes over 10 s: %+v", f2)
		}
	}
}

// TestVerifyH10LongStatementExecute: Execute on an empty store with the 2 s
// deadline (the parser is the job's NORNICDB_PARSER: default or antlr). The
// time until it returns is the real time (limit 12 min). Control: an 8-clause
// prefix with the same deadline.
func TestVerifyH10LongStatementExecute(t *testing.T) {
	lines := verifyH10MovieClauses(t)
	full := strings.Join(lines, "\n")

	short := verifyH10Prefix(lines, 8)
	ctx0, cancel0 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel0()
	o := verifyH10Time(t, "execute-prefix-2s", "verifyH10Execute", 2*time.Minute, func() error { return verifyH10Execute(ctx0, short) })
	f := verifyH10Fields(o)
	f["case"], f["role"], f["clauses"], f["chars"] = "execute-prefix-8-2s-deadline", "control", 8, len(short)
	verifyH10Emit(t, f)
	if !o.returned || o.err != nil || o.elapsed > 2*time.Second {
		t.Errorf("control: 8-clause prefix did not execute within its 2 s deadline: %+v", f)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	o = verifyH10Time(t, "execute-full-2s", "verifyH10Execute", 12*time.Minute, func() error { return verifyH10Execute(ctx, full) })
	f = verifyH10Fields(o)
	f["case"], f["role"], f["chars"], f["deadline_s"] = "execute-full-2s-deadline", "fail", len(full), 2
	verifyH10Emit(t, f)
	if !o.returned || o.elapsed > 10*time.Second {
		t.Errorf("DISAGREE Execute with a 2 s deadline did not return within 10 s: %+v", f)
	}
}
