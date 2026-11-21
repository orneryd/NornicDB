//go:build closeout_l2_e20_r1

package cypher

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E20 Reproduction 1 (the executor): a statement whose one expression is
// about 720 string literals joined by `+` runs for about half a minute on its
// first calls, and a 100 ms deadline on the call does not stop it.
//
// The parser is the process's (NORNICDB_PARSER, read at init); the lab runs
// this file once per parser.
//
// Thresholds (wall time, but the gap is ~300x): the entry's defect is
// 27-34 s for call 1 (100 ms deadline) and 27-32 s for call 2 (no deadline);
// the controls of the same size return in 0.04-0.45 s. A call 1 that has not
// returned within 2 s of its 100 ms deadline, or a call 2 that takes over
// 10 s, is the defect; a fixed executor is well under 1 s for both.

const (
	l2E20R1DeadlineBound = 2 * time.Second
	l2E20R1CallBound     = 10 * time.Second
)

func l2E20R1W() string {
	return "CREATE (:Probe {run: $run, k: " + strings.Repeat("'secret' + ", 721) + "'x'})"
}

func l2E20R1R() string {
	return "RETURN " + strings.Repeat("'secret' + ", 722) + "'x' AS v"
}

func l2E20R1OrChain() string {
	return "MATCH (n:Probe) WHERE " + strings.Repeat("n.k = 'secret' OR ", 440) + "false RETURN count(n) AS c"
}

func l2E20R1ListLiteral() string {
	return "RETURN size([" + strings.Repeat("'secret', ", 794) + "'x']) AS v"
}

func l2E20R1Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open in-memory badger: %v", err)
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(base, "probe"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

func l2E20R1Int(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}

type l2E20R1Call struct {
	elapsed time.Duration
	err     error
	count   int64 // -1 when not counted
}

func (c l2E20R1Call) String() string {
	errText := "ok"
	if c.err != nil {
		errText = c.err.Error()
		if len(errText) > 80 {
			errText = errText[:80]
		}
	}
	if c.count >= 0 {
		return fmt.Sprintf("%.3fs(%s, count=%d)", c.elapsed.Seconds(), errText, c.count)
	}
	return fmt.Sprintf("%.3fs(%s)", c.elapsed.Seconds(), errText)
}

// l2E20R1RunThree runs statement three times on exec: call 1 with a 100 ms
// deadline, calls 2 and 3 without one. When countRuns is true, each call has
// its own $run and the :Probe nodes of that run are counted afterwards.
func l2E20R1RunThree(t *testing.T, exec *StorageExecutor, label, statement string, countRuns bool) [3]l2E20R1Call {
	t.Helper()
	var calls [3]l2E20R1Call
	for i := 0; i < 3; i++ {
		run := fmt.Sprintf("%s-%d", label, i+1)
		ctx := WithTransactionControl(context.Background())
		var cancel context.CancelFunc = func() {}
		if i == 0 {
			ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
		}
		started := time.Now()
		_, err := exec.Execute(ctx, statement, map[string]interface{}{"run": run})
		calls[i] = l2E20R1Call{elapsed: time.Since(started), err: err, count: -1}
		cancel()
		if i > 0 && err != nil {
			t.Errorf("%s call %d (no deadline) failed: %v", label, i+1, err)
		}
		if countRuns {
			res, cerr := exec.Execute(WithTransactionControl(context.Background()), "MATCH (n:Probe {run: $run}) RETURN count(n) AS c", map[string]interface{}{"run": run})
			if cerr != nil || res == nil || len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
				t.Fatalf("%s count after call %d: err=%v res=%v", label, i+1, cerr, res)
			}
			calls[i].count = l2E20R1Int(res.Rows[0][0])
		}
		t.Logf("%s call %d: %s", label, i+1, calls[i])
	}
	return calls
}

func TestL2_e20_r1_Repro(t *testing.T) {
	type shape struct {
		label     string
		statement string
		count     bool
	}
	shapes := []shape{
		{"W", l2E20R1W(), true},
		{"R", l2E20R1R(), false},
	}
	var decisive []string
	defect := false
	for _, s := range shapes {
		t.Logf("%s: %d bytes", s.label, len(s.statement))
		calls := l2E20R1RunThree(t, l2E20R1Executor(t), s.label, s.statement, s.count)
		decisive = append(decisive, fmt.Sprintf("%s=[%s / %s / %s]", s.label, calls[0], calls[1], calls[2]))
		if calls[0].elapsed > l2E20R1DeadlineBound {
			defect = true
			t.Errorf("%s call 1 with a 100 ms deadline returned after %v (bound %v): the deadline does not stop the call", s.label, calls[0].elapsed, l2E20R1DeadlineBound)
		}
		if calls[1].elapsed > l2E20R1CallBound {
			defect = true
			t.Errorf("%s call 2 (no deadline) took %v (bound %v)", s.label, calls[1].elapsed, l2E20R1CallBound)
		}
		if calls[2].elapsed > l2E20R1CallBound {
			defect = true
			t.Errorf("%s call 3 (no deadline) took %v (bound %v)", s.label, calls[2].elapsed, l2E20R1CallBound)
		}
		if s.count && calls[0].err != nil && calls[0].count != 0 {
			// Not the entry's defect (the entry found 0); reported so a
			// partial write after a failed call is never hidden.
			t.Errorf("%s call 1 failed (%v) but stored %d :Probe node(s)", s.label, calls[0].err, calls[0].count)
		}
	}
	if defect {
		t.Logf("L2RESULT E20-R1 DEFECT %s", strings.Join(decisive, " "))
	} else {
		t.Logf("L2RESULT E20-R1 OK %s", strings.Join(decisive, " "))
	}
}

// TestL2_e20_r1_Control runs the entry's same-size controls (an OR chain and
// a list literal) the same way: their deadline is honoured and they finish
// quickly, so the harness and the store are sound.
func TestL2_e20_r1_Control(t *testing.T) {
	for _, s := range []struct{ label, statement string }{
		{"OR", l2E20R1OrChain()},
		{"LIST", l2E20R1ListLiteral()},
	} {
		t.Logf("%s: %d bytes", s.label, len(s.statement))
		calls := l2E20R1RunThree(t, l2E20R1Executor(t), s.label, s.statement, false)
		if calls[0].elapsed > l2E20R1DeadlineBound {
			t.Errorf("control %s call 1 took %v", s.label, calls[0].elapsed)
		}
		if calls[0].err != nil && !errors.Is(calls[0].err, context.DeadlineExceeded) && !strings.Contains(calls[0].err.Error(), "deadline") {
			t.Errorf("control %s call 1 failed with a non-deadline error: %v", s.label, calls[0].err)
		}
		for i := 1; i < 3; i++ {
			if calls[i].elapsed > l2E20R1CallBound {
				t.Errorf("control %s call %d took %v", s.label, i+1, calls[i].elapsed)
			}
		}
	}
	// A short W of the same shape (20 terms) commits and is counted.
	exec := l2E20R1Executor(t)
	short := "CREATE (:Probe {run: $run, k: " + strings.Repeat("'secret' + ", 20) + "'x'})"
	ctx := WithTransactionControl(context.Background())
	if _, err := exec.Execute(ctx, short, map[string]interface{}{"run": "short"}); err != nil {
		t.Fatalf("short W: %v", err)
	}
	res, err := exec.Execute(ctx, "MATCH (n:Probe {run: 'short'}) RETURN n.k AS k", nil)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("short W count: err=%v res=%v", err, res)
	}
	want := strings.Repeat("secret", 20) + "x"
	if got, _ := res.Rows[0][0].(string); got != want {
		t.Errorf("short W stored k=%q, want %q", got, want)
	}
}

// TestL2_e20_r1_Cause times the owner the entry names,
// expressionTextAccepted (pkg/cypher/runtime_type_errors.go:317 at the pin),
// directly on the `+` chain at several sizes, with a live and with a
// cancelled context, and samples the stacks of a long W call at 1 s and 5 s.
// It only logs: it shows whether the form recogniser itself is the slow,
// growing step and whether a cancelled context stops it.
func TestL2_e20_r1_Cause(t *testing.T) {
	for _, terms := range []int{90, 180, 360, 721} {
		expr := strings.Repeat("'secret' + ", terms) + "'x'"
		var live []time.Duration
		var accepted bool
		for i := 0; i < 3; i++ {
			started := time.Now()
			accepted = expressionTextAccepted(context.Background(), expr, expressionTextOperand)
			live = append(live, time.Since(started))
		}
		sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		started := time.Now()
		acceptedCancelled := expressionTextAccepted(cancelled, expr, expressionTextOperand)
		cancelledElapsed := time.Since(started)
		t.Logf("L2CAUSE E20-R1 expressionTextAccepted terms=%d bytes=%d median=%v accepted=%v cancelled=%v acceptedCancelled=%v",
			terms, len(expr), live[1], accepted, cancelledElapsed, acceptedCancelled)
	}

	// Stack samples of a long first W call, as in the entry.
	exec := l2E20R1Executor(t)
	done := make(chan time.Duration, 1)
	go func() {
		ctx, cancel := context.WithTimeout(WithTransactionControl(context.Background()), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, _ = exec.Execute(ctx, l2E20R1W(), map[string]interface{}{"run": "cause"})
		done <- time.Since(started)
	}()
	started := time.Now()
	for _, at := range []time.Duration{time.Second, 5 * time.Second} {
		select {
		case elapsed := <-done:
			t.Logf("L2CAUSE E20-R1 W call returned after %v, before the %v sample", elapsed, at)
			return
		case <-time.After(at - time.Since(started)):
		}
		buf := make([]byte, 4<<20)
		n := runtime.Stack(buf, true)
		for _, block := range strings.Split(string(buf[:n]), "\n\n") {
			if !strings.Contains(block, "(*StorageExecutor).Execute") {
				continue
			}
			lines := strings.Split(block, "\n")
			if len(lines) > 41 {
				lines = lines[:41]
			}
			t.Logf("L2CAUSE E20-R1 stack at %v:\n%s", at, strings.Join(lines, "\n"))
		}
	}
	select {
	case elapsed := <-done:
		t.Logf("L2CAUSE E20-R1 W call returned after %v", elapsed)
	case <-time.After(120 * time.Second):
		t.Logf("L2CAUSE E20-R1 W call still running after 120 s")
	}
}
