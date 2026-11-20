package server

// Batch-3b probe, packet K row 72 (research only, untracked; same text on the
// final and the previous build). An HTTP auto-commit DELETE under fixed small
// server transaction timeouts (NORNICDB_HTTP_TX_TIMEOUT): for each request the
// phase the statement is in when the timeout passes (executing / committing /
// gone, from goroutine dumps), the answer and its time, and the node count
// right after the answer and once settled. Fresh server and seed per request.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type zz3bSample struct {
	at    time.Duration
	phase string
}

func TestZZ3BServerTimeoutPhase(t *testing.T) {
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	plan := []struct {
		nodes  int
		limits []time.Duration
	}{
		{2000, []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, time.Second}},
		{60000, []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}},
	}
	const reps = 3
	summary := map[string]int{}
	for _, p := range plan {
		nodes := p.nodes
		seed := func(env *zz3bEnv) {
			out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
			require.Empty(t, out.Err)
		}
		count := func(env *zz3bEnv) string {
			return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		}
		settle := func(env *zz3bEnv) string {
			last, settled, stable := "", "", 0
			for i := 0; i < 80 && stable < 4; i++ {
				time.Sleep(150 * time.Millisecond)
				settled = count(env)
				if settled == last {
					stable++
				} else {
					stable = 0
				}
				last = settled
			}
			return settled
		}
		t.Run(fmt.Sprintf("unconstrained/%d", nodes), func(t *testing.T) {
			t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
			env := zz3bStart(t)
			var ds []string
			for i := 0; i < 3; i++ {
				seed(env)
				start := time.Now()
				status, out, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
				d := time.Since(start)
				require.NoError(t, err)
				ds = append(ds, fmt.Sprintf("%v(%d %s)", d.Round(time.Millisecond), status, zz3bHTTPOutcome(status, out, err)))
				require.Equal(t, "[[0]]", settle(env))
			}
			t.Logf("ZZ3B K72 nodes=%d unconstrained durations: %s", nodes, strings.Join(ds, " "))
		})
		for _, limit := range p.limits {
			for rep := 1; rep <= reps; rep++ {
				name := fmt.Sprintf("timeout/%d/%v/%d", nodes, limit, rep)
				t.Run(name, func(t *testing.T) {
					t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", limit.String())
					env := zz3bStart(t)
					seed(env)
					var mu sync.Mutex
					var samples []zz3bSample
					stop := make(chan struct{})
					start := time.Now()
					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						tick := time.NewTicker(20 * time.Millisecond)
						defer tick.Stop()
						for {
							select {
							case <-stop:
								return
							case <-tick.C:
								ph, _ := zz3bPhase()
								mu.Lock()
								samples = append(samples, zz3bSample{time.Since(start), ph})
								mu.Unlock()
							}
						}
					}()
					atLimit := make(chan string, 1)
					go func() {
						defer wg.Done()
						select {
						case <-stop:
							atLimit <- "answered before the limit"
						case <-time.After(limit):
							ph, top := zz3bPhase()
							atLimit <- fmt.Sprintf("%s at %v [%s]", ph, time.Since(start).Round(time.Millisecond), top)
						}
					}()
					status, out, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
					answered := time.Since(start)
					close(stop)
					wg.Wait()
					right := count(env)
					settled := settle(env)
					mu.Lock()
					firstCommit, lastSeen := time.Duration(-1), time.Duration(-1)
					for _, s := range samples {
						if s.phase == "committing" && firstCommit < 0 {
							firstCommit = s.at
						}
						if s.phase != "gone" {
							lastSeen = s.at
						}
					}
					mu.Unlock()
					deleted := settled == "[[0]]"
					success := err == nil && status == 200 && len(out.Errors) == 0
					verdict := "consistent"
					switch {
					case settled != "[[0]]" && settled != fmt.Sprintf("[[%d]]", nodes):
						verdict = "PARTIAL"
					case deleted && !success:
						verdict = "ERROR-BUT-COMMITTED"
					case !deleted && success:
						verdict = "SUCCESS-BUT-NOT-DELETED"
					}
					late := ""
					if answered > limit {
						late = fmt.Sprintf(" LATE by %v (%.2fx limit)", (answered - limit).Round(time.Millisecond), float64(answered)/float64(limit))
					}
					key := fmt.Sprintf("nodes=%d limit=%v status=%d deleted=%v", nodes, limit, status, deleted)
					summary[key]++
					t.Logf("ZZ3B K72 %s: status=%d answered after %v%s response=%s; phase when the limit passed: %s; first commit sample %v; statement last seen %v; count right after=%s settled=%s; %s",
						name, status, answered.Round(time.Millisecond), late, zz3bHTTPOutcome(status, out, err), <-atLimit,
						firstCommit.Round(time.Millisecond), lastSeen.Round(time.Millisecond), right, settled, verdict)
				})
			}
		}
	}
	keys := make([]string, 0, len(summary))
	for k := range summary {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("ZZ3B K72 summary %s: %d", k, summary[k])
	}
}
