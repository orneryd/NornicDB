package server

// Batch 2 L527 rerun (research only, untracked): the deadline or client
// disconnect lands at fractions of the delete's own measured duration, so some
// runs land while the commit is in progress. Same text on both builds.

import (
	"context"
	"slices"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func TestZZEI2L527CommitWindow(t *testing.T) {
	zzei2L527Sweep(t, []int{2000, 60000}, []string{"server-timeout", "client-disconnect"})
}

// L527 rerun of the default parser's missing half: 60,000 nodes, server timeout only.
func TestZZEI2L527Server60k(t *testing.T) {
	zzei2L527Sweep(t, []int{60000}, []string{"server-timeout"})
}

var zzei2L527MedianBase bool

var zzei2L527FullStacks bool

// Client disconnects alone with full stacks of the statement at the cut.
func TestZZEI2L527DisconnectStacks(t *testing.T) {
	zzei2L527FullStacks = true
	defer func() { zzei2L527FullStacks = false }()
	zzei2L527Sweep(t, []int{2000}, []string{"client-disconnect"})
}

// The 60,000-node server-timeout half again, with the median unconstrained
// duration as the base (one outlier made the longest a poor base).
func TestZZEI2L527Server60kMedian(t *testing.T) {
	zzei2L527MedianBase = true
	defer func() { zzei2L527MedianBase = false }()
	zzei2L527Sweep(t, []int{60000}, []string{"server-timeout"})
}

// Client disconnects alone, and after the server-timeout loop (the
// arrangement of the first commit-window sweep), 2,000 nodes.
func TestZZEI2L527DisconnectAlone(t *testing.T) {
	zzei2L527Sweep(t, []int{2000}, []string{"client-disconnect"})
}

func TestZZEI2L527DisconnectAfterTimeouts(t *testing.T) {
	zzei2L527Sweep(t, []int{2000}, []string{"server-timeout", "client-disconnect"})
}

// Bolt: the client's session deadline drops the connection at the cut.
func TestZZEI2L527BoltDisconnectStacks(t *testing.T) {
	zzei2L527FullStacks = true
	defer func() { zzei2L527FullStacks = false }()
	zzei2L527Sweep(t, []int{2000}, []string{"bolt-disconnect"})
}

func zzei2L527Sweep(t *testing.T, sizes []int, modes []string) {
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	fractions := []float64{0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.2, 1.4}
	for _, nodes := range sizes {
		t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
		seedAndCount := func(env *zzei2Env) func() string {
			seed := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
			require.Empty(t, seed.Err)
			return func() string { return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String() }
		}
		settle := func(count func() string) string {
			last, settled, stable := "", "", 0
			for i := 0; i < 60 && stable < 4; i++ {
				time.Sleep(150 * time.Millisecond)
				settled = count()
				if settled == last {
					stable++
				} else {
					stable = 0
				}
				last = settled
			}
			return settled
		}
		// Unconstrained duration of the same delete (three runs, median-ish: middle value).
		var durations []time.Duration
		{
			t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
			env := zzei2Start(t)
			for i := 0; i < 3; i++ {
				count := seedAndCount(env)
				start := time.Now()
				status, out, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
				d := time.Since(start)
				require.NoError(t, err)
				require.Equal(t, 200, status, "%v", out.Errors)
				require.Equal(t, "[[0]]", settle(count))
				durations = append(durations, d)
			}
		}
		base := durations[0]
		for _, d := range durations[1:] {
			if d > base {
				base = d
			}
		}
		if zzei2L527MedianBase {
			sorted := append([]time.Duration(nil), durations...)
			slices.Sort(sorted)
			base = sorted[len(sorted)/2]
		}
		t.Logf("ZZEI2 L527b nodes=%d unconstrained HTTP auto-commit delete durations=%v (sweep base %v)", nodes, durations, base)
		bad := map[string]int{}
		for _, mode := range modes {
			var env *zzei2Env
			if mode == "client-disconnect" || mode == "bolt-disconnect" {
				t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
				env = zzei2Start(t)
			}
			for _, f := range fractions {
				limit := time.Duration(float64(base) * f)
				if limit < time.Millisecond {
					limit = time.Millisecond
				}
				name := fmt.Sprintf("%s/%d/%.2f", mode, nodes, f)
				if mode == "server-timeout" {
					t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", limit.String())
					env = zzei2Start(t)
				}
				count := seedAndCount(env)
				ctx := context.Background()
				cancel := context.CancelFunc(func() {})
				if mode == "client-disconnect" || mode == "bolt-disconnect" {
					ctx, cancel = context.WithTimeout(ctx, limit)
				}
				start := time.Now()
				phaseAtCut := make(chan string, 1)
				if mode == "client-disconnect" || mode == "bolt-disconnect" {
					go func() {
						time.Sleep(limit)
						ph, top := zzei2Phase()
						at := time.Since(start).Round(time.Millisecond)
						full := zzei2CypherGoroutines()
						time.Sleep(30 * time.Millisecond)
						later := zzei2CypherGoroutines()
						msg := fmt.Sprintf("%s at %v [%s]", ph, at, top)
						if zzei2L527FullStacks {
							for i, g := range full {
								msg += fmt.Sprintf("\n--- at the cut, goroutine %d:\n%s", i+1, g)
							}
							msg += fmt.Sprintf("\n--- 30 ms later: %d goroutines in Cypher code", len(later))
							for i, g := range later {
								if i >= 1 {
									break
								}
								msg += fmt.Sprintf("\n--- 30 ms later, goroutine %d:\n%s", i+1, g)
							}
						}
						phaseAtCut <- msg
					}()
				} else {
					phaseAtCut <- "n/a"
				}
				var status int
				var out zzei2HTTPResponse
				var err error
				if mode == "bolt-disconnect" {
					target := env.driver.Target()
					d, derr := neo4j.NewDriverWithContext("bolt://"+target.Host, neo4j.NoAuth())
					require.NoError(t, derr)
					session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
					result, rerr := session.Run(ctx, q, nil)
					if rerr == nil {
						_, rerr = result.Consume(ctx)
					}
					_ = session.Close(context.Background())
					_ = d.Close(context.Background())
					err = rerr
					if err == nil {
						status = 200
					}
				} else {
					status, out, err = env.post(ctx, "/db/nornic/tx/commit", q, nil)
				}
				cancel()
				elapsed := time.Since(start)
				immediately := count()
				settled := settle(count)
				deleted := settled == "[[0]]"
				partial := settled != "[[0]]" && settled != fmt.Sprintf("[[%d]]", nodes)
				success := err == nil && status == 200 && len(out.Errors) == 0
				verdict := "consistent"
				switch {
				case partial:
					verdict = "PARTIAL"
				case deleted && !success:
					verdict = "ERROR-BUT-COMMITTED"
				case !deleted && success:
					verdict = "SUCCESS-BUT-NOT-DELETED"
				}
				if verdict != "consistent" {
					bad[mode+" "+verdict]++
				}
				t.Logf("ZZEI2 L527b %s: phase at the cut: %s", name, <-phaseAtCut)
				t.Logf("ZZEI2 L527b %s: limit=%v status=%d after %v response=%s; count right after=%s; settled=%s; %s",
					name, limit.Round(time.Millisecond), status, elapsed.Round(time.Millisecond), zzei2HTTPOutcome(status, out, err), immediately, settled, verdict)
				if mode == "client-disconnect" || mode == "bolt-disconnect" {
					// clean up remaining nodes for the next fraction
					env.run("Bolt-auto", "MATCH (n:ZZDel) DETACH DELETE n", nil)
					settle(count)
				}
			}
		}
		t.Logf("ZZEI2 L527b nodes=%d SUMMARY inconsistent outcomes: %v", nodes, bad)
	}
}
