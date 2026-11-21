package server

// Batch 2 L527 cause probe (research only, untracked; same text on both
// builds): a client drops a running auto-commit DELETE at 30 % of the delete's
// own duration, over HTTP (request context deadline) and over Bolt (session
// context deadline; driver closed). Right after the drop and twice later the
// probe dumps every goroutine that runs Cypher code, so the stack shows where
// the statement is while its client is gone, and then whether it committed.

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

func zzei2CypherGoroutines() []string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	var out []string
	for _, g := range strings.Split(b.String(), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/cypher.") {
			continue
		}
		lines := strings.Split(g, "\n")
		if len(lines) > 90 {
			lines = append(lines[:90], "\t... (truncated)")
		}
		out = append(out, strings.Join(lines, "\n"))
	}
	return out
}

func TestZZEI2L527CancelTrace(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	const nodes = 20000
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	for _, route := range []string{"HTTP-client-deadline", "Bolt-session-deadline", "Bolt-driver-closed"} {
		t.Run(route, func(t *testing.T) {
			env := zzei2Start(t)
			seed := func() {
				out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
				require.Empty(t, out.Err)
			}
			count := func() string { return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String() }
			settle := func() string {
				last, settled, stable := "", "", 0
				for i := 0; i < 120 && stable < 4; i++ {
					time.Sleep(250 * time.Millisecond)
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
			// run issues the delete on this route; drop (non-nil) is called to end the client side.
			run := func(ctx context.Context) error {
				switch route {
				case "HTTP-client-deadline":
					status, out, err := env.post(ctx, "/db/nornic/tx/commit", q, nil)
					if err == nil && (status != 200 || len(out.Errors) > 0) {
						err = fmt.Errorf("HTTP %d %v", status, out.Errors)
					}
					return err
				default:
					driver := env.driver
					if route == "Bolt-driver-closed" {
						if d, ok := ctx.Value(zzei2DriverKey{}).(neo4j.DriverWithContext); ok {
							driver = d
						}
					}
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
					defer session.Close(context.Background())
					result, err := session.Run(ctx, q, nil)
					if err == nil {
						_, err = result.Consume(ctx)
					}
					return err
				}
			}
			// Unconstrained duration on this route.
			seed()
			began := time.Now()
			require.NoError(t, run(context.Background()))
			base := time.Since(began)
			require.Equal(t, "[[0]]", settle())
			cut := time.Duration(float64(base) * 0.3)
			t.Logf("ZZEI2 L527t %s: %d nodes, unconstrained delete %v, client drops at %v", route, nodes, base.Round(time.Millisecond), cut.Round(time.Millisecond))

			seed()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var dropDriver neo4j.DriverWithContext
			if route == "Bolt-driver-closed" {
				target := env.driver.Target()
				d, err := neo4j.NewDriverWithContext("bolt://"+target.Host, neo4j.NoAuth())
				require.NoError(t, err)
				dropDriver = d
				ctx = context.WithValue(ctx, zzei2DriverKey{}, d)
			}
			if route != "Bolt-driver-closed" {
				var c2 context.CancelFunc
				ctx, c2 = context.WithTimeout(ctx, cut)
				defer c2()
			}
			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- run(ctx) }()
			if dropDriver != nil {
				time.Sleep(cut)
				_ = dropDriver.Close(context.Background())
			}
			var clientErr error
			select {
			case clientErr = <-done:
			case <-time.After(base + 30*time.Second):
				clientErr = fmt.Errorf("client still waiting")
			}
			dropped := time.Since(started)
			right := count()
			for i, after := range []time.Duration{10 * time.Millisecond, base / 4, base / 2} {
				time.Sleep(after)
				gs := zzei2CypherGoroutines()
				t.Logf("ZZEI2 L527t %s: dump %d (%v after the client returned): %d goroutines in Cypher code", route, i+1, time.Since(started)-dropped, len(gs))
				for j, g := range gs {
					if j >= 3 {
						break
					}
					t.Logf("ZZEI2 L527t %s: dump %d goroutine %d:\n%s", route, i+1, j+1, g)
				}
			}
			settled := settle()
			t.Logf("ZZEI2 L527t %s: client returned after %v with err=%v; count right after=%s; settled=%s; deletions committed=%v",
				route, dropped.Round(time.Millisecond), clientErr, right, settled, settled == "[[0]]")
		})
	}
}

type zzei2DriverKey struct{}
