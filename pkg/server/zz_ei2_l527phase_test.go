package server

// Batch 2 L527 phase probe (research only, untracked; same text on both
// builds): a 2,000-node auto-commit DELETE is dropped by its client at a
// fraction of its own duration (HTTP request deadline; Bolt session deadline;
// Bolt hard drop by closing the TCP connection through a proxy). A sampler
// takes a goroutine dump every 3 ms and classifies where the statement is
// (executing, committing, gone), so each outcome is tied to the phase the
// statement was in when its client left.

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

type zzei2Proxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func zzei2StartProxy(t *testing.T, target string) *zzei2Proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &zzei2Proxy{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(u, c); _ = u.Close() }()
			go func() { _, _ = io.Copy(c, u); _ = c.Close() }()
		}
	}()
	t.Cleanup(func() { _ = ln.Close(); p.drop() })
	return p
}

func (p *zzei2Proxy) drop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// zzei2Phase classifies the statement goroutines of one dump.
func zzei2Phase() (string, string) {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	phase, top := "gone", ""
	for _, g := range strings.Split(b.String(), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/cypher.") || strings.Contains(g, "zz_ei2_l527phase") {
			continue
		}
		var frames []string
		committing := false
		for _, line := range strings.Split(g, "\n") {
			l := strings.TrimSpace(line)
			if !strings.HasPrefix(l, "github.com/orneryd/nornicdb/pkg/") {
				continue
			}
			fn := l
			if i := strings.Index(fn, "("); i > 0 {
				fn = fn[:i]
			}
			fn = strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/pkg/")
			if len(frames) < 6 {
				frames = append(frames, fn)
			}
			lower := strings.ToLower(fn)
			inEngine := strings.HasPrefix(fn, "cypher.") || strings.HasPrefix(fn, "storage.") || strings.HasPrefix(fn, "txsession.") || strings.HasPrefix(fn, "txphase.")
			if inEngine && strings.Contains(lower, "commit") && !strings.Contains(lower, "autocommit") {
				committing = true
			}
		}
		if committing {
			return "committing", strings.Join(frames, " < ")
		}
		phase, top = "executing", strings.Join(frames, " < ")
	}
	return phase, top
}

type zzei2Sample struct {
	at    time.Duration
	phase string
	top   string
}

func TestZZEI2L527Phase(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	const nodes = 2000
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	fractions := []float64{0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1}
	for _, route := range []string{"HTTP-client-deadline", "Bolt-session-deadline", "Bolt-tcp-drop"} {
		t.Run(route, func(t *testing.T) {
			env := zzei2Start(t)
			target := env.driver.Target()
			seed := func() {
				out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
				require.Empty(t, out.Err)
			}
			count := func() string { return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String() }
			settle := func() string {
				last, settled, stable := "", "", 0
				for i := 0; i < 80 && stable < 4; i++ {
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
			bolt := func(ctx context.Context, uri string) error {
				d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
				if err != nil {
					return err
				}
				defer d.Close(context.Background())
				s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				defer s.Close(context.Background())
				r, err := s.Run(ctx, q, nil)
				if err == nil {
					_, err = r.Consume(ctx)
				}
				return err
			}
			run := func(ctx context.Context, proxy *zzei2Proxy) error {
				switch route {
				case "HTTP-client-deadline":
					status, out, err := env.post(ctx, "/db/nornic/tx/commit", q, nil)
					if err == nil && (status != 200 || len(out.Errors) > 0) {
						err = fmt.Errorf("HTTP %d %v", status, out.Errors)
					}
					return err
				case "Bolt-tcp-drop":
					return bolt(ctx, "bolt://"+proxy.ln.Addr().String())
				default:
					return bolt(ctx, "bolt://"+target.Host)
				}
			}
			var durations []time.Duration
			for i := 0; i < 3; i++ {
				seed()
				p := zzei2StartProxy(t, target.Host)
				began := time.Now()
				require.NoError(t, run(context.Background(), p))
				durations = append(durations, time.Since(began))
				require.Equal(t, "[[0]]", settle())
			}
			base := max(durations[0], durations[1], durations[2])
			t.Logf("ZZEI2 L527p %s: %d nodes, unconstrained %v (base %v)", route, nodes, durations, base)
			committedPhases := map[string]int{}
			for _, f := range fractions {
				seed()
				proxy := zzei2StartProxy(t, target.Host)
				cut := time.Duration(float64(base) * f)
				ctx, cancel := context.WithCancel(context.Background())
				if route != "Bolt-tcp-drop" {
					ctx, cancel = context.WithTimeout(context.Background(), cut)
				}
				var samples []zzei2Sample
				var smu sync.Mutex
				stop := make(chan struct{})
				started := time.Now()
				go func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						ph, top := zzei2Phase()
						smu.Lock()
						samples = append(samples, zzei2Sample{time.Since(started), ph, top})
						smu.Unlock()
						time.Sleep(3 * time.Millisecond)
					}
				}()
				done := make(chan error, 1)
				go func() { done <- run(ctx, proxy) }()
				if route == "Bolt-tcp-drop" {
					time.Sleep(cut)
					proxy.drop()
				}
				var clientErr error
				select {
				case clientErr = <-done:
				case <-time.After(base + 20*time.Second):
					clientErr = fmt.Errorf("client still waiting")
				}
				returned := time.Since(started)
				time.Sleep(base + 300*time.Millisecond)
				close(stop)
				cancel()
				settled := settle()
				committed := settled == "[[0]]"
				smu.Lock()
				before, after := zzei2Sample{phase: "none"}, zzei2Sample{phase: "none"}
				var lastBusy zzei2Sample
				for _, s := range samples {
					if s.at <= cut {
						before = s
					} else if after.phase == "none" {
						after = s
					}
					if s.phase != "gone" {
						lastBusy = s
					}
				}
				smu.Unlock()
				if committed && clientErr != nil {
					committedPhases[before.phase]++
				}
				t.Logf("ZZEI2 L527p %s f=%.1f cut=%v: phase before cut=%s (%v) [%s]; first after cut=%s (%v) [%s]; statement last seen %v (%s); client returned %v err=%v; committed=%v",
					route, f, cut.Round(time.Millisecond), before.phase, before.at.Round(time.Millisecond), before.top, after.phase, after.at.Round(time.Millisecond), after.top,
					lastBusy.at.Round(time.Millisecond), lastBusy.phase, returned.Round(time.Millisecond), clientErr != nil, committed)
			}
			t.Logf("ZZEI2 L527p %s SUMMARY error-but-committed by phase at the cut: %v", route, committedPhases)
		})
	}
}

func TestZZEI2L527PhaseAtCut(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	const nodes = 2000
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	fractions := []float64{0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4}
	for _, route := range []string{"HTTP-client-deadline", "Bolt-session-deadline", "Bolt-tcp-drop"} {
		t.Run(route, func(t *testing.T) {
			env := zzei2Start(t)
			target := env.driver.Target()
			seed := func() {
				out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
				require.Empty(t, out.Err)
			}
			count := func() string { return env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String() }
			settle := func() string {
				last, settled, stable := "", "", 0
				for i := 0; i < 80 && stable < 4; i++ {
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
			bolt := func(ctx context.Context, uri string) error {
				d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
				if err != nil {
					return err
				}
				defer d.Close(context.Background())
				s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				defer s.Close(context.Background())
				r, err := s.Run(ctx, q, nil)
				if err == nil {
					_, err = r.Consume(ctx)
				}
				return err
			}
			run := func(ctx context.Context, proxy *zzei2Proxy) error {
				switch route {
				case "HTTP-client-deadline":
					status, out, err := env.post(ctx, "/db/nornic/tx/commit", q, nil)
					if err == nil && (status != 200 || len(out.Errors) > 0) {
						err = fmt.Errorf("HTTP %d %v", status, out.Errors)
					}
					return err
				case "Bolt-tcp-drop":
					return bolt(ctx, "bolt://"+proxy.ln.Addr().String())
				default:
					return bolt(ctx, "bolt://"+target.Host)
				}
			}
			var durations []time.Duration
			for i := 0; i < 3; i++ {
				seed()
				p := zzei2StartProxy(t, target.Host)
				began := time.Now()
				require.NoError(t, run(context.Background(), p))
				durations = append(durations, time.Since(began))
				require.Equal(t, "[[0]]", settle())
			}
			base := max(durations[0], durations[1], durations[2])
			t.Logf("ZZEI2 L527c %s: %d nodes, unconstrained %v (base %v)", route, nodes, durations, base)
			committedPhases := map[string]int{}
			for _, f := range fractions {
				seed()
				proxy := zzei2StartProxy(t, target.Host)
				cut := time.Duration(float64(base) * f)
				ctx, cancel := context.WithCancel(context.Background())
				if route != "Bolt-tcp-drop" {
					ctx, cancel = context.WithTimeout(context.Background(), cut)
				}
				started := time.Now()
				done := make(chan error, 1)
				go func() { done <- run(ctx, proxy) }()
				time.Sleep(cut - time.Since(started))
				if route == "Bolt-tcp-drop" {
					proxy.drop()
				}
				atCut, atTop := zzei2Phase()
				atWhen := time.Since(started)
				var clientErr error
				select {
				case clientErr = <-done:
				case <-time.After(base + 20*time.Second):
					clientErr = fmt.Errorf("client still waiting")
				}
				returned := time.Since(started)
				time.Sleep(base + 300*time.Millisecond)
				cancel()
				settled := settle()
				committed := settled == "[[0]]"
				before := zzei2Sample{at: atWhen, phase: atCut, top: atTop}
				if committed && clientErr != nil {
					committedPhases[before.phase]++
				}
				t.Logf("ZZEI2 L527c %s f=%.1f cut=%v: phase at the cut=%s (dump at %v) [%s]; client returned %v err=%v; committed=%v",
					route, f, cut.Round(time.Millisecond), before.phase, before.at.Round(time.Millisecond), before.top, returned.Round(time.Millisecond), clientErr, committed)
			}
			t.Logf("ZZEI2 L527c %s SUMMARY error-but-committed by phase at the cut: %v", route, committedPhases)
		})
	}
}
