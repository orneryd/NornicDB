package server

// Batch-3b probe (research only, untracked; same text on both builds): the
// 60,000-node HTTP DELETE repeated on ONE server whose database configuration
// is nornicdb.DefaultConfig() with only Database.MVCCRetentionMaxVersions
// changed (ZZ3B_RETENTION, default 0 = the default configuration). Server:
// DefaultConfig, Port 0, EmbeddingEnabled false (no embedder in the lab),
// authentication on. ZZ3B_RUNS deletes (default 60).
// A 100 ms sampler runs for the whole test: whether any goroutine is inside
// PruneMVCCVersions (one contiguous stretch = one prune cycle) or the delete
// cleanup, and NornicDB's published commit counter, so each prune cycle's
// commits and each delete's commits are counted. Per delete, per second:
// Badger retained/discard, NornicDB discard and open reads. On the final
// build a delete over 25 s gets its CPU profile summarised (the previous
// build's module has no profile parser).

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type zz3bSpan struct {
	start, end       time.Time
	pubStart, pubEnd uint64
}

type zz3bActivity struct {
	mu        sync.Mutex
	prune     []zz3bSpan
	cleanup   []zz3bSpan
	inPrune   bool
	inCleanup bool
}

func (a *zz3bActivity) sample(e *storage.BadgerEngine) {
	var b bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
	text := b.String()
	prune := strings.Contains(text, "PruneMVCCVersions")
	cleanup := strings.Contains(text, "compactDeletedEntries") || strings.Contains(text, "runDeleteCleanup")
	pub := zz3bReadOracles(e).published
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	track := func(on bool, in *bool, spans *[]zz3bSpan) {
		if on && !*in {
			*spans = append(*spans, zz3bSpan{start: now, pubStart: pub})
		}
		if on || *in {
			last := &(*spans)[len(*spans)-1]
			last.end, last.pubEnd = now, pub
		}
		*in = on
	}
	track(prune, &a.inPrune, &a.prune)
	track(cleanup, &a.inCleanup, &a.cleanup)
}

func zz3bOverlap(from, to time.Time, spans []zz3bSpan) (n int, commits uint64, desc []string) {
	for _, s := range spans {
		if s.end.Before(from) || s.start.After(to) {
			continue
		}
		n++
		commits += s.pubEnd - s.pubStart
		desc = append(desc, fmt.Sprintf("[%+.1fs..%+.1fs commits=%d]", s.start.Sub(from).Seconds(), s.end.Sub(from).Seconds(), s.pubEnd-s.pubStart))
	}
	return
}

func zz3bCauseServer(tb testing.TB, retention int) (*Server, *auth.Authenticator) {
	tmpDir, err := os.MkdirTemp("", "zz3b-cause-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { os.RemoveAll(tmpDir) })
	config := nornicdb.DefaultConfig()
	config.Database.MVCCRetentionMaxVersions = retention
	tb.Logf("ZZ3B CAUSE database config: MVCCRetentionMaxVersions=%d MVCCRetentionTTL=%v MVCCLifecycleEnabled=%v MVCCLifecycleCycleInterval=%v AsyncWritesEnabled=%v DecayEnabled=%v AutoLinksEnabled=%v SearchCursorMax=%d",
		config.Database.MVCCRetentionMaxVersions, config.Database.MVCCRetentionTTL, config.Database.MVCCLifecycleEnabled, config.Database.MVCCLifecycleCycleInterval,
		config.Database.AsyncWritesEnabled, config.Memory.DecayEnabled, config.Memory.AutoLinksEnabled, config.Memory.SearchCursorMax)
	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		tb.Fatal(err)
	}
	if err := zz3bCreateAdmin(authenticator); err != nil {
		tb.Fatal(err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	srv, err := New(db, authenticator, serverConfig)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	return srv, authenticator
}

func TestZZ3BCause(t *testing.T) {
	runs, retention := 60, 0
	if v := os.Getenv("ZZ3B_RUNS"); v != "" {
		fmt.Sscan(v, &runs)
	}
	if v := os.Getenv("ZZ3B_RETENTION"); v != "" {
		fmt.Sscan(v, &retention)
	}
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	srv, authenticator := zz3bCauseServer(t, retention)
	const nodes = 60000
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	peer := httptest.NewServer(srv.buildRouter())
	t.Cleanup(peer.Close)
	token := getAuthToken(t, authenticator, "admin")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	t.Cleanup(func() { _ = bs.Close(); <-done })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	env := &zz3bEnv{t: t, peer: peer, token: token, driver: driver}
	engines := zz3bFindEngines(srv)
	if len(engines) != 1 {
		t.Fatalf("engines reachable: %d", len(engines))
	}
	e := engines[0]
	t.Logf("ZZ3B CAUSE start retention=%d runs=%d GOMAXPROCS=%d NumCPU=%d loadavg=%s lifecycle=%v %s", retention, runs, runtime.GOMAXPROCS(0), runtime.NumCPU(), zz3bLoadavg(), e.LifecycleStatus(), zz3bReadOracles(e))
	act := &zz3bActivity{}
	stopAll := make(chan struct{})
	var all sync.WaitGroup
	all.Add(1)
	testStart := time.Now()
	go func() {
		defer all.Done()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopAll:
				return
			case <-tick.C:
				act.sample(e)
			}
		}
	}()
	slow := 0
	var walls []string
	for run := 1; run <= runs; run++ {
		out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
		if out.Err != "" {
			t.Fatal(out.Err)
		}
		before := zz3bReadOracles(e)
		stop := make(chan struct{})
		var samples []string
		var wg sync.WaitGroup
		wg.Add(1)
		start := time.Now()
		go func() {
			defer wg.Done()
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					s := zz3bReadOracles(e)
					samples = append(samples, fmt.Sprintf("%ds retained=%d bdiscard=%d published=%d ndiscard=%d inflight=%d reads=[%s]", int(time.Since(start).Round(time.Second)/time.Second), s.badgerRetained, s.badgerDiscard, s.published, s.discard, s.inflight, s.oldest))
				}
			}
		}()
		var prof bytes.Buffer
		profiling := pprof.StartCPUProfile(&prof) == nil
		cpu0 := zz3bCPU()
		status, resp, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
		wall := time.Since(start)
		cpu := zz3bCPU() - cpu0
		if profiling {
			pprof.StopCPUProfile()
		}
		end := time.Now()
		close(stop)
		wg.Wait()
		after := zz3bReadOracles(e)
		left := env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		act.mu.Lock()
		np, pc, pd := zz3bOverlap(start, end, act.prune)
		nc, cc, cd := zz3bOverlap(start, end, act.cleanup)
		act.mu.Unlock()
		total := after.published - before.published
		t.Logf("ZZ3B CAUSE run=%d wall_s=%.3f cpu_s=%.3f cores=%.2f status=%d outcome=%v left=%s commits_during_delete=%d prune_cycles_overlapping=%d prune_commits_in_those_cycles=%d %s cleanup_spans=%d cleanup_span_commits=%d %s retained_before=%d retained_after=%d",
			run, wall.Seconds(), cpu.Seconds(), float64(cpu)/float64(wall), status, zz3bHTTPOutcome(status, resp, err), left, total, np, pc, strings.Join(pd, ""), nc, cc, strings.Join(cd, ""), before.badgerRetained, after.badgerRetained)
		walls = append(walls, fmt.Sprintf("%.1f", wall.Seconds()))
		if wall > 25*time.Second {
			slow++
			t.Logf("ZZ3B CAUSE run=%d SLOW samples:\n    %s", run, strings.Join(samples, "\n    "))
			if profiling {
				t.Logf("ZZ3B CAUSE run=%d SLOW cpu profile:\n%s", run, zz3bProfileSummary(prof.Bytes()))
			}
		}
	}
	close(stopAll)
	all.Wait()
	act.mu.Lock()
	var cyc []string
	for _, s := range act.prune {
		cyc = append(cyc, fmt.Sprintf("[%+.0fs %.1fs commits=%d]", s.start.Sub(testStart).Seconds(), s.end.Sub(s.start).Seconds(), s.pubEnd-s.pubStart))
	}
	t.Logf("ZZ3B CAUSE summary retention=%d deletes=%d slow(>25s)=%d walls_s=[%s] prune_cycles_seen_in_whole_test=%d over %.0fs: %s lifecycle_end=%v",
		retention, runs, slow, strings.Join(walls, " "), len(act.prune), time.Since(testStart).Seconds(), strings.Join(cyc, " "), e.LifecycleStatus())
	act.mu.Unlock()
}
