package server

// Batch-3b probe (research only, untracked; same text on both builds): the
// row-72 server and 60,000-node HTTP DELETE, repeated, with every second:
// Badger's len(oracle.committedTxns) and discardTs; NornicDB's commitOracle
// published, discardTs, nextTs (commit rate), inflight count and the oldest
// open reads (readers map, read under the oracle's own mutex); the machine's
// load. When Badger retains more than 5,000 committed transactions during a
// delete, one full goroutine dump of the goroutines in NornicDB storage code
// is printed, to see which goroutines have reads or transactions open.
// TestZZ3BServerDeleteHolder runs on a Go worker; BenchmarkZZ3BServerDelete
// is the same body for the lab's bench slots (tnrun ab).

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type zz3bOracleState struct {
	badgerRetained              int
	badgerDiscard               uint64
	published, discard, nextTs  uint64
	inflight                    int
	oldest                      string
}

func zz3bReadOracles(e *storage.BadgerEngine) zz3bOracleState {
	var st zz3bOracleState
	managed := zz3bSField(reflect.ValueOf(e).Elem(), "db")
	if managed.IsNil() {
		return st
	}
	bdb := zz3bSField(managed.Elem(), "DB")
	orc := zz3bSField(bdb.Elem(), "orc")
	st.badgerRetained = zz3bSField(orc.Elem(), "committedTxns").Len()
	st.badgerDiscard = zz3bSField(orc.Elem(), "discardTs").Uint()
	co := zz3bSField(managed.Elem(), "oracle")
	if co.IsNil() {
		return st
	}
	c := co.Elem()
	st.published = zz3bSField(zz3bSField(c, "published"), "v").Uint()
	mu := zz3bSField(c, "mu").Addr().Interface().(*sync.Mutex)
	mu.Lock()
	st.discard = zz3bSField(c, "discardTs").Uint()
	st.nextTs = zz3bSField(c, "nextTs").Uint()
	st.inflight = zz3bSField(c, "inflight").Len()
	type rc struct {
		ts uint64
		n  int64
	}
	var rs []rc
	for it := zz3bSField(c, "readers").MapRange(); it.Next(); {
		rs = append(rs, rc{it.Key().Uint(), it.Value().Int()})
	}
	mu.Unlock()
	sort.Slice(rs, func(i, j int) bool { return rs[i].ts < rs[j].ts })
	var parts []string
	for i, r := range rs {
		if i == 4 {
			parts = append(parts, fmt.Sprintf("...(%d read ts)", len(rs)))
			break
		}
		parts = append(parts, fmt.Sprintf("%d×%d", r.ts, r.n))
	}
	st.oldest = strings.Join(parts, ",")
	return st
}

func (s zz3bOracleState) String() string {
	return fmt.Sprintf("badger retained=%d discard=%d | nornic published=%d discard=%d nextTs=%d inflight=%d reads=[%s]",
		s.badgerRetained, s.badgerDiscard, s.published, s.discard, s.nextTs, s.inflight, s.oldest)
}

func zz3bStorageGoroutines() string {
	var b strings.Builder
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	var keep []string
	for _, g := range strings.Split(b.String(), "\n\n") {
		if !strings.Contains(g, "nornicdb/pkg/storage") || strings.Contains(g, "zz_3b_holder") {
			continue
		}
		lines := strings.Split(g, "\n")
		if len(lines) > 80 {
			lines = append(lines[:80], "\t... (truncated)")
		}
		keep = append(keep, strings.Join(lines, "\n"))
	}
	return strings.Join(keep, "\n\n")
}

func zz3bHolderRuns(tb testing.TB, runs int, logf func(string, ...any)) {
	tb.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	srv, authenticator := setupTestServer(tb)
	zz3bHolderRunsOn(tb, srv, authenticator, runs, logf)
}

// zz3bHolderRunsOn runs the deletes on a given server. After each answer the
// node count is read until it is 0 (or 120 s pass), since with asynchronous
// writes the answer can come before the delete is applied.
func zz3bHolderRunsOn(tb testing.TB, srv *Server, authenticator *auth.Authenticator, runs int, logf func(string, ...any)) {
	const nodes = 60000
	const q = "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted"
	peer := httptest.NewServer(srv.buildRouter())
	tb.Cleanup(peer.Close)
	token := getAuthToken(tb, authenticator, "admin")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	tb.Cleanup(func() { _ = bs.Close(); <-done })
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = driver.Close(context.Background()) })
	tt, _ := tb.(*testing.T)
	env := &zz3bEnv{t: tt, peer: peer, token: token, driver: driver}
	engines := zz3bFindEngines(srv)
	if len(engines) != 1 {
		tb.Fatalf("engines reachable: %d", len(engines))
	}
	e := engines[0]
	logf("ZZ3B HOLDER start GOMAXPROCS=%d NumCPU=%d loadavg=%s; %s", runtime.GOMAXPROCS(0), runtime.NumCPU(), zz3bLoadavg(), zz3bReadOracles(e))
	for run := 1; run <= runs; run++ {
		out := env.run("Bolt-auto", "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]any{"n": nodes})
		if out.Err != "" {
			tb.Fatal(out.Err)
		}
		before := zz3bReadOracles(e)
		stop := make(chan struct{})
		var samples []string
		var dump string
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
					samples = append(samples, fmt.Sprintf("%ds: %s load=%s", int(time.Since(start).Round(time.Second)/time.Second), s, strings.Fields(zz3bLoadavg()+" x")[0]))
					if dump == "" && s.badgerRetained > 5000 {
						dump = fmt.Sprintf("at %v with %s:\n%s", time.Since(start).Round(time.Millisecond), s, zz3bStorageGoroutines())
					}
				}
			}
		}()
		cpu0 := zz3bCPU()
		status, resp, err := env.post(context.Background(), "/db/nornic/tx/commit", q, nil)
		wall := time.Since(start)
		cpu := zz3bCPU() - cpu0
		close(stop)
		wg.Wait()
		after := zz3bReadOracles(e)
		left := env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		gone := time.Duration(-1)
		for left != "[[0]]" && time.Since(start) < wall+120*time.Second {
			time.Sleep(100 * time.Millisecond)
			left = env.run("Bolt-auto", "MATCH (n:ZZDel) RETURN count(n) AS c", nil).String()
		}
		if left == "[[0]]" {
			gone = time.Since(start)
		}
		logf("ZZ3B HOLDER run=%d wall=%v gone=%v CPU=%v (%.2f cores) status=%d %s left=%s\n  before: %s\n  after:  %s\n  samples:\n    %s",
			run, wall.Round(time.Millisecond), gone.Round(time.Millisecond), cpu.Round(time.Millisecond), float64(cpu)/float64(wall), status, zz3bHTTPOutcome(status, resp, err), left,
			before, after, strings.Join(samples, "\n    "))
		if dump != "" {
			logf("ZZ3B HOLDER run=%d storage goroutines when Badger retained > 5000 %s", run, dump)
		}
	}
}

func TestZZ3BServerDeleteHolder(t *testing.T) {
	runs := 20
	if os.Getenv("ZZ3B_RUNS") != "" {
		fmt.Sscan(os.Getenv("ZZ3B_RUNS"), &runs)
	}
	zz3bHolderRuns(t, runs, t.Logf)
}

// BenchmarkZZ3BServerDelete is the same body for tnrun ab (bench slots), so the
// delete can be measured on the lab's dedicated bench cores; run with
// -benchtime 1x.
func BenchmarkZZ3BServerDelete(b *testing.B) {
	runs := 10
	if os.Getenv("ZZ3B_RUNS") != "" {
		fmt.Sscan(os.Getenv("ZZ3B_RUNS"), &runs)
	}
	for i := 0; i < b.N; i++ {
		zz3bHolderRuns(b, runs, b.Logf)
	}
}
