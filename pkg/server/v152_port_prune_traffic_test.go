package server

// Synthetic prune and concurrent protocol traffic probe. The client report: after ONE HTTP
// transaction statement runs past the HTTP tx timeout and gets a 503, every
// later HTTP tx statement (even RETURN 1, SHOW TRANSACTIONS) hangs until 503.
// This runs the reported sequence end to end on real HTTP and Bolt listeners,
// with the MVCC lifecycle loop running on its own ticker (history retention
// on, as deployed), and a short HTTP tx timeout. Modes, on one seeded store:
//   explicit: BEGIN on /db/nornic/tx, slow read on /tx/{id} (503), then probes
//   auto:     slow read on /db/nornic/tx/commit (503), then probes
//   noslow:   no slow statement at all, the same probes (control)
//   lcoff:    slow read on /tx/{id} with the lifecycle loop paused (control)

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type hl3Probe struct {
	name string
	run  func() string
}

// hl3Count sums goroutines (debug=1 groups) whose stack contains a marker.
func hl3Count(dump string, markers ...string) int {
	total := 0
	for _, block := range strings.Split(dump, "\n\n") {
		hit := false
		for _, m := range markers {
			if strings.Contains(block, m) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(block, "%d @", &n); err == nil {
			total += n
		} else {
			total++
		}
	}
	return total
}

func hl3Badger(engine storage.Engine) *storage.BadgerEngine {
	for i := 0; i < 8 && engine != nil; i++ {
		if b, ok := engine.(*storage.BadgerEngine); ok {
			return b
		}
		inner, ok := engine.(interface{ GetInnerEngine() storage.Engine })
		if !ok {
			return nil
		}
		engine = inner.GetInnerEngine()
	}
	return nil
}

func hl3Stacks(debug int) string {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, debug)
	return buf.String()
}

// hl3Routes groups every goroutine that is inside a NornicDB frame and parked
// (not running) by its blocking state and NornicDB call chain.
func hl3Routes(label string, full bool) {
	blocks := strings.Split(hl3Stacks(2), "\n\n")
	routes := map[string]int{}
	example := map[string]string{}
	for _, block := range blocks {
		if !strings.Contains(block, "orneryd/nornicdb/pkg/") {
			continue
		}
		lines := strings.Split(block, "\n")
		header := lines[0]
		state := header
		if open := strings.Index(header, "["); open >= 0 {
			state = strings.TrimSuffix(header[open+1:], "]:")
			if comma := strings.Index(state, ","); comma >= 0 {
				state = state[:comma]
			}
		}
		var chain []string
		for i := 1; i+1 < len(lines); i++ {
			fn := strings.TrimSpace(lines[i])
			if !strings.HasPrefix(fn, "github.com/orneryd/nornicdb/pkg/") {
				continue
			}
			fn = strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/pkg/")
			if p := strings.LastIndex(fn, "("); p > 0 {
				fn = fn[:p]
			}
			where := strings.TrimSpace(lines[i+1])
			if slash := strings.LastIndex(where, "/"); slash >= 0 {
				where = where[slash+1:]
			}
			if sp := strings.Index(where, " "); sp >= 0 {
				where = where[:sp]
			}
			chain = append(chain, fn+"@"+where)
			if len(chain) >= 9 {
				break
			}
		}
		key := "[" + state + "] " + strings.Join(chain, " <- ")
		routes[key]++
		if _, ok := example[key]; !ok {
			example[key] = block
		}
	}
	var keys []string
	for k := range routes {
		if strings.Contains(k, "zz_hl3_") || strings.Contains(k, "badger/v4") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(os.Stdout, "\n===HL3_ROUTES_BEGIN %s\n", label)
	for _, k := range keys {
		fmt.Fprintf(os.Stdout, "ROUTE n=%d %s\n", routes[k], k)
	}
	if full {
		for _, k := range keys {
			if strings.Contains(k, "acquireSnapshotReader") || strings.Contains(k, "PruneMVCCVersions") || strings.Contains(k, "Authenticat") || strings.Contains(k, "CurrentClaims") || strings.Contains(k, "txsession") || strings.Contains(k, "Mutex") {
				fmt.Fprintf(os.Stdout, "--- example for n=%d %s\n%s\n", routes[k], k, example[k])
			}
		}
	}
	fmt.Fprintf(os.Stdout, "===HL3_ROUTES_END %s\n\n", label)
}

func hl3Clip(text string, limit int) string {
	text = strings.ReplaceAll(text, "\n", " ")
	if len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

func hl3Query() string {
	var q strings.Builder
	for i := 0; i < 8; i++ {
		fmt.Fprintf(&q, "OPTIONAL MATCH (x%d:LockupDoc) WHERE x%d.document_id IN $chunk WITH ", i, i)
		for j := 0; j < i; j++ {
			fmt.Fprintf(&q, "a%d, ", j)
		}
		fmt.Fprintf(&q, "collect(x%d) AS a%d ", i, i)
	}
	q.WriteString("RETURN size(a0) AS s0, size(a7) AS s7")
	return q.String()
}

func hl3Chunk() []interface{} {
	chunk := make([]interface{}, 0, 7401)
	for i := 0; i < 7400; i++ {
		chunk = append(chunk, fmt.Sprintf("doc-%d", i*37))
	}
	return append(chunk, fmt.Sprintf("nonce-%d", time.Now().UnixNano()))
}

func runV152PortPruneTraffic(t *testing.T) {
	testStart := time.Now()
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "5s")
	s, authn := hl3Setup(t, func(c *nornicdb.Config) {
		c.Database.MVCCLifecycleEnabled = true
		c.Database.MVCCLifecycleCycleInterval = 2 * time.Second
		c.Database.MVCCRetentionMaxVersions = 100
	})
	since := func() string { return time.Since(testStart).Round(time.Millisecond).String() }
	base := s.db.GetBaseStorageForManager()
	lce, ok := base.(storage.MVCCLifecycleEngine)
	if !ok {
		t.Fatalf("%T is not an MVCCLifecycleEngine", base)
	}
	lce.PauseLifecycle()
	badger := hl3Badger(base)
	if badger == nil {
		t.Fatalf("no BadgerEngine under %T", base)
	}
	status := func() string {
		st := lce.LifecycleStatus()
		return fmt.Sprintf("enabled=%v running=%v paused=%v readers=%d", st["enabled"], st["running"], st["paused"], badger.ActiveReaders())
	}
	t.Logf("SETUP at=%s lifecycle=%s", since(), status())

	const users = 120
	for i := 0; i < users; i++ {
		if err := hl3CreateUser(authn, fmt.Sprintf("u%d", i)); err != nil {
			t.Fatalf("user %d: %v", i, err)
		}
	}
	var nextUser int32 = -1
	bearer := "Bearer " + getAuthToken(t, authn, "admin")
	basicAdmin := "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:password123"))

	engine, err := s.dbManager.GetStorage("nornic")
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string, params map[string]interface{}) error {
		executor, err := s.executorCache.Get("nornic")
		if err != nil {
			return err
		}
		_, err = executor.Execute(context.Background(), query, params)
		return err
	}
	const total = 300000
	const batch = 20000
	for offset := 0; offset < total; offset += 5000 {
		list := make([]*storage.Node, 0, 5000)
		for i := offset; i < offset+5000; i++ {
			list = append(list, &storage.Node{ID: storage.NodeID(fmt.Sprintf("lockup-%d", i)), Labels: []string{"LockupDoc", fmt.Sprintf("LockupB%d", i/batch)}, Properties: map[string]interface{}{"document_id": fmt.Sprintf("doc-%d", i), "v": 0}})
		}
		if err := hl3Bulk(engine, list); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	t.Logf("SEED_BULK at=%s", since())
	for b := 0; b < total/batch; b++ {
		if err := exec(fmt.Sprintf("MATCH (n:LockupB%d) SET n.v = 1", b), nil); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	t.Logf("SEEDED at=%s nodes=%d lifecycle=%s", since(), total, status())

	host := httptest.NewServer(s.buildRouter())
	defer func() {
		host.CloseClientConnections()
		done := make(chan struct{})
		go func() { host.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("CLEANUP httptest close blocked")
		}
	}()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, s.dbManager)
	served := make(chan error, 1)
	go func() { served <- bs.Serve(listener) }()
	defer func() {
		_ = bs.Close()
		select {
		case <-served:
		case <-time.After(10 * time.Second):
		}
	}()
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = driver.Close(ctx)
	}()

	post := func(timeout time.Duration, path, auth, query string, params map[string]interface{}) (int, string, http.Header, error) {
		statements := []map[string]interface{}{}
		if query != "" {
			statements = append(statements, map[string]interface{}{"statement": query, "parameters": params})
		}
		body, _ := json.Marshal(map[string]interface{}{"statements": statements})
		req, err := http.NewRequest(http.MethodPost, host.URL+path, bytes.NewReader(body))
		if err != nil {
			return 0, "", nil, err
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: timeout}).Do(req)
		if err != nil {
			return 0, "", nil, err
		}
		defer resp.Body.Close()
		out, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out), resp.Header, err
	}
	okBody := func(code int, body string, err error) bool {
		return err == nil && (code == 200 || code == 201) && !strings.Contains(body, "\"errors\":[{")
	}
	boltRun := func(explicit bool) string {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer session.Close(ctx)
		if explicit {
			tx, err := session.BeginTransaction(ctx)
			if err != nil {
				return "err=begin " + err.Error()
			}
			res, err := tx.Run(ctx, "RETURN 1", nil)
			if err == nil {
				_, err = res.Collect(ctx)
			}
			if err != nil {
				_ = tx.Rollback(ctx)
				return "err=" + err.Error()
			}
			if err := tx.Commit(ctx); err != nil {
				return "err=commit " + err.Error()
			}
			return "ok"
		}
		res, err := session.Run(ctx, "RETURN 1", nil)
		if err != nil {
			return "err=" + err.Error()
		}
		if _, err := res.Collect(ctx); err != nil {
			return "err=" + err.Error()
		}
		return "ok"
	}

	type probe = hl3Probe
	httpProbe := func(name, path string, auth func() string, query string) probe {
		return probe{name, func() string {
			code, body, _, err := post(45*time.Second, path, auth(), query, nil)
			mark := "ok"
			if !okBody(code, body, err) {
				mark = "FAIL"
			}
			return fmt.Sprintf("%s status=%d err=%v body=%s", mark, code, err, hl3Clip(body, 160))
		}}
	}
	fixed := func(v string) func() string { return func() string { return v } }
	freshBasic := func() string {
		n := atomic.AddInt32(&nextUser, 1) % users
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("u%d:password123", n)))
	}
	probes := []probe{
		httpProbe("http-auto-RETURN1-bearer", "/db/nornic/tx/commit", fixed(bearer), "RETURN 1"),
		httpProbe("http-auto-RETURN1-basic-cached", "/db/nornic/tx/commit", fixed(basicAdmin), "RETURN 1"),
		httpProbe("http-auto-RETURN1-basic-uncached", "/db/nornic/tx/commit", freshBasic, "RETURN 1"),
		httpProbe("http-auto-SHOWTX-bearer", "/db/nornic/tx/commit", fixed(bearer), "SHOW TRANSACTIONS"),
		httpProbe("http-auto-CREATE-bearer", "/db/nornic/tx/commit", fixed(bearer), "CREATE (:HL3Probe {at: timestamp()})"),
		{"http-explicit-new-tx-RETURN1-bearer", func() string {
			code, body, header, err := post(45*time.Second, "/db/nornic/tx", bearer, "RETURN 1", nil)
			if !okBody(code, body, err) {
				return fmt.Sprintf("FAIL begin status=%d err=%v body=%s", code, err, hl3Clip(body, 160))
			}
			loc, perr := url.Parse(header.Get("Location"))
			if perr != nil {
				return "FAIL location " + perr.Error()
			}
			code, body, _, err = post(45*time.Second, loc.Path+"/commit", bearer, "RETURN 1", nil)
			mark := "ok"
			if !okBody(code, body, err) {
				mark = "FAIL"
			}
			return fmt.Sprintf("%s commit status=%d err=%v body=%s", mark, code, err, hl3Clip(body, 160))
		}},
		{"bolt-auto-RETURN1", func() string { return boltRun(false) }},
		{"bolt-explicit-RETURN1", func() string { return boltRun(true) }},
		{"http-health", func() string {
			resp, err := (&http.Client{Timeout: 45 * time.Second}).Get(host.URL + "/health")
			if err != nil {
				return "FAIL err=" + err.Error()
			}
			defer resp.Body.Close()
			return fmt.Sprintf("ok status=%d", resp.StatusCode)
		}},
	}

	// Baseline (lifecycle paused): every probe must answer quickly; also warms
	// the admin Basic cache.
	for _, p := range probes {
		at := time.Now()
		out := p.run()
		t.Logf("BASELINE name=%s elapsed=%s %s", p.name, time.Since(at).Round(time.Millisecond), out)
	}

	slowQuery := hl3Query()
	runMode := func(mode string) {
		modeStart := time.Now()
		rel := func() string { return time.Since(modeStart).Round(time.Millisecond).String() }
		t.Logf("MODE_BEGIN mode=%s at=%s lifecycle=%s", mode, since(), status())

		// Sampler: prune pass start/end, registered readers, goroutines
		// parked in snapshot-reader registration and in the authenticator.
		var stop atomic.Bool
		samplerDone := make(chan struct{})
		var pruneNow atomic.Bool
		go func() {
			defer close(samplerDone)
			var pruneSince time.Time
			lastLog := time.Time{}
			for !stop.Load() {
				dump := hl3Stacks(1)
				running := strings.Contains(dump, "PruneMVCCVersions")
				if running && pruneSince.IsZero() {
					pruneSince = time.Now()
					pruneNow.Store(true)
					t.Logf("PRUNE_START mode=%s t=+%s readers=%d", mode, rel(), badger.ActiveReaders())
				}
				if !running && !pruneSince.IsZero() {
					t.Logf("PRUNE_END mode=%s t=+%s pass=%s", mode, rel(), time.Since(pruneSince).Round(time.Millisecond))
					pruneSince = time.Time{}
					pruneNow.Store(false)
				}
				if time.Since(lastLog) >= time.Second {
					lastLog = time.Now()
					t.Logf("SAMPLE mode=%s t=+%s prune=%v readers=%d snapWaiters=%d authWaiters=%d sessionWaiters=%d", mode, rel(), running, badger.ActiveReaders(),
						hl3Count(dump, "acquireSnapshotReader"), hl3Count(dump, "CurrentClaims", "(*Authenticator).Authenticate"), hl3Count(dump, "txsession.(*Manager)"))
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()

		slowLoc := ""
		if mode == "prunefirst" {
			// The client's order as its dump shows it: a prune pass is already
			// running when the long read arrives (BEGIN with the statement,
			// the first request of /tx then /tx/{id}).
			lce.ResumeLifecycle()
			t.Logf("LIFECYCLE_RESUMED mode=%s t=+%s", mode, rel())
			for i := 0; i < 100 && !pruneNow.Load(); i++ {
				time.Sleep(100 * time.Millisecond)
			}
			t.Logf("PRUNE_SEEN mode=%s t=+%s prune=%v readers=%d", mode, rel(), pruneNow.Load(), badger.ActiveReaders())
			slowStart := time.Now()
			code, body, header, err := post(120*time.Second, "/db/nornic/tx", bearer, slowQuery, map[string]interface{}{"chunk": hl3Chunk()})
			t.Logf("SLOW_RESPONSE mode=%s t=+%s elapsed=%s status=%d err=%v location=%q body=%s prune=%v readers=%d", mode, rel(), time.Since(slowStart).Round(time.Millisecond), code, err, header.Get("Location"), hl3Clip(body, 200), pruneNow.Load(), badger.ActiveReaders())
		} else if mode != "noslow" {
			path := "/db/nornic/tx/commit"
			if mode != "auto" {
				code, body, header, err := post(20*time.Second, "/db/nornic/tx", bearer, "", nil)
				if code != http.StatusCreated || err != nil {
					t.Fatalf("BEGIN mode=%s status=%d err=%v body=%s", mode, code, err, body)
				}
				loc, perr := url.Parse(header.Get("Location"))
				if perr != nil {
					t.Fatalf("location: %v", perr)
				}
				slowLoc = loc.Path
				path = slowLoc
			}
			type outcome struct {
				code    int
				body    string
				err     error
				elapsed time.Duration
			}
			slowStart := time.Now()
			got := make(chan outcome, 1)
			go func() {
				code, body, _, err := post(120*time.Second, path, bearer, slowQuery, map[string]interface{}{"chunk": hl3Chunk()})
				got <- outcome{code, body, err, time.Since(slowStart)}
			}()
			time.Sleep(time.Second)
			t.Logf("SLOW_RUNNING mode=%s path=%s t=+%s readers=%d", mode, path, rel(), badger.ActiveReaders())
			if mode != "lcoff" {
				lce.ResumeLifecycle()
				t.Logf("LIFECYCLE_RESUMED mode=%s t=+%s", mode, rel())
			}
			time.Sleep(2 * time.Second)
			hl3Routes(mode+"/during-slow", false)
			var o outcome
			select {
			case o = <-got:
			case <-time.After(130 * time.Second):
				o = outcome{err: fmt.Errorf("no response")}
			}
			t.Logf("SLOW_RESPONSE mode=%s t=+%s elapsed=%s status=%d err=%v body=%s readers=%d", mode, rel(), o.elapsed.Round(time.Millisecond), o.code, o.err, hl3Clip(o.body, 200), badger.ActiveReaders())
		} else {
			lce.ResumeLifecycle()
			t.Logf("LIFECYCLE_RESUMED mode=%s t=+%s", mode, rel())
		}

		type result struct {
			round   int
			name    string
			start   time.Duration
			elapsed time.Duration
			out     string
		}
		var mu sync.Mutex
		var results []result
		outstanding := map[string]time.Time{}
		var wg sync.WaitGroup
		windowStart := time.Now()
		fullDumped, laterDumped := false, false
		for round := 0; time.Since(windowStart) < 48*time.Second; round++ {
			for _, p := range probes {
				p := p
				key := fmt.Sprintf("%d/%s", round, p.name)
				wg.Add(1)
				mu.Lock()
				outstanding[key] = time.Now()
				mu.Unlock()
				go func(round int) {
					defer wg.Done()
					at := time.Now()
					out := p.run()
					mu.Lock()
					delete(outstanding, key)
					results = append(results, result{round, p.name, at.Sub(windowStart), time.Since(at), out})
					mu.Unlock()
				}(round)
			}
			for tick := 0; tick < 8; tick++ {
				time.Sleep(500 * time.Millisecond)
				mu.Lock()
				oldest := time.Duration(0)
				var stuck []string
				for k, at := range outstanding {
					if d := time.Since(at); d > 3*time.Second {
						stuck = append(stuck, k)
						if d > oldest {
							oldest = d
						}
					}
				}
				mu.Unlock()
				if len(stuck) > 0 && !fullDumped {
					fullDumped = true
					sort.Strings(stuck)
					t.Logf("STUCK mode=%s t=+%s window=+%s oldest=%s prune=%v readers=%d stuck=%v", mode, rel(), time.Since(windowStart).Round(time.Millisecond), oldest.Round(time.Millisecond), pruneNow.Load(), badger.ActiveReaders(), stuck)
					hl3Routes(mode+"/stuck-first", true)
				} else if len(stuck) > 0 && !laterDumped && time.Since(windowStart) > 25*time.Second {
					laterDumped = true
					t.Logf("STUCK_LATER mode=%s t=+%s window=+%s oldest=%s prune=%v readers=%d n=%d", mode, rel(), time.Since(windowStart).Round(time.Millisecond), oldest.Round(time.Millisecond), pruneNow.Load(), badger.ActiveReaders(), len(stuck))
					hl3Routes(mode+"/stuck-later", false)
				}
			}
		}
		lce.PauseLifecycle()
		t.Logf("LIFECYCLE_PAUSED mode=%s t=+%s", mode, rel())
		drained := make(chan struct{})
		go func() { wg.Wait(); close(drained) }()
		select {
		case <-drained:
		case <-time.After(100 * time.Second):
			t.Logf("DRAIN_TIMEOUT mode=%s", mode)
			hl3Routes(mode+"/drain-timeout", true)
		}
		for i := 0; i < 600 && pruneNow.Load(); i++ {
			time.Sleep(200 * time.Millisecond)
		}
		stop.Store(true)
		<-samplerDone
		if slowLoc != "" {
			code, body, _, err := post(20*time.Second, slowLoc+"/commit", bearer, "", nil)
			t.Logf("SLOW_TX_AFTER mode=%s commit status=%d err=%v body=%s", mode, code, err, hl3Clip(body, 200))
		}
		mu.Lock()
		sort.Slice(results, func(i, j int) bool {
			if results[i].round != results[j].round {
				return results[i].round < results[j].round
			}
			return results[i].name < results[j].name
		})
		type agg struct {
			n, slow, fail int
			max           time.Duration
		}
		sum := map[string]*agg{}
		for _, r := range results {
			t.Logf("PROBE mode=%s round=%d name=%s start=+%s elapsed=%s %s", mode, r.round, r.name, r.start.Round(time.Millisecond), r.elapsed.Round(time.Millisecond), r.out)
			a := sum[r.name]
			if a == nil {
				a = &agg{}
				sum[r.name] = a
			}
			a.n++
			if r.elapsed > 3*time.Second {
				a.slow++
			}
			if !strings.HasPrefix(r.out, "ok") {
				a.fail++
			}
			if r.elapsed > a.max {
				a.max = r.elapsed
			}
		}
		var names []string
		for n := range sum {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			a := sum[n]
			t.Logf("SUMMARY mode=%s name=%s n=%d over3s=%d fail=%d max=%s", mode, n, a.n, a.slow, a.fail, a.max.Round(time.Millisecond))
			if mode == "prunefirst" && (a.n != 12 || a.slow != 0 || a.fail != 0) {
				t.Errorf("prune traffic %s: got %d probes (%d over 3s, %d failed); want 12 successful probes under 3s", n, a.n, a.slow, a.fail)
			}
		}
		mu.Unlock()
		t.Logf("MODE_END mode=%s at=%s lifecycle=%s", mode, since(), status())
	}
	modes := []string{"lcoff", "explicit", "auto", "noslow"}
	if v := strings.TrimSpace(os.Getenv("HL3_MODES")); v != "" {
		modes = strings.Split(v, ",")
	}
	for _, mode := range modes {
		runMode(mode)
	}
	t.Logf("END at=%s lifecycle=%s", since(), status())
}

// TestV152PortPruneFirst: a fresh store; the long read arrives while a
// prune pass is running, then the same probes; noslow afterwards shows
// whether pruning resumes (a leaked reader stops it).
func TestV152PortPruneFirst(t *testing.T) {
	t.Setenv("HL3_MODES", "prunefirst,noslow")
	runV152PortPruneTraffic(t)
}
