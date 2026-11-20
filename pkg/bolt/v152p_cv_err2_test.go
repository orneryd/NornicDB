package bolt

// Research only (cdc-visibility follow-up). Scenario matrix on the client-shape
// workload of zz_cv_err_test.go, each scenario on a fresh database with change
// capture DIFF: writer counts and CPU burners are exact (0 means none); write
// kinds: explicit transaction, auto-commit, the driver's retrying transaction
// function (ExecuteWrite, retries counted through the driver's logger); MERGE
// matching a seeded node or creating its node. For every failed write: start and
// end offsets, step, Neo4j code and message, all properties of the node read at
// once, the node's last successful write, and how many change-feed events carry
// the failed write's value. ZZCV2_SAMPLE=1 samples all goroutine stacks every
// ZZCV2_SAMPLE_MS and counts, per storage_transaction.go line set and the
// functions on the stack, the goroutines in sizeTrackingEngine.CommitTransaction.

import (
	"context"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func cv2Int(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v >= 0 {
		return v
	}
	return def
}

type cv2Scenario struct {
	name                 string
	review, other, burns int
	mode                 string // explicit, auto, retry
	create               bool
	nocons, capoff       bool // r2: no unique constraints; change capture off
}

type cv2Logger struct {
	mu      sync.Mutex
	retries map[string]int
	sleeps  []string
}

var cv2RetryRe = regexp.MustCompile(`\[after ([^\]]+)\]`)

func (l *cv2Logger) note(msg string, args ...any) {
	s := fmt.Sprintf(msg, args...)
	if !strings.HasPrefix(s, "Retrying transaction") {
		return
	}
	key := s
	if i := strings.Index(key, " [after"); i > 0 {
		key = key[:i]
	}
	if i := strings.Index(key, "tx-"); i > 0 {
		key = key[:i] + "tx-…"
	}
	l.mu.Lock()
	l.retries[key]++
	if m := cv2RetryRe.FindStringSubmatch(s); m != nil {
		l.sleeps = append(l.sleeps, m[1])
	}
	l.mu.Unlock()
}
func (l *cv2Logger) Error(name, id string, err error)                {}
func (l *cv2Logger) Warnf(name, id string, msg string, args ...any)  { l.note(msg, args...) }
func (l *cv2Logger) Infof(name, id string, msg string, args ...any)  { l.note(msg, args...) }
func (l *cv2Logger) Debugf(name, id string, msg string, args ...any) { l.note(msg, args...) }

type cv2Sampler struct {
	mu       sync.Mutex
	samples  int
	byFrame  map[string]int
	busy     []time.Duration // sample offsets with a goroutine inside admitPrepared or a writeAdmission acquire
	examples map[string]string
}

var cv2LineRe = regexp.MustCompile(`storage_transaction\.go:(\d+)`)

func (s *cv2Sampler) run(stop <-chan struct{}, every time.Duration, began time.Time) {
	buf := make([]byte, 64<<20)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		n := runtime.Stack(buf, true)
		at := time.Since(began)
		gs := strings.Split(string(buf[:n]), "\n\n")
		s.mu.Lock()
		s.samples++
		busy := false
		for _, g := range gs {
			if !strings.Contains(g, "multidb.(*sizeTrackingEngine).CommitTransaction") {
				continue
			}
			var where []string
			for _, fn := range []string{"admitPrepared", "(*writeAdmission).acquire", "semaphore.(*Weighted).Acquire", "PublishCommit", "SubmitEntityCommit", "WaitOutcome", "PrepareEntityCommit", "reserveStorageUsage"} {
				if strings.Contains(g, fn) {
					where = append(where, fn)
				}
			}
			var ls []string
			for _, l := range cv2LineRe.FindAllStringSubmatch(g, -1) {
				ls = append(ls, l[1])
			}
			key := fmt.Sprintf("storage_transaction.go lines [%s] via [%s]", strings.Join(ls, ","), strings.Join(where, ","))
			s.byFrame[key]++
			if _, ok := s.examples[key]; !ok {
				s.examples[key] = g
			}
			if strings.Contains(g, "admitPrepared") || strings.Contains(g, "(*writeAdmission).acquire") {
				busy = true
			}
		}
		if busy {
			s.busy = append(s.busy, at)
		}
		s.mu.Unlock()
	}
}

// nearest returns the distance from at to the closest busy sample.
func (s *cv2Sampler) nearest(at time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.Search(len(s.busy), func(i int) bool { return s.busy[i] >= at })
	best := time.Duration(1 << 62)
	for _, j := range []int{i - 1, i} {
		if j >= 0 && j < len(s.busy) {
			d := s.busy[j] - at
			if d < 0 {
				d = -d
			}
			if d < best {
				best = d
			}
		}
	}
	return best
}

type cv2Fail struct {
	label, id, step, code, msg string
	v                          int64
	attempts                   int
	start, end                 time.Duration
	props                      any
	nodes                      int
	readErr                    error
	lastOK                     int64
	mapDiff                    string // r3: node's whole property map against its last successful write ("" = equal)
}

// cv2Expected is the whole property map a node must hold: that of its last
// successful write, else its seed (nil for a created id never written).
func cv2Expected(label, id string, last map[string]any) map[string]any {
	if last != nil {
		return last
	}
	switch {
	case label == "Review" && strings.HasPrefix(id, "review-"):
		return map[string]any{"id": id, "v": int64(-1), "c1": "a", "c2": "b", "c3": "c", "c4": "d", "c5": "e", "c6": "f", "c7": "g", "c8": "h", "c9": "i", "c10": "j", "c11": "k"}
	case label == "Other" && strings.HasPrefix(id, "other-"):
		return map[string]any{"id": id, "v": int64(-1)}
	}
	return nil
}

// cv2MapDiff compares a node's properties (nodes = how many nodes had the id)
// with the expected map; "" means equal (or absent where nothing is expected).
func cv2MapDiff(got any, want map[string]any, nodes int) string {
	if want == nil {
		if nodes == 0 {
			return ""
		}
		return fmt.Sprintf("node exists (%d) but none expected: %v", nodes, got)
	}
	m, ok := got.(map[string]any)
	if !ok || nodes == 0 {
		return "node missing"
	}
	var d []string
	for k, w := range want {
		g, ok := m[k]
		if !ok {
			d = append(d, "missing "+k)
			continue
		}
		if ws, isS := w.(string); isS {
			if gs, _ := g.(string); gs != ws {
				d = append(d, fmt.Sprintf("%s=%v want %v", k, g, w))
			}
		} else if cvInt(g) != cvInt(w) {
			d = append(d, fmt.Sprintf("%s=%v want %v", k, g, w))
		}
	}
	for k, g := range m {
		if _, ok := want[k]; !ok {
			d = append(d, fmt.Sprintf("extra %s=%v", k, g))
		}
	}
	sort.Strings(d)
	return strings.Join(d, ", ")
}

func TestZZCVWriteErrors2(t *testing.T) {
	seconds := cv2Int("ZZCV2_SECONDS", 120)
	all := cv2All()
	want := map[string]bool{}
	for _, n := range strings.Split(os.Getenv("ZZCV2_SCENARIOS"), ",") {
		if n = strings.TrimSpace(n); n != "" {
			want[n] = true
		}
	}
	if len(want) == 0 {
		for _, sc := range all[7:] {
			want[sc.name] = true
		}
	}
	for _, sc := range all {
		if !want[sc.name] {
			continue
		}
		cv2Run(t, sc, time.Duration(seconds)*time.Second)
	}
}

func cv2All() []cv2Scenario {
	load := cv2Int("ZZCV2_BURNERS_PER_CPU", 4) * runtime.NumCPU()
	return []cv2Scenario{
		{"noload-2w-explicit-match", 2, 0, 0, "explicit", false, false, false},
		{"noload-12w-explicit-match", 8, 4, 0, "explicit", false, false, false},
		{"load-12w-explicit-match", 8, 4, load, "explicit", false, false, false},
		{"load-12w-explicit-create", 8, 4, load, "explicit", true, false, false},
		{"load-12w-auto-match", 8, 4, load, "auto", false, false, false},
		{"load-12w-auto-create", 8, 4, load, "auto", true, false, false},
		{"load-12w-retry-match", 8, 4, load, "retry", false, false, false},
		// r2 scenarios (the default set when ZZCV2_SCENARIOS is empty).
		{"noload-2w-auto-create", 2, 0, 0, "auto", true, false, false},
		{"noload-12w-auto-create", 8, 4, 0, "auto", true, false, false},
		{"noload-2w-retry-match", 2, 0, 0, "retry", false, false, false},
		{"noload-12w-retry-match", 8, 4, 0, "retry", false, false, false},
		{"noload-2w-explicit-match-nocons", 2, 0, 0, "explicit", false, true, false},
		{"noload-12w-explicit-match-nocons", 8, 4, 0, "explicit", false, true, false},
		{"noload-12w-explicit-match-capoff", 8, 4, 0, "explicit", false, false, true},
		// r3: two writers and nothing else (no follower paging the feed).
		{"noload-2w-explicit-match-nofollow", 2, 0, 0, "explicit", false, false, false},
		{"noload-2w-auto-create-nofollow", 2, 0, 0, "auto", true, false, false},
	}
}

type cv2Result struct {
	ok       int64
	failed   int
	p50, p99 time.Duration
}

func cv2Run(t testing.TB, sc cv2Scenario, duration time.Duration) (res cv2Result) {
	sampleOn := os.Getenv("ZZCV2_SAMPLE") == "1"
	sampleEvery := time.Duration(cv2Int("ZZCV2_SAMPLE_MS", 5)) * time.Millisecond
	const limit = 25
	selectors := []any{map[string]any{"select": "n", "labels": []any{"Review"}}}
	root := t.TempDir()
	captureOn := !sc.capoff
	// serve opens the database in root and serves it over Bolt; closeAll stops both.
	serve := func(setCapture bool) (*nornicdb.DB, string, func()) {
		db, err := nornicdb.Open(root, rfbConfig(false))
		if err != nil {
			t.Fatal(err)
		}
		m, err := rfNewManager(db.GetBaseStorageForManager())
		if err != nil {
			t.Fatal(err)
		}
		if setCapture {
			capture, ok := storage.ChangeCaptureOf(db.GetBaseStorageForManager())
			if !ok {
				t.Fatal("no change capture on the base storage")
			}
			capture.SetChangeCapture("nornic", storage.ChangeCaptureDiff)
		}
		srv := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 256}, nil, m)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ln) }()
		var once sync.Once
		closeAll := func() {
			once.Do(func() {
				_ = srv.Close()
				<-done
				_ = db.Close()
			})
		}
		t.Cleanup(closeAll)
		return db, fmt.Sprintf("bolt://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port), closeAll
	}
	db, uri, closeAll := serve(captureOn)
	_ = db
	logger := &cv2Logger{retries: map[string]int{}}
	var dmu sync.Mutex
	var drivers []neo4j.DriverWithContext
	newDriver := func() neo4j.DriverWithContext {
		dmu.Lock()
		defer dmu.Unlock()
		d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth(), func(c *neo4j.Config) { c.Log = logger })
		if err != nil {
			panic(err)
		}
		drivers = append(drivers, d)
		return d
	}
	defer func() {
		for _, d := range drivers {
			_ = d.Close(context.Background())
		}
	}()
	admin := newDriver()
	n := (sc.review + sc.other) * 20
	setup := []string{
		"CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
		"CREATE CONSTRAINT other_id IF NOT EXISTS FOR (o:Other) REQUIRE o.id IS UNIQUE",
	}
	if sc.nocons {
		setup = nil
	}
	setup = append(setup,
		"UNWIND range(0, $n - 1) AS i CREATE (:Review {id: 'review-' + toString(i), v: -1, c1: 'a', c2: 'b', c3: 'c', c4: 'd', c5: 'e', c6: 'f', c7: 'g', c8: 'h', c9: 'i', c10: 'j', c11: 'k'})",
		"UNWIND range(0, $n - 1) AS i CREATE (:Other {id: 'other-' + toString(i), v: -1})")
	for _, q := range setup {
		if _, err := cvAuto(admin, q, map[string]any{"n": n}); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if rows, err := cvAuto(admin, "SHOW CONSTRAINTS YIELD name RETURN name", nil); err == nil {
		fmt.Printf("ZZCV2 %s constraints: %v\n", sc.name, rows)
	} else {
		fmt.Printf("ZZCV2 %s constraints: SHOW CONSTRAINTS failed: %v\n", sc.name, err)
	}
	start := ""
	if captureOn {
		startRows, err := cvAuto(admin, "CALL db.cdc.current() YIELD id RETURN id", nil)
		if err != nil {
			t.Fatalf("current: %v", err)
		}
		start = startRows[0][0].(string)
	}
	reviewSet := cvClientSet
	otherSet := "MERGE (o:Other {id: $id}) SET o.v = $v, o.x = $v"
	fmt.Printf("ZZCV2 === scenario %s: Review writers=%d Other writers=%d burners=%d (NumCPU=%d GOMAXPROCS=%d) mode=%s create=%v constraints=%v capture=%v duration=%s sample=%v/%s\n",
		sc.name, sc.review, sc.other, sc.burns, runtime.NumCPU(), runtime.GOMAXPROCS(0), sc.mode, sc.create, !sc.nocons, captureOn, duration, sampleOn, sampleEvery)

	stop := make(chan struct{})
	var burn sync.WaitGroup
	for i := 0; i < sc.burns; i++ {
		burn.Add(1)
		go func() {
			defer burn.Done()
			x := uint64(i)
			for {
				select {
				case <-stop:
					_ = x
					return
				default:
				}
				for j := 0; j < 20000; j++ {
					x = x*6364136223846793005 + 1442695040888963407
				}
			}
		}()
	}
	began := time.Now()
	var sampler *cv2Sampler
	samplerDone := make(chan struct{})
	if sampleOn {
		sampler = &cv2Sampler{byFrame: map[string]int{}, examples: map[string]string{}}
		go func() { sampler.run(stop, sampleEvery, began); close(samplerDone) }()
	} else {
		close(samplerDone)
	}
	var nextV atomic.Int64
	var okN, retriedCalls, attemptsTotal atomic.Int64
	var mu sync.Mutex
	var failures []cv2Fail
	lastOK := map[string]int64{}
	lastProps := map[string]map[string]any{}
	okVs := map[int64]bool{}
	// r3: "-nofollow" scenarios run the writers only, no change-feed paging.
	follow := !strings.HasSuffix(sc.name, "-nofollow")
	beganUnix := began.UnixNano()
	var latOne, latRetried []time.Duration

	// write performs one write the scenario's way; it returns the failed step,
	// the error and the number of attempts the work function saw.
	write := func(d neo4j.DriverWithContext, q string, p map[string]any) (string, int, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer s.Close(ctx)
		switch sc.mode {
		case "auto":
			res, err := s.Run(ctx, q, p)
			if err != nil {
				return "RUN", 1, err
			}
			if _, err := res.Consume(ctx); err != nil {
				return "CONSUME", 1, err
			}
			return "", 1, nil
		case "retry":
			attempts := 0
			_, err := s.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				attempts++
				res, err := tx.Run(ctx, q, p)
				if err != nil {
					return nil, err
				}
				return res.Consume(ctx)
			})
			if err != nil {
				return "EXECUTE_WRITE", attempts, err
			}
			return "", attempts, nil
		}
		tx, err := s.BeginTransaction(ctx)
		if err != nil {
			return "BEGIN", 1, err
		}
		res, err := tx.Run(ctx, q, p)
		if err != nil {
			_ = tx.Rollback(ctx)
			return "RUN", 1, err
		}
		if _, err = res.Consume(ctx); err != nil {
			_ = tx.Rollback(ctx)
			return "CONSUME", 1, err
		}
		if err := tx.Commit(ctx); err != nil {
			return "COMMIT", 1, err
		}
		return "", 1, nil
	}
	one := func(d neo4j.DriverWithContext, label, id, q string, p map[string]any) bool {
		v := p["v"].(int64)
		t0 := time.Now()
		step, attempts, err := write(d, q, p)
		t1 := time.Now()
		attemptsTotal.Add(int64(attempts))
		if attempts > 1 {
			retriedCalls.Add(1)
		}
		if err == nil {
			okN.Add(1)
			want := make(map[string]any, len(p)+1)
			for k, x := range p {
				want[k] = x
			}
			if label == "Other" {
				want["x"] = v
			}
			mu.Lock()
			lastOK[label+"/"+id] = v
			lastProps[label+"/"+id] = want
			okVs[v] = true
			if attempts > 1 {
				latRetried = append(latRetried, t1.Sub(t0))
			} else {
				latOne = append(latOne, t1.Sub(t0))
			}
			mu.Unlock()
			return true
		}
		code, msg := cvErrText(err)
		f := cv2Fail{label: label, id: id, step: step, code: code, msg: msg, v: v, attempts: attempts, start: t0.Sub(began), end: t1.Sub(began)}
		rows, rerr := cvAuto(d, "MATCH (n:"+label+" {id: $id}) RETURN properties(n)", map[string]any{"id": id})
		f.readErr, f.nodes = rerr, len(rows)
		if len(rows) > 0 {
			f.props = rows[0][0]
		}
		mu.Lock()
		f.lastOK = lastOK[label+"/"+id]
		f.mapDiff = cv2MapDiff(f.props, cv2Expected(label, id, lastProps[label+"/"+id]), f.nodes)
		failures = append(failures, f)
		mu.Unlock()
		return false
	}

	var wg sync.WaitGroup
	for w := 0; w < sc.other; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			d := newDriver()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("other-%d", (sc.review+w)*20+i%20)
				if sc.create {
					id = fmt.Sprintf("onew-%d-%d", w, i)
				}
				one(d, "Other", id, otherSet, map[string]any{"id": id, "v": nextV.Add(1)})
			}
		}(w)
	}
	for w := 0; w < sc.review; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wd, fd := newDriver(), newDriver()
			cursor := start
			page := func() {
				if !follow {
					return
				}
				if !captureOn {
					_, _ = cvAuto(fd, "MATCH (r:Review) RETURN count(r)", nil)
					return
				}
				for {
					var rows []cvEvent
					err := cvRead(fd, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
						var err error
						rows, err = cvRows(ctx, tx, "CALL db.cdc.query($cursor, $selectors, $limit)"+cvQueryReturn, map[string]any{"cursor": cursor, "selectors": selectors, "limit": limit})
						return err
					})
					if err != nil {
						return
					}
					if len(rows) > 0 {
						cursor = rows[len(rows)-1].id
					}
					if len(rows) < limit {
						return
					}
				}
			}
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("review-%d", w*20+i%20)
				if sc.create {
					id = fmt.Sprintf("new-%d-%d", w, i)
				}
				if one(wd, "Review", id, reviewSet, map[string]any{
					"id": id, "v": nextV.Add(1), "w": w, "seq": i, "ts": time.Now().UnixNano(),
					"c1": "a", "c2": "b", "c3": "c", "c4": "d", "c5": "e", "c6": "f", "c7": "g", "c8": "h", "c9": "i", "c10": "j", "c11": "k",
				}) {
					page()
				}
			}
		}(w)
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	ran := time.Since(began)
	burn.Wait()
	<-samplerDone

	var final []cvEvent
	if captureOn {
		if err := cvRead(admin, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
			var err error
			final, err = cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": start})
			return err
		}); err != nil {
			t.Fatalf("final read: %v", err)
		}
	}
	listed := map[int64]int{}
	for _, e := range final {
		listed[e.v]++
	}
	fmt.Printf("ZZCV2 %s ran %s: OK writes=%d failed calls=%d feed events=%d attempts=%d calls_with_more_than_one_attempt=%d\n",
		sc.name, ran.Round(time.Millisecond), okN.Load(), len(failures), len(final), attemptsTotal.Load(), retriedCalls.Load())
	kinds := map[string]int{}
	for _, f := range failures {
		key := fmt.Sprintf("%s %s %s: %s", f.label, f.step, f.code, f.msg)
		if i := strings.Index(key, "tx-"); i > 0 {
			key = key[:i] + "tx-…"
		}
		kinds[key]++
		near := ""
		if sampler != nil {
			near = fmt.Sprintf(" nearest_busy_sample=%s", sampler.nearest(f.start).Round(time.Millisecond))
		}
		fmt.Printf("ZZCV2 %s failure %s %s v=%d start=%s end=%s start_unix_ns=%d end_unix_ns=%d step=%s attempts=%d code=%s nodes_with_id=%d properties_read_at_once=%v (read err=%v) last_successful_v=%d whole_map_vs_last_successful_write=%q feed_events_with_failed_v=%d%s msg=%q\n",
			sc.name, f.label, f.id, f.v, f.start.Round(time.Millisecond), f.end.Round(time.Millisecond), beganUnix+int64(f.start), beganUnix+int64(f.end), f.step, f.attempts, f.code, f.nodes, f.props, f.readErr, f.lastOK, cv2Equal(f.mapDiff), listed[f.v], near, f.msg)
	}
	keys := make([]string, 0, len(kinds))
	for k := range kinds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("ZZCV2 %s kind x%d: %s\n", sc.name, kinds[k], k)
	}
	logger.mu.Lock()
	for k, c := range logger.retries {
		fmt.Printf("ZZCV2 %s driver retry log x%d: %s\n", sc.name, c, k)
	}
	if len(logger.sleeps) > 0 {
		fmt.Printf("ZZCV2 %s driver retry sleeps (first 40 of %d): %v\n", sc.name, len(logger.sleeps), logger.sleeps[:min(40, len(logger.sleeps))])
	}
	logger.mu.Unlock()
	pct := func(xs []time.Duration) string {
		if len(xs) == 0 {
			return "n=0"
		}
		sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
		q := func(p float64) time.Duration { return xs[int(p*float64(len(xs)-1))] }
		return fmt.Sprintf("n=%d p50=%s p99=%s max=%s", len(xs), q(.5).Round(time.Microsecond), q(.99).Round(time.Microsecond), xs[len(xs)-1].Round(time.Microsecond))
	}
	mu.Lock()
	fmt.Printf("ZZCV2 %s latency of successful calls: one attempt %s; more than one attempt %s\n", sc.name, pct(latOne), pct(latRetried))
	res = cv2Result{ok: okN.Load(), failed: len(failures)}
	if len(latOne) > 0 {
		res.p50, res.p99 = latOne[len(latOne)/2], latOne[int(.99*float64(len(latOne)-1))]
	}
	mu.Unlock()
	if sampler != nil {
		sampler.mu.Lock()
		fmt.Printf("ZZCV2 %s stack samples=%d every %s; samples with a goroutine in admitPrepared or a writeAdmission acquire=%d\n", sc.name, sampler.samples, sampleEvery, len(sampler.busy))
		fk := make([]string, 0, len(sampler.byFrame))
		for k := range sampler.byFrame {
			fk = append(fk, k)
		}
		sort.Strings(fk)
		for _, k := range fk {
			fmt.Printf("ZZCV2 %s goroutines-in-CommitTransaction x%d: %s\n", sc.name, sampler.byFrame[k], k)
		}
		for _, k := range fk {
			fmt.Printf("ZZCV2 %s example stack for [%s] >>>\n%s\nZZCV2 example <<<\n", sc.name, k, sampler.examples[k])
		}
		sampler.mu.Unlock()
	}
	fmt.Printf("ZZCV2 %s summary: failed=%d, failed v found on the node=%d, failed v in the feed=%d, whole map differs from last successful write=%d\n", sc.name, len(failures),
		cv2Count(failures, func(f cv2Fail) bool { return cv2HasV(f.props, f.v) }), cv2Count(failures, func(f cv2Fail) bool { return listed[f.v] > 0 }),
		cv2Count(failures, func(f cv2Fail) bool { return f.mapDiff != "" }))

	// r2: the store against the successful writes, in this session and after a
	// close and reopen of the same directory.
	failedVs := map[int64]bool{}
	for _, f := range failures {
		failedVs[f.v] = true
	}
	checkStore := func(phase string, d neo4j.DriverWithContext) {
		rows, err := cvAuto(d, "MATCH (n) WHERE n:Review OR n:Other RETURN CASE WHEN n:Review THEN 'Review' ELSE 'Other' END, n.id, n.v, properties(n)", nil)
		if err != nil {
			fmt.Printf("ZZCV2 %s store[%s]: read failed: %v\n", sc.name, phase, err)
			return
		}
		expect := map[string]int64{}
		for i := 0; i < n; i++ {
			expect[fmt.Sprintf("Review/review-%d", i)] = -1
			expect[fmt.Sprintf("Other/other-%d", i)] = -1
		}
		mu.Lock()
		for k, v := range lastOK {
			expect[k] = v
		}
		mu.Unlock()
		seen := map[string]int{}
		wrong, extra, failedOnNode, mapDiffer := 0, 0, 0, 0
		var examples, mapExamples []string
		for _, r := range rows {
			k := fmt.Sprintf("%v/%v", r[0], r[1])
			seen[k]++
			v := cvInt(r[2])
			if failedVs[v] {
				failedOnNode++
			}
			mu.Lock()
			lp := lastProps[k]
			mu.Unlock()
			if want := cv2Expected(fmt.Sprint(r[0]), fmt.Sprint(r[1]), lp); want != nil {
				if diff := cv2MapDiff(r[3], want, 1); diff != "" {
					mapDiffer++
					if len(mapExamples) < 5 {
						mapExamples = append(mapExamples, k+": "+diff)
					}
				}
			}
			want, ok := expect[k]
			switch {
			case !ok:
				extra++
				if len(examples) < 8 {
					examples = append(examples, fmt.Sprintf("unexpected %s v=%d", k, v))
				}
			case want != v:
				wrong++
				if len(examples) < 8 {
					examples = append(examples, fmt.Sprintf("%s v=%d want %d", k, v, want))
				}
			}
		}
		missing, dup := 0, 0
		for k := range expect {
			switch seen[k] {
			case 0:
				missing++
				if len(examples) < 8 {
					examples = append(examples, "missing "+k)
				}
			case 1:
			default:
				dup++
			}
		}
		fmt.Printf("ZZCV2 %s store[%s]: nodes=%d expected=%d wrong_v=%d missing=%d unexpected=%d duplicated_id=%d nodes_holding_a_refused_v=%d whole_map_differs=%d examples=%v map_examples=%v\n",
			sc.name, phase, len(rows), len(expect), wrong, missing, extra, dup, failedOnNode, mapDiffer, examples, mapExamples)
	}
	checkFeed := func(phase string, d neo4j.DriverWithContext) {
		if !captureOn {
			fmt.Printf("ZZCV2 %s feed[%s]: capture off, no feed\n", sc.name, phase)
			return
		}
		var evs []cvEvent
		err := cvRead(d, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
			var err error
			evs, err = cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": start})
			return err
		})
		if err != nil {
			fmt.Printf("ZZCV2 %s feed[%s]: query failed: %v\n", sc.name, phase, err)
			return
		}
		count := map[int64]int{}
		for _, e := range evs {
			count[e.v]++
		}
		missing, dup, refused, other := 0, 0, 0, 0
		mu.Lock()
		for v := range okVs {
			switch count[v] {
			case 0:
				missing++
			case 1:
			default:
				dup++
			}
		}
		for v := range count {
			if failedVs[v] {
				refused++
			} else if !okVs[v] {
				other++
			}
		}
		okN := len(okVs)
		mu.Unlock()
		fmt.Printf("ZZCV2 %s feed[%s]: events=%d successful_writes=%d successful_missing=%d successful_listed_more_than_once=%d refused_listed=%d events_with_other_v=%d\n",
			sc.name, phase, len(evs), okN, missing, dup, refused, other)
	}
	checkStore("in session", admin)
	checkFeed("in session", admin)
	closeAll()
	var db2 *nornicdb.DB
	db2, uri, closeAll = serve(false)
	re := newDriver()
	checkStore("after reopen", re)
	checkFeed("after reopen, capture not set again", re)
	if captureOn {
		// r4: the capture setting is the server's runtime configuration; set it
		// again on the reopened database and read the stored feed.
		if c, ok := storage.ChangeCaptureOf(db2.GetBaseStorageForManager()); ok {
			c.SetChangeCapture("nornic", storage.ChangeCaptureDiff)
			checkFeed("after reopen, capture set again", re)
		}
	}
	closeAll()
	return res
}

func cv2Equal(diff string) string {
	if diff == "" {
		return "equal"
	}
	return diff
}

func cv2Count(fs []cv2Fail, pred func(cv2Fail) bool) int {
	n := 0
	for _, f := range fs {
		if pred(f) {
			n++
		}
	}
	return n
}

func cv2HasV(props any, v int64) bool {
	m, ok := props.(map[string]any)
	if !ok {
		return false
	}
	return cvInt(m["v"]) == v || cvInt(m["x"]) == v
}
