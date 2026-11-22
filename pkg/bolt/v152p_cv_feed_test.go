package bolt

// Research only (cdc-visibility). A database with change capture FULL, served
// over Bolt (bolt.NewWithDatabaseManager on the database's manager, official
// Go driver, no authentication), under CPU load (busy goroutines), for a
// fixed time:
//   - writers, each on its own driver: an explicit transaction
//     MERGE (n:Item {k: $k}) SET n.v = $v (v unique per commit), COMMIT, then
//     two more statements; after the COMMIT's answer, on another driver (another
//     connection), a read transaction runs db.cdc.query(<cursor taken before
//     the write>) and looks for the write's event (by v); a miss is looked for
//     again after 200 ms and 2 s;
//   - two readers page the feed by cursor in read transactions
//     (db.cdc.query($cursor, $selectors, $limit), the cursor being the last
//     id returned), one without selectors, limit 37, one with a node/label
//     selector, limit 50;
//   - a checker runs, in one read transaction, db.cdc.current() and then
//     db.cdc.query(<previous current>) and compares them.
// Afterwards every committed write is counted against the events: those the
// final full read lists, those each reader was given, by id.
// Uses rfbConfig (zz_rf_bolt_test.go) and rfNewManager (zz_rf_manager_test.go,
// per build).

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type cvPos struct {
	commit uint64
	seq    uint32
}

func (p cvPos) less(o cvPos) bool { return p.commit < o.commit || p.commit == o.commit && p.seq < o.seq }

func cvDecode(id string) cvPos {
	raw, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil || len(raw) != 21 {
		return cvPos{}
	}
	return cvPos{binary.BigEndian.Uint64(raw[9:17]), binary.BigEndian.Uint32(raw[17:21])}
}

func cvInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return -1
}

type cvEvent struct {
	pos cvPos
	id  string
	v   int64
	op  string
}

const cvQueryReturn = " YIELD id, event RETURN id, event.state.after.properties.v AS v, event.operation AS op"

func cvRows(ctx context.Context, tx neo4j.ExplicitTransaction, q string, p map[string]any) ([]cvEvent, error) {
	res, err := tx.Run(ctx, q, p)
	if err != nil {
		return nil, err
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]cvEvent, 0, len(recs))
	for _, r := range recs {
		id, _ := r.Values[0].(string)
		op, _ := r.Values[2].(string)
		v := int64(-1)
		if r.Values[1] != nil {
			v = cvInt(r.Values[1])
		}
		out = append(out, cvEvent{pos: cvDecode(id), id: id, v: v, op: op})
	}
	return out, nil
}

// cvRead runs fn in one read transaction on d.
func cvRead(d neo4j.DriverWithContext, fn func(ctx context.Context, tx neo4j.ExplicitTransaction) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic", AccessMode: neo4j.AccessModeRead})
	defer s.Close(ctx)
	tx, err := s.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Close(ctx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func cvAuto(d neo4j.DriverWithContext, q string, p map[string]any) ([][]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer s.Close(ctx)
	res, err := s.Run(ctx, q, p)
	if err != nil {
		return nil, err
	}
	recs, err := res.Collect(ctx)
	if err != nil {
		return nil, err
	}
	var rows [][]any
	for _, r := range recs {
		rows = append(rows, r.Values)
	}
	return rows, nil
}

func cvEnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

type cvCommit struct {
	writer   int
	k        int
	v        int64
	answered time.Time
	err      error
}

func TestZZCVFeedVisibility(t *testing.T) {
	duration := time.Duration(cvEnvInt("ZZCV_SECONDS", 180)) * time.Second
	writers := cvEnvInt("ZZCV_WRITERS", 8)
	burners := cvEnvInt("ZZCV_BURNERS_PER_CPU", 4) * runtime.NumCPU()
	root := t.TempDir()
	db, err := nornicdb.Open(root, rfbConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, err := rfNewManager(db.GetBaseStorageForManager())
	if err != nil {
		t.Fatal(err)
	}
	// After the manager: it applies each database's txLogEnrichment option when it loads.
	capture, ok := storage.ChangeCaptureOf(db.GetBaseStorageForManager())
	if !ok {
		t.Fatal("no change capture on the base storage")
	}
	capture.SetChangeCapture("nornic", storage.ChangeCaptureFull)
	srv := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 256}, nil, m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	defer func() { _ = srv.Close(); <-done }()
	uri := fmt.Sprintf("bolt://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	var drivers []neo4j.DriverWithContext
	newDriver := func() neo4j.DriverWithContext {
		d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		if err != nil {
			t.Fatal(err)
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
	if _, err := cvAuto(admin, "CREATE CONSTRAINT item_k IF NOT EXISTS FOR (n:Item) REQUIRE n.k IS UNIQUE", nil); err != nil {
		t.Fatalf("constraint: %v", err)
	}
	if _, err := cvAuto(admin, "UNWIND range(0, $n - 1) AS k CREATE (:Item {k: k, v: -1})", map[string]any{"n": writers * 50}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	startRows, err := cvAuto(admin, "CALL db.cdc.current() YIELD id RETURN id", nil)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	start := startRows[0][0].(string)
	fmt.Printf("ZZCV start cursor %s pos=%v; writers=%d burners=%d (NumCPU=%d GOMAXPROCS=%d) duration=%s\n", start, cvDecode(start), writers, burners, runtime.NumCPU(), runtime.GOMAXPROCS(0), duration)

	stop := make(chan struct{})
	var burn sync.WaitGroup
	var spin atomic.Uint64
	for i := 0; i < burners; i++ {
		burn.Add(1)
		go func() {
			defer burn.Done()
			x := uint64(i)
			for {
				select {
				case <-stop:
					spin.Add(x & 1)
					return
				default:
				}
				for j := 0; j < 20000; j++ {
					x = x*6364136223846793005 + 1442695040888963407
				}
			}
		}()
	}

	var nextV atomic.Int64
	var mu sync.Mutex
	var commits []cvCommit
	var immediateMiss, lateFound200, lateFound2s, neverFound, checkErr, writeErr atomic.Int64
	var missExamples []string
	var wg sync.WaitGroup
	writersDone := make(chan struct{})
	var wwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wwg.Add(1)
		go func(w int) {
			defer wwg.Done()
			wd, cd := newDriverLocked(&mu, newDriver), newDriverLocked(&mu, newDriver)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := w*50 + i%50
				v := nextV.Add(1)
				// The cursor before the write, on the check connection.
				cur, err := cvAuto(cd, "CALL db.cdc.current() YIELD id RETURN id", nil)
				if err != nil || len(cur) == 0 {
					checkErr.Add(1)
					continue
				}
				before := cur[0][0].(string)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				s := wd.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				tx, err := s.BeginTransaction(ctx)
				if err == nil {
					var res neo4j.ResultWithContext
					res, err = tx.Run(ctx, "MERGE (n:Item {k: $k}) SET n.v = $v, n.w = $w RETURN n.v", map[string]any{"k": k, "v": v, "w": w})
					if err == nil {
						_, err = res.Consume(ctx)
					}
					if err == nil {
						err = tx.Commit(ctx)
					} else {
						_ = tx.Rollback(ctx)
					}
				}
				answered := time.Now()
				s.Close(ctx)
				cancel()
				mu.Lock()
				commits = append(commits, cvCommit{writer: w, k: k, v: v, answered: answered, err: err})
				mu.Unlock()
				if err != nil {
					writeErr.Add(1)
					continue
				}
				// A few more statements on the writer's connection.
				_, _ = cvAuto(wd, "MATCH (n:Item {k: $k}) RETURN n.v", map[string]any{"k": k})
				_, _ = cvAuto(wd, "RETURN 1", nil)
				find := func() (bool, error) {
					found := false
					err := cvRead(cd, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
						rows, err := cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": before})
						for _, r := range rows {
							if r.v == v {
								found = true
							}
						}
						return err
					})
					return found, err
				}
				found, err := find()
				if err != nil {
					checkErr.Add(1)
					continue
				}
				if !found {
					immediateMiss.Add(1)
					time.Sleep(200 * time.Millisecond)
					f2, _ := find()
					f3 := f2
					if !f2 {
						time.Sleep(1800 * time.Millisecond)
						f3, _ = find()
					}
					switch {
					case f2:
						lateFound200.Add(1)
					case f3:
						lateFound2s.Add(1)
					default:
						neverFound.Add(1)
					}
					mu.Lock()
					if len(missExamples) < 20 {
						missExamples = append(missExamples, fmt.Sprintf("writer=%d k=%d v=%d before=%v found200ms=%v found2s=%v", w, k, v, cvDecode(before), f2, f3))
					}
					mu.Unlock()
				}
			}
		}(w)
	}
	go func() { wwg.Wait(); close(writersDone) }()

	// Readers paging the feed by cursor.
	type readerResult struct {
		name      string
		given     []cvEvent
		pages     int
		empty     int
		errs      int
		backwards int
	}
	readerCfg := []struct {
		name      string
		selectors []any
		limit     int
	}{
		{"no-selector-limit37", nil, 37},
		{"node-label-selector-limit50", []any{map[string]any{"select": "n", "labels": []any{"Item"}}}, 50},
	}
	results := make([]*readerResult, len(readerCfg))
	for r, cfg := range readerCfg {
		results[r] = &readerResult{name: cfg.name}
		wg.Add(1)
		go func(res *readerResult, selectors []any, limit int) {
			defer wg.Done()
			d := newDriverLocked(&mu, newDriver)
			cursor := start
			last := cvDecode(start)
			drained := false
			for {
				var page []cvEvent
				err := cvRead(d, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
					var err error
					page, err = cvRows(ctx, tx, "CALL db.cdc.query($cursor, $selectors, $limit)"+cvQueryReturn, map[string]any{"cursor": cursor, "selectors": selectors, "limit": limit})
					return err
				})
				if err != nil {
					res.errs++
					time.Sleep(10 * time.Millisecond)
					continue
				}
				res.pages++
				for _, e := range page {
					if !last.less(e.pos) {
						res.backwards++
					}
					last = e.pos
					res.given = append(res.given, e)
				}
				if len(page) > 0 {
					cursor = page[len(page)-1].id
				}
				if len(page) < limit {
					res.empty++
					select {
					case <-writersDone:
						if drained {
							return
						}
						drained = true // one more full pass after the writers ended
					default:
						time.Sleep(5 * time.Millisecond)
					}
				}
			}
		}(results[r], cfg.selectors, cfg.limit)
	}

	// current() against query() in one read transaction.
	var cqRuns, cqEqual, cqQueryBeyond, cqCurrentBeyond, cqNoEvents, cqErr atomic.Int64
	var cqExamples []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		d := newDriverLocked(&mu, newDriver)
		prev := start
		for {
			select {
			case <-writersDone:
				return
			default:
			}
			var cur string
			var events []cvEvent
			err := cvRead(d, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
				res, err := tx.Run(ctx, "CALL db.cdc.current() YIELD id RETURN id", nil)
				if err != nil {
					return err
				}
				rec, err := res.Single(ctx)
				if err != nil {
					return err
				}
				cur, _ = rec.Values[0].(string)
				events, err = cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": prev})
				return err
			})
			if err != nil {
				cqErr.Add(1)
				continue
			}
			cqRuns.Add(1)
			c := cvDecode(cur)
			if len(events) == 0 {
				cqNoEvents.Add(1)
			} else {
				lastPos := events[len(events)-1].pos
				switch {
				case lastPos.commit == c.commit:
					cqEqual.Add(1)
				case lastPos.commit > c.commit:
					cqQueryBeyond.Add(1)
				default:
					cqCurrentBeyond.Add(1)
				}
				if lastPos.commit != c.commit {
					mu.Lock()
					if len(cqExamples) < 10 {
						cqExamples = append(cqExamples, fmt.Sprintf("current=%v query last=%v (from %v, %d events)", c, lastPos, cvDecode(prev), len(events)))
					}
					mu.Unlock()
				}
			}
			prev = cur
			time.Sleep(20 * time.Millisecond)
		}
	}()

	began := time.Now()
	time.Sleep(duration)
	close(stop)
	<-writersDone
	ran := time.Since(began)
	wg.Wait()
	burn.Wait()

	// The final full read, in a read transaction begun now.
	var final []cvEvent
	if err := cvRead(admin, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
		var err error
		final, err = cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": start})
		return err
	}); err != nil {
		t.Fatalf("final read: %v", err)
	}
	finalByV := map[int64][]cvEvent{}
	finalOrdered := 0
	for i, e := range final {
		finalByV[e.v] = append(finalByV[e.v], e)
		if i > 0 && !final[i-1].pos.less(e.pos) {
			finalOrdered++
		}
	}
	ok2, failed, missingFinal, dupFinal, failedListed := 0, 0, 0, 0, 0
	var missing []string
	for _, c := range commits {
		n := len(finalByV[c.v])
		if c.err != nil {
			failed++
			if n > 0 {
				failedListed++
			}
			continue
		}
		ok2++
		switch {
		case n == 0:
			missingFinal++
			if len(missing) < 20 {
				missing = append(missing, fmt.Sprintf("writer=%d k=%d v=%d answered=%s", c.writer, c.k, c.v, c.answered.Format(time.RFC3339Nano)))
			}
		case n > 1:
			dupFinal++
		}
	}
	committedV := map[int64]bool{}
	for _, c := range commits {
		if c.err == nil {
			committedV[c.v] = true
		}
	}
	unknownEvents := 0
	for _, e := range final {
		if !committedV[e.v] {
			unknownEvents++
		}
	}
	fmt.Printf("ZZCV ran %s; spin=%d; commits answered OK=%d, failed=%d (failed but listed=%d); final read events=%d, out-of-order=%d; committed writes missing from the final read=%d, listed more than once=%d; events not matching an OK commit=%d\n",
		ran.Round(time.Millisecond), spin.Load(), ok2, failed, failedListed, len(final), finalOrdered, missingFinal, dupFinal, unknownEvents)
	for _, s := range missing {
		fmt.Printf("ZZCV missing from final read: %s\n", s)
	}
	fmt.Printf("ZZCV per-commit check on another connection after the COMMIT answer: checked=%d immediate misses=%d (found after 200 ms=%d, after 2 s=%d, never=%d), check errors=%d, write errors=%d\n",
		int64(ok2)-checkErr.Load(), immediateMiss.Load(), lateFound200.Load(), lateFound2s.Load(), neverFound.Load(), checkErr.Load(), writeErr.Load())
	for _, s := range missExamples {
		fmt.Printf("ZZCV immediate miss: %s\n", s)
	}
	finalPos := map[cvPos]cvEvent{}
	for _, e := range final {
		finalPos[e.pos] = e
	}
	for _, res := range results {
		given := map[cvPos]int{}
		dups := 0
		var maxPos cvPos
		for _, e := range res.given {
			given[e.pos]++
			if given[e.pos] == 2 {
				dups++
			}
			if maxPos.less(e.pos) {
				maxPos = e.pos
			}
		}
		skipped, notInFinal, after := 0, 0, 0
		var skippedEx []string
		for _, e := range final {
			if maxPos.less(e.pos) {
				after++
			}
			if given[e.pos] == 0 && !maxPos.less(e.pos) {
				skipped++
				if len(skippedEx) < 10 {
					skippedEx = append(skippedEx, fmt.Sprintf("%v v=%d", e.pos, e.v))
				}
			}
		}
		for p := range given {
			if _, ok := finalPos[p]; !ok {
				notInFinal++
			}
		}
		keys := make([]cvPos, 0, len(given))
		for p := range given {
			keys = append(keys, p)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
		fmt.Printf("ZZCV reader %s: pages=%d (short pages=%d, errors=%d), events given=%d distinct=%d duplicates=%d, given out of order=%d, final-read events at or below its last id it was not given=%d, given but not in the final read=%d, final events after its last id=%d\n",
			res.name, res.pages, res.empty, res.errs, len(res.given), len(given), dups, res.backwards, skipped, notInFinal, after)
		for _, s := range skippedEx {
			fmt.Printf("ZZCV reader %s skipped: %s\n", res.name, s)
		}
	}
	fmt.Printf("ZZCV current() vs query() in one read transaction: runs=%d, query last commit == current=%d, query beyond current=%d, current beyond query=%d, no events=%d, errors=%d\n",
		cqRuns.Load(), cqEqual.Load(), cqQueryBeyond.Load(), cqCurrentBeyond.Load(), cqNoEvents.Load(), cqErr.Load())
	for _, s := range cqExamples {
		fmt.Printf("ZZCV current/query differ: %s\n", s)
	}
	if missingFinal > 0 || immediateMiss.Load() > 0 || finalOrdered > 0 {
		t.Errorf("committed writes missing: final=%d immediate=%d; out-of-order=%d", missingFinal, immediateMiss.Load(), finalOrdered)
	}
}

func newDriverLocked(mu *sync.Mutex, newDriver func() neo4j.DriverWithContext) neo4j.DriverWithContext {
	mu.Lock()
	defer mu.Unlock()
	return newDriver()
}
