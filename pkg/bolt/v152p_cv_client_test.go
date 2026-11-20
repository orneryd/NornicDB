package bolt

// Research only (cdc-visibility, the client's shape). Capture mode DIFF,
// served over Bolt, under CPU load, for a fixed time:
//   - "Review" writers, each on its own driver, each owning existing nodes:
//     an explicit transaction MERGE (r:Review {id: $id}) SET of fifteen
//     properties, four of which change each time (v unique per commit), COMMIT;
//   - each Review writer has a follower on another driver (another connection)
//     with its own cursor (the id of the last row it was returned): after the
//     writer's COMMIT is answered, the follower pages
//     db.cdc.query($cursor, [{select:'n', labels:['Review']}], 25) until a page
//     is short, each statement its own read transaction (even followers: an
//     auto-commit statement in a read session; odd followers: a one-statement
//     explicit read transaction), and looks for the write's event (by v) among
//     every row it has been given; a miss is looked for again after 200 ms
//     and 2 s with the same paging;
//   - "Other" writers commit on another label at the same time (the selector
//     excludes them).
// Afterwards every committed Review write is counted against a final full read
// and against what each follower was given. Uses the helpers of
// zz_cv_feed_test.go, rfbConfig and rfNewManager.

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const cvClientSet = "MERGE (r:Review {id: $id}) SET r.v = $v, r.w = $w, r.seq = $seq, r.ts = $ts, " +
	"r.c1 = $c1, r.c2 = $c2, r.c3 = $c3, r.c4 = $c4, r.c5 = $c5, r.c6 = $c6, r.c7 = $c7, r.c8 = $c8, r.c9 = $c9, r.c10 = $c10, r.c11 = $c11"

func TestZZCVClientShape(t *testing.T) {
	duration := time.Duration(cvEnvInt("ZZCV_SECONDS", 180)) * time.Second
	reviewWriters := cvEnvInt("ZZCV_WRITERS", 8)
	otherWriters := cvEnvInt("ZZCV_OTHER_WRITERS", 4)
	burners := cvEnvInt("ZZCV_BURNERS_PER_CPU", 4) * runtime.NumCPU()
	const limit = 25
	selectors := []any{map[string]any{"select": "n", "labels": []any{"Review"}}}
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
	capture.SetChangeCapture("nornic", storage.ChangeCaptureDiff)
	srv := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 256}, nil, m)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	defer func() { _ = srv.Close(); <-done }()
	uri := fmt.Sprintf("bolt://127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	var dmu sync.Mutex
	var drivers []neo4j.DriverWithContext
	newDriver := func() neo4j.DriverWithContext {
		dmu.Lock()
		defer dmu.Unlock()
		d, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
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
	for _, q := range []string{
		"CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
		"CREATE CONSTRAINT other_id IF NOT EXISTS FOR (o:Other) REQUIRE o.id IS UNIQUE",
		"UNWIND range(0, $n - 1) AS i CREATE (:Review {id: 'review-' + toString(i), v: -1, c1: 'a', c2: 'b', c3: 'c', c4: 'd', c5: 'e', c6: 'f', c7: 'g', c8: 'h', c9: 'i', c10: 'j', c11: 'k'})",
		"UNWIND range(0, $n - 1) AS i CREATE (:Other {id: 'other-' + toString(i), v: -1})",
	} {
		if _, err := cvAuto(admin, q, map[string]any{"n": (reviewWriters + otherWriters) * 20}); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	startRows, err := cvAuto(admin, "CALL db.cdc.current() YIELD id RETURN id", nil)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	start := startRows[0][0].(string)
	fmt.Printf("ZZCVC start cursor pos=%v; capture DIFF; Review writers=%d (each with a follower), Other writers=%d, burners=%d (NumCPU=%d GOMAXPROCS=%d), duration=%s, selector labels [Review], limit %d\n",
		cvDecode(start), reviewWriters, otherWriters, burners, runtime.NumCPU(), runtime.GOMAXPROCS(0), duration, limit)

	stop := make(chan struct{})
	var burn sync.WaitGroup
	for i := 0; i < burners; i++ {
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

	var nextV atomic.Int64
	var mu sync.Mutex
	type commit struct {
		writer int
		id     string
		v      int64
		err    error
	}
	var commits []commit
	var checked, immediateMiss, late200, late2s, never, followErr, writeErr, otherCommits, otherErr atomic.Int64
	var missEx []string
	given := make([][]cvEvent, reviewWriters)

	write := func(d neo4j.DriverWithContext, q string, p map[string]any) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer s.Close(ctx)
		tx, err := s.BeginTransaction(ctx)
		if err != nil {
			return err
		}
		res, err := tx.Run(ctx, q, p)
		if err == nil {
			_, err = res.Consume(ctx)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		return tx.Commit(ctx)
	}

	var wg sync.WaitGroup
	for w := 0; w < otherWriters; w++ {
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
				id := fmt.Sprintf("other-%d", (reviewWriters+w)*20+i%20)
				if err := write(d, "MERGE (o:Other {id: $id}) SET o.v = $v, o.x = $v", map[string]any{"id": id, "v": nextV.Add(1)}); err != nil {
					otherErr.Add(1)
				} else {
					otherCommits.Add(1)
				}
			}
		}(w)
	}
	for w := 0; w < reviewWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wd, fd := newDriver(), newDriver()
			cursor := start
			seen := map[int64]bool{}
			// page pages from the follower's cursor until a page is short; each
			// statement its own read transaction.
			page := func() error {
				for {
					var rows []cvEvent
					p := map[string]any{"cursor": cursor, "selectors": selectors, "limit": limit}
					q := "CALL db.cdc.query($cursor, $selectors, $limit)" + cvQueryReturn
					var err error
					if w%2 == 0 {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
						s := fd.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic", AccessMode: neo4j.AccessModeRead})
						var res neo4j.ResultWithContext
						res, err = s.Run(ctx, q, p)
						if err == nil {
							var recs []*neo4j.Record
							recs, err = res.Collect(ctx)
							for _, r := range recs {
								id, _ := r.Values[0].(string)
								op, _ := r.Values[2].(string)
								v := int64(-1)
								if r.Values[1] != nil {
									v = cvInt(r.Values[1])
								}
								rows = append(rows, cvEvent{pos: cvDecode(id), id: id, v: v, op: op})
							}
						}
						s.Close(ctx)
						cancel()
					} else {
						err = cvRead(fd, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
							var err error
							rows, err = cvRows(ctx, tx, q, p)
							return err
						})
					}
					if err != nil {
						return err
					}
					for _, e := range rows {
						seen[e.v] = true
					}
					mu.Lock()
					given[w] = append(given[w], rows...)
					mu.Unlock()
					if len(rows) > 0 {
						cursor = rows[len(rows)-1].id
					}
					if len(rows) < limit {
						return nil
					}
				}
			}
			for i := 0; ; i++ {
				select {
				case <-stop:
					_ = page()
					return
				default:
				}
				id := fmt.Sprintf("review-%d", w*20+i%20)
				v := nextV.Add(1)
				err := write(wd, cvClientSet, map[string]any{
					"id": id, "v": v, "w": w, "seq": i, "ts": time.Now().UnixNano(),
					"c1": "a", "c2": "b", "c3": "c", "c4": "d", "c5": "e", "c6": "f", "c7": "g", "c8": "h", "c9": "i", "c10": "j", "c11": "k",
				})
				mu.Lock()
				commits = append(commits, commit{writer: w, id: id, v: v, err: err})
				mu.Unlock()
				if err != nil {
					writeErr.Add(1)
					continue
				}
				// After the COMMIT's answer: the follower pages and looks for the write.
				if err := page(); err != nil {
					followErr.Add(1)
					continue
				}
				checked.Add(1)
				if seen[v] {
					continue
				}
				immediateMiss.Add(1)
				time.Sleep(200 * time.Millisecond)
				_ = page()
				f200 := seen[v]
				if !f200 {
					time.Sleep(1800 * time.Millisecond)
					_ = page()
				}
				f2 := seen[v]
				switch {
				case f200:
					late200.Add(1)
				case f2:
					late2s.Add(1)
				default:
					never.Add(1)
				}
				mu.Lock()
				if len(missEx) < 20 {
					missEx = append(missEx, fmt.Sprintf("writer=%d id=%s v=%d cursor-after=%v found200ms=%v found2s=%v", w, id, v, cvDecode(cursor), f200, f2))
				}
				mu.Unlock()
			}
		}(w)
	}
	began := time.Now()
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	ran := time.Since(began)
	burn.Wait()

	var final []cvEvent
	if err := cvRead(admin, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
		var err error
		final, err = cvRows(ctx, tx, "CALL db.cdc.query($from, $selectors)"+cvQueryReturn, map[string]any{"from": start, "selectors": selectors})
		return err
	}); err != nil {
		t.Fatalf("final read: %v", err)
	}
	byV := map[int64]int{}
	outOfOrder := 0
	for i, e := range final {
		byV[e.v]++
		if i > 0 && !final[i-1].pos.less(e.pos) {
			outOfOrder++
		}
	}
	okN, failed, missing, dup := 0, 0, 0, 0
	var missingEx []string
	for _, c := range commits {
		if c.err != nil {
			failed++
			continue
		}
		okN++
		switch byV[c.v] {
		case 0:
			missing++
			if len(missingEx) < 20 {
				missingEx = append(missingEx, fmt.Sprintf("writer=%d id=%s v=%d", c.writer, c.id, c.v))
			}
		case 1:
		default:
			dup++
		}
	}
	fmt.Printf("ZZCVC ran %s: Review commits answered OK=%d, failed=%d; Other commits OK=%d, failed=%d; final read (Review selector) events=%d, out-of-order=%d; OK Review commits missing from the final read=%d, listed more than once=%d\n",
		ran.Round(time.Millisecond), okN, failed, otherCommits.Load(), otherErr.Load(), len(final), outOfOrder, missing, dup)
	for _, s := range missingEx {
		fmt.Printf("ZZCVC missing from final read: %s\n", s)
	}
	fmt.Printf("ZZCVC follower checks after the COMMIT answer: checked=%d, not listed at once=%d (listed after 200 ms=%d, after 2 s=%d, never=%d); follower errors=%d, write errors=%d\n",
		checked.Load(), immediateMiss.Load(), late200.Load(), late2s.Load(), never.Load(), followErr.Load(), writeErr.Load())
	for _, s := range missEx {
		fmt.Printf("ZZCVC miss: %s\n", s)
	}
	finalPos := map[cvPos]bool{}
	for _, e := range final {
		finalPos[e.pos] = true
	}
	for w := range given {
		got := map[cvPos]int{}
		var maxPos cvPos
		backwards := 0
		for i, e := range given[w] {
			got[e.pos]++
			if i > 0 && !given[w][i-1].pos.less(e.pos) {
				backwards++
			}
			if maxPos.less(e.pos) {
				maxPos = e.pos
			}
		}
		skipped, after, extra, dups := 0, 0, 0, 0
		for _, e := range final {
			if maxPos.less(e.pos) {
				after++
			} else if got[e.pos] == 0 {
				skipped++
			}
		}
		for p, n := range got {
			if !finalPos[p] {
				extra++
			}
			if n > 1 {
				dups++
			}
		}
		mode := "auto-commit read"
		if w%2 == 1 {
			mode = "one-statement read transaction"
		}
		fmt.Printf("ZZCVC follower %d (%s): given=%d distinct=%d duplicates=%d out-of-order=%d; final events at or below its last id not given=%d; after its last id=%d; given but not in the final read=%d\n",
			w, mode, len(given[w]), len(got), dups, backwards, skipped, after, extra)
	}
	if missing > 0 || immediateMiss.Load() > 0 || outOfOrder > 0 {
		t.Errorf("missing=%d immediate=%d out-of-order=%d", missing, immediateMiss.Load(), outOfOrder)
	}
}
