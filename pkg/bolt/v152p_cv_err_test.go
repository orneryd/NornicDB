package bolt

// Research only (cdc-visibility, failed writes). The client-shape workload of
// zz_cv_client_test.go (8 Review writers with followers, 4 Other writers, CPU
// load, 180 s), recording every failed write: the step that returned the error
// (BEGIN, RUN, CONSUME, COMMIT), the error as the driver gives it (Neo4j code
// and message), and whether the write was stored, read at once from the node
// (each writer owns its nodes, so no one else writes them) and from the change
// feed after the run. ZZCV_CAPTURE=off runs the same with change capture off.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type cvFailure struct {
	label, id, step, code, msg string
	v                          int64
	at                         time.Duration
	elapsed                    time.Duration
	storedV                    any
	readErr                    error
}

func cvErrText(err error) (string, string) {
	var ne *neo4j.Neo4jError
	if errors.As(err, &ne) {
		return ne.Code, ne.Msg
	}
	return fmt.Sprintf("%T", err), err.Error()
}

func TestZZCVWriteErrors(t *testing.T) {
	duration := time.Duration(cvEnvInt("ZZCV_SECONDS", 180)) * time.Second
	reviewWriters := cvEnvInt("ZZCV_WRITERS", 8)
	otherWriters := cvEnvInt("ZZCV_OTHER_WRITERS", 4)
	burners := cvEnvInt("ZZCV_BURNERS_PER_CPU", 4) * runtime.NumCPU()
	captureOn := os.Getenv("ZZCV_CAPTURE") != "off"
	matchMode := os.Getenv("ZZCV_WRITE") == "match"
	reviewSet, otherSet := cvClientSet, "MERGE (o:Other {id: $id}) SET o.v = $v, o.x = $v"
	if matchMode {
		reviewSet = strings.Replace(reviewSet, "MERGE (r:Review", "MATCH (r:Review", 1)
		otherSet = strings.Replace(otherSet, "MERGE (o:Other", "MATCH (o:Other", 1)
	}
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
	if captureOn {
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
	start := ""
	if captureOn {
		startRows, err := cvAuto(admin, "CALL db.cdc.current() YIELD id RETURN id", nil)
		if err != nil {
			t.Fatalf("current: %v", err)
		}
		start = startRows[0][0].(string)
	}
	fmt.Printf("ZZCVE write=%q capture=%v Review writers=%d Other writers=%d burners=%d (NumCPU=%d GOMAXPROCS=%d) duration=%s\n",
		reviewSet[:5], captureOn, reviewWriters, otherWriters, burners, runtime.NumCPU(), runtime.GOMAXPROCS(0), duration)

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
	began := time.Now()
	var nextV atomic.Int64
	var okReview, okOther atomic.Int64
	var mu sync.Mutex
	var failures []cvFailure

	// write runs one explicit transaction and reports the step that failed.
	write := func(d neo4j.DriverWithContext, q string, p map[string]any) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		s := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer s.Close(ctx)
		tx, err := s.BeginTransaction(ctx)
		if err != nil {
			return "BEGIN", err
		}
		res, err := tx.Run(ctx, q, p)
		if err != nil {
			_ = tx.Rollback(ctx)
			return "RUN", err
		}
		if _, err = res.Consume(ctx); err != nil {
			_ = tx.Rollback(ctx)
			return "CONSUME", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "COMMIT", err
		}
		return "", nil
	}
	record := func(d neo4j.DriverWithContext, label, id, step string, err error, v int64, t0 time.Time) {
		code, msg := cvErrText(err)
		f := cvFailure{label: label, id: id, step: step, code: code, msg: msg, v: v, at: t0.Sub(began), elapsed: time.Since(t0)}
		rows, rerr := cvAuto(d, "MATCH (n:"+label+" {id: $id}) RETURN n.v", map[string]any{"id": id})
		f.readErr = rerr
		if rerr == nil && len(rows) == 1 {
			f.storedV = rows[0][0]
		}
		mu.Lock()
		failures = append(failures, f)
		mu.Unlock()
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
				v := nextV.Add(1)
				t0 := time.Now()
				if step, err := write(d, otherSet, map[string]any{"id": id, "v": v}); err != nil {
					record(d, "Other", id, step, err, v, t0)
				} else {
					okOther.Add(1)
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
			page := func() {
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
				v := nextV.Add(1)
				t0 := time.Now()
				step, err := write(wd, reviewSet, map[string]any{
					"id": id, "v": v, "w": w, "seq": i, "ts": time.Now().UnixNano(),
					"c1": "a", "c2": "b", "c3": "c", "c4": "d", "c5": "e", "c6": "f", "c7": "g", "c8": "h", "c9": "i", "c10": "j", "c11": "k",
				})
				if err != nil {
					record(wd, "Review", id, step, err, v, t0)
					continue
				}
				okReview.Add(1)
				page()
			}
		}(w)
	}
	time.Sleep(duration)
	close(stop)
	wg.Wait()
	ran := time.Since(began)
	burn.Wait()

	listed := map[int64]bool{}
	if captureOn {
		var final []cvEvent
		if err := cvRead(admin, func(ctx context.Context, tx neo4j.ExplicitTransaction) error {
			var err error
			final, err = cvRows(ctx, tx, "CALL db.cdc.query($from)"+cvQueryReturn, map[string]any{"from": start})
			return err
		}); err != nil {
			t.Fatalf("final read: %v", err)
		}
		for _, e := range final {
			listed[e.v] = true
		}
	}
	fmt.Printf("ZZCVE ran %s: Review OK=%d, Other OK=%d, failed=%d\n", ran.Round(time.Millisecond), okReview.Load(), okOther.Load(), len(failures))
	kinds := map[string]int{}
	stored := 0
	for _, f := range failures {
		key := fmt.Sprintf("%s %s %s: %s", f.label, f.step, f.code, f.msg)
		if i := strings.Index(key, "tx-"); i > 0 {
			key = key[:i] + "tx-…"
		}
		kinds[key]++
		if cvInt(f.storedV) == f.v {
			stored++
		}
		fmt.Printf("ZZCVE failure %s %s v=%d at=%s after=%s step=%s code=%s stored_v_read_at_once=%v (read err=%v) stored=%v listed_in_feed=%v msg=%q\n",
			f.label, f.id, f.v, f.at.Round(time.Millisecond), f.elapsed.Round(time.Millisecond), f.step, f.code, f.storedV, f.readErr, cvInt(f.storedV) == f.v, listed[f.v], f.msg)
	}
	keys := make([]string, 0, len(kinds))
	for k := range kinds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("ZZCVE kind x%d: %s\n", kinds[k], k)
	}
	fmt.Printf("ZZCVE summary: failures=%d, of which stored (node holds the failed write's v)=%d\n", len(failures), stored)
}
