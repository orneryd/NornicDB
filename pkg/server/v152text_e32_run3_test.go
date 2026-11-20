package server

// v1.5.2 verification, rebuilt from report entry 32, run 3: the default
// 5-minute journal snapshot interval and three commit paths. Seed 40,000 Doc
// nodes of about 20 KB with compaction off; reopen with the defaults; watch the
// snapshot directory every 2 s; send nothing for 330 s; then for 630 s run three
// writers (HTTP explicit commit, HTTP auto-commit, Bolt explicit commit), one
// write a second each, timing the commit. Measurement: calls over 1 s are
// printed with their start time; no bound is asserted. Uses the slow-boundaries
// probe helpers (v152p_slowb_probe_test.go, v152p_slowb2_test.go).

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
)

func TestV152TextE32Run3DefaultIntervalThreePaths(t *testing.T) {
	root := t.TempDir()
	tee := &slowbLogTee{start: time.Now()}
	l, closeSeed, _ := slowb2Open(t, root, slowb2Opts{compaction: -1}, tee)
	slowb2Seed(l, "d", 0, slowbEnvInt("E32R3_N", 40000), slowb2Text(slowbEnvInt("SLOWB_TEXT_BYTES", 20000)), "Doc")
	closeSeed()

	start := time.Now()
	tee2 := &slowbLogTee{start: start}
	l, closeRun, openTook := slowb2Open(t, root, slowb2Opts{}, tee2)
	defer closeRun()
	since := func() float64 { return time.Since(start).Seconds() }
	fmt.Printf("E32R3 reopened in %v\n", openTook)

	stopWatch := make(chan struct{})
	var watch sync.WaitGroup
	watch.Add(1)
	go func() {
		defer watch.Done()
		seen := map[string]bool{}
		for {
			select {
			case <-stopWatch:
				return
			case <-time.After(2 * time.Second):
			}
			entries, _ := os.ReadDir(filepath.Join(root, "snapshots"))
			for _, e := range entries {
				if !seen[e.Name()] {
					seen[e.Name()] = true
					info, _ := e.Info()
					size := int64(0)
					if info != nil {
						size = info.Size()
					}
					fmt.Printf("E32R3 SNAPSHOT +%.1fs %s %d bytes\n", since(), e.Name(), size)
				}
			}
		}
	}()
	time.Sleep(time.Duration(slowbEnvInt("E32R3_QUIET_SECONDS", 330)) * time.Second)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bs := bolt.NewWithDatabaseManager(&bolt.Config{Port: 0, MaxConnections: 16}, nil, l.srv.GetDatabaseManager())
	done := make(chan error, 1)
	go func() { done <- bs.Serve(listener) }()
	defer func() { _ = bs.Close(); <-done }()
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(ctx)

	type call struct {
		at, took float64
		ok       bool
	}
	var mu sync.Mutex
	calls := map[string][]call{}
	record := func(path string, at time.Time, took time.Duration, ok bool) {
		mu.Lock()
		calls[path] = append(calls[path], call{at.Sub(start).Seconds(), took.Seconds(), ok})
		mu.Unlock()
		if took > time.Second {
			fmt.Printf("E32R3 SLOW path=%s started=+%.2fs took=%.2fs ok=%v\n", path, at.Sub(start).Seconds(), took.Seconds(), ok)
		}
	}
	deadline := time.Now().Add(time.Duration(slowbEnvInt("E32R3_WRITE_SECONDS", 630)) * time.Second)
	var writers sync.WaitGroup
	writers.Add(3)
	go func() {
		defer writers.Done()
		for i := 0; time.Now().Before(deadline); i++ {
			opened, status, _ := l.post("/db/nornic/tx", map[string]any{"statements": []any{map[string]any{"statement": "CREATE (:Probe {k: 'httptx', i: $i})", "parameters": map[string]any{"i": i}}}})
			commit, _ := opened["commit"].(string)
			if status >= 300 || commit == "" {
				record("http-explicit", time.Now(), 0, false)
				time.Sleep(time.Second)
				continue
			}
			if j := strings.Index(commit, "/db/"); j > 0 {
				commit = commit[j:]
			}
			t0 := time.Now()
			result, status, took := l.post(commit, map[string]any{"statements": []any{}})
			errs, _ := result["errors"].([]any)
			record("http-explicit", t0, took, status < 300 && len(errs) == 0)
			time.Sleep(time.Second)
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; time.Now().Before(deadline); i++ {
			t0 := time.Now()
			result, status, took := l.post("/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": "CREATE (:Probe {k: 'httpauto', i: $i})", "parameters": map[string]any{"i": i}}}})
			errs, _ := result["errors"].([]any)
			record("http-auto", t0, took, status < 300 && len(errs) == 0)
			time.Sleep(time.Second)
		}
	}()
	go func() {
		defer writers.Done()
		for i := 0; time.Now().Before(deadline); i++ {
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic", AccessMode: neo4j.AccessModeWrite})
			tx, err := session.BeginTransaction(ctx)
			ok := err == nil
			var took time.Duration
			t0 := time.Now()
			if ok {
				result, err := tx.Run(ctx, "CREATE (:Probe {k: 'bolt', i: $i})", map[string]any{"i": i})
				if err == nil {
					_, err = result.Consume(ctx)
				}
				ok = err == nil
				t0 = time.Now()
				if ok {
					ok = tx.Commit(ctx) == nil
				}
				took = time.Since(t0)
			}
			_ = session.Close(ctx)
			record("bolt-explicit", t0, took, ok)
			time.Sleep(time.Second)
		}
	}()
	writers.Wait()
	time.Sleep(5 * time.Second)
	close(stopWatch)
	watch.Wait()
	for _, path := range []string{"http-explicit", "http-auto", "bolt-explicit"} {
		cs := calls[path]
		took := make([]float64, 0, len(cs))
		over, failed := 0, 0
		for _, c := range cs {
			took = append(took, c.took)
			if c.took > 1 {
				over++
			}
			if !c.ok {
				failed++
			}
		}
		sort.Float64s(took)
		median, max := 0.0, 0.0
		if len(took) > 0 {
			median, max = took[len(took)/2], took[len(took)-1]
		}
		t.Logf("E32R3 SUMMARY path=%s calls=%d over1s=%d failed=%d median=%.3fs max=%.2fs", path, len(cs), over, failed, median, max)
	}
}
