package server

// CPU profile of the 18b36a85d -> 1527cc83b harness workloads, summarized in
// process (no pprof tool needed): top functions by flat and cumulative samples
// and top source lines by flat samples. Runs only when REGR_PROFILE names a
// workload: subjects, view_range, keyed, writers.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	neo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func TestRegr18b1527Profile(t *testing.T) {
	workload := os.Getenv("REGR_PROFILE")
	if workload == "" {
		t.Skip("REGR_PROFILE not set")
	}
	e := regrStart(t)
	e.setup(t, regrSchema...)
	ctx := context.Background()
	var op func() error
	iterations := 0
	switch workload {
	case "subjects":
		e.fill(t, 8000, regrSubjectFill)
		if _, err := e.boltWrite(ctx, "CREATE (:PDGraphNode:PDDocument {id: 'd'}) "+
			"CREATE (:PDGraphNode:PDEntity {id: 'elsewhere', document_id: 'e', source_id: 's', origin: 'automatic'})", nil); err != nil {
			t.Fatal(err)
		}
		params := map[string]any{"targets": append(regrTwenty(), "absent", "elsewhere"), "document_id": "d", "source_id": "s", "current": true, "items": []any{}}
		op = func() error { _, err := e.boltRead(ctx, regrSubjectsStatement, params); return err }
		iterations = 150
	case "view_range":
		if _, err := e.boltWrite(ctx, "CREATE (s:PDSearchSource {id: 's'}) SET s += $pieces", map[string]any{"pieces": regrPieces(200)}); err != nil {
			t.Fatal(err)
		}
		query := "MATCH (s:PDSearchSource {id: 's'}) RETURN " + regrViewTextRange + " AS found"
		params := map[string]any{"start": int64(12), "end": int64(14)}
		op = func() error { _, err := e.boltAuto(ctx, query, params); return err }
		iterations = 4000
	case "keyed":
		e.fill(t, 500, regrSubjectFill)
		params := map[string]any{"id": "n7"}
		op = func() error {
			_, err := e.boltAuto(ctx, "MATCH (n:PDGraphNode {id: $id}) RETURN n.id AS id, n.name AS name", params)
			return err
		}
		iterations = 6000
	case "writers":
		e.setup(t, "CREATE (:PDCollection {id:'c'})",
			"UNWIND range(0, 15) AS w UNWIND range(0, 49) AS d CREATE (:PDDocument:PDGraphNode {id: 'd' + toString(w) + '-' + toString(d)})")
		turn := 0
		op = func() error {
			params := map[string]any{"id": fmt.Sprintf("d0-%d", turn%50), "token": fmt.Sprint(turn)}
			turn++
			_, err := e.boltTx(ctx, neo4j.AccessModeWrite, regrSmallWrites, params)
			return err
		}
		iterations = 3000
	default:
		t.Fatalf("unknown workload %q", workload)
	}
	for range 30 {
		if err := op(); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	for range iterations {
		if err := op(); err != nil {
			pprof.StopCPUProfile()
			t.Fatal(err)
		}
	}
	took := time.Since(began)
	pprof.StopCPUProfile()
	fmt.Printf("REGR_PROFILE workload=%s iterations=%d total=%s per_op=%s\n", workload, iterations, took, took/time.Duration(iterations))
	p, err := profile.Parse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	regrSummarize(p, iterations)
}

func regrSummarize(p *profile.Profile, iterations int) {
	valueIndex := len(p.SampleType) - 1 // cpu nanoseconds
	flat := map[string]int64{}
	cum := map[string]int64{}
	lines := map[string]int64{}
	var total int64
	for _, sample := range p.Sample {
		v := sample.Value[valueIndex]
		total += v
		seen := map[string]bool{}
		for depth, location := range sample.Location {
			for li, line := range location.Line {
				if line.Function == nil {
					continue
				}
				name := line.Function.Name
				if depth == 0 && li == 0 {
					flat[name] += v
					file := line.Function.Filename
					if at := strings.LastIndex(file, "/"); at >= 0 {
						file = file[at+1:]
					}
					lines[fmt.Sprintf("%s %s:%d", name, file, line.Line)] += v
				}
				if !seen[name] {
					seen[name] = true
					cum[name] += v
				}
			}
		}
	}
	show := func(title string, m map[string]int64, n int, filter func(string) bool) {
		type kv struct {
			k string
			v int64
		}
		var rows []kv
		for k, v := range m {
			if filter == nil || filter(k) {
				rows = append(rows, kv{k, v})
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].v > rows[j].v })
		fmt.Printf("REGR_PROFILE %s (total %.3fs, %.1f us/op)\n", title, float64(total)/1e9, float64(total)/1e3/float64(iterations))
		for i, r := range rows {
			if i >= n {
				break
			}
			fmt.Printf("REGR_PROFILE   %6.2f%% %9.1f us/op  %s\n", 100*float64(r.v)/float64(total), float64(r.v)/1e3/float64(iterations), r.k)
		}
	}
	show("flat", flat, 40, nil)
	show("cum", cum, 60, nil)
	show("cum nornicdb", cum, 80, func(k string) bool { return strings.Contains(k, "nornicdb/pkg/") })
	show("lines flat", lines, 40, nil)
}
