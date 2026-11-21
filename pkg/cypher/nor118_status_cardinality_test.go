package cypher

// NOR-118 reproduction: a repeated-upload status statement whose logical join
// produces 1.8 million intermediate rows for 3,000 output rows.
//
// Statement: the application's status statement (labels and property names
// neutralised, shape unchanged; also in sig/repro1618/status.cypher of the
// reporting lab). For every occurrence u of one upload batch it resolves u's
// version v, then every occurrence f of that version, orders by
// f.uploaded_at, f.id, groups by (u, v) and keeps the first f.batch_id.
//
// Data shape (as recorded with the application's report): one batch of 3,000
// occurrence nodes, five version nodes, 600 occurrences per version, one
// OF_VERSION relationship per occurrence. Occurrence nodes carry 12 scalar
// properties (about 830 JSON bytes), version nodes 10 (about 1,770 bytes).
// Logical rows before grouping: 3,000 x 600 = 1,800,000. Output: 3,000 rows.
//
// The test also runs a same-result control that first deduplicates the
// versions, computes the first batch once per version and joins the batch's
// occurrences back (the form the application now uses; written here from its
// description, not copied). Both statements must return exactly the expected
// 3,000 rows, computed independently in Go.
//
// Cost assertion: the original statement must finish within NOR118_MAX_SECONDS
// (default 10) and its sampled peak live heap growth must stay within
// NOR118_MAX_HEAP_MIB (default 1024). These two bounds are ours, stated so that
// the test fails when the cost reported on the ticket is present; they are not
// an acceptance bound written by the maintainer. Every figure is printed.
//
// NOR118_PER_VERSION (default 600) scales the fixture: logical rows are
// 5 x k x k. A smaller warm-up size (k=120, 72,000 rows) runs first.
//
// Run (both parsers):
//
//	go test -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR118RepeatedUploadStatusCardinality$' -count=1 -v -timeout=30m
//	NORNICDB_PARSER=antlr go test -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR118RepeatedUploadStatusCardinality$' -count=1 -v -timeout=30m

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const nor118Status = `MATCH (u:Occurrence {batch_id: $batch_id})
MATCH (v:Version {id: u.version_id})
MATCH (f:Occurrence)-[:OF_VERSION]->(v)
WITH u, v, f ORDER BY f.uploaded_at, f.id
WITH u, v, collect(f.batch_id)[0] AS first_batch_id
RETURN u.id AS occurrence_id, u.member AS member, u.filename AS filename,
u.parent_occurrence_id AS parent_occurrence_id, u.document_id AS document_id,
u.version_id AS version_id, v.original_id AS original_id,
v.processing_status AS processing_status, v.processing_error_code AS code,
v.processing_error_message AS message, v.processing_next_action AS next_action,
v.processing_workflow_id AS processing_workflow_id, v.current_source_id AS source_id,
first_batch_id ORDER BY u.member, u.id`

const nor118DedupedControl = `MATCH (u0:Occurrence {batch_id: $batch_id})
WITH DISTINCT u0.version_id AS version_id
MATCH (v:Version {id: version_id})
CALL {
  WITH v
  MATCH (f:Occurrence)-[:OF_VERSION]->(v)
  WITH f ORDER BY f.uploaded_at, f.id
  RETURN collect(f.batch_id)[0] AS first_batch_id
}
MATCH (u:Occurrence {batch_id: $batch_id, version_id: v.id})
RETURN u.id AS occurrence_id, u.member AS member, u.filename AS filename,
u.parent_occurrence_id AS parent_occurrence_id, u.document_id AS document_id,
u.version_id AS version_id, v.original_id AS original_id,
v.processing_status AS processing_status, v.processing_error_code AS code,
v.processing_error_message AS message, v.processing_next_action AS next_action,
v.processing_workflow_id AS processing_workflow_id, v.current_source_id AS source_id,
first_batch_id ORDER BY u.member, u.id`

func nor118EnvInt(t *testing.T, name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return n
	}
	return def
}

// nor118Fixture loads 5 versions x perVersion occurrences, all of batch "batch",
// and returns the expected status rows in output order.
func nor118Fixture(t *testing.T, e *StorageExecutor, perVersion int) [][]interface{} {
	ctx := context.Background()
	versions := make([]interface{}, 5)
	var expected [][]interface{}
	type occ struct {
		member int64
		row    []interface{}
	}
	var occs []occ
	occurrences := make([]map[string]interface{}, 0, 5*perVersion)
	for v := 0; v < 5; v++ {
		version := fmt.Sprintf("version-%02d", v)
		vp := map[string]interface{}{"id": version, "original_id": fmt.Sprintf("original-%02d", v), "processing_status": "complete",
			"processing_error_code": "", "processing_error_message": "", "processing_next_action": "none", "processing_workflow_id": "batch",
			"current_source_id": fmt.Sprintf("source-%02d", v), "content_checksum": strings.Repeat("c", 64), "metadata": strings.Repeat("version metadata; ", 80)}
		versions[v] = vp
		for i := 0; i < perVersion; i++ {
			n := v*perVersion + i
			o := map[string]interface{}{"id": fmt.Sprintf("occurrence-%04d", n), "batch_id": "batch", "version_id": version,
				"uploaded_at": int64(1700000000 + i), "member": int64(n),
				"filename":             fmt.Sprintf("batch/segment-%02d/", v) + strings.Repeat("neutral-member-", 8) + ".txt",
				"parent_occurrence_id": "occurrence-root", "document_id": fmt.Sprintf("document-%04d-0000-0000-000000000000", n),
				"media_type": "text/plain", "content_size": int64(16384 + n), "checksum": strings.Repeat("a", 64),
				"metadata": strings.Repeat("occurrence metadata; ", 16)}
			occurrences = append(occurrences, o)
			occs = append(occs, occ{member: int64(n), row: []interface{}{o["id"], o["member"], o["filename"], o["parent_occurrence_id"], o["document_id"],
				o["version_id"], vp["original_id"], vp["processing_status"], vp["processing_error_code"], vp["processing_error_message"],
				vp["processing_next_action"], vp["processing_workflow_id"], vp["current_source_id"], "batch"}})
		}
	}
	if _, err := e.Execute(ctx, "UNWIND $rows AS row CREATE (v:Version) SET v = row RETURN count(*) AS n", map[string]interface{}{"rows": versions}); err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(occurrences); start += 100 {
		end := min(start+100, len(occurrences))
		batch := make([]interface{}, 0, end-start)
		for _, o := range occurrences[start:end] {
			batch = append(batch, o)
		}
		if _, err := e.Execute(ctx, "UNWIND $rows AS row MATCH (v:Version {id: row.version_id}) CREATE (u:Occurrence) SET u = row CREATE (u)-[:OF_VERSION]->(v) RETURN count(*) AS n",
			map[string]interface{}{"rows": batch}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		q    string
		want int
	}{{"MATCH (u:Occurrence) RETURN count(u)", 5 * perVersion}, {"MATCH (v:Version) RETURN count(v)", 5}, {"MATCH (:Occurrence)-[r:OF_VERSION]->(:Version) RETURN count(r)", 5 * perVersion}} {
		r, err := e.Execute(ctx, c.q, nil)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(r.Rows) != fmt.Sprintf("[[%d]]", c.want) {
			t.Fatalf("fixture %q = %v, want %d", c.q, r.Rows, c.want)
		}
	}
	sort.Slice(occs, func(i, j int) bool { return occs[i].member < occs[j].member })
	for _, o := range occs {
		expected = append(expected, o.row)
	}
	return expected
}

type nor118Measure struct {
	elapsed   time.Duration
	allocated uint64
	peakHeap  uint64 // sampled peak of live heap objects above the starting value
	rows      [][]interface{}
	err       error
}

func nor118Run(e *StorageExecutor, q string) nor118Measure {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	base := sample[0].Value.Uint64()
	var peak atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				metrics.Read(s)
				if v := s[0].Value.Uint64(); v > base && v-base > peak.Load() {
					peak.Store(v - base)
				}
			}
		}
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	r, err := e.Execute(context.Background(), q, map[string]interface{}{"batch_id": "batch"})
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	close(stop)
	wg.Wait()
	m := nor118Measure{elapsed: elapsed, allocated: after.TotalAlloc - before.TotalAlloc, peakHeap: peak.Load(), err: err}
	if r != nil {
		m.rows = r.Rows
	}
	return m
}

func nor118Compare(t *testing.T, name string, got, want [][]interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d rows, want %d", name, len(got), len(want))
		return
	}
	for i := range want {
		if fmt.Sprint(got[i]) != fmt.Sprint(want[i]) {
			t.Errorf("%s: row %d = %v, want %v", name, i, got[i], want[i])
			return
		}
	}
}

func TestNOR118RepeatedUploadStatusCardinality(t *testing.T) {
	parser := "default"
	if nconfig.IsANTLRParser() {
		parser = "antlr"
	}
	full := nor118EnvInt(t, "NOR118_PER_VERSION", 600)
	maxSeconds := nor118EnvInt(t, "NOR118_MAX_SECONDS", 10)
	maxHeapMiB := nor118EnvInt(t, "NOR118_MAX_HEAP_MIB", 1024)
	sizes := []int{full}
	if full > 120 {
		sizes = []int{120, full}
	}
	for _, k := range sizes {
		t.Run(fmt.Sprintf("per_version_%d", k), func(t *testing.T) {
			engine, err := storage.NewBadgerEngine(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close()
			e := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nor118"))
			e.cache = nil
			want := nor118Fixture(t, e, k)
			logical := 5 * k * k

			control := nor118Run(e, nor118DedupedControl)
			if control.err != nil {
				t.Errorf("control statement: %v", control.err)
			} else {
				nor118Compare(t, "control", control.rows, want)
			}
			orig := nor118Run(e, nor118Status)
			if orig.err != nil {
				t.Fatalf("status statement: %v (after %s)", orig.err, orig.elapsed)
			}
			nor118Compare(t, "status", orig.rows, want)

			t.Logf("NOR118 parser=%s go=%s gomaxprocs=%d occurrences=%d logical_rows=%d output_rows=%d status_seconds=%.3f status_alloc_bytes=%d status_alloc_per_logical_row=%.0f status_peak_heap_mib=%.1f control_seconds=%.3f control_alloc_bytes=%d control_peak_heap_mib=%.1f",
				parser, runtime.Version(), runtime.GOMAXPROCS(0), 5*k, logical, len(orig.rows), orig.elapsed.Seconds(), orig.allocated,
				float64(orig.allocated)/float64(logical), float64(orig.peakHeap)/(1<<20),
				control.elapsed.Seconds(), control.allocated, float64(control.peakHeap)/(1<<20))
			if k == full {
				if orig.elapsed > time.Duration(maxSeconds)*time.Second {
					t.Errorf("status statement over %d logical rows took %.3fs, over the %ds bound (same-result control: %.3fs)", logical, orig.elapsed.Seconds(), maxSeconds, control.elapsed.Seconds())
				}
				if orig.peakHeap > uint64(maxHeapMiB)<<20 {
					t.Errorf("status statement peak live heap growth %.1f MiB, over the %d MiB bound (same-result control: %.1f MiB)", float64(orig.peakHeap)/(1<<20), maxHeapMiB, float64(control.peakHeap)/(1<<20))
				}
			}
		})
	}
}
