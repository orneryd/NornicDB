//go:build closeout_l2_e35_r2

package cypher_test

// E35-R2 (light form): with lazy warming, the first statement that calls the
// vector index carries the whole index build (962-987 s on the entry's
// 172,000-node store with 143,818 vectors of 2,048 values).
//
// Ported: the store is 30,000 Source nodes with one managed chunk vector of
// 256 values each (written with BulkCreateNodes), not the store of issue 37;
// the timed statement is the vector call of the entry's statement
// (CALL db.index.vector.queryNodes('default', ...)) without its other
// subqueries, which the entry's timing included but which are not the
// subject. HNSW is forced (NORNICDB_VECTOR_CPU_BRUTE_MAX_N=0, as above the
// brute-force threshold at the entry's size) and efConstruction is raised to
// 400 so the build cost stays decisive at this size. As in the entry: the
// executor's result cache is off, the service is put into lazy warming
// directly (SetLazyWarming with a WarmFunc that runs BuildIndexes), and each
// of ten statements runs as BEGIN, statement (timed), ROLLBACK.

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	l2E35R2Nodes = 30000
	l2E35R2Dims  = 256
	l2E35R2Runs  = 10
	// DEFECT when the first statement takes at least l2E35R2MinFirst and at
	// least l2E35R2Ratio times the median of the nine later ones: the first
	// statement then carries the build. A first statement answered without
	// the build (from storage or a partial index) stays within a few times
	// the later ones.
	l2E35R2MinFirst = 2 * time.Second
	l2E35R2Ratio    = 10.0
)

const l2E35R2Query = "CALL db.index.vector.queryNodes('default', 25, $query_input) YIELD node, score RETURN node.id AS id, score ORDER BY score DESC, id LIMIT 25"

func l2E35R2Env(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_EF_CONSTRUCTION", "400")
}

func l2E35R2Input() []float64 {
	v := make([]float64, l2E35R2Dims)
	for j := range v {
		v[j] = float64(float32((j*17)%101+1) / 101)
	}
	return v
}

// l2E35R2Store writes the fixed store: vectors drawn in node order from
// rand.New(rand.NewSource(37)), each value Float32().
func l2E35R2Store(t *testing.T) storage.Engine {
	t.Helper()
	ctx := context.Background()
	base, err := storage.NewBadgerEngine(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewBadgerEngine: %v", err)
	}
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "nornic")
	const batch = 1000
	r := rand.New(rand.NewSource(37))
	for start := 0; start < l2E35R2Nodes; start += batch {
		nodes := make([]*storage.Node, 0, batch)
		for i := start; i < start+batch && i < l2E35R2Nodes; i++ {
			v := make([]float32, l2E35R2Dims)
			for j := range v {
				v[j] = r.Float32()
			}
			nodes = append(nodes, &storage.Node{
				ID:              storage.NodeID(fmt.Sprintf("src-%06d", i)),
				Labels:          []string{"Source"},
				Properties:      map[string]any{"id": fmt.Sprintf("src-%06d", i), "document_id": fmt.Sprintf("d%d", i%1771)},
				ChunkEmbeddings: [][]float32{v},
				EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": 1, "embedding_dimensions": l2E35R2Dims},
			})
		}
		if err := store.BulkCreateNodes(ctx, nodes); err != nil {
			t.Fatalf("BulkCreateNodes from %d: %v", start, err)
		}
	}
	return store
}

type l2E35R2Outcome struct {
	mode       string
	buildDur   time.Duration // eager only
	times      []time.Duration
	rows       []int
	errs       []error
	stackWarm  bool // a goroutine was in EnsureWarm during the first statement
	stackBuild bool // a goroutine was in BuildIndexes during the first statement
	progress   search.BuildProgress
}

func l2E35R2Median(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	c := append([]time.Duration(nil), ds...)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	return c[len(c)/2]
}

func (o l2E35R2Outcome) first() time.Duration {
	if len(o.times) == 0 {
		return 0
	}
	return o.times[0]
}

func (o l2E35R2Outcome) laterMedian() time.Duration {
	if len(o.times) < 2 {
		return 0
	}
	return l2E35R2Median(o.times[1:])
}

func (o l2E35R2Outcome) String() string {
	return fmt.Sprintf("mode=%s eager_build=%v first=%v later_median=%v all=%v rows=%v errs=%v stack_EnsureWarm=%v stack_BuildIndexes=%v progress_during_first=%+v",
		o.mode, o.buildDur, o.first(), o.laterMedian(), o.times, o.rows, o.errs, o.stackWarm, o.stackBuild, o.progress)
}

// l2E35R2Run builds a store, a service (lazy or eager) and an executor, and
// runs the ten timed statements.
func l2E35R2Run(t *testing.T, lazy bool) l2E35R2Outcome {
	t.Helper()
	ctx := context.Background()
	store := l2E35R2Store(t)
	exec := cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: 0})
	svc := search.NewService(store, l2E35R2Dims, nil)
	t.Cleanup(func() { _ = svc.Close() })
	out := l2E35R2Outcome{mode: "eager"}
	if lazy {
		out.mode = "lazy"
		svc.SetLazyWarming(true, search.WarmFunc(func() { _ = svc.BuildIndexes(ctx) }))
	} else {
		start := time.Now()
		if err := svc.BuildIndexes(ctx); err != nil {
			t.Fatalf("BuildIndexes: %v", err)
		}
		out.buildDur = time.Since(start)
	}
	exec.SetSearchService(svc)
	params := map[string]any{"query_input": l2E35R2Input()}
	for run := 0; run < l2E35R2Runs; run++ {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		stop := make(chan struct{})
		sampled := make(chan struct{})
		if run == 0 {
			go func() {
				defer close(sampled)
				select {
				case <-stop:
					return
				case <-time.After(500 * time.Millisecond):
				}
				buf := make([]byte, 16<<20)
				n := runtime.Stack(buf, true)
				s := string(buf[:n])
				out.stackWarm = strings.Contains(s, ").EnsureWarm(")
				out.stackBuild = strings.Contains(s, ").BuildIndexes(")
				out.progress = svc.GetBuildProgress()
			}()
		} else {
			close(sampled)
		}
		start := time.Now()
		res, err := exec.Execute(ctx, l2E35R2Query, params)
		elapsed := time.Since(start)
		close(stop)
		<-sampled
		out.times = append(out.times, elapsed)
		out.errs = append(out.errs, err)
		n := -1
		if err == nil && res != nil {
			n = len(res.Rows)
		}
		out.rows = append(out.rows, n)
		if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
			t.Fatalf("ROLLBACK: %v", err)
		}
	}
	return out
}

func TestL2_e35_r2_Repro(t *testing.T) {
	l2E35R2Env(t)
	eager := l2E35R2Run(t, false)
	lazy := l2E35R2Run(t, true)
	t.Logf("eager: %s", eager)
	t.Logf("lazy: %s", lazy)
	for i, err := range lazy.errs {
		if err != nil || lazy.rows[i] != 25 {
			t.Fatalf("harness: lazy statement %d returned %d rows, err %v", i, lazy.rows[i], err)
		}
	}
	buildCost := eager.buildDur + eager.first()
	if buildCost < l2E35R2MinFirst {
		t.Logf("L2RESULT E35-R2 INCONCLUSIVE eager_build=%v eager_first=%v (build cost below %v at this size; scale the store up) %s", eager.buildDur, eager.first(), l2E35R2MinFirst, lazy)
		t.Fatalf("inconclusive: the eager build cost %v is below %v", buildCost, l2E35R2MinFirst)
	}
	later := lazy.laterMedian()
	ratio := 0.0
	if later > 0 {
		ratio = float64(lazy.first()) / float64(later)
	}
	line := fmt.Sprintf("lazy_first=%v lazy_later_median=%v ratio=%.1f eager_build=%v eager_first=%v eager_later_median=%v threshold=first>=%v&&ratio>=%.0f",
		lazy.first(), later, ratio, eager.buildDur, eager.first(), eager.laterMedian(), l2E35R2MinFirst, l2E35R2Ratio)
	if lazy.first() >= l2E35R2MinFirst && ratio >= l2E35R2Ratio {
		t.Logf("L2RESULT E35-R2 DEFECT %s", line)
		t.Errorf("with lazy warming the first vector statement took %v (%.1fx the later median %v): it carries the index build", lazy.first(), ratio, later)
		return
	}
	t.Logf("L2RESULT E35-R2 OK %s", line)
}

// Control: the eager service (BuildIndexes before the first statement):
// every statement returns 25 rows, and the statements after the first answer
// in normal time.
func TestL2_e35_r2_Control(t *testing.T) {
	l2E35R2Env(t)
	eager := l2E35R2Run(t, false)
	t.Logf("control eager: %s", eager)
	for i, err := range eager.errs {
		if err != nil || eager.rows[i] != 25 {
			t.Errorf("control: statement %d returned %d rows, err %v", i, eager.rows[i], err)
		}
	}
	if eager.laterMedian() >= l2E35R2MinFirst {
		t.Errorf("control: the later statements take %v (median) after an eager build", eager.laterMedian())
	}
}

// Cause: with lazy warming the statement's vector call reaches
// Service.EnsureWarm (pkg/search/search.go), which fires the WarmFunc once
// and waits for the build to finish; the statement therefore waits for
// BuildIndexes (and the HNSW construction it starts). The entry cites the
// server mode installed when every enabled index has warming 'lazy'
// (pkg/nornicdb/search_services.go). This logs, 500 ms into the first
// statement, whether a goroutine is in EnsureWarm and in BuildIndexes, and
// the build progress.
func TestL2_e35_r2_Cause(t *testing.T) {
	l2E35R2Env(t)
	lazy := l2E35R2Run(t, true)
	t.Logf("CAUSE stack_EnsureWarm=%v stack_BuildIndexes=%v progress=%+v first=%v later_median=%v", lazy.stackWarm, lazy.stackBuild, lazy.progress, lazy.first(), lazy.laterMedian())
}
