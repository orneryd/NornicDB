package cypher

// v1.5.2 close-out lane L4 (application register rows): shared helpers.
// Test files only, on a scratch branch; not for merge. Every check prints one
// "L4_RESULT <json>" line with its decisive values and fails the test with
// t.Errorf when the expected behaviour does not hold.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func l4Report(t *testing.T, item, check string, pass bool, detail map[string]any) {
	t.Helper()
	if detail == nil {
		detail = map[string]any{}
	}
	detail["item"] = item
	detail["check"] = check
	detail["pass"] = pass
	data, err := json.Marshal(detail)
	if err != nil {
		data = []byte(fmt.Sprintf("%q", fmt.Sprint(detail)))
	}
	fmt.Fprintf(os.Stdout, "L4_RESULT %s\n", data)
	if !pass {
		t.Errorf("%s %s: expected behaviour does not hold: %s", item, check, data)
	}
}

func l4MemStore(t *testing.T, ns string) storage.Engine {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	return storage.NewNamespacedEngine(base, ns)
}

// l4DiskStack is the server's stack: disk Badger -> WAL -> namespace. close
// shuts it down so the directory can be opened again.
type l4DiskStack struct {
	dir    string
	base   *storage.BadgerEngine
	wal    *storage.WAL
	engine storage.Engine
}

func l4OpenDisk(t *testing.T, dir string) *l4DiskStack {
	t.Helper()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
	require.NoError(t, err)
	s := &l4DiskStack{dir: dir, base: base, wal: wal}
	s.engine = storage.NewNamespacedEngine(storage.NewWALEngine(base, wal), "l4")
	t.Cleanup(s.close)
	return s
}

func (s *l4DiskStack) close() {
	if s.wal != nil {
		_ = s.wal.Close()
		s.wal = nil
	}
	if s.base != nil {
		_ = s.base.Close()
		s.base = nil
	}
}

func l4Run(t *testing.T, exec *StorageExecutor, query string, params map[string]any) [][]any {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err, query)
	return result.Rows
}

// l4Batch runs an UNWIND range($start, $end - 1) AS k ... statement in
// batches of 2,000 rows.
func l4Batch(t *testing.T, exec *StorageExecutor, count int, query string) {
	t.Helper()
	for start := 0; start < count; start += 2000 {
		end := start + 2000
		if end > count {
			end = count
		}
		l4Run(t, exec, query, map[string]any{"start": int64(start), "end": int64(end)})
	}
}

// l4Cost is one statement's observed cost: PROFILE's storage reads (when
// the plan says they were observed) and the fastest of three timed runs.
type l4Cost struct {
	Hits     int64   `json:"hits"`
	Observed bool    `json:"hitsObserved"`
	Millis   float64 `json:"ms"`
	Rows     int     `json:"rows"`
}

func l4Measure(t *testing.T, exec *StorageExecutor, query string, params map[string]any) l4Cost {
	t.Helper()
	var cost l4Cost
	best := time.Duration(-1)
	for run := 0; run < 3; run++ {
		start := time.Now()
		result, err := exec.Execute(context.Background(), "PROFILE "+query, params)
		elapsed := time.Since(start)
		require.NoError(t, err, query)
		if best < 0 || elapsed < best {
			best = elapsed
		}
		cost.Rows = len(result.Rows)
		if plan, ok := result.Metadata["plan"].(*ExecutionPlan); ok && plan != nil {
			cost.Hits = plan.TotalDBHits
			cost.Observed = plan.TotalDBHitsObserved
		}
	}
	cost.Millis = float64(best.Microseconds()) / 1000
	return cost
}

// l4Scaling builds the same graph with a small and a large population
// (build's second argument) and compares the statement's cost. A statement
// whose work does not depend on the population keeps a ratio near 1; one
// that scans it grows with the population (100x here).
func l4Scaling(t *testing.T, item, check string, build func(t *testing.T, exec *StorageExecutor, population int), query string, params map[string]any, wantRows int) {
	t.Helper()
	const small, large = 200, 20000
	costs := map[int]l4Cost{}
	for _, population := range []int{small, large} {
		exec := NewStorageExecutor(l4MemStore(t, "l4"))
		build(t, exec, population)
		costs[population] = l4Measure(t, exec, query, params)
	}
	s, l := costs[small], costs[large]
	timeRatio := l.Millis / l4Max(s.Millis, 0.05)
	hitRatio := -1.0
	basis := "time"
	ratio := timeRatio
	if s.Observed && l.Observed && s.Hits > 0 {
		hitRatio = float64(l.Hits) / float64(s.Hits)
		basis = "dbHits"
		ratio = hitRatio
	}
	rowsOK := s.Rows == wantRows && l.Rows == wantRows
	l4Report(t, item, check, ratio < 5 && rowsOK, map[string]any{
		"small": s, "large": l, "populationSmall": small, "populationLarge": large,
		"hitRatio": hitRatio, "timeRatio": timeRatio, "basis": basis, "wantRows": wantRows,
	})
}

func l4Max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
