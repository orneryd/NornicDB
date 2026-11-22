//go:build closeout_l2_e38_8

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E38.8: in UNWIND $rows AS x MATCH (a:Hub)-[r:R]->(b:Leaf) WHERE <terms>
// RETURN count(*) with the relationship pinned by elementId(r) = x.r, extra
// top-level AND terms add 0.03-0.24 ms per input row (2dbc67e96: 115.7 ms for
// the pin alone, 178.8 ms with two end-node id terms, 314.7 ms with 31
// property terms, 585.7 ms with 31 end-node id terms, over 2,000 rows).
//
// Threshold: the median time of the pin with 31 `AND elementId(a) = x.a`
// terms over the median time of the pin alone. The entry's defect is 5.1
// (585.7 / 115.7); evaluating 31 trivial equalities per row with a seed that
// does not re-read every term costs a few microseconds per row, about 1-2x of
// the 58 us row. A ratio above 3.0 is the defect. The other variants and the
// allocation figures are logged. Measured in one process on one store,
// median of 5 after a warm-up, GC before each run (as the entry).
//
// Default parser in the entry; the parser is the process's (NORNICDB_PARSER).

const (
	l2E38_8Leaves   = 2000
	l2E38_8Runs     = 5
	l2E38_8MaxRatio = 3.0
	l2E38_8Prefix   = "UNWIND $rows AS x MATCH (a:Hub)-[r:R]->(b:Leaf) WHERE "
	l2E38_8Suffix   = " RETURN count(*) AS c"
	l2E38_8Pin      = "elementId(r) = x.r"
)

type l2E38_8Fixture struct {
	exec *StorageExecutor
	rows []interface{}
	ids  [][3]string // r, a, b element ids
}

func l2E38_8Open(t *testing.T) *l2E38_8Fixture {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "seedprobe")
	exec := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	ctx := context.Background()
	if _, err := ns.CreateNode(ctx, &storage.Node{ID: "hub", Labels: []string{"Hub"}, Properties: map[string]interface{}{"k": "hub"}}); err != nil {
		t.Fatalf("hub: %v", err)
	}
	for i := 0; i < l2E38_8Leaves; i++ {
		leaf := storage.NodeID(fmt.Sprintf("leaf%d", i))
		if _, err := ns.CreateNode(ctx, &storage.Node{ID: leaf, Labels: []string{"Leaf"}, Properties: map[string]interface{}{"i": int64(i)}}); err != nil {
			t.Fatalf("leaf %d: %v", i, err)
		}
		if err := ns.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "hub", EndNode: leaf, Type: "R", Properties: map[string]interface{}{"w": int64(i)}}); err != nil {
			t.Fatalf("relationship %d: %v", i, err)
		}
	}
	res, err := exec.Execute(ctx, "MATCH (a:Hub)-[r:R]->(b:Leaf) RETURN elementId(r) AS r, elementId(a) AS a, elementId(b) AS b, r.w AS w ORDER BY w", nil)
	if err != nil {
		t.Fatalf("read ids: %v", err)
	}
	if len(res.Rows) != l2E38_8Leaves {
		t.Fatalf("seed has %d relationships, want %d", len(res.Rows), l2E38_8Leaves)
	}
	f := &l2E38_8Fixture{exec: exec}
	for _, row := range res.Rows {
		r, a, b := fmt.Sprint(row[0]), fmt.Sprint(row[1]), fmt.Sprint(row[2])
		f.rows = append(f.rows, map[string]interface{}{"r": r, "a": a, "b": b, "w": row[3]})
		f.ids = append(f.ids, [3]string{r, a, b})
	}
	return f
}

func l2E38_8Int(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}

type l2E38_8Figure struct {
	name          string
	low, med, max time.Duration
	allocMB       float64
}

func (f l2E38_8Figure) String() string {
	return fmt.Sprintf("%s: %.1f/%.1f/%.1f ms %.1f MB", f.name,
		float64(f.low.Microseconds())/1000, float64(f.med.Microseconds())/1000, float64(f.max.Microseconds())/1000, f.allocMB)
}

func (f *l2E38_8Fixture) run(t *testing.T, where string, rows []interface{}) {
	t.Helper()
	res, err := f.exec.Execute(context.Background(), l2E38_8Prefix+where+l2E38_8Suffix, map[string]interface{}{"rows": rows})
	if err != nil {
		t.Fatalf("WHERE %s: %v", where, err)
	}
	if len(res.Rows) != 1 || l2E38_8Int(res.Rows[0][0]) != int64(len(rows)) {
		t.Fatalf("WHERE %s returned %v, want %d", where, res.Rows, len(rows))
	}
}

func (f *l2E38_8Fixture) measure(t *testing.T, name, where string, rows []interface{}, runs int) l2E38_8Figure {
	t.Helper()
	f.run(t, where, rows)
	var times []time.Duration
	var allocs []uint64
	for i := 0; i < runs; i++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		f.run(t, where, rows)
		times = append(times, time.Since(started))
		runtime.ReadMemStats(&after)
		allocs = append(allocs, after.TotalAlloc-before.TotalAlloc)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	sort.Slice(allocs, func(i, j int) bool { return allocs[i] < allocs[j] })
	return l2E38_8Figure{name: name, low: times[0], med: times[len(times)/2], max: times[len(times)-1], allocMB: float64(allocs[len(allocs)/2]) / 1e6}
}

func l2E38_8Repeat(term string, n int) string {
	return strings.Repeat(" AND "+term, n)
}

func TestL2_e38_8_Repro(t *testing.T) {
	f := l2E38_8Open(t)
	pin := f.measure(t, "pin", l2E38_8Pin, f.rows, l2E38_8Runs)
	ends := f.measure(t, "pin+2 end ids", l2E38_8Pin+" AND elementId(a) = x.a AND elementId(b) = x.b", f.rows, l2E38_8Runs)
	props := f.measure(t, "pin+31 r.w>=0", l2E38_8Pin+l2E38_8Repeat("r.w >= 0", 31), f.rows, l2E38_8Runs)
	idTerms := f.measure(t, "pin+31 elementId(a)", l2E38_8Pin+l2E38_8Repeat("elementId(a) = x.a", 31), f.rows, l2E38_8Runs)
	figures := []l2E38_8Figure{pin, ends, props, idTerms}
	var lines []string
	for _, fig := range figures {
		perRow := float64(fig.med-pin.med) / float64(l2E38_8Leaves) / float64(time.Microsecond)
		lines = append(lines, fmt.Sprintf("%s (+%.1f us/row)", fig, perRow))
		t.Logf("%s; added per row vs pin: %.1f us", fig, perRow)
	}
	ratio := float64(idTerms.med) / float64(pin.med)
	propsRatio := float64(props.med) / float64(pin.med)
	endsRatio := float64(ends.med) / float64(pin.med)
	verdict := "OK"
	if ratio > l2E38_8MaxRatio {
		verdict = "DEFECT"
		t.Errorf("31 end-node id terms make the pinned statement %.2f times slower (bound %.1f): %v vs %v", ratio, l2E38_8MaxRatio, idTerms.med, pin.med)
	}
	t.Logf("L2RESULT E38.8 %s ratio31id=%.2f ratio31prop=%.2f ratio2ends=%.2f %s", verdict, ratio, propsRatio, endsRatio, strings.Join(lines, "; "))
}

// TestL2_e38_8_Control: the pin works as a seed (the statement that pins the
// relationship by elementId is at least 10 times faster per row than the same
// statement without the pin, on 200 rows), and every variant counts its rows.
func TestL2_e38_8_Control(t *testing.T) {
	f := l2E38_8Open(t)
	rows := f.rows[:200]
	noPin := f.measure(t, "no pin (r.w = x.w), 200 rows", "r.w = x.w", rows, 1)
	pin := f.measure(t, "pin, 200 rows", l2E38_8Pin, rows, 3)
	t.Logf("control: %s; %s", noPin, pin)
	if float64(noPin.med) < 10*float64(pin.med) {
		t.Errorf("the elementId pin does not seed the traversal: pinned %v vs unpinned %v on 200 rows", pin.med, noPin.med)
	}
}

// TestL2_e38_8_Cause calls the seed the entry names, relationshipIDSeedEdge
// (pkg/cypher/traversal_relationship_id_seed.go:23-73 at the pin; the loop
// over every top-level AND term with idEqualityLookup at :35-39), directly
// 2,000 times with literal ids, for the WHERE with 1 term and with 1 + 31 id
// terms. Logged only: a per-call cost that grows by the same amount as the
// statement's per-row cost places the added cost in the seed's per-row
// re-reading of the WHERE.
func TestL2_e38_8_Cause(t *testing.T) {
	f := l2E38_8Open(t)
	ctx := context.Background()
	pattern := "(a:Hub)-[r:R]->(b:Leaf)"
	for _, extra := range []int{0, 2, 31} {
		var wheres []string
		for _, ids := range f.ids {
			where := fmt.Sprintf("elementId(r) = '%s'", ids[0])
			if extra == 2 {
				where += fmt.Sprintf(" AND elementId(a) = '%s' AND elementId(b) = '%s'", ids[1], ids[2])
			} else if extra > 0 {
				where += strings.Repeat(fmt.Sprintf(" AND elementId(a) = '%s'", ids[1]), extra)
			}
			wheres = append(wheres, where)
		}
		pinned := 0
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		for _, where := range wheres {
			if edge, none, ok := f.exec.relationshipIDSeedEdge(ctx, pattern, where); ok && !none && edge != nil {
				pinned++
			}
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		t.Logf("L2CAUSE E38.8 relationshipIDSeedEdge extraTerms=%d: %v per call, %d B per call, pinned %d of %d",
			extra, elapsed/time.Duration(len(wheres)), (after.TotalAlloc-before.TotalAlloc)/uint64(len(wheres)), pinned, len(wheres))
	}
}
