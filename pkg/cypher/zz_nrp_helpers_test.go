package cypher

// Neutral reproductions: shared helpers. Identical on both builds; reflection
// only adapts the context argument that storage methods gained between them.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func nrpCall(t testing.TB, target any, method string, args ...any) []reflect.Value {
	t.Helper()
	m := reflect.ValueOf(target).MethodByName(method)
	if !m.IsValid() {
		t.Fatalf("missing method %s", method)
	}
	values := []reflect.Value{}
	if m.Type().NumIn() == len(args)+1 {
		values = append(values, reflect.ValueOf(context.Background()))
	}
	for _, v := range args {
		values = append(values, reflect.ValueOf(v))
	}
	out := m.Call(values)
	if len(out) > 0 {
		last := out[len(out)-1]
		if last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !last.IsNil() {
			t.Fatalf("%s: %v", method, last.Interface())
		}
	}
	return out
}

// nrpStore opens an engine (on disk when disk is true) and a namespace over it.
func nrpStore(t testing.TB, disk bool, namespace string) storage.Engine {
	t.Helper()
	var b *storage.BadgerEngine
	var err error
	if disk {
		b, err = storage.NewBadgerEngine(t.TempDir())
	} else {
		b, err = storage.NewBadgerEngineInMemory()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return storage.NewNamespacedEngine(b, namespace)
}

// nrpExecutor builds an executor; cache=false turns the query-result cache
// off through the documented environment setting.
func nrpExecutor(t *testing.T, s storage.Engine, cache bool) *StorageExecutor {
	t.Helper()
	if cache {
		t.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "true")
	} else {
		t.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "false")
	}
	return NewStorageExecutor(s)
}

func nrpCtx() context.Context { return WithTransactionControl(context.Background()) }

func nrpExec(t testing.TB, e *StorageExecutor, q string, p map[string]any) *ExecuteResult {
	t.Helper()
	r, err := e.Execute(nrpCtx(), q, p)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return r
}

func nrpNode(t testing.TB, s storage.Engine, n *storage.Node) { nrpCall(t, s, "CreateNode", n) }

func nrpEdge(t testing.TB, s storage.Engine, id, a, b, kind string, p map[string]any) {
	nrpCall(t, s, "CreateEdge", &storage.Edge{ID: storage.EdgeID(id), StartNode: storage.NodeID(a), EndNode: storage.NodeID(b), Type: kind, Properties: p})
}

func nrpBulk(t testing.TB, s storage.Engine, nodes []*storage.Node) {
	nrpCall(t, s, "BulkCreateNodes", nodes)
}

func nrpService(t testing.TB, s storage.Engine, dimensions int) *search.Service {
	t.Helper()
	// nrpNewService is defined per build (zz_nrp_svc_head_test.go /
	// zz_nrp_svc_b6_test.go): both construct a service of exactly these dimensions.
	svc := nrpNewService(s, dimensions)
	if got := svc.VectorIndexDimensions(); got != dimensions {
		t.Fatalf("search service dimensions %d, want %d", got, dimensions)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func nrpHash(rows [][]any) string {
	b, _ := json.Marshal(rows)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

type nrpObs struct {
	elapsed time.Duration
	alloc   uint64
	rows    int
	hash    string
}

// nrpMeasure runs one statement (between its own BEGIN and ROLLBACK when tx),
// timing only the statement and reading TotalAlloc around it after a GC.
func nrpMeasure(t testing.TB, e *StorageExecutor, name, q string, p map[string]any, tx bool) nrpObs {
	t.Helper()
	if tx {
		nrpExec(t, e, "BEGIN", nil)
	}
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	start := time.Now()
	r, err := e.Execute(nrpCtx(), q, p)
	elapsed := time.Since(start)
	runtime.ReadMemStats(&b)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	o := nrpObs{elapsed: elapsed, alloc: b.TotalAlloc - a.TotalAlloc, rows: len(r.Rows), hash: nrpHash(r.Rows)}
	if tx {
		nrpExec(t, e, "ROLLBACK", nil)
	}
	t.Logf("NRP %s tx=%v ms=%.3f allocMB=%.3f rows=%d hash=%s", name, tx, float64(elapsed.Microseconds())/1000, float64(o.alloc)/1e6, o.rows, o.hash)
	return o
}
