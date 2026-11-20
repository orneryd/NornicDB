package cypher

// Earlier-issues batch 5, item L428 (untracked probe; never committed):
// does a statement's deadline reach the composite database lookup that graph
// functions make (graphFunctionStorage -> manager.GetStorageForUse)? The
// manager adapter below delegates to the real multidb manager and, while its
// gate is closed, holds every lookup (as a catalog transition holding the
// manager's lock would). Each lookup's caller is recorded.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

type ei5GatedManager struct {
	*testDatabaseManagerAdapter
	mu      sync.Mutex
	gate    chan struct{}
	calls   atomic.Int64
	callers []string
}

func (g *ei5GatedManager) GetStorageForUse(name string, authToken string) (interface{}, error) {
	g.calls.Add(1)
	buf := make([]byte, 16384)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])
	caller := "other"
	switch {
	case strings.Contains(stack, "graphFunctionStorage"):
		caller = "graphFunctionStorage"
	case strings.Contains(stack, "executor_use.go"):
		caller = "executor_use"
	case strings.Contains(stack, "executor_fabric.go"):
		caller = "executor_fabric"
	}
	g.mu.Lock()
	g.callers = append(g.callers, name+"<-"+caller)
	gate := g.gate
	g.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return g.testDatabaseManagerAdapter.GetStorageForUse(name, authToken)
}

func (g *ei5GatedManager) take() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := g.callers
	g.callers = nil
	return out
}

func ei5CompositeExec(t *testing.T) (*StorageExecutor, *ei5GatedManager) {
	mgr, adapter := ei5CompositeManager(t)
	gated := &ei5GatedManager{testDatabaseManagerAdapter: adapter}
	for db, v := range map[string]string{"pother": "o", "pthird": "t"} {
		store, err := mgr.GetStorage(db)
		if err != nil {
			t.Fatal(err)
		}
		exec := NewStorageExecutor(store)
		exec.SetDatabaseManager(gated)
		if _, err := exec.Execute(context.Background(), "CREATE (a:EI {v: $v})-[:R {w: 1}]->(b:EI {v: $v + '2'})", map[string]interface{}{"v": v}); err != nil {
			t.Fatal(err)
		}
	}
	store, err := mgr.GetStorage("pcomp")
	if err != nil {
		t.Fatal(err)
	}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(gated)
	return exec, gated
}

func ei5Exec(exec *StorageExecutor, ctx context.Context, q string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC: %v", r)
		}
	}()
	res, err := exec.Execute(ctx, q, nil)
	if err != nil {
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return "ERROR: " + msg
	}
	return fmt.Sprintf("rows=%v", res.Rows)
}

func TestEi5CompositeGraphFunctionDeadline(t *testing.T) {
	shapes := []string{
		"USE pcomp.other MATCH (n:EI) RETURN n.v ORDER BY n.v",
		"USE pcomp.other MATCH (a:EI)-[r:R]->(b) RETURN startNode(r).v, endNode(r).v",
		"USE pcomp.other MATCH (n:EI) RETURN gds.util.asNode(id(n)).v ORDER BY n.v",
		"USE pcomp.other MATCH (n:EI) RETURN apoc.node.degree(n) ORDER BY n.v",
		"USE pcomp.other MATCH (n:EI {v: 'o'}) RETURN size([(n)-[:R]->(m) | m]) AS c",
		"MATCH (n:EI) RETURN n.v ORDER BY n.v",
		"CALL { USE pcomp.other MATCH (a:EI)-[r:R]->() RETURN r } RETURN startNode(r).v",
	}
	exec, gated := ei5CompositeExec(t)
	gated.take()
	for _, q := range shapes {
		start := time.Now()
		out := ei5Exec(exec, context.Background(), q)
		t.Logf("EI5_COMPOSITE ungated %-90s => %s in %v; lookups=%v", q, out, time.Since(start).Round(time.Microsecond), gated.take())
	}
	// Second pass: each statement with a 200 ms deadline while lookups are held for 3 s.
	for _, q := range shapes {
		gated.mu.Lock()
		gated.gate = make(chan struct{})
		gate := gated.gate
		gated.mu.Unlock()
		timer := time.AfterFunc(3*time.Second, func() { close(gate) })
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		start := time.Now()
		out := ei5Exec(exec, ctx, q)
		elapsed := time.Since(start)
		cancel()
		timer.Stop()
		select {
		case <-gate:
		default:
			close(gate)
		}
		gated.mu.Lock()
		gated.gate = nil
		gated.mu.Unlock()
		t.Logf("EI5_COMPOSITE gated deadline=200ms hold=3s %-90s => %s in %v; lookups=%v", q, out, elapsed.Round(time.Millisecond), gated.take())
	}
	_ = multidb.ConstituentRef{}
}

// TestEi5CompositeLookupUnderCatalogChurn: the real manager (no gate), while
// another goroutine creates and drops databases in a loop (catalog
// transitions), 300 composite statements that use graph functions run with a
// 200 ms deadline each. Reports the latency distribution and the errors.
func TestEi5CompositeLookupUnderCatalogChurn(t *testing.T) {
	mgr, adapter := ei5CompositeManager(t)
	gated := &ei5GatedManager{testDatabaseManagerAdapter: adapter}
	for db, v := range map[string]string{"pother": "o", "pthird": "t"} {
		store, err := mgr.GetStorage(db)
		if err != nil {
			t.Fatal(err)
		}
		exec := NewStorageExecutor(store)
		exec.SetDatabaseManager(gated)
		if _, err := exec.Execute(context.Background(), "CREATE (a:EI {v: $v})-[:R {w: 1}]->(b:EI {v: $v + '2'})", map[string]interface{}{"v": v}); err != nil {
			t.Fatal(err)
		}
	}
	store, err := mgr.GetStorage("pcomp")
	if err != nil {
		t.Fatal(err)
	}
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(gated)
	stop := make(chan struct{})
	var churns, churnErrs atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := ei5Churn(mgr, i); err != nil {
				churnErrs.Add(1)
			}
			churns.Add(1)
		}
	}()
	var lat []time.Duration
	errs := map[string]int{}
	for i := 0; i < 300; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		start := time.Now()
		out := ei5Exec(exec, ctx, "USE pcomp.other MATCH (a:EI)-[r:R]->(b) RETURN startNode(r).v, gds.util.asNode(id(b)).v")
		lat = append(lat, time.Since(start))
		cancel()
		if !strings.HasPrefix(out, "rows=") {
			errs[out]++
		}
	}
	close(stop)
	<-done
	sortDur := func(d []time.Duration) {
		for i := 1; i < len(d); i++ {
			for j := i; j > 0 && d[j] < d[j-1]; j-- {
				d[j], d[j-1] = d[j-1], d[j]
			}
		}
	}
	sortDur(lat)
	t.Logf("EI5_CHURN statements=300 p50=%v p99=%v max=%v errors=%v churn_cycles=%d churn_errors=%d lookups=%d", lat[150], lat[296], lat[299], errs, churns.Load(), churnErrs.Load(), gated.calls.Load())
}
