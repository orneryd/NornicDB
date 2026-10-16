package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func review1238Graph(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review1238"))
	c := context.Background()
	_, err := e.Execute(c, "CREATE (a:S {id: 1})-[:T {w: 2}]->(b:S {id: 2})-[:T {w: 3}]->(d:S {id: 4}), (a)-[:T {w: 3}]->(x:S {id: 3})-[:T {w: 4}]->(y:S {id: 5})-[:T {w: 5}]->(d)", nil)
	if err != nil {
		t.Fatal(err)
	}
	return e, c
}

func TestReview1238Predicates(t *testing.T) {
	cases := []struct {
		name, query string
		want        int64
	}{
		{"quoted-endpoint", "MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->())+(b:S WHERE `b`.id = 4) RETURN length(p) AS l", 2},
		{"inline-cross-property", "MATCH p = ANY SHORTEST (a:S {id: 1})((x)-[r:T {w: x.id + 1}]->(y))+(b:S {id: 4}) RETURN length(p) AS l", 2},
		{"inline-cross-where-control", "MATCH p = ANY SHORTEST (a:S {id: 1})((x)-[r:T WHERE r.w = x.id + 1]->(y))+(b:S {id: 4}) RETURN length(p) AS l", 2},
		{"cross-endpoints", "MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->())+(b:S WHERE b.id = a.id + 3) RETURN length(p) AS l", 2},
		{"bound-cross-endpoints-control", "MATCH (a:S {id: 1}) MATCH p = ANY SHORTEST (a)(()-[:T]->())+(b:S WHERE b.id = a.id + 3) RETURN length(p) AS l", 2},
		{"quoted-space-match", "MATCH (`a b`:S {id: 1}), (b:S {id: 4}) MATCH p = shortestPath((`a b`)-[:T*]->(b)) RETURN length(p) AS l", 2},
		{"quoted-space-value", "MATCH (`a b`:S {id: 1}), (b:S {id: 4}) RETURN length(shortestPath((`a b`)-[:T*]->(b))) AS l", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, c := review1238Graph(t)
			r, err := e.Execute(c, tc.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Rows) != 1 || r.Rows[0][0] != tc.want {
				t.Fatalf("query %s: got %v, want [[%d]]", tc.query, r.Rows, tc.want)
			}
		})
	}
}

func TestReview1238Replay(t *testing.T) {
	for _, bounds := range []string{"(a:S {id: 1})", "(a)"} {
		t.Run(bounds, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "replay"))
			c := context.Background()
			_, err := e.Execute(c, "CREATE (:S {id: 1})-[:T]->(:S {id: 2})-[:T]->(:S {id: 3})-[:T]->(:S {id: 4})", nil)
			if err != nil {
				t.Fatal(err)
			}
			prefix := ""
			if bounds == "(a)" {
				prefix = "MATCH (a:S {id: 1}), (b:S {id: 4}) "
			}
			q := prefix + "MATCH p = ALL SHORTEST " + bounds + "(()-[r:T]->())+(()-[s:T]->())+(b:S {id: 4}) RETURN size(r) AS r, size(s) AS s ORDER BY r, s"
			r, err := e.Execute(c, q, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s -> %v", q, r.Rows)
			for _, control := range []string{"MATCH (a:S {id: 1}) MATCH p = ALL SHORTEST (a)(()-[r:T]->())+(()-[s:T]->())+(b:S {id: 4}) RETURN size(r) AS r, size(s) AS s ORDER BY r, s", "MATCH p = SHORTEST 2 GROUPS (a:S {id: 1})(()-[r:T]->())+(()-[s:T]->())+(b:S {id: 4}) RETURN size(r) AS r, size(s) AS s ORDER BY r, s", "MATCH p = (a:S {id: 1})(()-[r:T]->())+(()-[s:T]->())+(b:S {id: 4}) RETURN size(r) AS r, size(s) AS s ORDER BY r, s"} {
				controlResult, controlErr := e.Execute(c, control, nil)
				if controlErr != nil {
					t.Fatal(controlErr)
				}
				t.Logf("control %s -> %v", control, controlResult.Rows)
			}
			if len(r.Rows) != 2 {
				t.Fatalf("got %v, want [[1 2] [2 1]]", r.Rows)
			}
		})
	}
}

func TestReview1238CancelledReconstruction(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reviewcancel"))
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	node := func(id string) *storage.Node {
		n := &storage.Node{ID: storage.NodeID(id), Labels: []string{"N"}, Properties: map[string]interface{}{}}
		if _, err := e.storage.CreateNode(n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	start := node("s")
	previous := start
	for i := 0; i < 10; i++ {
		left, right, next := node(fmt.Sprintf("l%d", i)), node(fmt.Sprintf("r%d", i)), node(fmt.Sprintf("n%d", i))
		for j, pair := range [][2]*storage.Node{{previous, left}, {previous, right}, {left, next}, {right, next}} {
			err := e.storage.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d-%d", i, j)), Type: "T", StartNode: pair[0].ID, EndNode: pair[1].ID, Properties: map[string]interface{}{}})
			if err != nil {
				t.Fatal(err)
			}
		}
		previous = next
	}
	plan, _, ok := compileSelectedPathPlan("(a)-[:T*]->(b)", "p", "", nil)
	if !ok {
		t.Fatal("compile")
	}
	s := e.newPathSearch(c, plan.forward, pipelineRow{}, -1, start)
	calls := 0
	rows, ok := s.uniformPair(start, previous, true, func(p pathSearchPath) (pipelineRow, bool) {
		calls++
		if calls == 1 {
			cancel()
		}
		return pipelineRow{}, true
	})
	t.Logf("uniform ok=%v rows=%d callbacks=%d after cancellation", ok, len(rows), calls)
	if calls > 1 {
		t.Fatalf("continued to bind %d paths after cancellation", calls)
	}
}

func TestReview1238BoundCompilation(t *testing.T) {
	for _, bound := range []int{100, 1000, 10000} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		p, _, ok := buildSelectedPathPlan(fmt.Sprintf("(a)(()-[:T]->()){1,%d}(b)", bound), "p", "", nil)
		runtime.ReadMemStats(&after)
		if !ok {
			t.Fatal("compile")
		}
		t.Logf("bound=%d slots=%d+%d allocated=%d elapsed=%s", bound, len(p.forward.slots), len(p.backward.slots), after.TotalAlloc-before.TotalAlloc, time.Since(start))
	}
}

func TestReview1238CompilationDeadline(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "compiledeadline"))
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r, err := e.Execute(c, "MATCH p = ANY SHORTEST (a)(()-[:T]->()){1,100000}(b) RETURN count(p)", nil)
	runtime.ReadMemStats(&after)
	t.Logf("empty graph; deadline=5ms; elapsed=%s allocated=%d result=%v err=%v", time.Since(start), after.TotalAlloc-before.TotalAlloc, r, err)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("compilation ignored deadline")
	}
}

func TestReview1238CancelledLevel(t *testing.T) {
	e, c := review1238Graph(t)
	ns, err := e.pipelineMatchRows(c, pipelineRow{}, "MATCH (a:S {id: 1})")
	if err != nil {
		t.Fatal(err)
	}
	source := ns[0]["a"].(*storage.Node)
	p, _, ok := compileSelectedPathPlan("(a)(()-[:T]->())+(b)", "p", "", nil)
	if !ok {
		t.Fatal("compile")
	}
	c, cancel := context.WithCancel(c)
	s := e.newPathSearch(c, p.forward, pipelineRow{}, -1, source)
	s.begin(source)
	before := len(s.products)
	cancel()
	s.nextLevel()
	t.Logf("products before cancellation=%d after canceled nextLevel=%d", before, len(s.products))
	if len(s.products) > before {
		t.Fatal("expanded after cancellation")
	}
}

func TestReview1238BoundList(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boundlist"))
	c := context.Background()
	_, err := e.Execute(c, "CREATE (a:S {id: 1})-[:T]->(b:S {id: 3}), (a)-[:T]->(:S {id: 2})-[:T]->(b)", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, form := range []string{"shortestPath((a)-[r:T*]->(b))", "allShortestPaths((a)-[r:T*]->(b))", "ANY SHORTEST (a)-[r:T*]->(b)", "ALL SHORTEST (a)-[r:T*]->(b)"} {
		q := "MATCH (a:S {id: 1})-[r:T*2]->(b:S {id: 3}) WITH a,b,r MATCH p = " + form + " RETURN length(p), size(r)"
		r, err := e.Execute(c, q, nil)
		if form == "shortestPath((a)-[r:T*]->(b))" || form == "allShortestPaths((a)-[r:T*]->(b))" {
			requireStatus(t, e, q, nil, "Neo.ClientError.Statement.SyntaxError", "Bound relationships not allowed in "+strings.SplitN(form, "(", 2)[0]+"(...)")
			continue
		}
		if err != nil {
			t.Error(err)
			continue
		}
		t.Logf("%s -> %v", q, r.Rows)
		if len(r.Rows) != 1 || r.Rows[0][0] != int64(2) {
			t.Errorf("got %v want [[2 2]]", r.Rows)
		}
	}
}

func TestReview1238ZeroSameEnds(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zerosameends"))
	c := context.Background()
	_, err := e.Execute(c, "CREATE (a:S {id: 1})-[:T]->(a)", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, form := range []string{"shortestPath((a)-[:T*0..]->(a))", "allShortestPaths((a)-[:T*0..]->(a))"} {
		q := "MATCH (a:S {id: 1}) MATCH p = " + form + " WHERE length(p) > 0 RETURN length(p)"
		r, err := e.Execute(c, q, nil)
		t.Logf("%s -> result=%v err=%v", q, r, err)
		if err == nil {
			t.Errorf("got %v want shortest path common-end-nodes error (verified Neo4j 5.26.30 EXPLAIN)", r.Rows)
		}
	}
}
