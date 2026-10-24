package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"testing"
)

func TestSelectedPathCancelledReconstruction(t *testing.T) {
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
	fixtureAdmission1698Context, fixtureAdmission1698 := statementTestContext(c, ("MATCH "+("(a)-[:T*]->(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1698Context
	s := e.relationshipExecutionFor(fixtureAdmission1698, fixtureAdmission1698Context, "(a)-[:T*]->(b)", "").newPathSearch(c, plan.forward, pipelineRow{}, -1, start)
	calls := 0
	fixtureAdmission1699Context, fixtureAdmission1699 := statementTestContext(context.Background(), ("MATCH "+("(a)-[:T*]->(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1699Context
	rows, ok := s.uniformPair(fixtureAdmission1699, start, previous, true, func(p pathSearchPath) (pipelineRow, bool) {
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

func TestSelectedPathCancelledLevel(t *testing.T) {
	e, c := newShortestSelectorExecutor(t)
	fixtureAdmission773Context, fixtureAdmission773 := statementTestContext(c, "MATCH (a:S {id: 1})")
	_ = fixtureAdmission773
	ns, err := e.pipelineMatchRows(fixtureAdmission773, fixtureAdmission773Context, pipelineRow{}, "MATCH (a:S {id: 1})")
	if err != nil {
		t.Fatal(err)
	}
	source := ns[0]["a"].(*storage.Node)
	p, _, ok := compileSelectedPathPlan("(a)(()-[:T]->())+(b)", "p", "", nil)
	if !ok {
		t.Fatal("compile")
	}
	c, cancel := context.WithCancel(c)
	fixtureAdmission1700Context, fixtureAdmission1700 := statementTestContext(c, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1700Context
	s := e.relationshipExecutionFor(fixtureAdmission1700, fixtureAdmission1700Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(c, p.forward, pipelineRow{}, -1, source)
	fixtureAdmission1701Context, fixtureAdmission1701 := statementTestContext(context.Background(), ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1701Context
	s.begin(fixtureAdmission1701, source)
	before := len(s.products)
	cancel()
	fixtureAdmission1702Context, fixtureAdmission1702 := statementTestContext(context.Background(), ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1702Context
	s.nextLevel(fixtureAdmission1702)
	t.Logf("products before cancellation=%d after canceled nextLevel=%d", before, len(s.products))
	if len(s.products) > before {
		t.Fatal("expanded after cancellation")
	}
}
