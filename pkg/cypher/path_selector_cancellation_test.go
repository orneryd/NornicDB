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
	fixtureAdmission1680Context, fixtureAdmission1680 := statementTestContext(c, ("MATCH "+("(a)-[:T*]->(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1680Context
	s := e.relationshipExecutionFor(fixtureAdmission1680, fixtureAdmission1680Context, "(a)-[:T*]->(b)", "").newPathSearch(c, plan.forward, pipelineRow{}, -1, start)
	calls := 0
	fixtureAdmission1681Context, fixtureAdmission1681 := statementTestContext(context.Background(), ("MATCH "+("(a)-[:T*]->(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1681Context
	rows, ok := s.uniformPair(fixtureAdmission1681, start, previous, true, func(p pathSearchPath) (pipelineRow, bool) {
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
	fixtureAdmission769Context, fixtureAdmission769 := statementTestContext(c, "MATCH (a:S {id: 1})")
	_ = fixtureAdmission769
	ns, err := e.pipelineMatchRows(fixtureAdmission769, fixtureAdmission769Context, pipelineRow{}, "MATCH (a:S {id: 1})")
	if err != nil {
		t.Fatal(err)
	}
	source := ns[0]["a"].(*storage.Node)
	p, _, ok := compileSelectedPathPlan("(a)(()-[:T]->())+(b)", "p", "", nil)
	if !ok {
		t.Fatal("compile")
	}
	c, cancel := context.WithCancel(c)
	fixtureAdmission1682Context, fixtureAdmission1682 := statementTestContext(c, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1682Context
	s := e.relationshipExecutionFor(fixtureAdmission1682, fixtureAdmission1682Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(c, p.forward, pipelineRow{}, -1, source)
	fixtureAdmission1683Context, fixtureAdmission1683 := statementTestContext(context.Background(), ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1683Context
	s.begin(fixtureAdmission1683, source)
	before := len(s.products)
	cancel()
	fixtureAdmission1684Context, fixtureAdmission1684 := statementTestContext(context.Background(), ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1684Context
	s.nextLevel(fixtureAdmission1684)
	t.Logf("products before cancellation=%d after canceled nextLevel=%d", before, len(s.products))
	if len(s.products) > before {
		t.Fatal("expanded after cancellation")
	}
}
