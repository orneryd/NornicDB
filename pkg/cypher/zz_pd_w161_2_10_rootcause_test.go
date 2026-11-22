package cypher

// v1.5.2 close-out lane L4: root-cause runs for the two reproduced rows.
//
// H-417 (2): a required MATCH from a variable an OPTIONAL MATCH left null
// reads every relationship of the type. Named step: pipelineBoundNode
// (pipeline_traversal_match.go:250-260) reports a variable bound to null as
// "not bound", so pipelineTraversalSeedNodes (:228) neither seeds from it nor
// rejects the row, and the caller (:101) expands the whole pattern and joins
// afterwards. Diagnostic: the seed decision for a null-bound row and for a
// node-bound row (control); bypass: the same statement with the null row
// removed before the MATCH (WITH a WHERE a IS NOT NULL) reads nothing.
//
// H-382: db.awaitIndexes / db.awaitIndex return while an index is
// POPULATING. Named steps: callDbAwaitIndexes (call_index_mgmt.go:16-17)
// returns an empty result without looking at any index; callNamedIndexManagement
// (call_index_mgmt.go:24-45) returns as soon as the name is in the schema
// catalog. Minimal reproduction at the step: both owner functions called
// directly while the build runs return at once, before the service is ready.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloseoutL4RootCauseH417NullBoundSeed(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(l4MemStore(t, "l4"))
		l4Run(t, exec, "CREATE (:A417 {id: 'a1'})-[:R417]->(:B417)", nil)
		pattern := "(a:A417)-[:R417]->(b)"
		start, end, rejected := exec.pipelineTraversalSeedNodes(context.Background(), pattern, pipelineRow{"a": nil})
		_, nullBound := pipelineBoundNode(pipelineRow{"a": nil}, "a")
		node := &storage.Node{ID: "x", Labels: []string{"A417"}, Properties: map[string]any{}}
		cStart, _, cRejected := exec.pipelineTraversalSeedNodes(context.Background(), pattern, pipelineRow{"a": node})
		l4Report(t, "H-417", "rootcause-seed-decision", true, map[string]any{
			"nullRow": map[string]any{"pipelineBoundNodeOK": nullBound, "startSeeds": len(start), "endSeeds": len(end), "rejected": rejected},
			"nodeRowControl": map[string]any{"startSeeds": len(cStart), "rejected": cRejected},
			"decisive": !nullBound && len(start) == 0 && !rejected && len(cStart) == 1,
		})
		if nullBound || len(start) != 0 || rejected || len(cStart) != 1 {
			t.Errorf("H-417 root cause: seed decision is not the named one: nullBound=%v start=%d rejected=%v control=%d", nullBound, len(start), rejected, len(cStart))
		}
		// Bypass: drop the null row before the required MATCH.
		build := func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_a417_id FOR (n:A417) ON (n.id)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:A417 {id: 'a' + toString(k)})-[:R417]->(:B417)")
		}
		l4Scaling(t, "H-417", "rootcause-bypass-null-removed", build, "OPTIONAL MATCH (a:A417 {id: 'missing'}) WITH a WHERE a IS NOT NULL MATCH (a)-[:R417]->(b) RETURN count(b) AS n", nil, 1)
		l4Scaling(t, "H-417", "rootcause-control-bound-node", build, "OPTIONAL MATCH (a:A417 {id: 'a7'}) MATCH (a)-[:R417]->(b) RETURN count(b) AS n", nil, 1)
	})
}

func TestCloseoutL4RootCauseH382AwaitOwners(t *testing.T) {
	engine := l4MemStore(t, "l4")
	exec := NewStorageExecutor(engine)
	first := search.NewService(engine, 3, nil)
	t.Cleanup(func() { _ = first.Close() })
	require.NoError(t, first.BuildIndexes(context.Background()))
	exec.SetSearchService(first)
	l4Run(t, exec, "CREATE VECTOR INDEX l4vec382 FOR (n:Doc382) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	l4Batch(t, exec, 60000, "UNWIND range($start, $end - 1) AS k CREATE (:Doc382 {id: k, embedding: [toFloat(k % 7) + 1.0, 1.0, 0.5]})")
	restarted := NewStorageExecutor(engine)
	service := search.NewService(engine, 3, nil)
	t.Cleanup(func() { _ = service.Close() })
	restarted.SetSearchService(service)
	done := make(chan error, 1)
	go func() { done <- service.BuildIndexes(context.Background()) }()
	deadline := time.Now().Add(10 * time.Second)
	for !service.BuildInProgress() && !service.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	building := service.BuildInProgress() && !service.IsReady()
	startAll := time.Now()
	resultAll, errAll := restarted.callDbAwaitIndexes()
	allMs := time.Since(startAll).Milliseconds()
	readyAfterAll := service.IsReady()
	startOne := time.Now()
	resultOne, errOne := restarted.callNamedIndexManagement(context.Background(), "db.awaitIndex", []interface{}{"l4vec382", int64(120)})
	oneMs := time.Since(startOne).Milliseconds()
	readyAfterOne := service.IsReady()
	state := fmt.Sprint(l4Run(t, restarted, "SHOW INDEXES YIELD name, state WHERE name = 'l4vec382' RETURN state", nil))
	<-done
	rowsAll, rowsOne := -1, -1
	if resultAll != nil {
		rowsAll = len(resultAll.Rows)
	}
	if resultOne != nil {
		rowsOne = len(resultOne.Rows)
	}
	l4Report(t, "H-382", "rootcause-owner-returns-during-build", true, map[string]any{
		"buildingWhenCalled": building, "stateWhileBuilding": state,
		"callDbAwaitIndexes": map[string]any{"ms": allMs, "rows": rowsAll, "error": fmt.Sprint(errAll), "readyAfter": readyAfterAll},
		"callNamedIndexManagement": map[string]any{"ms": oneMs, "rows": rowsOne, "error": fmt.Sprint(errOne), "readyAfter": readyAfterOne},
		"decisive": building && !readyAfterAll && !readyAfterOne,
	})
	if !building || readyAfterAll || readyAfterOne {
		t.Errorf("H-382 root cause run not decisive: building=%v readyAfterAll=%v readyAfterOne=%v", building, readyAfterAll, readyAfterOne)
	}
}
