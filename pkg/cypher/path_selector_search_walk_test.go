package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// failingBatchEngine fails reading nodes in a batch.
type failingBatchEngine struct {
	storage.Engine
}

func (e *failingBatchEngine) BatchGetNodes([]storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return nil, errEdgeRead
}

// The walk search between bound ends of a uniform pattern
// (path_selector_search_walk.go): which patterns are uniform, a walk the
// pattern doesn't take, nodes that don't load (the general search answers
// then), ends the pattern's end nodes don't take, a failed relationship
// read and a cancelled search.
func TestSelectedPathSearchUniformWalks(t *testing.T) {
	for pattern, uniform := range map[string]bool{
		"(a)(()-[:T]->())+(b)":                 true,
		"(a)-[:T*]-(b)":                        true,
		"(a:S)(()-[:T]->()){2,}(b:S {id: 4})":  true,
		"(a)(()-[:T]->())+(()-[:U]->())+(b)":   false,
		"(a)(()-[:T|U]->())+(()-[:T]->())+(b)": false,
		"(a)(()-[:T]->())+(()<-[:T]-())+(b)":   false,
		"(a)(()-[r:T WHERE r.w > 1]->())+(b)":  false,
		"(a)-[r:T]->(b)":                       false,
		"(a)(()-[:T]->(:S))+(b)":               false,
		"(a)(()-[:T]->()){0}(b)":               false,
	} {
		plan, _, ok := compileSelectedPathPlan(pattern, "p", "", nil)
		require.True(t, ok, pattern)
		require.Equal(t, uniform, plan.forward.uniform != nil, pattern)
	}

	// Three optional repetitions take at most three steps; the search for a
	// fourth meets a slot at a step it already failed from.
	plan, _, ok := compileSelectedPathPlan("(a)(()-[:T]->()){0,1}(()-[:T]->()){0,1}(()-[:T]->()){0,1}(b)", "p", "", nil)
	require.True(t, ok)
	_, ok = plan.forward.walkPath(nil, make([]pathSearchPathStep, 4))
	require.False(t, ok)
	_, ok = plan.forward.walkPath(nil, make([]pathSearchPathStep, 2))
	require.True(t, ok)

	exec, ctx := newShortestSelectorExecutor(t)
	byID := func(exec *StorageExecutor, id int64) *storage.Node {
		result, err := exec.Execute(ctx, "MATCH (n:S {id: $id}) RETURN n", map[string]interface{}{"id": id})
		require.NoError(t, err)
		return result.Rows[0][0].(*storage.Node)
	}
	edge := func(exec *StorageExecutor, from, to *storage.Node) *storage.Edge {
		edges, err := exec.storage.GetOutgoingEdges(from.ID)
		require.NoError(t, err)
		for _, candidate := range edges {
			if candidate.EndNode == to.ID && candidate.Type == "T" {
				return candidate
			}
		}
		t.Fatalf("no relationship %s -> %s", from.ID, to.ID)
		return nil
	}
	s1, s3, s4 := byID(exec, 1), byID(exec, 3), byID(exec, 4)
	walk := [][]*storage.Edge{{edge(exec, s1, s3), edge(exec, s3, s4)}}
	bindFor := func(statement admittedRelationshipStatement, exec *StorageExecutor, plan *selectedPathPlan) func(pathSearchPath) (pipelineRow, bool) {
		return func(path pathSearchPath) (pipelineRow, bool) {
			return exec.bindSelectedPath(statement, ctx, plan, plan.forward, false, pipelineRow{}, path)
		}
	}
	plus, _, ok := compileSelectedPathPlan("(a)(()-[:T]->())+(b)", "p", "", nil)
	require.True(t, ok)
	fixtureAdmission1733Context, fixtureAdmission1733 := statementTestContext(ctx, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1733Context
	rows, ok := exec.relationshipExecutionFor(fixtureAdmission1733, fixtureAdmission1733Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(ctx, plus.forward, pipelineRow{}, -1, s1).bindWalks(s1, s4, walk, false, bindFor(fixtureAdmission1733, exec, plus))
	require.True(t, ok)
	require.Len(t, rows, 1)
	// A walk the pattern doesn't take or bind, a node that doesn't load and
	// a failed batch read leave the pair to the general search.
	fixtureAdmission1734Context, fixtureAdmission1734 := statementTestContext(ctx, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1734Context
	_, ok = exec.relationshipExecutionFor(fixtureAdmission1734, fixtureAdmission1734Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(ctx, plus.forward, pipelineRow{}, -1, s1).bindWalks(s1, s4, walk, false,
		func(pathSearchPath) (pipelineRow, bool) { return nil, false })
	require.False(t, ok)
	three, _, ok := compileSelectedPathPlan("(a)(()-[:T]->()){3}(b)", "p", "", nil)
	require.True(t, ok)
	fixtureAdmission1735Context, fixtureAdmission1735 := statementTestContext(ctx, ("MATCH "+("(a)(()-[:T]->()){3}(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1735Context
	_, ok = exec.relationshipExecutionFor(fixtureAdmission1735, fixtureAdmission1735Context, "(a)(()-[:T]->()){3}(b)", "").newPathSearch(ctx, three.forward, pipelineRow{}, -1, s1).bindWalks(s1, s4, walk, false, bindFor(fixtureAdmission1735, exec, three))
	require.False(t, ok)
	dangling := [][]*storage.Edge{{{ID: "dangling", Type: "T", StartNode: s1.ID, EndNode: "missing"}, {ID: "back", Type: "T", StartNode: "missing", EndNode: s4.ID}}}
	fixtureAdmission1736Context, fixtureAdmission1736 := statementTestContext(ctx, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1736Context
	_, ok = exec.relationshipExecutionFor(fixtureAdmission1736, fixtureAdmission1736Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(ctx, plus.forward, pipelineRow{}, -1, s1).bindWalks(s1, s4, dangling, false, bindFor(fixtureAdmission1736, exec, plus))
	require.False(t, ok)
	failing := NewStorageExecutor(&failingBatchEngine{Engine: exec.storage})
	fixtureAdmission1737Context, fixtureAdmission1737 := statementTestContext(ctx, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1737Context
	_, ok = failing.relationshipExecutionFor(fixtureAdmission1737, fixtureAdmission1737Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(ctx, plus.forward, pipelineRow{}, -1, s1).bindWalks(s1, s4, walk, false, bindFor(fixtureAdmission1737, failing, plus))
	require.False(t, ok)
	result, err := failing.Execute(ctx, "MATCH (a:S {id: 1}), (b:S {id: 4}) MATCH p = shortestPath((a)-[:T*]->(b)) RETURN length(p) AS l", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	requireRows(t, exec, ctx, nil, map[string][][]interface{}{
		"MATCH (a:S {id: 1}), (b:S {id: 3}) MATCH p = ANY SHORTEST (a:Nope)(()-[:T]->())+(b) RETURN count(*) AS c": {{int64(0)}},
		"MATCH (a:S {id: 1}), (b:S {id: 3}) MATCH p = ANY SHORTEST (a)(()-[:T]->())+(b:Nope) RETURN count(*) AS c": {{int64(0)}},
	})

	// A cancelled search ends within the level it is extending.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	fixtureAdmission1738Context, fixtureAdmission1738 := statementTestContext(cancelled, ("MATCH "+("(a)(()-[:T]->())+(b)"))+"\n"+("RETURN "+("")))
	_ = fixtureAdmission1738Context
	search := exec.relationshipExecutionFor(fixtureAdmission1738, fixtureAdmission1738Context, "(a)(()-[:T]->())+(b)", "").newPathSearch(cancelled, plus.forward, pipelineRow{}, -1, s1)
	side := search.newWalkSide(s1.ID, "outgoing", true)
	for len(side.frontier) < 300 {
		side.frontier = append(side.frontier, s1.ID)
	}
	shortest, meets := search.walkLevel(side, search.newWalkSide(s4.ID, "incoming", true), true)
	require.Equal(t, -1, shortest)
	require.Nil(t, meets)
	require.Len(t, side.reached, 1)
	require.Zero(t, side.radius)
}
