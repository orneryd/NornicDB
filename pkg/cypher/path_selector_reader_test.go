package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type observedIncidentEngine struct {
	storage.Engine
	outgoing, incoming int
}

func (e *observedIncidentEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.outgoing++
	return e.Engine.GetOutgoingEdges(id)
}
func (e *observedIncidentEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	e.incoming++
	return e.Engine.GetIncomingEdges(id)
}
func TestIncidentReaderCancellationWork(t *testing.T) {
	executor, ctx := newShortestSelectorExecutor(t)
	r, err := executor.Execute(ctx, "MATCH (a:S {id:1}) RETURN a", nil)
	require.NoError(t, err)
	source := r.Rows[0][0].(*storage.Node)
	for _, direction := range []string{"outgoing", "incoming", "both"} {
		for budget := 1; budget <= 8; budget++ {
			reader := &observedIncidentEngine{Engine: executor.storage}
			atOut, atIn := 0, 0
			canceled := newCheckpointContext(budget, func() { atOut, atIn = reader.outgoing, reader.incoming })
			_, _, err := incidentEdgeListsContext(canceled, reader, source.ID, direction)
			if canceled.Context.Err() != nil {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, atOut, reader.outgoing)
				require.Equal(t, atIn, reader.incoming)
			} else {
				require.NoError(t, err)
			}
			canceled.cancel()
			adjacent := &adjacentEdgeEngine{Engine: reader}
			atCalls := 0
			canceled = newCheckpointContext(budget, func() { atCalls = adjacent.calls })
			_, _, err = incidentEdgeListsContext(canceled, adjacent, source.ID, direction)
			if canceled.Context.Err() != nil {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, atCalls, adjacent.calls)
			} else {
				require.NoError(t, err)
			}
			canceled.cancel()
		}
	}
	edges := make([]*storage.Edge, 1024)
	for index := range edges {
		edges[index] = &storage.Edge{ID: "parallel", StartNode: "a", EndNode: "b"}
	}
	for budget := 1; budget <= 20; budget++ {
		canceled := newCheckpointContext(budget, nil)
		combined, err := combineIncidentEdges(canceled, "a", edges, edges)
		if canceled.Context.Err() != nil {
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, combined)
		} else {
			require.NoError(t, err)
			require.Len(t, combined, 2048)
		}
		canceled.cancel()
	}
}
func TestShortestFunctionCancellationAndBoundErrors(t *testing.T) {
	e, ctx := newShortestSelectorExecutor(t)
	r, err := e.Execute(ctx, "MATCH (a:S {id:1}), (b:S {id:4}) RETURN a,b", nil)
	require.NoError(t, err)
	source, target := r.Rows[0][0].(*storage.Node), r.Rows[0][1].(*storage.Node)
	for budget := 1; budget <= 128; budget++ {
		canceled := newCheckpointContext(budget, nil)
		fixtureAdmission770Context, fixtureAdmission770 := expressionTestContext(canceled, "shortestPath"+"("+"(a)-[:T*1..3]->(b)"+")")
		_ = fixtureAdmission770
		_, _, err := e.evaluateShortestPathValue(fixtureAdmission770, fixtureAdmission770Context, "shortestPath", "(a)-[:T*1..3]->(b)", map[string]*storage.Node{"a": source, "b": target})
		if canceled.Context.Err() != nil {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, err)
		}
		canceled.cancel()
		canceled = newCheckpointContext(budget, nil)
		fixtureAdmission1704Context, fixtureAdmission1704 := statementTestContext(canceled, "p = shortestPath((a)-[:T*1..3]->(b))")
		_ = fixtureAdmission1704Context
		_, _, err = e.parseShortestPathMatch(fixtureAdmission1704, fixtureAdmission1704Context, "p = shortestPath((a)-[:T*1..3]->(b))")
		if canceled.Context.Err() != nil {
			require.ErrorIs(t, err, context.Canceled)
		} else {
			require.NoError(t, err)
		}
		canceled.cancel()
	}
	fixtureAdmission771Context, fixtureAdmission771 := traversalTestContext(ctx, "(a)-[r:T*]->(b)", "", nil)
	_ = fixtureAdmission771
	traversal := e.parseTraversalPattern(fixtureAdmission771, fixtureAdmission771Context, "(a)-[r:T*]->(b)")
	plan, _, ok := compileSelectedPathPlan("(a)-[r:T*]->(b)", "p", "", nil)
	require.True(t, ok)
	for _, all := range []bool{false, true} {
		fixtureAdmission1705Context, fixtureAdmission1705 := statementTestContext(ctx, ("MATCH "+("(a)-[r:T*]->(b)"))+"\n"+("MATCH "+("(a)-[r:T*]->(b)")))
		_ = fixtureAdmission1705Context
		_, err := e.shortestPathsBetween(fixtureAdmission1705, fixtureAdmission1705Context, &shortestPathMatch{findAll: all, traversal: traversal, plan: plan}, source, target, pipelineRow{"r": []interface{}{}})
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError: Bound relationships not allowed in "+shortestPathFunctionName(all)+"(...)", statusText(err))
	}
	traversal.EndNode.variable = traversal.StartNode.variable
	traversal.Relationship.MinHops = 0
	require.Equal(t, statusText(errShortestCommonEndpoints()), statusText(shortestPathPatternError("shortestPath", "(a)-[:T*0..]->(a)", traversal)))
	require.NoError(t, validateShortestCallBindings("MATCH (a) RETURN a", nil))
	require.NoError(t, validateShortestCallBindings("MATCH p = shortestPath((a)-[:T*]->(b)) WHERE "+pathSelectorFunction+"('SHORTEST',1,false,'WALK',true)", nil))
}
