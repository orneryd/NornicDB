package cypher

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type createMatchErrEngine struct {
	storage.Engine
	labelErrs       map[string]error
	allErr          error
	edgesBetweenErr error
}

func (e *createMatchErrEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	if err, ok := e.labelErrs[label]; ok && err != nil {
		return nil, err
	}
	return e.Engine.GetNodesByLabel(ctx, label)
}

func (e *createMatchErrEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	return fixtureowner.ReadWithFailure(ctx, e.allErr, func(ctx context.Context) ([]*storage.Node, error) {
		return e.Engine.AllNodes(ctx)
	})
}

func (e *createMatchErrEngine) GetOutgoingEdges(ctx context.Context, startID storage.NodeID) ([]*storage.Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.edgesBetweenErr, func(ctx context.Context) ([]*storage.Edge, error) {
		return e.Engine.GetOutgoingEdges(ctx, startID)
	})
}

func (e *createMatchErrEngine) GetIncomingEdges(ctx context.Context, endID storage.NodeID) ([]*storage.Edge, error) {
	return fixtureowner.ReadWithFailure(ctx, e.edgesBetweenErr, func(ctx context.Context) ([]*storage.Edge, error) {
		return e.Engine.GetIncomingEdges(ctx, endID)
	})
}

func TestExecuteCompoundMatchCreate_SurfacesMatchLabelLookupError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_label")

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)

	lookupErr := errors.New("label lookup failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{
		Engine:    store,
		labelErrs: map[string]error{"A": lookupErr},
	})

	_, err = errExec.Execute(context.Background(),
		"MATCH (a:A),(b:B) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, lookupErr)
}

func TestExecuteCompoundMatchCreate_SurfacesAllNodesError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_all")

	allErr := errors.New("all nodes failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{Engine: store, allErr: allErr})

	_, err := errExec.Execute(context.Background(),
		"MATCH (a),(b) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, allErr)
}

func TestExecuteCompoundMatchCreate_SurfacesRelationshipCheckError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_rel")

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)
	query := "MATCH (a:A),(b:B) WHERE a.id = 'a1' AND b.id = 'b1' AND NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n"

	okExec := NewStorageExecutor(store)
	okRes, err := okExec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.NotNil(t, okRes)

	outErr := errors.New("edges-between lookup failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{Engine: store, edgesBetweenErr: outErr})

	_, err = errExec.Execute(context.Background(), query, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, outErr)
	edges, err := store.GetOutgoingEdges(context.Background(), "a1")
	require.NoError(t, err)
	require.Len(t, edges, 1)
}

func TestGh908MatchCreatePredicateReadFailureStopsWrites(t *testing.T) {
	for _, direction := range []string{"(a)-[:R]->(b)", "(a)<-[:R]-(b)", "(a)-[:R]-(b)"} {
		t.Run(direction, func(t *testing.T) {
			base := newTestMemoryEngine(t)
			store := storage.NewNamespacedEngine(base, "predicate_failure")
			for _, identifier := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: identifier, Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"id": string(identifier)}})
				require.NoError(t, err)
			}
			lookupErr := errors.New("predicate adjacency failed")
			exec := NewStorageExecutor(&createMatchErrEngine{Engine: store, edgesBetweenErr: lookupErr})
			_, err := exec.Execute(context.Background(),
				"MATCH (a:Endpoint {id:'a'}), (b:Endpoint {id:'b'}) WHERE NOT "+direction+" CREATE (:Unexpected)", nil)
			require.ErrorIs(t, err, lookupErr)
			created, err := store.GetNodesByLabel(context.Background(), "Unexpected")
			require.NoError(t, err)
			require.Empty(t, created)
		})
	}
}

func TestBuildCombinationsUsingWhereJoin_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	// Single-variable component path (unconstrained `c` component).
	single, ok := exec.buildCombinationsUsingWhereJoin(
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{variable: "a", nodes: []*storage.Node{{ID: "a1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "b", nodes: []*storage.Node{{ID: "b1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "c", nodes: []*storage.Node{{ID: "c1", Properties: map[string]interface{}{"id": "z"}}}},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, single, 1)
	require.Equal(t, storage.NodeID("a1"), single[0]["a"].ID)
	require.Equal(t, storage.NodeID("b1"), single[0]["b"].ID)
	require.Equal(t, storage.NodeID("c1"), single[0]["c"].ID)

	// Multi-variable path with rows that trigger nil/missing branches before producing one valid join.
	joined, ok := exec.buildCombinationsUsingWhereJoin(
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{
				variable: "a",
				nodes: []*storage.Node{
					nil,
					{ID: "a_nil_props", Properties: nil},
					{ID: "a_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "a_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
			{
				variable: "b",
				nodes: []*storage.Node{
					{ID: "b_nil_props", Properties: nil},
					{ID: "b_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "b_no_match", Properties: map[string]interface{}{"id": "k2"}},
					{ID: "b_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, joined, 1)
	require.Equal(t, storage.NodeID("a_ok"), joined[0]["a"].ID)
	require.Equal(t, storage.NodeID("b_ok"), joined[0]["b"].ID)
}
