package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type matchErrEngine struct {
	storage.Engine
	err error
}

func (e *matchErrEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	return nil, e.err
}

func (e *matchErrEngine) AllNodes() ([]*storage.Node, error) {
	return nil, e.err
}

type createErrEngine struct {
	storage.Engine
	err error
}

func (e *createErrEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	return "", e.err
}

func TestSplitPipelineClauses_GuardBranches(t *testing.T) {
	clauses, ok := splitPipelineClauses("   ")
	require.False(t, ok)
	require.Nil(t, clauses)

	clauses, ok = splitPipelineClauses("x MATCH (n) RETURN n")
	require.False(t, ok)
	require.Nil(t, clauses)

	clauses, ok = splitPipelineClauses("MATCH (n) WHERE n.name STARTS WITH 'a' RETURN n")
	require.True(t, ok)
	require.Len(t, clauses, 2)
	require.Equal(t, pipelineClauseMatch, clauses[0].kind)
	require.Equal(t, pipelineClauseReturn, clauses[1].kind)
}

func TestExecutePipeline_ErrorAndFallbackBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "pipeline_err_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {id:'p1'})", nil)
	require.NoError(t, err)

	var res *ExecuteResult
	var handled bool

	// MATCH application hard error path via storage failure.
	matchErr := errors.New("match lookup failed")
	errExec := NewStorageExecutor(&matchErrEngine{Engine: store, err: matchErr})
	res, handled, err = errExec.executePipeline(ctx, "MATCH (n:Person) WITH n RETURN n")
	require.Error(t, err)
	require.ErrorIs(t, err, matchErr)
	require.True(t, handled)
	require.Nil(t, res)

	// WITH projection fallback path.
	res, handled, err = exec.executePipeline(ctx, "MATCH (n:Person) WITH unknownExpr AS x RETURN x")
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, res)

	// CREATE application hard error path.
	createErr := errors.New("create failed")
	createExec := NewStorageExecutor(&createErrEngine{Engine: store, err: createErr})
	res, handled, err = createExec.executePipeline(ctx, "MATCH (n:Person) WITH n CREATE (:Tmp {id:'t1'}) RETURN n")
	require.Error(t, err)
	require.ErrorIs(t, err, createErr)
	require.True(t, handled)
	require.Nil(t, res)

	// UNWIND parse fallback path.
	res, handled, err = exec.executePipeline(ctx, "MATCH (n:Person) WITH n UNWIND [1] RETURN n")
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, res)

	// RETURN projection fallback path.
	res, handled, err = exec.executePipeline(ctx, "MATCH (n:Person) WITH n RETURN missing")
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, res)
}

func TestExecuteCreateWithMalformedTailSurfacesError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "pipeline_bad_create_with")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (n:ImplicitRollback {id: 1})
WITH n
CREAT (m:ImplicitRollback {id: 2})
RETURN n
`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE...WITH")

	res, err := exec.Execute(ctx, "MATCH (n:ImplicitRollback) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(0), res.Rows[0][0])
}

func TestCreateWithPipeline_SupportedShapes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "pipeline_create_with_supported")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("create with return", func(t *testing.T) {
		res, err := exec.Execute(ctx, "CREATE (n:Tmp {id:'cwr-1'}) WITH n RETURN n.id AS id", nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, "cwr-1", res.Rows[0][0])
	})

	t.Run("create with alias return", func(t *testing.T) {
		res, err := exec.Execute(ctx, "CREATE (n:Tmp {id:'cwr-2'}) WITH n AS created RETURN created.id AS id", nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, "cwr-2", res.Rows[0][0])
	})

	t.Run("create with match return", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CREATE (:Lookup {id:'lookup-1'})", nil)
		require.NoError(t, err)

		res, err := exec.Execute(ctx, "CREATE (n:Tmp {id:'cwmr-1'}) WITH n MATCH (l:Lookup {id:'lookup-1'}) RETURN n.id AS nid, l.id AS lid", nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		require.Equal(t, "cwmr-1", res.Rows[0][0])
		require.Equal(t, "lookup-1", res.Rows[0][1])
	})
}

func TestPipelineApplyMatch_AdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "pipeline_match_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	n := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "p1"}}
	_, err := store.CreateNode(n)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "R", StartNode: "n1", EndNode: "n1"}))

	rows := []pipelineRow{{
		"node": n,
		"edge": &storage.Edge{ID: "e2", Type: "R", StartNode: "n1", EndNode: "n1"},
		"m":    map[string]interface{}{"id": "p1"},
		"x":    int64(1),
	}}

	out, ok, err := exec.pipelineApplyMatch(ctx, rows, "MATCH (p:Person {id: m.id})")
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, out, 1)
	require.NotNil(t, out[0]["p"])

	matchErr := errors.New("match lookup failed")
	errExec := NewStorageExecutor(&matchErrEngine{Engine: store, err: matchErr})
	out, ok, err = errExec.pipelineApplyMatch(ctx, rows, "MATCH (p:Person)")
	require.Error(t, err)
	require.ErrorIs(t, err, matchErr)
	require.True(t, ok)
	require.Nil(t, out)
}
