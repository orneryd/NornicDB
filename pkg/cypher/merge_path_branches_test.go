package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// mergeConflictEngine fails node creation: with a conflict after running
// race (another MERGE's write, which may be nothing), or with failure.
// lookupFailure fails GetNodesByLabel, from the start or (lookupAfterCreate)
// once a create was tried.
type mergeConflictEngine struct {
	storage.Engine
	race              func()
	failure           error
	lookupFailure     error
	lookupAfterCreate bool
	createTried       bool
}

func (e *mergeConflictEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.lookupFailure != nil && (!e.lookupAfterCreate || e.createTried) {
		return nil, e.lookupFailure
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *mergeConflictEngine) createFailure() error {
	e.createTried = true
	if e.failure != nil {
		return e.failure
	}
	if e.race != nil {
		e.race()
		e.race = nil
	}
	return storage.ErrAlreadyExists
}

func (e *mergeConflictEngine) CreateNode(*storage.Node) (storage.NodeID, error) {
	return "", e.createFailure()
}

func (e *mergeConflictEngine) BulkCreateNodes([]*storage.Node) error {
	return e.createFailure()
}

// A MERGE whose create collides with a concurrent MERGE's write matches
// what that MERGE wrote, or fails when nothing matches; any other create
// failure fails the MERGE (#907).
func TestMergeCreateConflictRecovery(t *testing.T) {
	newConflict := func(t *testing.T, raceQuery string, failure error) *StorageExecutor {
		inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_conflict")
		engine := &mergeConflictEngine{Engine: inner, failure: failure}
		if raceQuery != "" {
			engine.race = func() {
				_, err := NewStorageExecutor(inner).Execute(context.Background(), raceQuery, nil)
				require.NoError(t, err)
			}
		}
		return NewStorageExecutor(engine)
	}
	ctx := context.Background()

	path := "MERGE p = (a:MA)-[:R]->(b:MB)-[:S]->(c:MC) RETURN length(p) AS l"
	result, err := newConflict(t, "CREATE (:MA)-[:R]->(:MB)-[:S]->(:MC)", nil).Execute(ctx, path, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	_, err = newConflict(t, "", nil).Execute(ctx, path, nil)
	require.ErrorIs(t, err, storage.ErrAlreadyExists)
	failure := errors.New("disk full")
	_, err = newConflict(t, "", failure).Execute(ctx, path, nil)
	require.ErrorIs(t, err, failure)

	node := "MERGE (n:MU {k: 1}) RETURN n.k AS k"
	result, err = newConflict(t, "CREATE (:MU {k: 1})", nil).Execute(ctx, node, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = newConflict(t, "", nil).Execute(ctx, node, nil)
	require.ErrorIs(t, err, storage.ErrAlreadyExists)

	// A failing read: while matching the path, or while finding the node a
	// concurrent MERGE created.
	lookupFailure := errors.New("scan failed")
	failingReads := func(afterCreate bool) *StorageExecutor {
		inner := storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_lookup")
		_, err := NewStorageExecutor(inner).Execute(ctx, "CREATE (:MA), (:MB), (:MC)", nil)
		require.NoError(t, err)
		return NewStorageExecutor(&mergeConflictEngine{Engine: inner, lookupFailure: lookupFailure, lookupAfterCreate: afterCreate})
	}
	_, err = failingReads(false).Execute(ctx, path, nil)
	require.ErrorIs(t, err, lookupFailure)
	_, err = failingReads(true).Execute(ctx, node, nil)
	require.ErrorIs(t, err, lookupFailure)
}

// The failures a MERGE path and its actions report (#907).
func TestMergePathFailures(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_path_failures"))
	_, err := exec.Execute(ctx, "CREATE (:MA)-[:R]->(:MB)", nil)
	require.NoError(t, err)
	for query, code := range map[string]string{
		"MERGE (a:MA {k: 0.0 / 0.0})-[:R]->(b:MB)-[:S]->(c:MC) RETURN 1 AS v":            "Neo.ClientError.Statement.SemanticError",
		"MERGE (a:MX)-[:R]->(b:MB)-[:S]->(c:MC) ON CREATE SET a.k = 1 / 0 RETURN 1 AS v": "Neo.ClientError.Statement.ArithmeticError",
		"MERGE (a:MA)-[r:R]->(b:MB) ON MATCH SET r.k = 1 / 0 RETURN 1 AS v":              "Neo.ClientError.Statement.ArithmeticError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		got, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, code, got, query)
	}
	// An action whose value storage can't hold fails the MERGE: on a path's
	// ON CREATE, on a relationship's ON MATCH.
	for _, query := range []string{
		"MERGE (a:MY)-[:R]->(b:MB)-[:S]->(c:MC) ON CREATE SET a.k = $m RETURN 1 AS v",
		"MATCH (a:MA), (b:MB) MERGE (a)-[r:R]->(b) ON MATCH SET r.k = $m RETURN 1 AS v",
	} {
		_, err := exec.Execute(ctx, query, map[string]interface{}{"m": map[string]interface{}{"x": int64(1)}})
		require.Error(t, err, query)
	}

	// Text the SET applier or the shape reader can't run, and a cancelled match.
	require.Error(t, exec.applyMergeActions(ctx, nil, "SET", &QueryStats{}))
	_, _, _, err = exec.pipelineMergePath(ctx, pipelineRow{}, "(a)-[:R]->(b) x-[:S]->(c)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, _, err = exec.pipelineMergePath(cancelled, pipelineRow{}, "(a:MA)-[:R]->(b:MB)-[:S]->(c:MC)", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.Error(t, err)
}
