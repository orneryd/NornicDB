package storage

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// statementRemoteOperation is one operation of the statement-runner path: the
// access mode it must carry, whether it reads, and one execution of it.
type statementRemoteOperation struct {
	read bool
	run  func(*RemoteEngine) error
}

// A remote engine bound to a statement runner runs every operation's
// statements through the runner, writes and reads alike, and nothing through
// a session of its own; a transaction begun on it joins the runner's, so its
// Commit and Rollback end nothing. An error of the runner is the operation's.
// (The runner answers from the statement-reply fixture, so every operation
// reaches its result; what the reply means for each operation is
// TestRemoteEngineEngineMethods' subject, over the same fixture.)
func TestStatementRemoteEngineRunsEveryOperationThroughItsRunner(t *testing.T) {
	var statements []string
	var modes []RemoteAccessMode
	fail := error(nil)
	engine := NewStatementRemoteEngine(nil, func(_ context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
		statements = append(statements, statement)
		modes = append(modes, mode)
		if fail != nil {
			return nil, nil, fail
		}
		return remoteEngineMethodReply(t, statement, params)
	})
	operations := map[string]statementRemoteOperation{
		"CreateNode": {run: func(engine *RemoteEngine) error {
			_, err := engine.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
			return err
		}},
		"UpdateNode": {run: func(engine *RemoteEngine) error {
			return engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"x": 1}}})
		}},
		"DeleteNode": {run: func(engine *RemoteEngine) error { return engine.DeleteNode("n1") }},
		"CreateEdge": {run: func(engine *RemoteEngine) error {
			return engine.CreateEdge(&Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"})
		}},
		"UpdateEdge": {run: func(engine *RemoteEngine) error {
			return engine.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"}})
		}},
		"DeleteEdge": {run: func(engine *RemoteEngine) error { return engine.DeleteEdge("e1") }},
		"BulkCreateNodes": {run: func(engine *RemoteEngine) error {
			return engine.BulkCreateNodes([]*Node{{ID: "n2", Labels: []string{"Person"}}})
		}},
		"BulkCreateEdges": {run: func(engine *RemoteEngine) error {
			return engine.BulkCreateEdges([]*Edge{{ID: "e2", StartNode: "a", EndNode: "b", Type: "KNOWS"}})
		}},
		"BulkDeleteNodes": {run: func(engine *RemoteEngine) error { return engine.BulkDeleteNodes([]NodeID{"n2"}) }},
		"BulkDeleteEdges": {run: func(engine *RemoteEngine) error { return engine.BulkDeleteEdges([]EdgeID{"e2"}) }},
		"GetNodesByLabel": {read: true, run: func(engine *RemoteEngine) error {
			_, err := engine.GetNodesByLabel("Person")
			return err
		}},
		"NodeCount": {read: true, run: func(engine *RemoteEngine) error {
			_, err := engine.NodeCount()
			return err
		}},
		"QueryCypher": {read: true, run: func(engine *RemoteEngine) error {
			_, _, err := engine.QueryCypher(context.Background(), "RETURN 1", nil, RemoteAccessRead)
			return err
		}},
	}
	// Sorted names keep the twice-run table in the same order.
	names := make([]string, 0, len(operations))
	for name := range operations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		operation := operations[name]
		before := len(statements)
		require.NoError(t, operation.run(engine), "%s completes over the fixture's reply", name)
		require.Greater(t, len(statements), before, "%s runs through the runner", name)
		wantMode := RemoteAccessWrite
		if operation.read {
			wantMode = RemoteAccessRead
		}
		require.Equal(t, wantMode, modes[len(modes)-1], "%s carries its explicit operation mode", name)
	}

	tx, err := engine.BeginCypherTx(context.Background(), RemoteAccessWrite)
	require.NoError(t, err)
	before := len(statements)
	_, _, err = tx.QueryCypher(context.Background(), "CREATE (:Joined)", nil)
	require.NoError(t, err)
	require.Equal(t, before+1, len(statements), "a transaction begun on it runs in the runner's")
	require.NoError(t, tx.Commit(context.Background()))
	require.NoError(t, tx.Rollback(context.Background()))
	require.Equal(t, before+1, len(statements), "its Commit and Rollback end nothing")
	require.NoError(t, engine.Close())

	fail = errors.New("the statement failed")
	for _, name := range names {
		require.ErrorIs(t, operations[name].run(engine), fail, "%s preserves its owner's failure", name)
	}

	_, err = engine.CreateNode(&Node{ID: "n3", Labels: []string{"Person"}})
	require.ErrorIs(t, err, fail)
}
