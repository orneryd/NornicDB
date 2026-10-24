package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// A remote engine bound to a statement runner runs every operation's
// statements through the runner, writes and reads alike, and nothing through
// a session of its own; a transaction begun on it joins the runner's, so its
// Commit and Rollback end nothing. An error of the runner is the operation's.
// (The runner here returns no rows: an operation that needs them fails after
// running its statement, which is all this checks.)
func TestStatementRemoteEngineRunsEveryOperationThroughItsRunner(t *testing.T) {
	var statements []string
	fail := error(nil)
	engine := NewStatementRemoteEngine(func(_ context.Context, statement string, _ map[string]interface{}) ([]string, [][]interface{}, error) {
		statements = append(statements, statement)
		return nil, nil, fail
	})
	operations := map[string]func() error{
		"CreateNode": func() error { _, err := engine.CreateNode(&Node{ID: "n1", Labels: []string{"L"}}); return err },
		"UpdateNode": func() error { return engine.UpdateNode(&Node{ID: "n1", Labels: []string{"L"}}) },
		"DeleteNode": func() error { return engine.DeleteNode("n1") },
		"CreateEdge": func() error {
			return engine.CreateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "R"})
		},
		"UpdateEdge":      func() error { return engine.UpdateEdge(&Edge{ID: "e1", StartNode: "n1", EndNode: "n1", Type: "R"}) },
		"DeleteEdge":      func() error { return engine.DeleteEdge("e1") },
		"BulkCreateNodes": func() error { return engine.BulkCreateNodes([]*Node{{ID: "n2", Labels: []string{"L"}}}) },
		"BulkCreateEdges": func() error {
			return engine.BulkCreateEdges([]*Edge{{ID: "e2", StartNode: "n1", EndNode: "n2", Type: "R"}})
		},
		"BulkDeleteNodes": func() error { return engine.BulkDeleteNodes([]NodeID{"n2"}) },
		"BulkDeleteEdges": func() error { return engine.BulkDeleteEdges([]EdgeID{"e2"}) },
		"GetNodesByLabel": func() error { _, err := engine.GetNodesByLabel("L"); return err },
		"NodeCount":       func() error { _, err := engine.NodeCount(); return err },
		"QueryCypher": func() error {
			_, _, err := engine.QueryCypher(context.Background(), "RETURN 1", nil)
			return err
		},
	}
	for name, operation := range operations {
		before := len(statements)
		_ = operation() // what the runner returns is not a remote's reply; only the routing matters here
		require.Greater(t, len(statements), before, "%s runs through the runner", name)
	}

	tx, err := engine.BeginCypherTx(context.Background())
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
	_, err = engine.CreateNode(&Node{ID: "n3", Labels: []string{"L"}})
	require.ErrorIs(t, err, fail)
}
