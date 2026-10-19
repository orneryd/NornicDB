package cypher

import (
	"context"
	"strings"
	"testing"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func perRowTestExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	nodes := []*storage.Node{
		{ID: "a", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "a"}},
		{ID: "b", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "b"}},
		{ID: "c", Labels: []string{"P"}, Properties: map[string]interface{}{"id": "c"}},
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		t.Fatal(err)
	}
	if err := store.BulkCreateEdges([]*storage.Edge{{ID: "r1", Type: "R", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"kind": "k"}}}); err != nil {
		t.Fatal(err)
	}
	return NewStorageExecutor(store), store
}

func runPerRowRound5Statements(t *testing.T, statements string) ([][]interface{}, error) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5"))
	var result *ExecuteResult
	var err error
	for _, statement := range strings.Split(statements, ";;") {
		result, err = exec.Execute(context.Background(), strings.TrimSpace(statement), nil)
		if err != nil {
			return nil, err
		}
	}
	return result.Rows, nil
}
