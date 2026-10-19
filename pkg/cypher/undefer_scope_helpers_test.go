package cypher

import (
	"context"
	"strings"
	"testing"
	"github.com/orneryd/nornicdb/pkg/storage"
)


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
