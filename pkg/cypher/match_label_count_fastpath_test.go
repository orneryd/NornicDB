package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type labelCountGuardEngine struct {
	storage.Engine
	forbidLabelScan bool
	labelCountCalls int
}

func (e *labelCountGuardEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("GetNodesByLabel should not be called for simple labeled count")
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *labelCountGuardEngine) AllNodes() ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("AllNodes should not be called for simple labeled count")
	}
	return e.Engine.AllNodes()
}

func (e *labelCountGuardEngine) NodeCountByLabel(label string) (int64, error) {
	e.labelCountCalls++
	if counter, ok := e.Engine.(interface {
		NodeCountByLabel(string) (int64, error)
	}); ok {
		return counter.NodeCountByLabel(label)
	}
	return storage.CountNodesWithLabel(context.Background(), e.Engine, label)
}

func TestMatchCountUsesLabelCountFastPath(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &labelCountGuardEngine{Engine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:p-%d", i)), Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	_, err := eng.CreateNode(&storage.Node{ID: "nornic:o-1", Labels: []string{"Other"}})
	require.NoError(t, err)

	eng.forbidLabelScan = true

	res, err := exec.Execute(ctx, "MATCH (n:Person) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][0])
	require.Equal(t, 1, eng.labelCountCalls)
}

func TestMatchCountUsesLabelCountFastPathInsideTransaction(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	eng := &labelCountGuardEngine{Engine: base}

	for i := 0; i < 3; i++ {
		_, err = eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:person-%d", i)), Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	_, err = eng.CreateNode(&storage.Node{ID: "nornic:other", Labels: []string{"Other"}})
	require.NoError(t, err)

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	txStore := &transactionStorageWrapper{
		tx:             tx,
		underlying:     eng,
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	exec := NewStorageExecutor(txStore)
	eng.forbidLabelScan = true

	result, err := exec.Execute(context.Background(), "MATCH (n:Person) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
	require.Equal(t, 1, eng.labelCountCalls)

	_, err = txStore.CreateNode(&storage.Node{ID: "nornic:person-pending", Labels: []string{"Person"}})
	require.NoError(t, err)
	result, err = exec.Execute(context.Background(), "MATCH (person:Person) RETURN count(person) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
	require.Equal(t, 1, eng.labelCountCalls, "pending mutations require a transaction-visible count")
}
