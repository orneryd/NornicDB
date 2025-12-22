package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetWithReturnAlias tests that MATCH ... SET ... RETURN n returns
// the node under the correct alias 'n', not 'matched'
func TestSetWithReturnAlias(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("SET += with RETURN n", func(t *testing.T) {
		// Create a node
		_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-123', name: 'Original'})`, nil)
		require.NoError(t, err)

		// Update with SET += and RETURN n
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-123'})
			SET n += {status: 'updated', count: 42}
			RETURN n
		`, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"n"}, result.Columns, "Column should be 'n', not 'matched'")
		require.Len(t, result.Rows, 1)

		// Verify the returned node has updated properties
		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "test-123", node.Properties["id"])
		assert.Equal(t, "Original", node.Properties["name"])
		assert.Equal(t, "updated", node.Properties["status"])
		assert.Equal(t, int64(42), node.Properties["count"])
	})

	t.Run("SET property with RETURN n", func(t *testing.T) {
		// Create a node
		_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-456', value: 1})`, nil)
		require.NoError(t, err)

		// Update with regular SET and RETURN n
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-456'})
			SET n.value = 10
			RETURN n
		`, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"n"}, result.Columns, "Column should be 'n', not 'matched'")
		require.Len(t, result.Rows, 1)

		// Verify the returned node has updated property
		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "test-456", node.Properties["id"])
		assert.Equal(t, int64(10), node.Properties["value"])
	})

	t.Run("SET += without RETURN returns matched count", func(t *testing.T) {
		// Create a node
		_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-789'})`, nil)
		require.NoError(t, err)

		// Update with SET += but no RETURN
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-789'})
			SET n += {updated: true}
		`, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"matched"}, result.Columns, "Without RETURN, should return 'matched' count")
		require.Len(t, result.Rows, 1)
		assert.Equal(t, 1, result.Rows[0][0])
	})

	t.Run("SET += with RETURN n.property", func(t *testing.T) {
		// Create a node
		_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-abc', name: 'Test'})`, nil)
		require.NoError(t, err)

		// Update and return specific property
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-abc'})
			SET n += {version: 2}
			RETURN n.name, n.version
		`, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"n.name", "n.version"}, result.Columns)
		require.Len(t, result.Rows, 1)
		assert.Equal(t, "Test", result.Rows[0][0])
		assert.Equal(t, int64(2), result.Rows[0][1])
	})

	t.Run("SET += with aliased RETURN", func(t *testing.T) {
		// Create a node
		_, err := exec.Execute(ctx, `CREATE (n:Node {id: 'test-def', title: 'MyTitle'})`, nil)
		require.NoError(t, err)

		// Update and return with alias
		result, err := exec.Execute(ctx, `
			MATCH (n:Node {id: 'test-def'})
			SET n += {active: true}
			RETURN n AS node
		`, nil)

		require.NoError(t, err)
		assert.Equal(t, []string{"node"}, result.Columns, "Should use alias 'node'")
		require.Len(t, result.Rows, 1)

		node := result.Rows[0][0].(*storage.Node)
		assert.Equal(t, "test-def", node.Properties["id"])
		assert.Equal(t, "MyTitle", node.Properties["title"])
		assert.Equal(t, true, node.Properties["active"])
	})
}

// TestSetReturnMultipleNodes tests SET with multiple matched nodes
func TestSetReturnMultipleNodes(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple nodes
	_, err := exec.Execute(ctx, `
		CREATE (n1:Item {id: 'item1', status: 'pending'})
		CREATE (n2:Item {id: 'item2', status: 'pending'})
		CREATE (n3:Item {id: 'item3', status: 'pending'})
	`, nil)
	require.NoError(t, err)

	// Update all and return
	result, err := exec.Execute(ctx, `
		MATCH (n:Item)
		SET n += {status: 'active'}
		RETURN n
	`, nil)

	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, result.Columns, "Column should be 'n', not 'matched'")
	assert.Len(t, result.Rows, 3, "Should return all 3 updated nodes")

	// Verify all nodes have updated status
	for _, row := range result.Rows {
		node := row[0].(*storage.Node)
		assert.Equal(t, "active", node.Properties["status"])
	}
}

// TestSetReturnAliasRegressionBug is a regression test for the bug where
// MATCH ... SET ... RETURN n was returning results under 'matched' instead of 'n'
func TestSetReturnAliasRegressionBug(t *testing.T) {
	// This reproduces the exact scenario from the Mimir GraphManager
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a task node (simulating Mimir's task structure)
	_, err := exec.Execute(ctx, `
		CREATE (n:Node {
			id: 'task-123',
			type: 'task',
			title: 'Implement feature',
			status: 'pending'
		})
	`, nil)
	require.NoError(t, err)

	// Update task using the exact pattern from GraphManager.updateNode()
	result, err := exec.Execute(ctx, `
		MATCH (n:Node {id: 'task-123'})
		SET n += {status: 'in_progress', assignee: 'alice'}
		RETURN n
	`, nil)

	require.NoError(t, err)

	// BUG: This was returning 'matched' instead of 'n'
	assert.Equal(t, []string{"n"}, result.Columns,
		"RETURN n should return column 'n', not 'matched' (regression from Mimir bug)")

	require.Len(t, result.Rows, 1)

	node := result.Rows[0][0].(*storage.Node)
	assert.Equal(t, "task-123", node.Properties["id"])
	assert.Equal(t, "in_progress", node.Properties["status"])
	assert.Equal(t, "alice", node.Properties["assignee"])
	assert.Equal(t, "Implement feature", node.Properties["title"])
}
