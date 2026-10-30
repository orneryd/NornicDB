package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteWithINQuery tests the IN operator in WHERE clauses for DELETE operations.
// This test suite verifies that IN queries work correctly and don't accidentally
// delete unintended nodes, which was the original bug.
func TestDeleteWithINQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupTestNodes := func(t *testing.T) ([]string, []string) {
		// Clear existing nodes first
		_, err := exec.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
		require.NoError(t, err)

		// Create nodes with explicit IDs
		nodes := []struct {
			id    string
			name  string
			label string
		}{
			{"node-1", "Alice", "Person"},
			{"node-2", "Bob", "Person"},
			{"node-3", "Charlie", "Person"},
			{"node-4", "Diana", "Person"},
		}

		nodeIds := make([]string, 0, len(nodes))
		for _, n := range nodes {
			node := &storage.Node{
				ID:         storage.NodeID(n.id),
				Labels:     []string{n.label},
				Properties: map[string]interface{}{"name": n.name, "id": n.id},
			}
			_, err := store.CreateNode(node)
			require.NoError(t, err)
			nodeIds = append(nodeIds, n.id)
		}

		// Verify all nodes were created
		result, err := exec.Execute(ctx, "MATCH (n:Person) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(4), count, "Should have 4 nodes initially")

		return nodeIds[:2], nodeIds[2:] // Return IDs to delete and IDs to keep
	}

	t.Run("delete with literal IN list using n.id property", func(t *testing.T) {
		toDelete, toKeep := setupTestNodes(t)

		// Delete nodes using literal IN list with n.id property
		query := `MATCH (n:Person) WHERE n.id IN ["node-1", "node-2"] DETACH DELETE n RETURN count(n) as deleted`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
		assert.NotContains(t, remainingIds, toDelete[0], "Should not have node-1")
		assert.NotContains(t, remainingIds, toDelete[1], "Should not have node-2")
	})

	t.Run("delete with parameter IN list using n.id property", func(t *testing.T) {
		_, toKeep := setupTestNodes(t)

		// Delete nodes using parameter IN list with n.id property
		query := `MATCH (n:Person) WHERE n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
	})

	t.Run("delete with id(n) IN parameter list", func(t *testing.T) {
		_, toKeep := setupTestNodes(t)

		// Delete nodes using id(n) function with IN parameter list
		query := `MATCH (n:Person) WHERE id(n) IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
	})

	t.Run("delete with elementId(n) IN parameter list", func(t *testing.T) {
		_, toKeep := setupTestNodes(t)

		query := `MATCH (n:Person) WHERE elementId(n) IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{storage.NodeElementID("test", "node-1"), storage.NodeElementID("test", "node-2")},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
	})

	t.Run("delete with OR condition id(n) IN OR n.id IN - should not delete all", func(t *testing.T) {
		toDelete, toKeep := setupTestNodes(t)

		// This is the problematic query pattern from the UI bug
		// WHERE id(n) IN $ids OR n.id IN $ids
		// This should only delete the specified nodes, not all nodes
		query := `MATCH (n:Person) WHERE id(n) IN $ids OR n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)

		// CRITICAL: Should only delete 2 nodes, not all 4
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes, not all nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
		assert.NotContains(t, remainingIds, toDelete[0], "Should not have node-1")
		assert.NotContains(t, remainingIds, toDelete[1], "Should not have node-2")
	})

	t.Run("delete with OR condition elementId(n) IN OR n.id IN - should not delete all", func(t *testing.T) {
		toDelete, toKeep := setupTestNodes(t)

		query := `MATCH (n:Person) WHERE elementId(n) IN $ids OR n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{storage.NodeElementID("test", "node-1"), storage.NodeElementID("test", "node-2")},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete exactly 2 nodes, not all nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
		assert.NotContains(t, remainingIds, toDelete[0], "Should not have node-1")
		assert.NotContains(t, remainingIds, toDelete[1], "Should not have node-2")
	})

	t.Run("delete with single node ID - should only delete that node", func(t *testing.T) {
		setupTestNodes(t)

		// Delete a single node using id(n) OR n.id pattern
		query := `MATCH (n:Person) WHERE id(n) = $nodeId OR n.id = $nodeId DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"nodeId": "node-1",
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(1), deleted, "Should delete exactly 1 node")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 3, "Should have 3 nodes remaining")
		remainingIds := make([]string, len(result.Rows))
		for i, row := range result.Rows {
			remainingIds[i] = row[0].(string)
		}
		assert.NotContains(t, remainingIds, "node-1", "Should not have node-1")
		assert.Contains(t, remainingIds, "node-2", "Should keep node-2")
		assert.Contains(t, remainingIds, "node-3", "Should keep node-3")
		assert.Contains(t, remainingIds, "node-4", "Should keep node-4")
	})

	t.Run("delete with empty IN list - should delete nothing", func(t *testing.T) {
		setupTestNodes(t)

		// Delete with empty IN list
		query := `MATCH (n:Person) WHERE n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(0), deleted, "Should delete 0 nodes")

		// Verify all nodes still exist
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(4), count, "Should still have all 4 nodes")
	})

	t.Run("delete with non-matching IN list - should delete nothing", func(t *testing.T) {
		setupTestNodes(t)

		// Delete with IDs that don't exist
		query := `MATCH (n:Person) WHERE n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{"non-existent-1", "non-existent-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(0), deleted, "Should delete 0 nodes")

		// Verify all nodes still exist
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN count(n) as cnt", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		count := result.Rows[0][0].(int64)
		assert.Equal(t, int64(4), count, "Should still have all 4 nodes")
	})

	t.Run("delete with partial match IN list - should only delete matching nodes", func(t *testing.T) {
		_, toKeep := setupTestNodes(t)

		// Delete with mix of existing and non-existing IDs
		query := `MATCH (n:Person) WHERE n.id IN $ids DETACH DELETE n RETURN count(n) as deleted`
		params := map[string]interface{}{
			"ids": []string{"node-1", "non-existent", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		deleted := result.Rows[0][0].(int64)
		assert.Equal(t, int64(2), deleted, "Should delete only the 2 existing nodes")

		// Verify remaining nodes
		result, err = exec.Execute(ctx, "MATCH (n:Person) RETURN n.id as id ORDER BY n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should have 2 nodes remaining")
		remainingIds := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, remainingIds, toKeep[0], "Should keep node-3")
		assert.Contains(t, remainingIds, toKeep[1], "Should keep node-4")
	})
}

// TestMatchWithINQuery tests the IN operator in WHERE clauses for MATCH operations.
// This verifies that IN queries work correctly for selection, not just deletion.
func TestMatchWithINQuery(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	setupTestNodes := func(t *testing.T) {
		// Clear existing nodes first
		_, err := exec.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
		require.NoError(t, err)

		nodes := []struct {
			id    string
			name  string
			label string
		}{
			{"node-1", "Alice", "Person"},
			{"node-2", "Bob", "Person"},
			{"node-3", "Charlie", "Person"},
			{"node-4", "Diana", "Person"},
		}

		for _, n := range nodes {
			node := &storage.Node{
				ID:         storage.NodeID(n.id),
				Labels:     []string{n.label},
				Properties: map[string]interface{}{"name": n.name, "id": n.id},
			}
			_, err := store.CreateNode(node)
			require.NoError(t, err)
		}
	}

	t.Run("match with literal IN list using n.id property", func(t *testing.T) {
		setupTestNodes(t)

		query := `MATCH (n:Person) WHERE n.id IN ["node-1", "node-2"] RETURN n.id as id ORDER BY n.id`
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match 2 nodes")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
		assert.NotContains(t, ids, "node-3")
		assert.NotContains(t, ids, "node-4")
	})

	t.Run("match with parameter IN list using n.id property", func(t *testing.T) {
		setupTestNodes(t)

		query := `MATCH (n:Person) WHERE n.id IN $ids RETURN n.id as id ORDER BY n.id`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match 2 nodes")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
	})

	t.Run("match with id(n) IN parameter list", func(t *testing.T) {
		setupTestNodes(t)

		query := `MATCH (n:Person) WHERE id(n) IN $ids RETURN n.id as id ORDER BY n.id`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match 2 nodes")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
	})

	t.Run("match with elementId(n) IN parameter list", func(t *testing.T) {
		setupTestNodes(t)

		query := `MATCH (n:Person) WHERE elementId(n) IN $ids RETURN n.id as id ORDER BY n.id`
		params := map[string]interface{}{
			"ids": []string{storage.NodeElementID("test", "node-1"), storage.NodeElementID("test", "node-2")},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match 2 nodes")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
	})

	t.Run("match with OR condition id(n) IN OR n.id IN", func(t *testing.T) {
		setupTestNodes(t)

		// This is the problematic query pattern - should only match specified nodes
		query := `MATCH (n:Person) WHERE id(n) IN $ids OR n.id IN $ids RETURN n.id as id ORDER BY n.id`
		params := map[string]interface{}{
			"ids": []string{"node-1", "node-2"},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match exactly 2 nodes, not all 4")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
		assert.NotContains(t, ids, "node-3")
		assert.NotContains(t, ids, "node-4")
	})

	t.Run("match with OR condition elementId(n) IN OR n.id IN", func(t *testing.T) {
		setupTestNodes(t)

		query := `MATCH (n:Person) WHERE elementId(n) IN $ids OR n.id IN $ids RETURN n.id as id ORDER BY n.id`
		params := map[string]interface{}{
			"ids": []string{storage.NodeElementID("test", "node-1"), storage.NodeElementID("test", "node-2")},
		}
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should match exactly 2 nodes, not all 4")
		ids := []string{
			result.Rows[0][0].(string),
			result.Rows[1][0].(string),
		}
		assert.Contains(t, ids, "node-1")
		assert.Contains(t, ids, "node-2")
		assert.NotContains(t, ids, "node-3")
		assert.NotContains(t, ids, "node-4")
	})
}
