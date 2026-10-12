package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// In-transaction UNIQUE / NODE KEY / TEMPORAL checks compare a node against
// the transaction's other pending nodes through the pending-node index once
// the transaction holds more than pendingIndexMinNodes nodes. These tests pin
// that the indexed path finds exactly what the old pass over every pending
// node found, including after pending nodes are rewritten or deleted.

func pendingNode(id string, label string, props map[string]interface{}) *Node {
	return &Node{ID: NodeID(prefixTestID(id)), Labels: []string{label}, Properties: props}
}

func TestPendingConstraintIndex_UniqueDuplicatesInTransaction(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("u_k", "Item", "k"))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	for i := 0; i < 3*pendingIndexMinNodes; i++ {
		_, err := tx.CreateNode(pendingNode(fmt.Sprintf("u-%d", i), "Item", map[string]interface{}{"k": int64(i)}))
		require.NoError(t, err)
	}
	// A numeric value equal across types is a duplicate, as before.
	_, err = tx.CreateNode(pendingNode("u-dup", "Item", map[string]interface{}{"k": float64(5)}))
	require.Error(t, err, "duplicate of a pending value must be rejected")

	// Rewriting a pending node frees its old value and takes the new one.
	require.NoError(t, tx.UpdateNode(pendingNode("u-3", "Item", map[string]interface{}{"k": int64(1000)})))
	_, err = tx.CreateNode(pendingNode("u-reuse", "Item", map[string]interface{}{"k": int64(3)}))
	require.NoError(t, err, "the value a rewritten pending node gave up is free again")
	_, err = tx.CreateNode(pendingNode("u-dup2", "Item", map[string]interface{}{"k": int64(1000)}))
	require.Error(t, err, "the rewritten node's new value is taken")

	// Deleting a pending node frees its value.
	require.NoError(t, tx.DeleteNode(NodeID(prefixTestID("u-7"))))
	_, err = tx.CreateNode(pendingNode("u-reuse7", "Item", map[string]interface{}{"k": int64(7)}))
	require.NoError(t, err, "the value of a deleted pending node is free again")

	// Another label with the same value is not a duplicate.
	_, err = tx.CreateNode(pendingNode("other-5", "Other", map[string]interface{}{"k": int64(5)}))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func TestPendingConstraintIndex_NodeKeyDuplicatesInTransaction(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
		Name: "user_key", Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "name"},
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	for i := 0; i < 3*pendingIndexMinNodes; i++ {
		_, err := tx.CreateNode(pendingNode(fmt.Sprintf("nk-%d", i), "User", map[string]interface{}{"tenant": "t1", "name": fmt.Sprintf("user%d", i)}))
		require.NoError(t, err)
	}
	_, err = tx.CreateNode(pendingNode("nk-other-tenant", "User", map[string]interface{}{"tenant": "t2", "name": "user4"}))
	require.NoError(t, err, "same name under another tenant is a different key")
	_, err = tx.CreateNode(pendingNode("nk-dup", "User", map[string]interface{}{"tenant": "t1", "name": "user4"}))
	require.Error(t, err, "the full composite key of a pending node must be rejected")
	require.NoError(t, tx.Commit())
}

func TestPendingConstraintIndex_TemporalOverlapInTransaction(t *testing.T) {
	engine, cleanup := setupTestBadgerEngine(t)
	defer cleanup()
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
		Name: "fact_window", Type: ConstraintTemporal, EntityType: ConstraintEntityNode,
		Label: "Fact", Properties: []string{"record_id", "valid_from", "valid_to"},
	}))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	// Many records, each with two adjacent non-overlapping windows.
	for r := 0; r < 2*pendingIndexMinNodes; r++ {
		for w := 0; w < 2; w++ {
			_, err := tx.CreateNode(pendingNode(fmt.Sprintf("f-%d-%d", r, w), "Fact", map[string]interface{}{
				"record_id": int64(r), "valid_from": base.Add(time.Duration(w) * day), "valid_to": base.Add(time.Duration(w+1) * day),
			}))
			require.NoError(t, err)
		}
	}
	_, err = tx.CreateNode(pendingNode("f-overlap", "Fact", map[string]interface{}{
		"record_id": int64(3), "valid_from": base.Add(12 * time.Hour), "valid_to": base.Add(36 * time.Hour),
	}))
	require.Error(t, err, "a window overlapping a pending window of the same record must be rejected")
	_, err = tx.CreateNode(pendingNode("f-other-record", "Fact", map[string]interface{}{
		"record_id": int64(999), "valid_from": base.Add(12 * time.Hour), "valid_to": base.Add(36 * time.Hour),
	}))
	require.NoError(t, err, "the same window on another record does not overlap")
	require.NoError(t, tx.Commit())
}

// Validating each node of a large transaction must not visit every other
// pending node: an n-node transaction was O(n^2), once while it wrote and
// again at commit. With the pending index the work per node is constant, so
// 8x the nodes costs about 8x the time (quadratic would be 64x).
func TestPendingConstraintIndex_LargeTransactionScalesLinearly(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	run := func(n int) time.Duration {
		engine, cleanup := setupTestBadgerEngine(t)
		defer cleanup()
		require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("u_k", "Item", "k"))
		require.NoError(t, RefreshUniqueConstraintValuesForEngine(engine, engine.GetSchemaForNamespace("test")))
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		start := time.Now()
		for i := 0; i < n; i++ {
			_, err := tx.CreateNode(pendingNode(fmt.Sprintf("s-%d", i), "Item", map[string]interface{}{"k": int64(i)}))
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
		return time.Since(start)
	}
	run(500) // warm up
	small, large := run(2000), run(16000)
	ratio := float64(large) / float64(small)
	t.Logf("2000 nodes: %v, 16000 nodes: %v, ratio %.1f (linear ~8, quadratic ~64)", small, large, ratio)
	require.Less(t, ratio, 24.0, "a large transaction's constraint validation grows faster than linearly")
}
