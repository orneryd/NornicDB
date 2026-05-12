package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if tx == nil {
		t.Fatal("Expected non-nil transaction")
	}
	if tx.ID == "" {
		t.Error("Transaction ID should be set")
	}
	if tx.Status != TxStatusActive {
		t.Errorf("Expected active status, got %s", tx.Status)
	}
	if tx.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}
	if tx.readTS.IsZero() {
		t.Error("readTS should be anchored at transaction begin")
	}
}

func TestTransaction_CreateNode_Basic(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	node := &Node{
		ID:         NodeID(prefixTestID("tx-node-1")),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "Test Node"},
	}

	// Create in transaction
	_, err := tx.CreateNode(node)
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Node should NOT be visible in engine yet (not committed)
	_, err = engine.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != ErrNotFound {
		t.Error("Node should not be visible before commit")
	}

	// Node should be visible within transaction (read-your-writes)
	txNode, err := tx.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != nil {
		t.Errorf("GetNode in transaction failed: %v", err)
	}
	if txNode.Properties["name"] != "Test Node" {
		t.Error("Node properties mismatch")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now node should be visible in engine
	stored, err := engine.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != nil {
		t.Fatalf("GetNode after commit failed: %v", err)
	}
	if stored.Properties["name"] != "Test Node" {
		t.Error("Node properties mismatch after commit")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Create some nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("rollback-node-" + string(rune('0'+i)))),
			Labels: []string{"Rollback"},
		}
		if _, err := tx.CreateNode(node); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}

	// Verify operations buffered
	if tx.OperationCount() != 5 {
		t.Errorf("Expected 5 operations, got %d", tx.OperationCount())
	}

	// Rollback
	err := tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify status
	if tx.Status != TxStatusRolledBack {
		t.Errorf("Expected rolled_back status, got %s", tx.Status)
	}

	// Verify nodes not in engine
	for i := 0; i < 5; i++ {
		_, err := engine.GetNode(NodeID(prefixTestID("rollback-node-" + string(rune('0'+i)))))
		if err != ErrNotFound {
			t.Error("Node should not exist after rollback")
		}
	}
}

func TestTransaction_Atomicity(t *testing.T) {
	engine := NewMemoryEngine()

	// Pre-create a node that will cause conflict
	conflictNode := &Node{ID: NodeID(prefixTestID("conflict-node")), Labels: []string{"Conflict"}}
	if _, err := engine.CreateNode(conflictNode); err != nil {
		t.Fatalf("Pre-create failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Create some nodes
	for i := 0; i < 3; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("atomic-node-" + string(rune('0'+i)))),
			Labels: []string{"Atomic"},
		}
		if _, err := tx.CreateNode(node); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}

	// Try to create the conflicting node (should fail at commit)
	node := &Node{ID: NodeID(prefixTestID("conflict-node")), Labels: []string{"Conflict"}}
	// This will succeed in transaction (we check at commit time)
	// But when we commit, it should fail

	// For this test, let's verify that creating a node with same ID in TX fails
	_, err := tx.CreateNode(node)
	if err != ErrAlreadyExists {
		t.Errorf("Expected ErrAlreadyExists, got %v", err)
	}

	// Commit the valid operations
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// All atomic nodes should exist
	for i := 0; i < 3; i++ {
		_, err := engine.GetNode(NodeID(prefixTestID("atomic-node-" + string(rune('0'+i)))))
		if err != nil {
			t.Errorf("Node atomic-node-%d should exist after commit", i)
		}
	}
}

func TestTransaction_DeleteNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create a node first
	node := &Node{ID: NodeID(prefixTestID("delete-me")), Labels: []string{"Delete"}}
	if _, err := engine.CreateNode(node); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Delete in transaction
	err := tx.DeleteNode(NodeID(prefixTestID("delete-me")))
	if err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	// Node should NOT be deleted from engine yet
	_, err = engine.GetNode(NodeID(prefixTestID("delete-me")))
	if err != nil {
		t.Error("Node should still exist before commit")
	}

	// But should not be visible in transaction
	_, err = tx.GetNode(NodeID(prefixTestID("delete-me")))
	if err != ErrNotFound {
		t.Error("Node should not be visible in transaction after delete")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now node should be gone
	_, err = engine.GetNode(NodeID(prefixTestID("delete-me")))
	if err != ErrNotFound {
		t.Error("Node should not exist after commit")
	}
}

func TestTransaction_UpdateNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create a node first
	node := &Node{
		ID:         NodeID(prefixTestID("update-me")),
		Labels:     []string{"Update"},
		Properties: map[string]interface{}{"version": 1},
	}
	if _, err := engine.CreateNode(node); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Update in transaction
	updatedNode := &Node{
		ID:         NodeID(prefixTestID("update-me")),
		Labels:     []string{"Updated"},
		Properties: map[string]interface{}{"version": 2},
	}
	err := tx.UpdateNode(updatedNode)
	if err != nil {
		t.Fatalf("UpdateNode failed: %v", err)
	}

	// Engine should still have old version
	old, _ := engine.GetNode(NodeID(prefixTestID("update-me")))
	if old.Properties["version"] != 1 {
		t.Error("Engine should still have old version before commit")
	}

	// Transaction should have new version
	txNode, _ := tx.GetNode(NodeID(prefixTestID("update-me")))
	if txNode.Properties["version"] != 2 {
		t.Error("Transaction should have new version")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Engine should have new version
	updated, err := engine.GetNode(NodeID(prefixTestID("update-me")))
	if err != nil {
		t.Fatalf("GetNode after commit failed: %v", err)
	}
	// Note: JSON serialization may convert int to float64
	version, ok := updated.Properties["version"].(float64)
	if !ok {
		vInt, ok := updated.Properties["version"].(int)
		if ok {
			version = float64(vInt)
		}
	}
	if version != 2 {
		t.Errorf("Engine should have new version after commit, got version=%v (type %T)",
			updated.Properties["version"], updated.Properties["version"])
	}
}

func TestTransaction_CommitRejectsConstraintContractViolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	contract := ConstraintContract{
		Name:              "person_contract",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Definition:        "CREATE CONSTRAINT person_contract FOR (n:Person) REQUIRE { n.status IN ['active', 'inactive'] }",
		Entries: []ConstraintContractEntry{{
			Kind:       ConstraintContractKindBooleanNode,
			Expression: "n.status IN ['active', 'inactive']",
		}},
	}
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraintContractBundle(contract, nil, nil, false))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	_, err = tx.CreateNode(&Node{
		ID:     NodeID("test:contract-violator"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"status": "paused",
		},
	})
	require.NoError(t, err)

	err = tx.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "constraint contract person_contract violated")
	require.Contains(t, err.Error(), "n.status IN ['active', 'inactive']")

	_, err = engine.GetNode(NodeID("test:contract-violator"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTransaction_CreateEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes first
	node1 := &Node{ID: NodeID(prefixTestID("edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("edge-node-2")), Labels: []string{"Node"}}
	engine.CreateNode(node1)
	engine.CreateNode(node2)

	tx, _ := engine.BeginTransaction()

	// Create edge in transaction
	edge := &Edge{
		ID:        EdgeID(prefixTestID("tx-edge-1")),
		StartNode: NodeID(prefixTestID("edge-node-1")),
		EndNode:   NodeID(prefixTestID("edge-node-2")),
		Type:      "CONNECTS",
	}
	err := tx.CreateEdge(edge)
	if err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	// Edge should NOT exist in engine yet
	_, err = engine.GetEdge(EdgeID(prefixTestID("tx-edge-1")))
	if err != ErrNotFound {
		t.Error("Edge should not exist before commit")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Edge should exist now
	stored, err := engine.GetEdge(EdgeID(prefixTestID("tx-edge-1")))
	if err != nil {
		t.Fatalf("GetEdge after commit failed: %v", err)
	}
	if stored.Type != "CONNECTS" {
		t.Error("Edge type mismatch")
	}
}

func TestTransaction_UpdateEdge(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	node1 := &Node{ID: NodeID(prefixTestID("update-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("update-edge-node-2")), Labels: []string{"Node"}}
	node3 := &Node{ID: NodeID(prefixTestID("update-edge-node-3")), Labels: []string{"Node"}}
	if _, err := engine.CreateNode(node1); err != nil {
		t.Fatalf("CreateNode node1 failed: %v", err)
	}
	if _, err := engine.CreateNode(node2); err != nil {
		t.Fatalf("CreateNode node2 failed: %v", err)
	}
	if _, err := engine.CreateNode(node3); err != nil {
		t.Fatalf("CreateNode node3 failed: %v", err)
	}

	edgeID := EdgeID(prefixTestID("update-edge-1"))
	if err := engine.CreateEdge(&Edge{
		ID:         edgeID,
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Type:       "OLD",
		Properties: map[string]interface{}{"version": 1},
	}); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()
	updated := &Edge{
		ID:         edgeID,
		StartNode:  node2.ID,
		EndNode:    node3.ID,
		Type:       "NEW",
		Properties: map[string]interface{}{"version": 2},
	}
	if err := tx.UpdateEdge(updated); err != nil {
		t.Fatalf("UpdateEdge failed: %v", err)
	}

	storedBefore, err := engine.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("GetEdge before commit failed: %v", err)
	}
	if storedBefore.Type != "OLD" || storedBefore.StartNode != node1.ID || storedBefore.EndNode != node2.ID {
		t.Error("Engine edge should remain unchanged before commit")
	}

	txEdge, ok := tx.pendingEdges[edgeID]
	if !ok {
		t.Fatal("Transaction should track updated edge in pendingEdges")
	}
	if txEdge.Type != "NEW" || txEdge.StartNode != node2.ID || txEdge.EndNode != node3.ID {
		t.Error("Transaction should expose updated edge")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	storedAfter, err := engine.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("GetEdge after commit failed: %v", err)
	}
	if storedAfter.Type != "NEW" || storedAfter.StartNode != node2.ID || storedAfter.EndNode != node3.ID {
		t.Error("Engine should store updated edge after commit")
	}

	outgoingOld, _ := engine.GetOutgoingEdges(node1.ID)
	if len(outgoingOld) != 0 {
		t.Error("Old outgoing index should be cleared")
	}
	outgoingNew, _ := engine.GetOutgoingEdges(node2.ID)
	if len(outgoingNew) != 1 || outgoingNew[0].ID != edgeID {
		t.Error("New outgoing index should include updated edge")
	}
	incomingOld, _ := engine.GetIncomingEdges(node2.ID)
	if len(incomingOld) != 0 {
		t.Error("Old incoming index should be cleared")
	}
	incomingNew, _ := engine.GetIncomingEdges(node3.ID)
	if len(incomingNew) != 1 || incomingNew[0].ID != edgeID {
		t.Error("New incoming index should include updated edge")
	}
	typed, _ := engine.GetEdgesByType("NEW")
	if len(typed) != 1 || typed[0].ID != edgeID {
		t.Error("Updated type index should include edge")
	}
}

func TestTransaction_UpdateEdge_ValidationPaths(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, _ := engine.BeginTransaction()
	if err := tx.UpdateEdge(nil); err != ErrInvalidData {
		t.Errorf("Expected ErrInvalidData, got %v", err)
	}
	if err := tx.UpdateEdge(&Edge{}); err != ErrInvalidID {
		t.Errorf("Expected ErrInvalidID, got %v", err)
	}

	node1 := &Node{ID: NodeID(prefixTestID("validate-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("validate-edge-node-2")), Labels: []string{"Node"}}
	if _, err := engine.CreateNode(node1); err != nil {
		t.Fatalf("CreateNode node1 failed: %v", err)
	}
	if _, err := engine.CreateNode(node2); err != nil {
		t.Fatalf("CreateNode node2 failed: %v", err)
	}
	edgeID := EdgeID(prefixTestID("validate-edge-1"))
	if err := engine.CreateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	tx2, _ := engine.BeginTransaction()
	if err := tx2.DeleteEdge(edgeID); err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}
	if err := tx2.UpdateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for deleted edge, got %v", err)
	}

	tx3, _ := engine.BeginTransaction()
	err := tx3.UpdateEdge(&Edge{
		ID:        edgeID,
		StartNode: node1.ID,
		EndNode:   NodeID(prefixTestID("missing-node")),
		Type:      "REL",
	})
	if err == nil {
		t.Error("Expected missing endpoint validation error")
	}

	if err := tx3.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if err := tx.UpdateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransaction_CreateEdgeWithNewNodes(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Create nodes IN transaction
	node1 := &Node{ID: NodeID(prefixTestID("new-edge-node-1")), Labels: []string{"New"}}
	node2 := &Node{ID: NodeID(prefixTestID("new-edge-node-2")), Labels: []string{"New"}}
	tx.CreateNode(node1)
	tx.CreateNode(node2)

	// Create edge between new nodes (should work!)
	edge := &Edge{
		ID:        EdgeID(prefixTestID("new-edge-1")),
		StartNode: NodeID(prefixTestID("new-edge-node-1")),
		EndNode:   NodeID(prefixTestID("new-edge-node-2")),
		Type:      "LINKS",
	}
	err := tx.CreateEdge(edge)
	if err != nil {
		t.Fatalf("CreateEdge with new nodes failed: %v", err)
	}

	// Commit all
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all exist
	_, err = engine.GetNode(NodeID(prefixTestID("new-edge-node-1")))
	if err != nil {
		t.Error("Node 1 should exist")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("new-edge-node-2")))
	if err != nil {
		t.Error("Node 2 should exist")
	}
	_, err = engine.GetEdge(EdgeID(prefixTestID("new-edge-1")))
	if err != nil {
		t.Error("Edge should exist")
	}
}

func TestTransaction_DeleteEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes and edge first
	node1 := &Node{ID: NodeID(prefixTestID("del-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("del-edge-node-2")), Labels: []string{"Node"}}
	engine.CreateNode(node1)
	engine.CreateNode(node2)
	edge := &Edge{
		ID:        EdgeID(prefixTestID("delete-edge-1")),
		StartNode: NodeID(prefixTestID("del-edge-node-1")),
		EndNode:   NodeID(prefixTestID("del-edge-node-2")),
		Type:      "DELETE_ME",
	}
	engine.CreateEdge(edge)

	tx, _ := engine.BeginTransaction()

	// Delete edge in transaction
	err := tx.DeleteEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}

	// Edge should still exist in engine
	_, err = engine.GetEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != nil {
		t.Error("Edge should still exist before commit")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Edge should be gone
	_, err = engine.GetEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != ErrNotFound {
		t.Error("Edge should not exist after commit")
	}
}

func TestTransaction_ClosedTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Commit first
	tx.Commit()

	// Try operations on closed transaction
	node := &Node{ID: NodeID(prefixTestID("closed-test")), Labels: []string{"Test"}}
	_, err := tx.CreateNode(node)
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.UpdateNode(node)
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.DeleteNode(NodeID(prefixTestID("any")))
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.Commit()
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.Rollback()
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransaction_IsActive(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	if !tx.IsActive() {
		t.Error("New transaction should be active")
	}

	tx.Commit()

	if tx.IsActive() {
		t.Error("Committed transaction should not be active")
	}
}

func TestTransaction_ConfigSettersAndSkipCreateHelpers(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, _ := engine.BeginTransaction()
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		t.Fatalf("SetDeferredConstraintValidation failed: %v", err)
	}
	if !tx.deferConstraintValidation {
		t.Error("deferConstraintValidation should be enabled")
	}
	if err := tx.SetSkipCreateExistenceCheck(true); err != nil {
		t.Fatalf("SetSkipCreateExistenceCheck failed: %v", err)
	}
	if !tx.skipCreateExistenceCheck {
		t.Error("skipCreateExistenceCheck should be enabled")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := tx.SetDeferredConstraintValidation(false); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
	if err := tx.SetSkipCreateExistenceCheck(false); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	if shouldSkipCreateExistenceCheck(NodeID("test:550e8400-e29b-41d4-a716-446655440000")) != true {
		t.Error("Expected UUID-prefixed node ID to skip existence check")
	}
	if shouldSkipCreateExistenceCheck(NodeID(prefixTestID("non-uuid"))) {
		t.Error("Expected non-UUID node ID not to skip existence check")
	}
	if shouldSkipCreateExistenceCheck(NodeID("missingprefix")) {
		t.Error("Expected non-prefixed node ID not to skip existence check")
	}
}

func TestTransaction_CheckTemporalConstraint(t *testing.T) {
	newTemporalNode := func(id, key string, start interface{}, end interface{}) *Node {
		return &Node{
			ID:     NodeID(id),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": key,
				"from":    start,
				"to":      end,
			},
		}
	}

	constraint := Constraint{
		Type:       ConstraintTemporal,
		Label:      "Person",
		Properties: []string{"account", "from", "to"},
	}
	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)

	t.Run("validates basic temporal inputs", func(t *testing.T) {
		engine := NewMemoryEngine()
		defer engine.Close()
		tx, _ := engine.BeginTransaction()

		err := tx.checkTemporalConstraint(newTemporalNode(prefixTestID("temporal-count"), "acct", now, now.Add(time.Hour)), Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"account", "from"},
		})
		if err == nil {
			t.Fatal("expected property count error")
		}

		node := newTemporalNode(prefixTestID("temporal-null"), "", now, now.Add(time.Hour))
		node.Properties["account"] = nil
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected null key error")
		}

		node = newTemporalNode(prefixTestID("temporal-bad-start"), "acct", "bad", now.Add(time.Hour))
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected invalid start error")
		}

		node = newTemporalNode("unprefixed", "acct", now, now.Add(time.Hour))
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected unprefixed id error")
		}
	})

	t.Run("detects overlap and invalid pending nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		defer engine.Close()
		tx, _ := engine.BeginTransaction()

		tx.pendingNodes[NodeID(prefixTestID("pending-invalid"))] = &Node{
			ID:     NodeID(prefixTestID("pending-invalid")),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": "acct",
				"from":    "bad",
				"to":      now.Add(time.Hour),
			},
		}

		err := tx.checkTemporalConstraint(newTemporalNode(prefixTestID("pending-check"), "acct", now, now.Add(time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected invalid pending node error")
		}

		delete(tx.pendingNodes, NodeID(prefixTestID("pending-invalid")))
		tx.pendingNodes[NodeID(prefixTestID("pending-overlap"))] = newTemporalNode(prefixTestID("pending-overlap"), "acct", now, now.Add(2*time.Hour))

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("pending-check"), "acct", now.Add(time.Hour), now.Add(3*time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected pending overlap error")
		}
	})

	t.Run("detects committed invalid and overlapping intervals", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{
			ID:     NodeID(prefixTestID("committed-invalid")),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": "acct",
				"from":    "bad",
				"to":      now.Add(time.Hour),
			},
		})
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("committed-check"), "acct", now, now.Add(time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected committed invalid start error")
		}

		engine = createTestBadgerEngine(t)
		_, err = engine.CreateNode(newTemporalNode(prefixTestID("committed-overlap"), "acct", now, now.Add(2*time.Hour)))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
		tx, err = engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("committed-check"), "acct", now.Add(time.Hour), now.Add(3*time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected committed overlap error")
		}
	})

	t.Run("allows non-overlapping intervals and ignores same node id", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(newTemporalNode(prefixTestID("same-node"), "acct", now, now.Add(time.Hour)))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("same-node"), "acct", now, now.Add(time.Hour)), constraint)
		if err != nil {
			t.Fatalf("expected same-node update to be ignored, got %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("non-overlap"), "acct", now.Add(2*time.Hour), now.Add(3*time.Hour)), constraint)
		if err != nil {
			t.Fatalf("expected non-overlapping interval to pass, got %v", err)
		}
	})
}

func TestTransaction_CheckNodeKeyConstraintAndValidateNodeConstraints(t *testing.T) {
	newNode := func(id string, props map[string]interface{}) *Node {
		return &Node{
			ID:         NodeID(id),
			Labels:     []string{"Person"},
			Properties: props,
		}
	}

	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)

	t.Run("checkNodeKeyConstraint validates nulls pending duplicates and committed duplicates", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		c := Constraint{Type: ConstraintNodeKey, Label: "Person", Properties: []string{"tenant", "external_id"}}

		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-null"), map[string]interface{}{"tenant": "t1"}), c)
		if err == nil {
			t.Fatal("expected null node-key property error")
		}

		tx.pendingNodes[NodeID(prefixTestID("nk-pending"))] = newNode(prefixTestID("nk-pending"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		})
		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-check"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}), c)
		if err == nil {
			t.Fatal("expected pending node-key duplicate error")
		}

		engine = createTestBadgerEngine(t)
		_, err = engine.CreateNode(newNode(prefixTestID("nk-existing"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
		tx, err = engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-check"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}), c)
		if err == nil {
			t.Fatal("expected committed node-key duplicate error")
		}
	})

	t.Run("validateNodeConstraints checks prefix schema constraints and property types", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		schema := engine.GetSchemaForNamespace("test")
		if err := schema.AddConstraint(Constraint{Name: "person_name_exists", Type: ConstraintExists, Label: "Person", Properties: []string{"name"}}); err != nil {
			t.Fatalf("AddConstraint exists failed: %v", err)
		}
		if err := schema.AddConstraint(Constraint{Name: "person_node_key", Type: ConstraintNodeKey, Label: "Person", Properties: []string{"tenant", "external_id"}}); err != nil {
			t.Fatalf("AddConstraint node key failed: %v", err)
		}
		if err := schema.AddConstraint(Constraint{Name: "person_temporal", Type: ConstraintTemporal, Label: "Person", Properties: []string{"account", "from", "to"}}); err != nil {
			t.Fatalf("AddConstraint temporal failed: %v", err)
		}
		if err := schema.AddPropertyTypeConstraint("person_age_type", "Person", "age", PropertyTypeInteger); err != nil {
			t.Fatalf("AddPropertyTypeConstraint failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.validateNodeConstraints(newNode("unprefixed", map[string]interface{}{}))
		if err == nil {
			t.Fatal("expected unprefixed id validation error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("missing-name"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err == nil {
			t.Fatal("expected exists constraint error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("bad-type"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         "ten",
		}))
		if err == nil {
			t.Fatal("expected property type constraint error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("valid"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err != nil {
			t.Fatalf("expected valid node to pass, got %v", err)
		}

		tx.pendingNodes[NodeID(prefixTestID("dupe"))] = newNode(prefixTestID("dupe"), map[string]interface{}{
			"name":        "Bob",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct-2",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(11),
		})

		err = tx.validateNodeConstraints(newNode(prefixTestID("valid"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct-3",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err == nil {
			t.Fatal("expected node-key duplicate through validateNodeConstraints")
		}
	})
}

func TestTransaction_DeleteEdgesWithPrefixBuffered(t *testing.T) {
	t.Run("buffers edge and index deletions for matched prefix", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("del-node-1")), Labels: []string{"Person"}})
		if err != nil {
			t.Fatalf("CreateNode 1 failed: %v", err)
		}
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("del-node-2")), Labels: []string{"Person"}})
		if err != nil {
			t.Fatalf("CreateNode 2 failed: %v", err)
		}
		edge := &Edge{ID: EdgeID(prefixTestID("del-edge-1")), StartNode: NodeID(prefixTestID("del-node-1")), EndNode: NodeID(prefixTestID("del-node-2")), Type: "KNOWS"}
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("CreateEdge failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		outPrefix := engine.outgoingIndexPrefixString(edge.StartNode)
		if outPrefix == nil {
			t.Fatal("outgoing prefix missing for seeded edge")
		}
		count, ids, err := tx.deleteEdgesWithPrefixBuffered(outPrefix)
		if err != nil {
			t.Fatalf("deleteEdgesWithPrefixBuffered failed: %v", err)
		}
		if count != 1 || len(ids) != 1 || ids[0] != edge.ID {
			t.Fatalf("unexpected delete result: count=%d ids=%v", count, ids)
		}
		outKey := engine.outgoingIndexKeyStringLookup(edge.StartNode, edge.ID)
		inKey := engine.incomingIndexKeyStringLookup(edge.EndNode, edge.ID)
		typeKey := engine.edgeTypeIndexKeyStringLookup(edge.Type, edge.ID)
		if !tx.pendingDeletes[string(edgeKey(edge.ID))] ||
			outKey == nil || !tx.pendingDeletes[string(outKey)] ||
			inKey == nil || !tx.pendingDeletes[string(inKey)] ||
			typeKey == nil || !tx.pendingDeletes[string(typeKey)] {
			t.Fatal("expected edge and index deletes to be buffered")
		}
	})

	t.Run("skips missing edges and errors on corrupt edge payloads", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("del-prefix"))
		otherID := NodeID(prefixTestID("del-other"))
		missingEdge := EdgeID(prefixTestID("missing-edge"))
		badEdge := EdgeID(prefixTestID("bad-edge"))

		if err := engine.withUpdate(func(txn *badger.Txn) error {
			missingOut, err := engine.outgoingIndexKeyString(txn, nodeID, missingEdge)
			if err != nil {
				return err
			}
			if err := txn.Set(missingOut, []byte{}); err != nil {
				return err
			}
			badOut, err := engine.outgoingIndexKeyString(txn, nodeID, badEdge)
			if err != nil {
				return err
			}
			if err := txn.Set(badOut, []byte{}); err != nil {
				return err
			}
			if err := txn.Set(edgeKey(badEdge), []byte("not-an-edge")); err != nil {
				return err
			}
			badIn, err := engine.incomingIndexKeyString(txn, otherID, badEdge)
			if err != nil {
				return err
			}
			if err := txn.Set(badIn, []byte{}); err != nil {
				return err
			}
			badType, err := engine.edgeTypeIndexKeyString(txn, "BROKEN", badEdge)
			if err != nil {
				return err
			}
			return txn.Set(badType, []byte{})
		}); err != nil {
			t.Fatalf("seed update failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		outPrefix := engine.outgoingIndexPrefixString(nodeID)
		if outPrefix == nil {
			t.Fatal("outgoing prefix missing for seeded node")
		}
		_, _, err = tx.deleteEdgesWithPrefixBuffered(outPrefix)
		if err == nil {
			t.Fatal("expected corrupt edge payload error")
		}
	})
}

func TestTransaction_DeleteNodeBuffered(t *testing.T) {
	t.Run("returns not found and buffers pending embedding cleanup for missing node", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		count, ids, err := tx.deleteNodeBuffered(NodeID(prefixTestID("missing-node")), nil)
		if err != ErrNotFound {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if count != 0 || len(ids) != 0 {
			t.Fatalf("unexpected delete result for missing node: count=%d ids=%v", count, ids)
		}
		if !tx.pendingDeletes[string(pendingEmbedKey(NodeID(prefixTestID("missing-node"))))] {
			t.Fatal("expected pending embedding key cleanup to be buffered")
		}
	})

	t.Run("uses provided old node and buffers node label and edge cleanup", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("buffered-node"))
		otherID := NodeID(prefixTestID("buffered-other"))
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Person", "Employee"}})
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
		_, err = engine.CreateNode(&Node{ID: otherID, Labels: []string{"Person"}})
		if err != nil {
			t.Fatalf("CreateNode other failed: %v", err)
		}
		edge := &Edge{ID: EdgeID(prefixTestID("buffered-edge")), StartNode: nodeID, EndNode: otherID, Type: "KNOWS"}
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("CreateEdge failed: %v", err)
		}
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			emb, err := encodeEmbedding([]float32{1, 2})
			if err != nil {
				return err
			}
			return txn.Set(embeddingKey(nodeID, 0), emb)
		}))

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		count, ids, err := tx.deleteNodeBuffered(nodeID, &Node{ID: nodeID, Labels: []string{"Person", "Employee"}})
		if err != nil {
			t.Fatalf("deleteNodeBuffered failed: %v", err)
		}
		if count != 1 || len(ids) != 1 || ids[0] != edge.ID {
			t.Fatalf("unexpected edge cleanup result: count=%d ids=%v", count, ids)
		}
		personKey := engine.labelIndexKeyStringLookup("Person", nodeID)
		employeeKey := engine.labelIndexKeyStringLookup("Employee", nodeID)
		if personKey == nil || employeeKey == nil {
			t.Fatal("label index numID missing after CreateNode")
		}
		if !tx.pendingDeletes[string(personKey)] ||
			!tx.pendingDeletes[string(employeeKey)] ||
			!tx.pendingDeletes[string(nodeKey(nodeID))] ||
			!tx.pendingDeletes[string(pendingEmbedKey(nodeID))] ||
			!tx.pendingDeletes[string(embeddingKey(nodeID, 0))] {
			t.Fatal("expected node, label, pending embedding, and embedding chunk deletes to be buffered")
		}
	})
}

func TestTransaction_BufferedWriteAndLifecycleEdgeCases(t *testing.T) {
	t.Run("begin transaction on closed engine fails", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.Close())

		tx, err := engine.BeginTransaction()
		require.Nil(t, tx)
		require.ErrorContains(t, err, "engine is closed")
	})

	t.Run("flushBufferedWrites applies delete wins semantics and clears buffers", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer tx.Rollback()

		keepKey := []byte("txn:keep")
		dropKey := []byte("txn:drop")
		tx.bufferSet(keepKey, []byte("value"))
		tx.bufferSet(dropKey, []byte("value"))
		tx.bufferDelete(dropKey)

		require.NoError(t, tx.flushBufferedWrites())
		require.Empty(t, tx.pendingWrites)
		require.Empty(t, tx.pendingDeletes)

		item, err := tx.badgerTx.Get(keepKey)
		require.NoError(t, err)
		err = item.Value(func(val []byte) error {
			require.Equal(t, []byte("value"), val)
			return nil
		})
		require.NoError(t, err)

		_, err = tx.badgerTx.Get(dropKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
	})

	t.Run("flushBufferedWrites surfaces invalid buffered deletes", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer tx.Rollback()

		tx.bufferDelete([]byte{})
		err = tx.flushBufferedWrites()
		require.Error(t, err)
	})

	t.Run("deleteNodeBuffered loads stored node when old node not provided", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("buffered-read-node"))
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Person", "Member"}})
		require.NoError(t, err)

		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer tx.Rollback()

		count, ids, err := tx.deleteNodeBuffered(nodeID, nil)
		require.NoError(t, err)
		require.Zero(t, count)
		require.Empty(t, ids)
		personKey := engine.labelIndexKeyStringLookup("Person", nodeID)
		memberKey := engine.labelIndexKeyStringLookup("Member", nodeID)
		require.NotNil(t, personKey)
		require.NotNil(t, memberKey)
		require.True(t, tx.pendingDeletes[string(personKey)])
		require.True(t, tx.pendingDeletes[string(memberKey)])
		require.True(t, tx.pendingDeletes[string(nodeKey(nodeID))])
		require.True(t, tx.pendingDeletes[string(pendingEmbedKey(nodeID))])
	})

	t.Run("commit on closed transaction returns ErrTransactionClosed", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.ErrorIs(t, tx.Commit(), ErrTransactionClosed)
	})
}

func TestTransaction_Isolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Transaction 1 creates a node
	tx1, _ := engine.BeginTransaction()
	node := &Node{ID: NodeID(prefixTestID("isolated-node")), Labels: []string{"Isolated"}}
	tx1.CreateNode(node)

	// Transaction 2 should NOT see this node
	tx2, _ := engine.BeginTransaction()
	_, err := tx2.GetNode(NodeID(prefixTestID("isolated-node")))
	if err != ErrNotFound {
		t.Error("TX2 should not see TX1's uncommitted node")
	}

	// Commit TX1
	require.NoError(t, tx1.Commit())

	// TX2 must remain pinned to its start snapshot.
	_, err = tx2.GetNode(NodeID(prefixTestID("isolated-node")))
	require.ErrorIs(t, err, ErrNotFound)

	// A new transaction started after the commit should see the node.
	tx3, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx3.Rollback()
	_, err = tx3.GetNode(NodeID(prefixTestID("isolated-node")))
	require.NoError(t, err)

	// Close TX2
	require.NoError(t, tx2.Rollback())
}

func TestTransaction_WriteWriteConflict(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	nodeID := NodeID(prefixTestID("ww-conflict-node"))
	_, err := engine.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"version": 1},
	})
	require.NoError(t, err)

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)

	require.NoError(t, tx1.UpdateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"version": 2},
	}))
	require.NoError(t, tx2.UpdateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"version": 3},
	}))

	require.NoError(t, tx1.Commit())
	require.ErrorIs(t, tx2.Commit(), ErrConflict)

	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stored.Properties["version"])
}

func TestTransaction_WriteWriteConflict_OnEdge(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	start := NodeID(prefixTestID("ww-edge-start"))
	end := NodeID(prefixTestID("ww-edge-end"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: end, Labels: []string{"Node"}})
	require.NoError(t, err)

	edgeID := EdgeID(prefixTestID("ww-conflict-edge"))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         edgeID,
		StartNode:  start,
		EndNode:    end,
		Type:       "LINKS",
		Properties: map[string]interface{}{"weight": 1},
	}))

	tx1, err := engine.BeginTransaction()
	require.NoError(t, err)
	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)

	require.NoError(t, tx1.UpdateEdge(&Edge{
		ID:         edgeID,
		StartNode:  start,
		EndNode:    end,
		Type:       "LINKS",
		Properties: map[string]interface{}{"weight": 2},
	}))
	require.NoError(t, tx2.UpdateEdge(&Edge{
		ID:         edgeID,
		StartNode:  start,
		EndNode:    end,
		Type:       "LINKS",
		Properties: map[string]interface{}{"weight": 3},
	}))

	require.NoError(t, tx1.Commit())
	require.ErrorIs(t, tx2.Commit(), ErrConflict)

	stored, err := engine.GetEdge(edgeID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, stored.Properties["weight"])
}

func TestTransaction_DeleteNodeConflictsWithConcurrentEdgeCreate(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	start := NodeID(prefixTestID("delete-node-start"))
	target := NodeID(prefixTestID("delete-node-target"))
	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}})
	require.NoError(t, err)

	txDelete, err := engine.BeginTransaction()
	require.NoError(t, err)
	txCreateEdge, err := engine.BeginTransaction()
	require.NoError(t, err)

	require.NoError(t, txDelete.DeleteNode(target))
	require.NoError(t, txCreateEdge.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("created-after-delete-snapshot")),
		StartNode: start,
		EndNode:   target,
		Type:      "LINKS",
	}))

	require.NoError(t, txCreateEdge.Commit())
	require.ErrorIs(t, txDelete.Commit(), ErrConflict)

	node, err := engine.GetNode(target)
	require.NoError(t, err)
	require.Equal(t, target, node.ID)
	edge, err := engine.GetEdge(EdgeID(prefixTestID("created-after-delete-snapshot")))
	require.NoError(t, err)
	require.Equal(t, target, edge.EndNode)
}

func TestTransaction_MultipleOperationTypes(t *testing.T) {
	engine := NewMemoryEngine()

	// Pre-create some data
	engine.CreateNode(&Node{ID: NodeID(prefixTestID("existing-1")), Labels: []string{"Existing"}})
	engine.CreateNode(&Node{ID: NodeID(prefixTestID("existing-2")), Labels: []string{"Existing"}})

	tx, _ := engine.BeginTransaction()

	// Mix of operations
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("new-1")), Labels: []string{"New"}})
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("new-2")), Labels: []string{"New"}})
	tx.UpdateNode(&Node{ID: NodeID(prefixTestID("existing-1")), Labels: []string{"Updated"}})
	tx.DeleteNode(NodeID(prefixTestID("existing-2")))

	// Verify operation count
	if tx.OperationCount() != 4 {
		t.Errorf("Expected 4 operations, got %d", tx.OperationCount())
	}

	// Commit
	tx.Commit()

	// Verify final state
	_, err := engine.GetNode(NodeID(prefixTestID("new-1")))
	if err != nil {
		t.Error("new-1 should exist")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("new-2")))
	if err != nil {
		t.Error("new-2 should exist")
	}
	updated, _ := engine.GetNode(NodeID(prefixTestID("existing-1")))
	if updated.Labels[0] != "Updated" {
		t.Error("existing-1 should be updated")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("existing-2")))
	if err != ErrNotFound {
		t.Error("existing-2 should be deleted")
	}
}

// Benchmark transaction overhead
func BenchmarkTransaction_CommitNodes(b *testing.B) {
	engine := NewMemoryEngine()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := engine.BeginTransaction()
		for j := 0; j < 10; j++ {
			node := &Node{
				ID:     NodeID(prefixTestID("bench-" + time.Now().Format("150405.000000") + "-" + string(rune('0'+j)))),
				Labels: []string{"Bench"},
			}
			tx.CreateNode(node)
		}
		tx.Commit()
	}
}

func BenchmarkTransaction_RollbackNodes(b *testing.B) {
	engine := NewMemoryEngine()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := engine.BeginTransaction()
		for j := 0; j < 10; j++ {
			node := &Node{
				ID:     NodeID(prefixTestID("bench-" + time.Now().Format("150405.000000") + "-" + string(rune('0'+j)))),
				Labels: []string{"Bench"},
			}
			tx.CreateNode(node)
		}
		tx.Rollback()
	}
}

func TestTransaction_CreateEdge_MissingNodes(t *testing.T) {
	engine := createTestBadgerEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	// Create only start node
	n1 := testNode("ce-n1")
	_, err = tx.CreateNode(n1)
	require.NoError(t, err)

	// CreateEdge should fail — end node doesn't exist
	edge := testEdge("ce-e1", "ce-n1", "ce-missing", "KNOWS")
	err = tx.CreateEdge(edge)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// CreateEdge should fail — start node doesn't exist
	edge2 := testEdge("ce-e2", "ce-missing", "ce-n1", "KNOWS")
	err = tx.CreateEdge(edge2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestTransaction_CreateEdge_Duplicate(t *testing.T) {
	engine := createTestBadgerEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	n1 := testNode("dup-n1")
	n2 := testNode("dup-n2")
	_, err = tx.CreateNode(n1)
	require.NoError(t, err)
	_, err = tx.CreateNode(n2)
	require.NoError(t, err)

	edge := testEdge("dup-e1", "dup-n1", "dup-n2", "KNOWS")
	require.NoError(t, tx.CreateEdge(edge))

	// Duplicate edge should fail
	err = tx.CreateEdge(edge)
	assert.ErrorIs(t, err, ErrAlreadyExists)
}

func TestTransaction_DeleteNode_CascadeEdges(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Create nodes and edges directly in the engine
	n1 := testNode("dc-n1")
	n2 := testNode("dc-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	e := testEdge("dc-e1", "dc-n1", "dc-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	// Delete node in transaction — should cascade delete edges
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	err = tx.DeleteNode(NodeID(prefixTestID("dc-n1")))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Node and edge should be gone
	_, err = engine.GetNode(NodeID(prefixTestID("dc-n1")))
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge(EdgeID(prefixTestID("dc-e1")))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestTransaction_UpdateEdge_EndpointChange(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("ue-n1")
	n2 := testNode("ue-n2")
	n3 := testNode("ue-n3")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)
	_, err = engine.CreateNode(n3)
	require.NoError(t, err)

	e := testEdge("ue-e1", "ue-n1", "ue-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	// Update edge endpoints in a transaction
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	updated := &Edge{
		ID:         EdgeID(prefixTestID("ue-e1")),
		StartNode:  NodeID(prefixTestID("ue-n1")),
		EndNode:    NodeID(prefixTestID("ue-n3")),
		Type:       "KNOWS",
		Properties: map[string]interface{}{},
	}
	err = tx.UpdateEdge(updated)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify endpoint changed
	got, err := engine.GetEdge(EdgeID(prefixTestID("ue-e1")))
	require.NoError(t, err)
	assert.Equal(t, NodeID(prefixTestID("ue-n3")), got.EndNode)
}

func TestTransaction_UpdateEdge_TypeChange(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("utc-n1")
	n2 := testNode("utc-n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	e := testEdge("utc-e1", "utc-n1", "utc-n2", "KNOWS")
	require.NoError(t, engine.CreateEdge(e))

	// Update edge type in a transaction
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	updated := &Edge{
		ID:         EdgeID(prefixTestID("utc-e1")),
		StartNode:  NodeID(prefixTestID("utc-n1")),
		EndNode:    NodeID(prefixTestID("utc-n2")),
		Type:       "LIKES",
		Properties: map[string]interface{}{},
	}
	err = tx.UpdateEdge(updated)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify type changed
	got, err := engine.GetEdge(EdgeID(prefixTestID("utc-e1")))
	require.NoError(t, err)
	assert.Equal(t, "LIKES", got.Type)

	// Verify edge type indexes updated
	likeEdges, err := engine.GetEdgesByType("LIKES")
	require.NoError(t, err)
	assert.Len(t, likeEdges, 1)
}

func TestTransaction_DeleteNode_NotFound(t *testing.T) {
	engine := createTestBadgerEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	err = tx.DeleteNode(NodeID(prefixTestID("nonexistent")))
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestTransaction_HasLabel(t *testing.T) {
	assert.True(t, hasLabel([]string{"Person", "Employee"}, "Person"))
	assert.True(t, hasLabel([]string{"Person", "Employee"}, "Employee"))
	assert.False(t, hasLabel([]string{"Person", "Employee"}, "Company"))
	assert.False(t, hasLabel(nil, "Person"))
	assert.False(t, hasLabel([]string{}, "Person"))
}

func TestTransaction_ValidateAllConstraints(t *testing.T) {
	engine := createTestBadgerEngine(t)

	// Add a unique constraint to the "test" namespace schema (matching prefixTestID)
	schema := engine.GetSchemaForNamespace("test")
	err := schema.AddConstraint(Constraint{
		Name:       "unique_name",
		Type:       ConstraintUnique,
		Label:      "Person",
		Properties: []string{"name"},
	})
	require.NoError(t, err)

	// Enable deferred constraint validation
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	err = tx.SetDeferredConstraintValidation(true)
	require.NoError(t, err)

	// Create two nodes with the same unique property — deferred so no error yet
	n1 := &Node{
		ID:         NodeID(prefixTestID("vac-n1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err = tx.CreateNode(n1)
	require.NoError(t, err)

	n2 := &Node{
		ID:         NodeID(prefixTestID("vac-n2")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err = tx.CreateNode(n2)
	require.NoError(t, err)

	// Commit should fail due to deferred constraint validation
	err = tx.Commit()
	assert.Error(t, err) // unique constraint violation
}

func TestTransaction_RelationshipConstraintUsesFinalEdgeStateAtCommit(t *testing.T) {
	engine := createTestBadgerEngine(t)

	schema := engine.GetSchemaForNamespace("test")
	err := schema.AddConstraint(Constraint{
		Name:       "knows_since_exists",
		Type:       ConstraintExists,
		EntityType: ConstraintEntityRelationship,
		Label:      "KNOWS",
		Properties: []string{"since"},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("edge-final-state-a")), Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("edge-final-state-b")), Labels: []string{"Person"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        EdgeID(prefixTestID("edge-final-state-rel")),
		StartNode: NodeID(prefixTestID("edge-final-state-a")),
		EndNode:   NodeID(prefixTestID("edge-final-state-b")),
		Type:      "KNOWS",
		Properties: map[string]interface{}{
			"note": "created before required property is set",
		},
	}))

	require.NoError(t, tx.UpdateEdge(&Edge{
		ID:        EdgeID(prefixTestID("edge-final-state-rel")),
		StartNode: NodeID(prefixTestID("edge-final-state-a")),
		EndNode:   NodeID(prefixTestID("edge-final-state-b")),
		Type:      "KNOWS",
		Properties: map[string]interface{}{
			"note":  "created before required property is set",
			"since": int64(2026),
		},
	}))

	require.NoError(t, tx.Commit())

	edge, err := engine.GetEdge(EdgeID(prefixTestID("edge-final-state-rel")))
	require.NoError(t, err)
	require.Equal(t, int64(2026), edge.Properties["since"])
	assert.Equal(t, "created before required property is set", edge.Properties["note"])
}

func TestTransaction_CreateThenUpdateNodeCollapsesToSingleCreateOperation(t *testing.T) {
	engine := createTestBadgerEngine(t)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)

	nodeID := NodeID(prefixTestID("create-then-update"))
	_, err = tx.CreateNode(&Node{ID: nodeID, Labels: []string{"MongoRecord"}})
	require.NoError(t, err)

	err = tx.UpdateNode(&Node{
		ID:     nodeID,
		Labels: []string{"MongoRecord"},
		Properties: map[string]interface{}{
			"mongo_id":    "bulk-1",
			"source":      "nornic_translation",
			"code":        1,
			"description": "entry-1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, tx.OperationCount(), "create+update in the same transaction should collapse into a single create operation")

	require.NoError(t, tx.Commit())

	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, "bulk-1", stored.Properties["mongo_id"])
	require.Equal(t, "entry-1", stored.Properties["description"])
}
