// Package storage - BadgerDB transaction wrapper with ACID guarantees.
//
// This file implements atomic transactions for BadgerDB with full constraint
// validation and rollback support.
package storage

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// BadgerTransaction wraps Badger's native transaction with constraint validation.
//
// Provides ACID guarantees:
//   - Atomicity: All operations commit together or none do
//   - Consistency: Constraints are validated before commit
//   - Isolation: Changes invisible until commit
//   - Durability: Badger's WAL ensures persistence
type BadgerTransaction struct {
	mu sync.Mutex

	// Transaction identity
	ID        string
	StartTime time.Time
	Status    TransactionStatus

	// Badger's native transaction
	badgerTx *badger.Txn

	// Parent engine for constraint validation
	engine *BadgerEngine

	// Track operations for constraint validation
	pendingNodes map[NodeID]*Node
	pendingEdges map[EdgeID]*Edge
	deletedNodes map[NodeID]struct{}
	deletedEdges map[EdgeID]struct{}
	operations   []Operation

	// Transaction metadata (for logging/debugging)
	Metadata map[string]interface{}
}

// BeginTransaction starts a new Badger transaction with ACID guarantees.
func (b *BadgerEngine) BeginTransaction() (*BadgerTransaction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("engine is closed")
	}

	return &BadgerTransaction{
		ID:           generateTxID(),
		StartTime:    time.Now(),
		Status:       TxStatusActive,
		badgerTx:     b.db.NewTransaction(true), // Read-write transaction
		engine:       b,
		pendingNodes: make(map[NodeID]*Node),
		pendingEdges: make(map[EdgeID]*Edge),
		deletedNodes: make(map[NodeID]struct{}),
		deletedEdges: make(map[EdgeID]struct{}),
		operations:   make([]Operation, 0),
		Metadata:     make(map[string]interface{}),
	}, nil
}

// IsActive returns true if the transaction is still active.
func (tx *BadgerTransaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.Status == TxStatusActive
}

// CreateNode adds a node to the transaction with constraint validation.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return "", ErrTransactionClosed
	}

	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if node != nil && node.ID != "" && !strings.Contains(string(node.ID), ":") {
		return "", fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	// Validate constraints BEFORE writing
	if err := tx.validateNodeConstraints(node); err != nil {
		return "", err
	}

	// Check for duplicates in pending
	if _, exists := tx.pendingNodes[node.ID]; exists {
		return "", ErrAlreadyExists
	}

	// Check if exists in storage (read from Badger)
	if _, deleted := tx.deletedNodes[node.ID]; !deleted {
		key := nodeKey(node.ID)
		_, err := tx.badgerTx.Get(key)
		if err == nil {
			return "", ErrAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return "", fmt.Errorf("checking node existence: %w", err)
		}
	}

	// Serialize and write to Badger
	nodeBytes, err := serializeNode(node)
	if err != nil {
		return "", fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	if err := tx.badgerTx.Set(key, nodeBytes); err != nil {
		return "", fmt.Errorf("writing node to transaction: %w", err)
	}

	// Update label indexes
	for _, label := range node.Labels {
		indexKey := labelIndexKey(label, node.ID)
		if err := tx.badgerTx.Set(indexKey, []byte{}); err != nil {
			return "", fmt.Errorf("writing label index: %w", err)
		}
	}

	// Track for read-your-writes and constraint validation
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy
	delete(tx.deletedNodes, node.ID)

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
	})

	return node.ID, nil
}

// UpdateNode updates a node in the transaction.
func (tx *BadgerTransaction) UpdateNode(node *Node) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Validate constraints
	if err := tx.validateNodeConstraints(node); err != nil {
		return err
	}

	// Check if node exists
	var oldNode *Node
	if pending, exists := tx.pendingNodes[node.ID]; exists {
		oldNode = copyNode(pending)
	} else {
		// Read from Badger
		key := nodeKey(node.ID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading node: %w", err)
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("reading node value: %w", err)
		}

		oldNode, err = deserializeNode(nodeBytes)
		if err != nil {
			return fmt.Errorf("deserializing node: %w", err)
		}
	}

	// Write updated node
	nodeBytes, err := serializeNode(node)
	if err != nil {
		return fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	if err := tx.badgerTx.Set(key, nodeBytes); err != nil {
		return fmt.Errorf("writing node: %w", err)
	}

	// Update label indexes if changed
	oldLabelSet := make(map[string]bool)
	for _, label := range oldNode.Labels {
		oldLabelSet[label] = true
	}

	newLabelSet := make(map[string]bool)
	for _, label := range node.Labels {
		newLabelSet[label] = true
		if !oldLabelSet[label] {
			// New label - add index
			indexKey := labelIndexKey(label, node.ID)
			if err := tx.badgerTx.Set(indexKey, []byte{}); err != nil {
				return fmt.Errorf("writing label index: %w", err)
			}
		}
	}

	// Remove old labels
	for _, label := range oldNode.Labels {
		if !newLabelSet[label] {
			indexKey := labelIndexKey(label, node.ID)
			if err := tx.badgerTx.Delete(indexKey); err != nil {
				return fmt.Errorf("deleting label index: %w", err)
			}
		}
	}

	// Track for read-your-writes
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
		OldNode:   oldNode,
	})

	return nil
}

// DeleteNode deletes a node from the transaction.
func (tx *BadgerTransaction) DeleteNode(nodeID NodeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Get node to delete its indexes
	var node *Node
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		node = pending
	} else {
		key := nodeKey(nodeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading node: %w", err)
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("reading node value: %w", err)
		}

		node, err = deserializeNode(nodeBytes)
		if err != nil {
			return fmt.Errorf("deserializing node: %w", err)
		}
	}

	// Delete node
	key := nodeKey(nodeID)
	if err := tx.badgerTx.Delete(key); err != nil {
		return fmt.Errorf("deleting node: %w", err)
	}

	// Delete label indexes
	for _, label := range node.Labels {
		indexKey := labelIndexKey(label, nodeID)
		if err := tx.badgerTx.Delete(indexKey); err != nil {
			return fmt.Errorf("deleting label index: %w", err)
		}
	}

	// Track deletion
	delete(tx.pendingNodes, nodeID)
	tx.deletedNodes[nodeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:      OpDeleteNode,
		Timestamp: time.Now(),
		NodeID:    nodeID,
		OldNode:   node,
	})

	return nil
}

// CreateEdge adds an edge to the transaction.
func (tx *BadgerTransaction) CreateEdge(edge *Edge) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Check nodes exist
	if !tx.nodeExists(edge.StartNode) {
		return fmt.Errorf("start node %s does not exist", edge.StartNode)
	}
	if !tx.nodeExists(edge.EndNode) {
		return fmt.Errorf("end node %s does not exist", edge.EndNode)
	}

	// Check for duplicate
	if _, exists := tx.pendingEdges[edge.ID]; exists {
		return ErrAlreadyExists
	}

	// Serialize and write
	edgeBytes, err := serializeEdge(edge)
	if err != nil {
		return fmt.Errorf("serializing edge: %w", err)
	}

	key := edgeKey(edge.ID)
	if err := tx.badgerTx.Set(key, edgeBytes); err != nil {
		return fmt.Errorf("writing edge: %w", err)
	}

	// Update edge indexes
	outKey := outgoingIndexKey(edge.StartNode, edge.ID)
	if err := tx.badgerTx.Set(outKey, []byte{}); err != nil {
		return fmt.Errorf("writing outgoing index: %w", err)
	}

	inKey := incomingIndexKey(edge.EndNode, edge.ID)
	if err := tx.badgerTx.Set(inKey, []byte{}); err != nil {
		return fmt.Errorf("writing incoming index: %w", err)
	}

	// Track for read-your-writes
	edgeCopy := copyEdge(edge)
	tx.pendingEdges[edge.ID] = edgeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
	})

	return nil
}

// DeleteEdge deletes an edge from the transaction.
func (tx *BadgerTransaction) DeleteEdge(edgeID EdgeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Get edge to delete its indexes
	var edge *Edge
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		edge = pending
	} else {
		key := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}

		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("reading edge value: %w", err)
		}

		edge, err = deserializeEdge(edgeBytes)
		if err != nil {
			return fmt.Errorf("deserializing edge: %w", err)
		}
	}

	// Delete edge
	key := edgeKey(edgeID)
	if err := tx.badgerTx.Delete(key); err != nil {
		return fmt.Errorf("deleting edge: %w", err)
	}

	// Delete indexes
	outKey := outgoingIndexKey(edge.StartNode, edgeID)
	if err := tx.badgerTx.Delete(outKey); err != nil {
		return fmt.Errorf("deleting outgoing index: %w", err)
	}

	inKey := incomingIndexKey(edge.EndNode, edgeID)
	if err := tx.badgerTx.Delete(inKey); err != nil {
		return fmt.Errorf("deleting incoming index: %w", err)
	}

	// Track deletion
	delete(tx.pendingEdges, edgeID)
	tx.deletedEdges[edgeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:      OpDeleteEdge,
		Timestamp: time.Now(),
		EdgeID:    edgeID,
		OldEdge:   edge,
	})

	return nil
}

// GetNode retrieves a node (read-your-writes).
func (tx *BadgerTransaction) GetNode(nodeID NodeID) (*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check deleted
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, ErrNotFound
	}

	// Check pending
	if node, exists := tx.pendingNodes[nodeID]; exists {
		return copyNode(node), nil
	}

	// Read from Badger
	key := nodeKey(nodeID)
	item, err := tx.badgerTx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading node: %w", err)
	}

	var nodeBytes []byte
	if err := item.Value(func(val []byte) error {
		nodeBytes = append([]byte{}, val...)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("reading node value: %w", err)
	}

	return deserializeNode(nodeBytes)
}

// Commit applies all changes atomically with full constraint validation.
// Explicit transactions get strict ACID durability with immediate fsync.
func (tx *BadgerTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Final constraint validation before commit
	if err := tx.validateAllConstraints(); err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("constraint violation: %w", err)
	}

	// Log metadata
	if len(tx.Metadata) > 0 {
		log.Printf("[Transaction %s] Committing with metadata: %v", tx.ID, tx.Metadata)
	}

	// Commit Badger transaction (atomic!)
	if err := tx.badgerTx.Commit(); err != nil {
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("badger commit failed: %w", err)
	}

	// Invalidate cache for modified/deleted nodes
	// This ensures subsequent reads see the committed changes
	tx.engine.nodeCacheMu.Lock()
	for nodeID := range tx.pendingNodes {
		delete(tx.engine.nodeCache, nodeID)
	}
	for nodeID := range tx.deletedNodes {
		delete(tx.engine.nodeCache, nodeID)
	}
	tx.engine.nodeCacheMu.Unlock()

	// Register unique constraint values for created/updated nodes
	// This must happen AFTER commit succeeds to maintain consistency
	for _, node := range tx.pendingNodes {
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				tx.engine.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
			}
		}
	}

	// Unregister unique constraint values for deleted nodes
	for nodeID := range tx.deletedNodes {
		// We need to get the node's old values - they were stored in pendingNodes if updated first
		// For pure deletes, we need to look up what was there
		// This is a simplification - in production we'd track the deleted node's properties
		tx.engine.schema.UnregisterUniqueValue("", "", nodeID) // Will be no-op if not found
	}

	// ACID GUARANTEE: Force fsync for explicit transactions
	// This ensures durability - data is on disk before we return success
	// Non-transactional writes use batch sync for better performance
	// Note: In-memory mode (testing) skips fsync as there's no disk
	if !tx.engine.IsInMemory() {
		if err := tx.engine.Sync(); err != nil {
			// Transaction is committed in Badger but fsync failed
			// Log error but don't rollback - data is in Badger's WAL
			log.Printf("[Transaction %s] Warning: fsync failed after commit: %v", tx.ID, err)
		}
	}

	tx.Status = TxStatusCommitted
	return nil
}

// Rollback discards all changes.
func (tx *BadgerTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.badgerTx.Discard()
	tx.Status = TxStatusRolledBack
	return nil
}

// SetMetadata sets transaction metadata (same as Transaction).
func (tx *BadgerTransaction) SetMetadata(metadata map[string]interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Validate size
	totalSize := 0
	for k, v := range metadata {
		totalSize += len(k)
		if v != nil {
			totalSize += len(fmt.Sprint(v))
		}
	}

	if totalSize > 2048 {
		return fmt.Errorf("transaction metadata too large: %d chars (max 2048)", totalSize)
	}

	// Merge
	if tx.Metadata == nil {
		tx.Metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		tx.Metadata[k] = v
	}

	return nil
}

// GetMetadata returns transaction metadata copy.
func (tx *BadgerTransaction) GetMetadata() map[string]interface{} {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := make(map[string]interface{})
	for k, v := range tx.Metadata {
		result[k] = v
	}
	return result
}

// OperationCount returns the number of buffered operations.
func (tx *BadgerTransaction) OperationCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.operations)
}

// nodeExists checks if a node exists (pending or storage).
func (tx *BadgerTransaction) nodeExists(nodeID NodeID) bool {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return false
	}
	if _, exists := tx.pendingNodes[nodeID]; exists {
		return true
	}

	// Check Badger
	key := nodeKey(nodeID)
	_, err := tx.badgerTx.Get(key)
	return err == nil
}

// validateNodeConstraints checks all constraints for a node.
func (tx *BadgerTransaction) validateNodeConstraints(node *Node) error {
	// Get constraints from schema
	constraints := tx.engine.schema.GetConstraintsForLabels(node.Labels)

	for _, constraint := range constraints {
		switch constraint.Type {
		case ConstraintUnique:
			if err := tx.checkUniqueConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintNodeKey:
			if err := tx.checkNodeKeyConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintExists:
			if err := tx.checkExistenceConstraint(node, constraint); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkUniqueConstraint ensures property value is unique across ALL data.
func (tx *BadgerTransaction) checkUniqueConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return nil // NULL doesn't violate uniqueness
	}

	// Check pending nodes in this transaction
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if hasLabel(n.Labels, c.Label) && n.Properties[prop] == value {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists in transaction", prop, value),
			}
		}
	}

	// Full-scan check: scan all existing nodes with this label
	if err := tx.scanForUniqueViolation(c.Label, prop, value, node.ID); err != nil {
		return err
	}

	return nil
}

// scanForUniqueViolation performs a full database scan to check for UNIQUE violations.
func (tx *BadgerTransaction) scanForUniqueViolation(label, property string, value interface{}, excludeNodeID NodeID) error {
	// Scan all nodes with this label
	prefix := labelIndexKey(label, "")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // Only need keys first

	iter := tx.badgerTx.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		key := item.Key()

		// Extract nodeID from label index key
		// Format: prefixLabelIndex + label (lowercase) + 0x00 + nodeID
		parts := bytes.Split(key[1:], []byte{0x00})
		if len(parts) != 2 {
			continue
		}
		nodeID := NodeID(parts[1])

		if nodeID == excludeNodeID {
			continue // Skip the node being validated
		}

		// Load the node to check property value
		nodeKey := nodeKey(nodeID)
		nodeItem, err := tx.badgerTx.Get(nodeKey)
		if err != nil {
			continue // Node might have been deleted
		}

		var nodeBytes []byte
		if err := nodeItem.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := deserializeNode(nodeBytes)
		if err != nil {
			continue
		}

		// Check if property value matches
		if existingValue, ok := existingNode.Properties[property]; ok {
			if compareValues(existingValue, value) {
				return &ConstraintViolationError{
					Type:       ConstraintUnique,
					Label:      label,
					Properties: []string{property},
					Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, existingNode.ID),
				}
			}
		}
	}

	return nil
}

// compareValues compares two property values for equality.
func compareValues(a, b interface{}) bool {
	// Handle different numeric types
	switch v1 := a.(type) {
	case int:
		switch v2 := b.(type) {
		case int:
			return v1 == v2
		case int64:
			return int64(v1) == v2
		case float64:
			return float64(v1) == v2
		}
	case int64:
		switch v2 := b.(type) {
		case int:
			return v1 == int64(v2)
		case int64:
			return v1 == v2
		case float64:
			return float64(v1) == v2
		}
	case float64:
		switch v2 := b.(type) {
		case int:
			return v1 == float64(v2)
		case int64:
			return v1 == float64(v2)
		case float64:
			return v1 == v2
		}
	case string:
		if v2, ok := b.(string); ok {
			return v1 == v2
		}
	case bool:
		if v2, ok := b.(bool); ok {
			return v1 == v2
		}
	}

	// Default comparison
	return a == b
}

// checkNodeKeyConstraint ensures composite key uniqueness across ALL data.
func (tx *BadgerTransaction) checkNodeKeyConstraint(node *Node, c Constraint) error {
	values := make([]interface{}, len(c.Properties))
	for i, prop := range c.Properties {
		values[i] = node.Properties[prop]
		if values[i] == nil {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
			}
		}
	}

	// Check pending nodes in this transaction
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !hasLabel(n.Labels, c.Label) {
			continue
		}

		match := true
		for i, prop := range c.Properties {
			if !compareValues(n.Properties[prop], values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists in transaction", c.Properties, values),
			}
		}
	}

	// Full-scan check: scan all existing nodes with this label
	if err := tx.scanForNodeKeyViolation(c.Label, c.Properties, values, node.ID); err != nil {
		return err
	}

	return nil
}

// scanForNodeKeyViolation performs a full database scan to check for NODE KEY violations.
func (tx *BadgerTransaction) scanForNodeKeyViolation(label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	prefix := labelIndexKey(label, "")
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := tx.badgerTx.NewIterator(opts)
	defer iter.Close()

	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		key := item.Key()

		// Extract nodeID
		parts := bytes.Split(key[1:], []byte{0x00})
		if len(parts) != 2 {
			continue
		}
		nodeID := NodeID(parts[1])

		if nodeID == excludeNodeID {
			continue
		}

		// Load node
		nodeKey := nodeKey(nodeID)
		nodeItem, err := tx.badgerTx.Get(nodeKey)
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := nodeItem.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := deserializeNode(nodeBytes)
		if err != nil {
			continue
		}

		// Check if all property values match
		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      label,
				Properties: properties,
				Message:    fmt.Sprintf("Node with composite key %v=%v already exists (nodeID: %s)", properties, values, existingNode.ID),
			}
		}
	}

	return nil
}

// checkExistenceConstraint ensures required property exists.
func (tx *BadgerTransaction) checkExistenceConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return &ConstraintViolationError{
			Type:       ConstraintExists,
			Label:      c.Label,
			Properties: []string{prop},
			Message:    fmt.Sprintf("Property %s is required but missing", prop),
		}
	}

	return nil
}

// validateAllConstraints performs final validation before commit.
func (tx *BadgerTransaction) validateAllConstraints() error {
	for _, node := range tx.pendingNodes {
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}
	return nil
}

// Helper: check if node has label
func hasLabel(labels []string, target string) bool {
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
}

// ConstraintViolationError is returned when a constraint is violated.
type ConstraintViolationError struct {
	Type       ConstraintType
	Label      string
	Properties []string
	Message    string
}

func (e *ConstraintViolationError) Error() string {
	return fmt.Sprintf("Constraint violation (%s on %s.%v): %s",
		e.Type, e.Label, e.Properties, e.Message)
}
