// Package storage provides composite engine for composite database support.
//
// CompositeEngine implements the Engine interface by routing operations to multiple
// constituent databases and merging results transparently.
package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// CompositeEngine is a storage engine that spans multiple constituent databases.
// It implements the Engine interface by routing operations to constituents and merging results.
//
// This enables composite databases (similar to Neo4j Fabric) where a single database
// view spans multiple physical databases.
type CompositeEngine struct {
	// Constituents maps alias names to their storage engines
	constituents map[string]Engine

	// ConstituentNames maps alias to actual database name (for routing)
	constituentNames map[string]string

	// Access modes for each constituent
	accessModes map[string]string // "read", "write", "read_write"

	// Routing configuration for label-based and property-based routing
	labelRouting     map[string][]string               // label -> []constituent aliases
	propertyRouting  map[string]map[interface{}]string // property name -> value -> constituent alias
	propertyDefaults map[string]string                 // property name -> default constituent

	// Track which constituent each node was created in (for same-statement edge creation)
	// Following Neo4j TransactionState pattern: track nodes created in current transaction
	nodeToConstituent map[NodeID]string

	mu sync.RWMutex
}

// NewCompositeEngine creates a new composite engine that spans multiple constituent databases.
//
// Parameters:
//   - constituents: Map of alias -> storage engine for each constituent
//   - constituentNames: Map of alias -> actual database name
//   - accessModes: Map of alias -> access mode ("read", "write", "read_write")
func NewCompositeEngine(
	constituents map[string]Engine,
	constituentNames map[string]string,
	accessModes map[string]string,
) *CompositeEngine {
	// Initialize routing configuration with intelligent defaults
	labelRouting := make(map[string][]string)
	propertyRouting := make(map[string]map[interface{}]string)
	propertyDefaults := make(map[string]string)

	// Auto-configure label-based routing: If a label matches a constituent alias, route to that constituent
	writableConstituents := make([]string, 0)
	for alias, mode := range accessModes {
		if mode == "write" || mode == "read_write" {
			writableConstituents = append(writableConstituents, alias)
			// Auto-configure: label matching alias routes to that constituent
			labelRouting[strings.ToLower(alias)] = []string{alias}
		}
	}

	// Set default constituents for common property-based routing
	if len(writableConstituents) > 0 {
		propertyDefaults["tenant_id"] = writableConstituents[0]
		propertyDefaults["database_id"] = writableConstituents[0]
	}

	return &CompositeEngine{
		constituents:      constituents,
		constituentNames:  constituentNames,
		accessModes:       accessModes,
		labelRouting:      labelRouting,
		propertyRouting:   propertyRouting,
		propertyDefaults:  propertyDefaults,
		nodeToConstituent: make(map[NodeID]string),
	}
}

// SetLabelRouting configures label-based routing for a specific label.
// This allows explicit configuration of which constituents should handle nodes with a given label.
func (c *CompositeEngine) SetLabelRouting(label string, constituents []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.labelRouting[strings.ToLower(label)] = constituents
}

// SetPropertyRouting configures property-based routing for a specific property value.
// This enables routing based on property values (e.g., tenant_id, database_id).
func (c *CompositeEngine) SetPropertyRouting(propertyName string, value interface{}, constituent string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.propertyRouting[propertyName] == nil {
		c.propertyRouting[propertyName] = make(map[interface{}]string)
	}
	c.propertyRouting[propertyName][value] = constituent
}

// SetPropertyDefault sets the default constituent for a property when the value is not found in routing rules.
func (c *CompositeEngine) SetPropertyDefault(propertyName string, constituent string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.propertyDefaults[propertyName] = constituent
}

// getConstituentsForRead returns all constituents that can be read from.
// Returns empty slice if no readable constituents are available.
func (c *CompositeEngine) getConstituentsForRead() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []string
	for alias, mode := range c.accessModes {
		if mode == "read" || mode == "read_write" {
			// Check if constituent engine exists and is accessible
			if engine, exists := c.constituents[alias]; exists && engine != nil {
				result = append(result, alias)
			}
		}
	}
	return result
}

// getConstituentsForWrite returns all constituents that can be written to.
// Returns empty slice if no writable constituents are available.
func (c *CompositeEngine) getConstituentsForWrite() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []string
	for alias, mode := range c.accessModes {
		if mode == "write" || mode == "read_write" {
			// Check if constituent engine exists and is accessible
			if engine, exists := c.constituents[alias]; exists && engine != nil {
				result = append(result, alias)
			}
		}
	}
	return result
}

// getConstituent returns a constituent engine by alias.
func (c *CompositeEngine) getConstituent(alias string) (Engine, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	engine, exists := c.constituents[alias]
	if !exists {
		return nil, fmt.Errorf("constituent '%s' not found", alias)
	}
	return engine, nil
}

// routeWrite determines which constituent should receive a write operation.
// Implements full routing using configured routing rules with intelligent fallbacks.
func (c *CompositeEngine) routeWrite(operation string, labels []string, properties map[string]interface{}, writableConstituents []string) string {
	if len(writableConstituents) == 0 {
		return ""
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 1. Label-based routing: Use configured routing rules
	if len(labels) > 0 {
		firstLabel := strings.ToLower(labels[0])
		if constituents, exists := c.labelRouting[firstLabel]; exists && len(constituents) > 0 {
			// Route to first configured constituent for this label
			// Verify it's writable
			for _, alias := range constituents {
				for _, writable := range writableConstituents {
					if alias == writable {
						return alias
					}
				}
			}
		}

		// Fallback: Check if label matches a constituent alias (auto-routing)
		for _, alias := range writableConstituents {
			if strings.ToLower(alias) == firstLabel {
				return alias
			}
		}
	}

	// 2. Property-based routing: Check configured property routing rules
	if properties != nil {
		for propName, propMap := range c.propertyRouting {
			if value, exists := properties[propName]; exists {
				if constituent, found := propMap[value]; found {
					// Verify constituent is writable
					for _, writable := range writableConstituents {
						if constituent == writable {
							return constituent
						}
					}
				}
				// Try default for this property
				if defaultConstituent, hasDefault := c.propertyDefaults[propName]; hasDefault {
					for _, writable := range writableConstituents {
						if defaultConstituent == writable {
							return defaultConstituent
						}
					}
				}
			}
		}

		// Fallback: Check common routing properties (tenant_id, database_id)
		// Use consistent hashing for unconfigured properties
		if tenantID, exists := properties["tenant_id"]; exists {
			tenantHash := hashValue(tenantID)
			index := tenantHash % len(writableConstituents)
			if index < 0 {
				index = -index
			}
			return writableConstituents[index]
		}

		if databaseID, exists := properties["database_id"]; exists {
			dbHash := hashValue(databaseID)
			index := dbHash % len(writableConstituents)
			if index < 0 {
				index = -index
			}
			return writableConstituents[index]
		}
	}

	// 3. Label hash-based routing: Use consistent hashing on first label
	// This provides deterministic routing when no explicit rules match
	if len(labels) > 0 {
		labelHash := hashString(labels[0])
		index := labelHash % len(writableConstituents)
		if index < 0 {
			index = -index
		}
		return writableConstituents[index]
	}

	// 4. Default: route to first writable constituent
	return writableConstituents[0]
}

// hashString computes a consistent hash for a string value.
func hashString(s string) int {
	hash := 0
	for _, r := range s {
		hash = hash*31 + int(r)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// hashValue computes a consistent hash for various value types.
func hashValue(v interface{}) int {
	switch val := v.(type) {
	case string:
		return hashString(val)
	case int64:
		hash := int(val)
		if hash < 0 {
			hash = -hash
		}
		return hash
	case int:
		if val < 0 {
			return -val
		}
		return val
	case int32:
		hash := int(val)
		if hash < 0 {
			hash = -hash
		}
		return hash
	default:
		// Convert to string and hash
		return hashString(fmt.Sprintf("%v", val))
	}
}

// ============================================================================
// Node Operations
// ============================================================================

// CreateNode creates a node. Routes to the appropriate constituent based on routing rules.
func (c *CompositeEngine) CreateNode(node *Node) (NodeID, error) {
	writeConstituents := c.getConstituentsForWrite()
	if len(writeConstituents) == 0 {
		return "", fmt.Errorf("no writable constituents available")
	}

	// Determine routing based on node labels and properties
	targetConstituent := c.routeWrite("create_node", node.Labels, node.Properties, writeConstituents)
	if targetConstituent == "" {
		// No routing rule matches - use first writable constituent
		targetConstituent = writeConstituents[0]
	}

	engine, err := c.getConstituent(targetConstituent)
	if err != nil {
		return "", err
	}

	// Create node and track which constituent it was created in
	// This allows CreateEdge to find nodes created in the same statement
	nodeID, err := engine.CreateNode(node)
	if err != nil {
		return "", err
	}

	// CRITICAL: For composite databases, creates must be atomic - no async write queue
	// Flush immediately to ensure node is visible when CreateEdge is called
	// This prevents race conditions where edge creation fails because node isn't persisted yet
	c.flushAsyncEngine(engine)

	// Track node -> constituent mapping (following Neo4j TransactionState pattern)
	// CRITICAL: Only store prefixed IDs in nodeToConstituent (we always use prefixed storage now)
	// Get the actual prefixed ID from the engine (NamespacedEngine returns unprefixed)
	actualPrefixedID := nodeID
	if namespacedEngine, ok := engine.(*NamespacedEngine); ok {
		// NamespacedEngine.CreateNode returns unprefixed, but we need the prefixed ID
		// Reconstruct it by prefixing the returned ID
		actualPrefixedID = namespacedEngine.prefixNodeID(nodeID)
	}
	c.mu.Lock()
	// Store only prefixed ID -> constituent mapping
	c.nodeToConstituent[actualPrefixedID] = targetConstituent
	c.mu.Unlock()

	// Return unprefixed ID to user (user-facing API)
	return nodeID, nil
}

// GetNode retrieves a node. Searches all readable constituents.
func (c *CompositeEngine) GetNode(id NodeID) (*Node, error) {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		node, err := engine.GetNode(id)
		if err == nil {
			return node, nil
		}
		if err != ErrNotFound {
			return nil, err
		}
	}

	return nil, ErrNotFound
}

// UpdateNode updates a node. Routes to the constituent that contains it.
func (c *CompositeEngine) UpdateNode(node *Node) error {
	// First find which constituent has this node
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		_, err = engine.GetNode(node.ID)
		if err == nil {
			// Found it, update in this constituent
			return engine.UpdateNode(node)
		}
		if err != ErrNotFound {
			return err
		}
	}

	return ErrNotFound
}

// DeleteNode deletes a node from the constituent that contains it.
func (c *CompositeEngine) DeleteNode(id NodeID) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		_, err = engine.GetNode(id)
		if err == nil {
			// Found it, delete from this constituent
			return engine.DeleteNode(id)
		}
		if err != ErrNotFound {
			return err
		}
	}

	return ErrNotFound
}

// ============================================================================
// Edge Operations
// ============================================================================

// CreateEdge creates an edge. Routes to the constituent containing the start node.
// Following Neo4j TransactionState pattern: check if nodes were created in current transaction,
// and if so, try that constituent first. Otherwise try all constituents.
func (c *CompositeEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return fmt.Errorf("edge cannot be nil")
	}

	writeConstituents := c.getConstituentsForWrite()
	if len(writeConstituents) == 0 {
		return fmt.Errorf("no writable constituents available")
	}

	// Check if start node was created in current transaction (following Neo4j pattern)
	// This allows us to route directly to the correct constituent
	// CRITICAL: edge.StartNode/EndNode may be unprefixed (from user), but nodeToConstituent
	// only stores prefixed IDs. We must prefix the IDs before looking them up.
	c.mu.RLock()
	// Prefix the node IDs before looking them up (we only store prefixed IDs)
	startNodeInTx := false
	endNodeInTx := false
	var startConstituent, endConstituent string

	// Try to find the constituent for start node by checking all constituents
	// (since we don't know which namespace the node belongs to)
	for _, engine := range c.constituents {
		if namespacedEngine, ok := engine.(*NamespacedEngine); ok {
			prefixedStartID := namespacedEngine.prefixNodeID(edge.StartNode)
			if constituent, found := c.nodeToConstituent[prefixedStartID]; found {
				startConstituent = constituent
				startNodeInTx = true
				break
			}
		}
	}

	// Try to find the constituent for end node
	for _, engine := range c.constituents {
		if namespacedEngine, ok := engine.(*NamespacedEngine); ok {
			prefixedEndID := namespacedEngine.prefixNodeID(edge.EndNode)
			if constituent, found := c.nodeToConstituent[prefixedEndID]; found {
				endConstituent = constituent
				endNodeInTx = true
				break
			}
		}
	}
	c.mu.RUnlock()

	// If both nodes were created in the same transaction, try that constituent first
	// Following Neo4j pattern: trust transaction state, don't verify with GetNode
	// The underlying engine will verify nodes exist when creating the edge
	if startNodeInTx && endNodeInTx && startConstituent == endConstituent {
		engine, err := c.getConstituent(startConstituent)
		if err == nil && engine != nil {
			// Trust transaction state - nodes were created here, so try creating edge
			createErr := engine.CreateEdge(edge)
			if createErr == nil {
				// CRITICAL: Flush async writes to ensure edge is immediately visible
				c.flushAsyncEngine(engine)
				return nil
			}
			// If error is not "not found", return it immediately
			if createErr != ErrNotFound && !strings.Contains(createErr.Error(), "not found") {
				return createErr
			}
		}
	}

	// If start node was created in transaction, try that constituent first
	if startNodeInTx && startConstituent != "" {
		engine, err := c.getConstituent(startConstituent)
		if err == nil && engine != nil {
			createErr := engine.CreateEdge(edge)
			if createErr == nil {
				// CRITICAL: Flush async writes to ensure edge is immediately visible
				c.flushAsyncEngine(engine)
				return nil
			}
			// If error is not "not found", return it immediately
			if createErr != ErrNotFound && !strings.Contains(createErr.Error(), "not found") {
				return createErr
			}
		}
	}

	// Try all writable constituents (fallback for nodes not in transaction state)
	// First, try to locate nodes by querying each constituent
	// This handles cases where nodes were created but not yet tracked in nodeToConstituent
	for _, alias := range writeConstituents {
		// Skip if already tried
		if startNodeInTx && alias == startConstituent {
			continue
		}

		engine, err := c.getConstituent(alias)
		if err != nil || engine == nil {
			continue
		}

		// Verify both nodes exist in this constituent before attempting edge creation
		// This prevents unnecessary CreateEdge calls that will fail
		_, err = engine.GetNode(edge.StartNode)
		if err != nil {
			continue // Start node not in this constituent
		}
		_, err = engine.GetNode(edge.EndNode)
		if err != nil {
			continue // End node not in this constituent
		}

		// Both nodes exist - try creating edge
		createErr := engine.CreateEdge(edge)
		if createErr == nil {
			// CRITICAL: Flush async writes to ensure edge is immediately visible
			c.flushAsyncEngine(engine)
			return nil
		}
		// If error is not "not found", return it immediately (something else went wrong)
		if createErr != ErrNotFound && !strings.Contains(createErr.Error(), "not found") {
			return createErr
		}
		// Continue to next constituent if it was "not found"
	}

	// If still not found, try read-only constituents as a last resort
	readConstituents := c.getConstituentsForRead()
	for _, alias := range readConstituents {
		// Skip if already tried in writeConstituents
		alreadyTried := false
		for _, writeAlias := range writeConstituents {
			if alias == writeAlias {
				alreadyTried = true
				break
			}
		}
		if alreadyTried {
			continue
		}

		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		_, err = engine.GetNode(edge.StartNode)
		if err == nil {
			// Start node found, but can't write to read-only constituent
			return fmt.Errorf("start node found in read-only constituent '%s', cannot create edge", alias)
		}
		if err != ErrNotFound {
			return err
		}
	}

	// If we got here, all attempts failed
	return fmt.Errorf("start node not found in any constituent")
}

// GetEdge retrieves an edge. Searches all constituents.
func (c *CompositeEngine) GetEdge(id EdgeID) (*Edge, error) {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edge, err := engine.GetEdge(id)
		if err == nil {
			return edge, nil
		}
		if err != ErrNotFound {
			return nil, err
		}
	}

	return nil, ErrNotFound
}

// UpdateEdge updates an edge. Routes to the constituent containing it.
func (c *CompositeEngine) UpdateEdge(edge *Edge) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		_, err = engine.GetEdge(edge.ID)
		if err == nil {
			return engine.UpdateEdge(edge)
		}
		if err != ErrNotFound {
			return err
		}
	}

	return ErrNotFound
}

// DeleteEdge deletes an edge from the constituent containing it.
func (c *CompositeEngine) DeleteEdge(id EdgeID) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		_, err = engine.GetEdge(id)
		if err == nil {
			return engine.DeleteEdge(id)
		}
		if err != ErrNotFound {
			return err
		}
	}

	return ErrNotFound
}

// ============================================================================
// Query Operations - Merge results from all constituents
// ============================================================================

// GetNodesByLabel returns nodes with the given label from all constituents.
// Duplicate nodes (same ID) are deduplicated - only the first occurrence is kept.
// Returns empty slice if no readable constituents are available.
func (c *CompositeEngine) GetNodesByLabel(label string) ([]*Node, error) {
	readConstituents := c.getConstituentsForRead()
	if len(readConstituents) == 0 {
		// No readable constituents available (empty composite or all offline)
		return []*Node{}, nil
	}

	seen := make(map[NodeID]bool)
	var allNodes []*Node

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			// Constituent not found or offline - skip it
			continue
		}

		nodes, err := engine.GetNodesByLabel(label)
		if err != nil {
			// Error querying constituent - skip it (constituent may be offline)
			continue
		}

		// Deduplicate: only add nodes we haven't seen before
		for _, node := range nodes {
			if !seen[node.ID] {
				seen[node.ID] = true
				allNodes = append(allNodes, node)
			}
		}
	}

	return allNodes, nil
}

// GetFirstNodeByLabel returns the first node with the given label from any constituent.
func (c *CompositeEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		node, err := engine.GetFirstNodeByLabel(label)
		if err == nil && node != nil {
			// Found a node - return it
			return node, nil
		}
		if err != nil && err != ErrNotFound {
			// Non-ErrNotFound error - propagate it
			return nil, err
		}
		// err == nil && node == nil: underlying engine returned nil, nil (no error but no node)
		// err == ErrNotFound: underlying engine returned nil, ErrNotFound
		// In both cases, continue to next constituent
	}

	// No node found in any constituent
	return nil, ErrNotFound
}

// GetOutgoingEdges returns outgoing edges from all constituents.
// Duplicate edges (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[EdgeID]bool)
	var allEdges []*Edge

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edges, err := engine.GetOutgoingEdges(nodeID)
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add edges we haven't seen before
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				allEdges = append(allEdges, edge)
			}
		}
	}

	return allEdges, nil
}

// GetIncomingEdges returns incoming edges from all constituents.
// Duplicate edges (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[EdgeID]bool)
	var allEdges []*Edge

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edges, err := engine.GetIncomingEdges(nodeID)
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add edges we haven't seen before
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				allEdges = append(allEdges, edge)
			}
		}
	}

	return allEdges, nil
}

// GetEdgesBetween returns edges between two nodes from all constituents.
// Duplicate edges (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[EdgeID]bool)
	var allEdges []*Edge

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edges, err := engine.GetEdgesBetween(startID, endID)
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add edges we haven't seen before
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				allEdges = append(allEdges, edge)
			}
		}
	}

	return allEdges, nil
}

// GetEdgeBetween returns an edge between two nodes from any constituent.
func (c *CompositeEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edge := engine.GetEdgeBetween(startID, endID, edgeType)
		if edge != nil {
			return edge
		}
	}

	return nil
}

// GetEdgesByType returns edges of the given type from all constituents.
// Duplicate edges (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[EdgeID]bool)
	var allEdges []*Edge

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edges, err := engine.GetEdgesByType(edgeType)
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add edges we haven't seen before
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				allEdges = append(allEdges, edge)
			}
		}
	}

	return allEdges, nil
}

// AllNodes returns all nodes from all constituents.
// Duplicate nodes (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) AllNodes() ([]*Node, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[NodeID]bool)
	var allNodes []*Node

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		nodes, err := engine.AllNodes()
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add nodes we haven't seen before
		for _, node := range nodes {
			if !seen[node.ID] {
				seen[node.ID] = true
				allNodes = append(allNodes, node)
			}
		}
	}

	return allNodes, nil
}

// AllEdges returns all edges from all constituents.
// Duplicate edges (same ID) are deduplicated - only the first occurrence is kept.
func (c *CompositeEngine) AllEdges() ([]*Edge, error) {
	readConstituents := c.getConstituentsForRead()
	seen := make(map[EdgeID]bool)
	var allEdges []*Edge

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		edges, err := engine.AllEdges()
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Deduplicate: only add edges we haven't seen before
		for _, edge := range edges {
			if !seen[edge.ID] {
				seen[edge.ID] = true
				allEdges = append(allEdges, edge)
			}
		}
	}

	return allEdges, nil
}

// GetAllNodes returns all nodes from all constituents (non-error version).
func (c *CompositeEngine) GetAllNodes() []*Node {
	nodes, _ := c.AllNodes()
	return nodes
}

// ============================================================================
// Degree Operations
// ============================================================================

// GetInDegree returns the in-degree of a node across all constituents.
func (c *CompositeEngine) GetInDegree(nodeID NodeID) int {
	readConstituents := c.getConstituentsForRead()
	total := 0

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		total += engine.GetInDegree(nodeID)
	}

	return total
}

// GetOutDegree returns the out-degree of a node across all constituents.
func (c *CompositeEngine) GetOutDegree(nodeID NodeID) int {
	readConstituents := c.getConstituentsForRead()
	total := 0

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		total += engine.GetOutDegree(nodeID)
	}

	return total
}

// ============================================================================
// Schema Operations
// ============================================================================

// GetSchema returns a merged schema from all constituents.
// Merges constraints and indexes from all constituent databases.
func (c *CompositeEngine) GetSchema() *SchemaManager {
	readConstituents := c.getConstituentsForRead()

	if len(readConstituents) == 0 {
		return NewSchemaManager()
	}

	mergedSchema := NewSchemaManager()

	// Merge schemas from all constituents
	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		constituentSchema := engine.GetSchema()
		if constituentSchema == nil {
			continue
		}

		// Merge constraints
		constraints := constituentSchema.GetConstraints()
		for _, constraint := range constraints {
			// Add constraint if not already present (by name)
			mergedConstraints := mergedSchema.GetConstraints()
			found := false
			for _, existing := range mergedConstraints {
				if existing.Name == constraint.Name {
					found = true
					break
				}
			}
			if !found {
				_ = mergedSchema.AddConstraint(Constraint{
					Name:       constraint.Name,
					Type:       ConstraintUnique,
					Label:      constraint.Label,
					Properties: []string{constraint.Property},
				})
			}
		}

		// Also get all constraints from the constraints map (includes all types)
		allConstraints := constituentSchema.GetConstraintsForLabels([]string{}) // Empty labels gets all
		for _, constraint := range allConstraints {
			mergedConstraints := mergedSchema.GetConstraints()
			found := false
			for _, existing := range mergedConstraints {
				if existing.Name == constraint.Name {
					found = true
					break
				}
			}
			if !found {
				_ = mergedSchema.AddConstraint(constraint)
			}
		}

		// Merge indexes from constituent
		// Note: We merge index metadata only - the actual indexed data stays in constituent engines
		// This allows SHOW INDEXES to work correctly while queries use indexes from constituents
		indexes := constituentSchema.GetIndexes()
		for _, idx := range indexes {
			if idxMap, ok := idx.(map[string]interface{}); ok {
				idxName, _ := idxMap["name"].(string)
				idxType, _ := idxMap["type"].(string)

				// Check if index already exists in merged schema
				mergedIndexes := mergedSchema.GetIndexes()
				found := false
				for _, existing := range mergedIndexes {
					if existingMap, ok := existing.(map[string]interface{}); ok {
						if existingName, _ := existingMap["name"].(string); existingName == idxName {
							found = true
							break
						}
					}
				}

				if !found {
					// Recreate index in merged schema based on type
					switch idxType {
					case "PROPERTY":
						if label, ok := idxMap["label"].(string); ok {
							if properties, ok := idxMap["properties"].([]string); ok && len(properties) > 0 {
								_ = mergedSchema.AddPropertyIndex(idxName, label, properties)
							}
						}
					case "COMPOSITE":
						if label, ok := idxMap["label"].(string); ok {
							if properties, ok := idxMap["properties"].([]string); ok && len(properties) >= 2 {
								_ = mergedSchema.AddCompositeIndex(idxName, label, properties)
							}
						}
					case "FULLTEXT":
						if labels, ok := idxMap["labels"].([]string); ok {
							if properties, ok := idxMap["properties"].([]string); ok {
								_ = mergedSchema.AddFulltextIndex(idxName, labels, properties)
							}
						}
					case "VECTOR":
						if label, ok := idxMap["label"].(string); ok {
							if property, ok := idxMap["property"].(string); ok {
								dimensions := 0
								if dims, ok := idxMap["dimensions"].(int); ok {
									dimensions = dims
								} else if dims, ok := idxMap["dimensions"].(int64); ok {
									dimensions = int(dims)
								}
								similarityFunc := "cosine"
								if sim, ok := idxMap["similarityFunc"].(string); ok {
									similarityFunc = sim
								}
								_ = mergedSchema.AddVectorIndex(idxName, label, property, dimensions, similarityFunc)
							}
						}
					case "RANGE":
						if label, ok := idxMap["label"].(string); ok {
							if property, ok := idxMap["property"].(string); ok {
								_ = mergedSchema.AddRangeIndex(idxName, label, property)
							}
						}
					}
				}
			}
		}
	}

	return mergedSchema
}

// ============================================================================
// Bulk Operations
// ============================================================================

// BulkCreateNodes creates nodes in bulk. Routes nodes to appropriate constituents based on routing rules.
func (c *CompositeEngine) BulkCreateNodes(nodes []*Node) error {
	writeConstituents := c.getConstituentsForWrite()
	if len(writeConstituents) == 0 {
		return fmt.Errorf("no writable constituents available")
	}

	// Group nodes by target constituent
	nodeGroups := make(map[string][]*Node)
	for _, node := range nodes {
		targetConstituent := c.routeWrite("create_node", node.Labels, node.Properties, writeConstituents)
		if targetConstituent == "" {
			// No routing rule - use first writable
			targetConstituent = writeConstituents[0]
		}
		nodeGroups[targetConstituent] = append(nodeGroups[targetConstituent], node)
	}

	// Create nodes in each constituent
	for constituent, groupNodes := range nodeGroups {
		engine, err := c.getConstituent(constituent)
		if err != nil {
			return fmt.Errorf("failed to get constituent '%s': %w", constituent, err)
		}
		if err := engine.BulkCreateNodes(groupNodes); err != nil {
			return fmt.Errorf("failed to create nodes in constituent '%s': %w", constituent, err)
		}
	}

	return nil
}

// BulkCreateEdges creates edges in bulk. Routes edges to constituents containing their start nodes.
func (c *CompositeEngine) BulkCreateEdges(edges []*Edge) error {
	writeConstituents := c.getConstituentsForWrite()
	if len(writeConstituents) == 0 {
		return fmt.Errorf("no writable constituents available")
	}

	readConstituents := c.getConstituentsForRead()

	// Group edges by constituent (based on start node location)
	edgeGroups := make(map[string][]*Edge)
	unroutedEdges := []*Edge{}

	for _, edge := range edges {
		// Find constituent with start node
		routed := false
		for _, alias := range readConstituents {
			engine, err := c.getConstituent(alias)
			if err != nil {
				continue
			}

			_, err = engine.GetNode(edge.StartNode)
			if err == nil {
				// Start node found in this constituent
				edgeGroups[alias] = append(edgeGroups[alias], edge)
				routed = true
				break
			}
			if err != ErrNotFound {
				return fmt.Errorf("error checking start node: %w", err)
			}
		}

		if !routed {
			unroutedEdges = append(unroutedEdges, edge)
		}
	}

	// Create edges in each constituent
	for constituent, groupEdges := range edgeGroups {
		engine, err := c.getConstituent(constituent)
		if err != nil {
			return fmt.Errorf("failed to get constituent '%s': %w", constituent, err)
		}
		if err := engine.BulkCreateEdges(groupEdges); err != nil {
			return fmt.Errorf("failed to create edges in constituent '%s': %w", constituent, err)
		}
	}

	// Handle unrouted edges (start node not found) - route to first writable
	if len(unroutedEdges) > 0 {
		engine, err := c.getConstituent(writeConstituents[0])
		if err != nil {
			return fmt.Errorf("failed to get default constituent: %w", err)
		}
		if err := engine.BulkCreateEdges(unroutedEdges); err != nil {
			return fmt.Errorf("failed to create unrouted edges: %w", err)
		}
	}

	return nil
}

// BulkDeleteNodes deletes nodes in bulk from all constituents.
func (c *CompositeEngine) BulkDeleteNodes(ids []NodeID) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		// Try to delete - ignore errors for nodes not in this constituent
		_ = engine.BulkDeleteNodes(ids)
	}

	return nil
}

// BulkDeleteEdges deletes edges in bulk from all constituents.
func (c *CompositeEngine) BulkDeleteEdges(ids []EdgeID) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		// Try to delete - ignore errors for edges not in this constituent
		_ = engine.BulkDeleteEdges(ids)
	}

	return nil
}

// ============================================================================
// Batch Operations
// ============================================================================

// BatchGetNodes retrieves multiple nodes from all constituents.
func (c *CompositeEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	readConstituents := c.getConstituentsForRead()
	result := make(map[NodeID]*Node)

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		nodes, err := engine.BatchGetNodes(ids)
		if err != nil {
			return nil, fmt.Errorf("error querying constituent '%s': %w", alias, err)
		}

		// Merge results (later constituents override earlier ones if duplicates)
		for id, node := range nodes {
			result[id] = node
		}
	}

	return result, nil
}

// ============================================================================
// Lifecycle
// ============================================================================

// Close closes all constituent engines.
func (c *CompositeEngine) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var lastErr error
	for alias, engine := range c.constituents {
		if err := engine.Close(); err != nil {
			lastErr = fmt.Errorf("error closing constituent '%s': %w", alias, err)
		}
	}

	return lastErr
}

// ============================================================================
// Stats
// ============================================================================

// NodeCount returns the total node count across all constituents.
func (c *CompositeEngine) NodeCount() (int64, error) {
	readConstituents := c.getConstituentsForRead()
	var total int64

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		count, err := engine.NodeCount()
		if err != nil {
			return 0, fmt.Errorf("error counting nodes in constituent '%s': %w", alias, err)
		}

		total += count
	}

	return total, nil
}

// EdgeCount returns the total edge count across all constituents.
func (c *CompositeEngine) EdgeCount() (int64, error) {
	readConstituents := c.getConstituentsForRead()
	var total int64

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		count, err := engine.EdgeCount()
		if err != nil {
			return 0, fmt.Errorf("error counting edges in constituent '%s': %w", alias, err)
		}

		total += count
	}

	return total, nil
}

// ============================================================================
// DeleteByPrefix
// ============================================================================

// DeleteByPrefix is not supported for composite databases.
// Composite databases don't have their own data - they're virtual views.
func (c *CompositeEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	return 0, 0, fmt.Errorf("DeleteByPrefix not supported on composite databases - delete from constituent databases instead")
}

// ============================================================================
// Streaming Support
// ============================================================================

// StreamNodes streams nodes from all constituents.
func (c *CompositeEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		if streamer, ok := engine.(StreamingEngine); ok {
			if err := streamer.StreamNodes(ctx, fn); err != nil {
				return fmt.Errorf("error streaming from constituent '%s': %w", alias, err)
			}
		} else {
			// Fallback to AllNodes
			nodes, err := engine.AllNodes()
			if err != nil {
				return fmt.Errorf("error querying constituent '%s': %w", alias, err)
			}

			for _, node := range nodes {
				if err := fn(node); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// StreamEdges streams edges from all constituents.
func (c *CompositeEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		if streamer, ok := engine.(StreamingEngine); ok {
			if err := streamer.StreamEdges(ctx, fn); err != nil {
				return fmt.Errorf("error streaming from constituent '%s': %w", alias, err)
			}
		} else {
			// Fallback to AllEdges
			edges, err := engine.AllEdges()
			if err != nil {
				return fmt.Errorf("error querying constituent '%s': %w", alias, err)
			}

			for _, edge := range edges {
				if err := fn(edge); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// StreamNodeChunks streams nodes in chunks from all constituents.
func (c *CompositeEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	readConstituents := c.getConstituentsForRead()

	for _, alias := range readConstituents {
		engine, err := c.getConstituent(alias)
		if err != nil {
			continue
		}

		if streamer, ok := engine.(StreamingEngine); ok {
			if err := streamer.StreamNodeChunks(ctx, chunkSize, fn); err != nil {
				return fmt.Errorf("error streaming from constituent '%s': %w", alias, err)
			}
		} else {
			// Fallback
			nodes, err := engine.AllNodes()
			if err != nil {
				return fmt.Errorf("error querying constituent '%s': %w", alias, err)
			}

			for i := 0; i < len(nodes); i += chunkSize {
				end := i + chunkSize
				if end > len(nodes) {
					end = len(nodes)
				}
				if err := fn(nodes[i:end]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// flushAsyncEngine flushes async writes if the engine is AsyncEngine (directly or wrapped in NamespacedEngine).
// This ensures atomicity for composite database operations - no async write queue delays.
// Waits for flush to complete and verifies queue is empty.
func (c *CompositeEngine) flushAsyncEngine(engine Engine) {
	var asyncEngine *AsyncEngine
	if ae, ok := engine.(*AsyncEngine); ok {
		asyncEngine = ae
	} else if namespacedEngine, ok := engine.(*NamespacedEngine); ok {
		// NamespacedEngine wraps another engine - check if it's AsyncEngine
		if ae, ok := namespacedEngine.inner.(*AsyncEngine); ok {
			asyncEngine = ae
		}
	}

	if asyncEngine != nil {
		// Flush and wait for completion (Flush is synchronous)
		if err := asyncEngine.Flush(); err != nil {
			// Log but don't fail - flush errors are best effort
			// The underlying engine will handle retries
			return
		}

		// Wait for queue to be empty (verify flush completed)
		// Poll with exponential backoff up to 100ms
		maxWait := 100 * time.Millisecond
		start := time.Now()
		for time.Since(start) < maxWait {
			if !asyncEngine.HasPendingWrites() {
				return // Queue is empty, flush complete
			}
			time.Sleep(5 * time.Millisecond)
		}
		// If we get here, queue still has pending writes but we've waited long enough
		// The nodes should be visible by now (flush is synchronous, this is just verification)
	}
}

// Ensure CompositeEngine implements Engine interface
var _ Engine = (*CompositeEngine)(nil)

// Ensure CompositeEngine implements StreamingEngine if constituents do
var _ StreamingEngine = (*CompositeEngine)(nil)
