// Package multidb provides automatic migration of existing unprefixed data.
//
// When upgrading from a pre-multi-database version of NornicDB, existing data
// without namespace prefixes is automatically migrated to the default database
// namespace. This ensures backwards compatibility and zero-downtime upgrades.
//
// Migration Process:
//  1. Detects unprefixed data (nodes/edges without "namespace:" prefix)
//  2. Migrates all unprefixed data to default database namespace
//  3. Updates all indexes automatically (via CreateNode/CreateEdge)
//  4. Marks migration as complete in metadata (prevents re-running)
//
// The migration runs automatically in NewDatabaseManager() and is completely
// transparent to users. No manual steps are required.
//
// Example:
//
//	// Before upgrade: data stored as "node-123"
//	// After upgrade: automatically becomes "nornic:node-123"
//	// User access remains the same - no changes needed!
package multidb

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	migrationStatusKey = "migration:legacy_data"
)

// migrateLegacyData migrates existing unprefixed data to the default database namespace.
//
// This function handles the upgrade path from pre-multi-database versions:
//   - Detects nodes/edges without namespace prefixes
//   - Migrates them to the default database namespace
//   - Updates all indexes (label, outgoing, incoming, edge type)
//   - Marks migration as complete in metadata
//
// This is a one-time operation that runs automatically on first startup
// after upgrading to multi-database support. The migration is idempotent
// and will not run again once marked as complete.
//
// Migration preserves all node/edge properties, relationships, and metadata.
// The process is transparent to users - existing data remains fully accessible
// through the default database.
//
// Returns an error if migration fails. On success, migration status is
// persisted in the system database to prevent re-running.
func (m *DatabaseManager) migrateLegacyData() error {
	// Check if migration already completed
	if m.isMigrationComplete() {
		return nil
	}

	// Use the engine directly (it implements Engine interface)
	// Migration works with any Engine implementation
	engine := m.inner

	// Detect unprefixed data
	hasUnprefixedData, err := m.detectUnprefixedData(engine)
	if err != nil {
		return fmt.Errorf("failed to detect unprefixed data: %w", err)
	}

	if !hasUnprefixedData {
		// No unprefixed data - mark migration as complete
		m.markMigrationComplete()
		return nil
	}

	// Perform migration
	if err := m.performMigration(engine); err != nil {
		return fmt.Errorf("failed to migrate legacy data: %w", err)
	}

	// Mark migration as complete
	m.markMigrationComplete()

	return nil
}

// isMigrationComplete checks if legacy data migration has been completed.
//
// Returns true if migration has already been run and marked as complete
// in the system database metadata. This prevents the migration from
// running multiple times.
func (m *DatabaseManager) isMigrationComplete() bool {
	// Check metadata for migration status
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)
	node, err := systemEngine.GetNode(storage.NodeID(migrationStatusKey))
	if err == storage.ErrNotFound {
		return false
	}
	if err != nil {
		return false
	}

	// Check if migration is marked as complete
	if status, ok := node.Properties["status"].(string); ok {
		return status == "complete"
	}

	return false
}

// markMigrationComplete marks the migration as complete in metadata.
//
// Stores migration status in the system database to prevent the migration
// from running again on subsequent startups. The status includes the
// target database name for reference.
func (m *DatabaseManager) markMigrationComplete() error {
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node := &storage.Node{
		ID:     storage.NodeID(migrationStatusKey),
		Labels: []string{"_System", "_Migration"},
		Properties: map[string]any{
			"status":      "complete",
			"migrated_to": m.config.DefaultDatabase,
		},
	}

	// Try update first, then create
	err := systemEngine.UpdateNode(node)
	if err == storage.ErrNotFound {
		return systemEngine.CreateNode(node)
	}
	return err
}

// detectUnprefixedData checks if there's any data without namespace prefixes.
//
// Scans all nodes and edges in the storage engine to detect data that
// doesn't have a namespace prefix (i.e., data created before multi-database
// support was added). A node/edge is considered unprefixed if its ID doesn't
// contain a colon (":"), which is used as the namespace separator.
//
// Returns true if any unprefixed data is found, false otherwise.
// Returns an error if scanning fails.
func (m *DatabaseManager) detectUnprefixedData(engine storage.Engine) (bool, error) {
	// Get all nodes
	allNodes, err := engine.AllNodes()
	if err != nil {
		return false, err
	}

	// Check if any node doesn't have a namespace prefix
	// A node has a namespace prefix if its ID contains ":"
	for _, node := range allNodes {
		nodeIDStr := string(node.ID)
		// Check if this looks like unprefixed data
		// Prefixed data will have format like "nornic:node-123" or "system:node-123"
		// Unprefixed data will be just "node-123"
		if !strings.Contains(nodeIDStr, ":") {
			return true, nil
		}
	}

	// Also check edges
	allEdges, err := engine.AllEdges()
	if err != nil {
		return false, err
	}

	for _, edge := range allEdges {
		edgeIDStr := string(edge.ID)
		if !strings.Contains(edgeIDStr, ":") {
			return true, nil
		}
	}

	return false, nil
}

// performMigration migrates all unprefixed data to the default database namespace.
//
// This function performs the actual migration:
//  1. Collects all unprefixed nodes and edges
//  2. Creates new versions with prefixed IDs (e.g., "nornic:node-123")
//  3. Updates all indexes automatically (via CreateNode/CreateEdge)
//  4. Deletes old unprefixed versions
//
// The migration preserves all properties, relationships, and metadata.
// Edge relationships are maintained by prefixing both start and end node IDs.
//
// Returns an error if migration fails. On success, all unprefixed data
// will be accessible through the default database namespace.
func (m *DatabaseManager) performMigration(engine storage.Engine) error {
	defaultDB := m.config.DefaultDatabase

	// Get all nodes and edges
	allNodes, err := engine.AllNodes()
	if err != nil {
		return fmt.Errorf("failed to get all nodes: %w", err)
	}

	allEdges, err := engine.AllEdges()
	if err != nil {
		return fmt.Errorf("failed to get all edges: %w", err)
	}

	// Collect unprefixed nodes and edges
	var unprefixedNodes []*storage.Node
	var unprefixedEdges []*storage.Edge

	for _, node := range allNodes {
		nodeIDStr := string(node.ID)
		if !strings.Contains(nodeIDStr, ":") {
			unprefixedNodes = append(unprefixedNodes, node)
		}
	}

	for _, edge := range allEdges {
		edgeIDStr := string(edge.ID)
		if !strings.Contains(edgeIDStr, ":") {
			unprefixedEdges = append(unprefixedEdges, edge)
		}
	}

	if len(unprefixedNodes) == 0 && len(unprefixedEdges) == 0 {
		// Nothing to migrate
		return nil
	}

	// Migrate nodes: create new prefixed versions and delete old ones
	for _, node := range unprefixedNodes {
		// Copy properties and ensure db property is set for migrated nodes
		properties := make(map[string]any)
		for k, v := range node.Properties {
			properties[k] = v
		}
		// Add db property to mark this node as belonging to the default database
		// This helps with queries that filter by db property
		if _, exists := properties["db"]; !exists {
			properties["db"] = defaultDB
		}

		// Create new node with prefixed ID
		newNode := &storage.Node{
			ID:           storage.NodeID(defaultDB + ":" + string(node.ID)),
			Labels:       node.Labels,
			Properties:   properties,
			CreatedAt:    node.CreatedAt,
			UpdatedAt:    node.UpdatedAt,
			DecayScore:   node.DecayScore,
			LastAccessed: node.LastAccessed,
			AccessCount:  node.AccessCount,
			Embedding:    node.Embedding,
		}

		// Create new node (this will create all indexes)
		if err := engine.CreateNode(newNode); err != nil {
			// If node already exists (shouldn't happen), skip
			if err == storage.ErrAlreadyExists {
				continue
			}
			return fmt.Errorf("failed to create migrated node %s: %w", newNode.ID, err)
		}

		// Delete old unprefixed node (this will clean up old indexes)
		if err := engine.DeleteNode(node.ID); err != nil {
			// Log but continue - old node might already be gone
			// This is not critical as the new node exists
			continue
		}
	}

	// Migrate edges: create new prefixed versions and delete old ones
	for _, edge := range unprefixedEdges {
		// Create new edge with prefixed IDs
		newEdge := &storage.Edge{
			ID:            storage.EdgeID(defaultDB + ":" + string(edge.ID)),
			Type:          edge.Type,
			StartNode:     storage.NodeID(defaultDB + ":" + string(edge.StartNode)),
			EndNode:       storage.NodeID(defaultDB + ":" + string(edge.EndNode)),
			Properties:    edge.Properties,
			CreatedAt:     edge.CreatedAt,
			UpdatedAt:     edge.UpdatedAt,
			Confidence:    edge.Confidence,
			AutoGenerated: edge.AutoGenerated,
		}

		// Create new edge (this will create all indexes)
		if err := engine.CreateEdge(newEdge); err != nil {
			// If edge already exists (shouldn't happen), skip
			if err == storage.ErrAlreadyExists {
				continue
			}
			return fmt.Errorf("failed to create migrated edge %s: %w", newEdge.ID, err)
		}

		// Delete old unprefixed edge (this will clean up old indexes)
		if err := engine.DeleteEdge(edge.ID); err != nil {
			// Log but continue - old edge might already be gone
			continue
		}
	}

	return nil
}

// ensureDefaultDatabaseProperty ensures all nodes in the default database have a db property.
//
// This is a post-migration step that adds the db property to nodes that were migrated
// or created before the db property was standard. The db property helps with queries
// that filter by database name, though the namespace prefix is what actually provides isolation.
//
// This function is idempotent - it only updates nodes that don't have the db property.
func (m *DatabaseManager) ensureDefaultDatabaseProperty() error {
	defaultDB := m.config.DefaultDatabase

	// Get namespaced engine for default database
	defaultEngine := storage.NewNamespacedEngine(m.inner, defaultDB)

	// Get all nodes in default database
	allNodes, err := defaultEngine.AllNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes from default database: %w", err)
	}

	// Update nodes that don't have db property
	updated := 0
	for _, node := range allNodes {
		// Check if node has db property
		if _, exists := node.Properties["db"]; !exists {
			// Add db property
			if node.Properties == nil {
				node.Properties = make(map[string]any)
			}
			node.Properties["db"] = defaultDB

			// Update the node
			if err := defaultEngine.UpdateNode(node); err != nil {
				// Log but continue - some nodes might fail to update
				continue
			}
			updated++
		}
	}

	// If we updated any nodes, log it (but don't fail)
	if updated > 0 {
		// This is a best-effort operation - namespace prefix provides the actual isolation
		// The db property is just for convenience in queries
	}

	return nil
}
