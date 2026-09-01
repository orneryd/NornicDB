// Package multidb provides metadata persistence for multi-database support.
package multidb

import (
	"encoding/json"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	metadataNodeID = "databases:metadata"
)

// loadMetadata loads database metadata from storage.
func (m *DatabaseManager) loadMetadata() error {
	// Use system namespace to load metadata
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node, err := systemEngine.GetNode(storage.NodeID(metadataNodeID))
	if err == storage.ErrNotFound {
		// No existing metadata - start fresh
		return nil
	}
	if err != nil {
		return err
	}

	// Parse metadata from properties
	if data, ok := node.Properties["data"].(string); ok {
		var databases map[string]*DatabaseInfo
		if err := json.Unmarshal([]byte(data), &databases); err != nil {
			return localizedError(localization.MultidbMetadataParseFailed(err), err)
		}
		m.databases = databases
	}

	return nil
}

// persistMetadata saves database metadata to storage.
func (m *DatabaseManager) persistMetadata() error {
	// Serialize metadata
	data, err := json.Marshal(m.databases)
	if err != nil {
		return localizedError(localization.MultidbMetadataSerializeFailed(err), err)
	}

	// Use system namespace to store metadata
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node := &storage.Node{
		ID:     storage.NodeID(metadataNodeID),
		Labels: []string{"_System", "_Metadata"},
		Properties: map[string]any{
			"data":       string(data),
			"type":       "databases",
			"updated_at": time.Now().Unix(),
		},
	}

	// Check if node exists
	existing, err := systemEngine.GetNode(storage.NodeID(metadataNodeID))
	if err == storage.ErrNotFound {
		// Create new
		_, err := systemEngine.CreateNode(node)
		return err
	}
	if err != nil {
		return err
	}

	// Update existing
	node.CreatedAt = existing.CreatedAt
	return systemEngine.UpdateNode(node)
}
