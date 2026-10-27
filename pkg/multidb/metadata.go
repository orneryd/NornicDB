// Package multidb provides metadata persistence for multi-database support.
package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	metadataNodeID = "databases:metadata"
)

// loadMetadata loads database metadata from storage.
func (m *DatabaseManager) loadMetadata(ctx context.Context) (bool, error) {
	// Use system namespace to load metadata
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node, err := systemEngine.GetNode(storage.NodeID(metadataNodeID))
	if err == storage.ErrNotFound {
		// No existing metadata - start fresh
		return false, nil
	}
	if err != nil {
		return false, err
	}

	m.serverID, _ = node.Properties["server_id"].(string)
	persistedTime := false
	if createdAt, ok := node.Properties["server_created_at"].(string); ok {
		persistedTime = true
		parsed, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return false, err
		}
		m.serverCreatedAt = parsed
	} else {
		m.serverCreatedAt = node.CreatedAt
	}

	// Parse metadata from properties
	if data, ok := node.Properties["data"].(string); ok {
		var databases map[string]*DatabaseInfo
		if err := jsontext.Unmarshal([]byte(data), &databases); err != nil {
			return false, localizedError(localization.MultidbMetadataParseFailed(err), err)
		}
		m.databases = databases
	}
	if encoded, ok := node.Properties["pending_drops"].(string); ok {
		var pending map[string]bool
		if err := jsontext.Unmarshal([]byte(encoded), &pending); err != nil {
			return false, err
		}
		for name, active := range pending {
			if active {
				m.transitionPendingDrop(name, true)
			}
		}
	}
	m.rebuildRuntimeConstituents()

	complete := persistedTime && m.serverID != "" && !m.serverCreatedAt.IsZero()
	for _, database := range m.databases {
		if database == nil || database.ID == "" {
			complete = false
			break
		}
	}
	return complete, nil
}

// catalogMetadataNode builds only the detached proposal's persisted image.
func (m *DatabaseManager) catalogMetadataNode() (*storage.Node, error) {
	if m.serverID == "" {
		m.serverID = uuid.NewString()
	}
	if m.serverCreatedAt.IsZero() {
		m.serverCreatedAt = time.Now().UTC()
	}
	for _, database := range m.databases {
		if database.ID == "" {
			database.ID = uuid.NewString()
		}
	}
	// Serialize metadata
	snapshot := make(map[string]*DatabaseInfo, len(m.databases))
	for name, info := range m.databases {
		snapshot[name] = cloneDatabaseInfo(info, false, databaseSnapshotFull)
	}
	data, err := jsontext.Marshal(snapshot)
	if err != nil {
		return nil, localizedError(localization.MultidbMetadataSerializeFailed(err), err)
	}

	pending, err := jsontext.Marshal(m.pendingDrops)
	if err != nil {
		return nil, err
	}

	node := &storage.Node{
		ID:     storage.NodeID(metadataNodeID),
		Labels: []string{"_System", "_Metadata"},
		Properties: map[string]any{
			"data":              string(data),
			"pending_drops":     string(pending),
			"type":              "databases",
			"updated_at":        time.Now().Unix(),
			"server_id":         m.serverID,
			"server_created_at": m.serverCreatedAt.Format(time.RFC3339Nano),
		},
	}

	return node, nil
}

// ServerIdentity returns the persisted installation UUID and creation time.
// They survive manager reloads and are shared by all databases in the installation.
// A read-only legacy store may return an empty ID until upgraded by its writer.
// For example, administrative introspection can use these values for serverID
// and the DBMS creationDate instead of inventing an identity or timestamp.
func (m *DatabaseManager) ServerIdentity() (string, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.serverID, m.serverCreatedAt
}

// SystemDatabaseName returns the configured metadata database name.
// For example, dbms.info uses this name rather than assuming it is "system".
func (m *DatabaseManager) SystemDatabaseName() string {
	return m.config.SystemDatabase
}

// DatabaseIdentity returns a database's persisted UUID and creation time.
// Reloading a manager preserves these values; dropping and recreating the
// database creates a new UUID. An unknown database returns empty values.
// For example, SHOW DATABASES uses these values for databaseID and creationTime.
func (m *DatabaseManager) DatabaseIdentity(name string) (string, time.Time) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if database := m.databases[name]; database != nil {
		return database.ID, database.CreatedAt
	}
	return "", time.Time{}
}
