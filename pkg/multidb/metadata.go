// Package multidb provides metadata persistence for multi-database support.
package multidb

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	metadataNodeID = "databases:metadata"
)

// loadMetadata loads database metadata from storage.
func (m *DatabaseManager) loadMetadata() error {
	m.catalogRefreshMu.Lock()
	defer m.catalogRefreshMu.Unlock()
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
	m.mu.Lock()
	changed := false
	defer func() { m.unlockStatementCacheIndexes(changed, false) }()

	m.serverID, _ = node.Properties["server_id"].(string)
	if createdAt, ok := node.Properties["server_created_at"].(string); ok {
		parsed, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return err
		}
		m.serverCreatedAt = parsed
	} else {
		m.serverCreatedAt = node.CreatedAt
	}

	// Parse metadata from properties
	if data, ok := node.Properties["data"].(string); ok {
		var databases map[string]*DatabaseInfo
		if err := json.Unmarshal([]byte(data), &databases); err != nil {
			return localizedError(localization.MultidbMetadataParseFailed(err), err)
		}
		births := make(map[string]uint64)
		if encoded, ok := node.Properties["namespace_lifetimes"].(string); ok {
			if err := json.Unmarshal([]byte(encoded), &births); err != nil {
				return err
			}
		}
		storage.ObserveNamespaceCatalogLifetimes(m.inner, births)
		for name, info := range databases {
			info.namespaceLifetime = births[name]
			if storage.NamespaceCatalogRetired(m.inner, name, info.namespaceLifetime) {
				delete(databases, name)
				continue
			}
			if previous := m.databases[name]; previous != nil && previous.ID == info.ID {
				copyDatabaseMetadata(previous, info)
				databases[name] = previous
			}
			if checker := m.limitCheckers[name]; checker != nil {
				checker.updateLimits(info.Limits)
			}
			m.applyChangeCapture(name, info.Options)
		}
		for name, previous := range m.databases {
			if current := databases[name]; current == nil || current.ID != previous.ID {
				m.dropDatabaseStatementCachesLocked(name)
				changed = true
				delete(m.engines, name)
				delete(m.limitCheckers, name)
				if current == nil {
					m.applyChangeCapture(name, nil)
				}
			}
		}
		m.databases = databases
	}

	return nil
}

// Preserve the live size-accounting object while replacing all catalog fields,
// including fields omitted by JSON when an alias/limit/option was removed.
func copyDatabaseMetadata(dst, src *DatabaseInfo) {
	dst.ID, dst.Name = src.ID, src.Name
	dst.CreatedAt, dst.CreatedBy = src.CreatedAt, src.CreatedBy
	dst.Status, dst.Type, dst.IsDefault = src.Status, src.Type, src.IsDefault
	dst.NodeCount, dst.UpdatedAt = src.NodeCount, src.UpdatedAt
	dst.Aliases, dst.Limits, dst.Constituents = src.Aliases, src.Limits, src.Constituents
	dst.Options, dst.namespaceLifetime = src.Options, src.namespaceLifetime
}

// persistMetadata saves database metadata to storage.
func (m *DatabaseManager) persistMetadata(completion ...bool) error {
	if len(completion) != 0 && completion[0] {
		if leader, ok := m.inner.(interface{ IsLeader() bool }); ok && !leader.IsLeader() {
			// Committed DROP and its durable markers already retire the
			// replica catalog. Only the primary publishes full metadata;
			// a late local snapshot must not overwrite a replayed CREATE.
			return nil
		}
	}
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
	data, err := json.Marshal(m.databases)
	if err != nil {
		return localizedError(localization.MultidbMetadataSerializeFailed(err), err)
	}
	births := make(map[string]uint64, len(m.databases))
	for name, info := range m.databases {
		births[name] = info.namespaceLifetime
	}
	lifetimes, err := json.Marshal(births)
	if err != nil {
		return err
	}

	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node := &storage.Node{
		ID:     storage.NodeID(metadataNodeID),
		Labels: []string{"_System", "_Metadata"},
		Properties: map[string]any{
			"data":                string(data),
			"namespace_lifetimes": string(lifetimes),
			"type":                "databases",
			"updated_at":          time.Now().Unix(),
			"server_id":           m.serverID,
			"server_created_at":   m.serverCreatedAt.Format(time.RFC3339Nano),
		},
	}

	// Capture the catalog publication order while holding m.mu, then release
	// it for every storage/snapshot/replication wait. A later snapshot cannot
	// overtake this one and overwrite a newer catalog with older metadata.
	previous := m.metadataTail
	done := make(chan struct{})
	m.metadataTail = done
	m.mu.Unlock()
	defer m.mu.Lock()
	defer close(done)
	if previous != nil {
		<-previous
	}

	// Check if node exists
	existing, err := systemEngine.GetNode(storage.NodeID(metadataNodeID))
	if err == storage.ErrNotFound {
		// Create new
		_, err := systemEngine.CreateNode(node)
		if len(completion) != 0 && completion[0] && storage.MutationApplied(err) {
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}

	// Update existing
	node.CreatedAt = existing.CreatedAt
	err = systemEngine.UpdateNode(node)
	if len(completion) != 0 && completion[0] && storage.MutationApplied(err) {
		return nil
	}
	return err
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
