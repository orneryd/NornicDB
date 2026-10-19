// Package multidb provides metadata persistence for multi-database support.
package multidb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
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
	defer m.mu.Unlock()

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
		var committed map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &committed); err != nil {
			return err
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
				delete(committed, name)
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
				delete(m.engines, name)
				delete(m.limitCheckers, name)
				if current == nil {
					m.applyChangeCapture(name, nil)
				}
			}
		}
		m.databases = databases
		m.committedCatalog = committed
		for name, p := range m.catalogPreparations {
			if bytes.Equal(committed[name], p.mutation.Value) {
				m.finishCatalogPreparationLocked(name, p)
			}
		}
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

// persistMetadata publishes namespace deltas, never a snapshot of pending intent.
// Callers hold m.mu; it is released for all storage and consensus waits.
func (m *DatabaseManager) persistMetadata() error {
	return m.persistMetadataContext(context.Background(), false)
}

func (m *DatabaseManager) catalogRecord() *storage.Node {
	return &storage.Node{ID: storage.NodeID(m.config.SystemDatabase + ":" + metadataNodeID), Labels: []string{"_System", "_Metadata"}, Properties: map[string]any{
		"type": "databases", "updated_at": time.Now().Unix(), "server_id": m.serverID, "server_created_at": m.serverCreatedAt.Format(time.RFC3339Nano),
	}}
}

func (m *DatabaseManager) persistMetadataContext(ctx context.Context, bootstrap bool) error {
	if m.serverID == "" {
		m.serverID = uuid.NewString()
	}
	if m.serverCreatedAt.IsZero() {
		m.serverCreatedAt = time.Now().UTC()
	}
	var mutations []storage.NamespaceCatalogMutation
	for name, info := range m.databases {
		if info.ID == "" {
			info.ID = uuid.NewString()
		}
		value, err := json.Marshal(info)
		if err != nil {
			return err
		}
		expected := m.committedCatalog[name]
		// Compare semantic catalog values: old records may omit newly added fields.
		var previous DatabaseInfo
		if len(expected) != 0 {
			if err := json.Unmarshal(expected, &previous); err != nil {
				return err
			}
		}
		normalized, _ := json.Marshal(&previous)
		if len(expected) == 0 || !bytes.Equal(value, normalized) {
			mutations = append(mutations, storage.NamespaceCatalogMutation{Namespace: name, Record: m.catalogRecord(), Expected: expected, Value: value, Create: len(expected) == 0, Bootstrap: bootstrap})
		}
	}
	// Restore committed metadata before releasing the manager lock. No pending
	// CREATE/ALTER can leak into reads or another namespace's publication.
	for name, info := range m.databases {
		raw := m.committedCatalog[name]
		if len(raw) == 0 {
			delete(m.databases, name)
			continue
		}
		var previous DatabaseInfo
		if err := json.Unmarshal(raw, &previous); err != nil {
			return err
		}
		previous.namespaceLifetime = info.namespaceLifetime
		copyDatabaseMetadata(info, &previous)
	}
	sort.Slice(mutations, func(i, j int) bool { return mutations[i].Namespace < mutations[j].Namespace })
	for _, mutation := range mutations {
		if m.catalogPreparations[mutation.Namespace] != nil {
			return storage.ErrConflict
		}
	}
	if m.catalogPreparations == nil {
		m.catalogPreparations = make(map[string]*catalogPreparation)
	}
	preparations := make([]*catalogPreparation, 0, len(mutations))
	for _, mutation := range mutations {
		p := &catalogPreparation{mutation: mutation, done: make(chan struct{}), operation: storage.NewNamespaceDrop(nil)}
		p.operation.OnResolved(func() { m.finishCatalogPreparation(p.mutation.Namespace, p) })
		m.catalogPreparations[mutation.Namespace] = p
		preparations = append(preparations, p)
	}
	m.mu.Unlock()
	defer m.mu.Lock()
	started := 0
	defer func() {
		for _, p := range preparations[started:] {
			m.finishCatalogPreparation(p.mutation.Namespace, p)
		}
	}()
	for _, p := range preparations {
		started++
		if err := storage.PublishNamespaceCatalogContext(storage.WithNamespaceDrop(ctx, p.operation), m.inner, p.mutation); err != nil {
			var outcome *storage.NamespaceDropError
			if !errors.As(err, &outcome) || outcome.Committed || outcome.Aborted {
				m.finishCatalogPreparation(p.mutation.Namespace, p)
			}
			return err
		}
		if !p.operation.Committed() {
			// Forwarded CREATE/ALTER can reach the leader before local replay.
			// Wait for the committed catalog, without owning the manager lock.
			select {
			case <-p.done:
			case <-ctx.Done():
				return &storage.NamespaceDropError{Committed: true, Cause: ctx.Err()}
			}
		}
	}
	// Legacy/mock wrappers without intrinsic observers still publish by reading
	// committed storage; no proposed value is installed directly.
	return m.loadMetadata()
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
