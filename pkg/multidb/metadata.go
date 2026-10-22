// Package multidb provides metadata persistence for multi-database support.
package multidb

import (
	"encoding/json"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/observability"
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
		m.databases = databases
	}
	m.primeRuntimeCredentialFastPaths()
	if m.hasLegacyRemoteCredentials() && !m.canMigrateRemoteCredentials() {
		log.Printf("WARNING: legacy remote credentials remain unencrypted; configure NORNICDB_REMOTE_CREDENTIALS_KEY and reopen on writable storage to migrate them")
		return nil
	}
	changed, err := m.normalizeConstituentMetadata()
	if err != nil {
		return err
	}
	m.primeRuntimeCredentialFastPaths()
	if changed {
		return m.persistMetadata()
	}

	return nil
}

// persistMetadata saves database metadata to storage.
func (m *DatabaseManager) persistMetadata() error {
	if _, err := m.normalizeConstituentMetadata(); err != nil {
		return err
	}
	m.primeRuntimeCredentialFastPaths()
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

	// Use system namespace to store metadata
	systemEngine := storage.NewNamespacedEngine(m.inner, m.config.SystemDatabase)

	node := &storage.Node{
		ID:     storage.NodeID(metadataNodeID),
		Labels: []string{"_System", "_Metadata"},
		Properties: map[string]any{
			"data":              string(data),
			"type":              "databases",
			"updated_at":        time.Now().Unix(),
			"server_id":         m.serverID,
			"server_created_at": m.serverCreatedAt.Format(time.RFC3339Nano),
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

// normalizeConstituentMetadata guards every metadata writer and the load/open
// migration. Work on copies so a failed normalization cannot partially change
// the live catalog. No credential is decrypted at this persistence boundary.
func (m *DatabaseManager) normalizeConstituentMetadata() (bool, error) {
	if !m.canMigrateRemoteCredentials() {
		// New input has already passed normalizeRemoteCredentials with
		// newInput=true. Preserve loaded legacy records until keyed/writable.
		return false, nil
	}
	type replacement struct {
		database *DatabaseInfo
		refs     []ConstituentRef
	}
	var replacements []replacement
	for _, database := range m.databases {
		var refs []ConstituentRef
		for i, original := range database.Constituents {
			normalized, err := m.normalizeRemoteCredentials(original, false)
			if err != nil {
				return false, err
			}
			if normalized != original {
				if refs == nil {
					refs = append([]ConstituentRef(nil), database.Constituents...)
				}
				refs[i] = normalized
			}
		}
		if refs != nil {
			replacements = append(replacements, replacement{database, refs})
		}
	}
	for _, update := range replacements {
		update.database.Constituents = update.refs
	}
	return len(replacements) != 0, nil
}

func (m *DatabaseManager) canMigrateRemoteCredentials() bool {
	if m.remoteCredentialCipher == nil {
		return false
	}
	leader, ok := m.inner.(interface{ IsLeader() bool })
	return !ok || leader.IsLeader()
}

func (m *DatabaseManager) hasLegacyRemoteCredentials() bool {
	for _, database := range m.databases {
		for _, ref := range database.Constituents {
			if ref.Type != "remote" {
				continue
			}
			if ref.Password != "" && !isEncryptedRemoteCredential(ref.Password) {
				return true
			}
			if !strings.EqualFold(strings.TrimSpace(ref.AuthMode), "user_password") && ref.User != "" && (!isEncryptedRemoteCredential(ref.User) || ref.Password == "") {
				return true
			}
			endpoint, err := url.Parse(ref.URI)
			if err == nil {
				legacy := false
				_ = observability.MapURLCredentials(endpoint, func(user *url.Userinfo) *url.Userinfo {
					legacy = true
					return user
				}, func(value string, _ error) (string, error) {
					legacy = legacy || !isEncryptedRemoteCredential(value)
					return value, nil
				})
				if legacy {
					return true
				}
			}
		}
	}
	return false
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
