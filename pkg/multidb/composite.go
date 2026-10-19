// Package multidb provides composite database support for multi-database functionality.
//
// Composite databases are virtual databases that span multiple physical databases,
// allowing queries to transparently access data from multiple constituent databases.
package multidb

import (
	"iter"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// RemoteEngineFactory creates storage engines for remote composite constituents.
// authToken is the original caller's auth token/header value, forwarded to preserve
// authentication context across distributed constituent queries.
type RemoteEngineFactory func(ref ConstituentRef, authToken string) (storage.Engine, error)

// ConstituentRef represents a reference to a constituent database within a composite database.
type ConstituentRef struct {
	// Alias is the name used within the composite database to reference this constituent.
	Alias string `json:"alias"`

	// DatabaseName is the actual database name (or alias) that this constituent points to.
	DatabaseName string `json:"database_name"`

	// Type is the type of constituent: "local" (same instance) or "remote" (another instance).
	Type string `json:"type"` // "local", "remote"

	// AccessMode controls what operations are allowed: "read", "write", "read_write".
	AccessMode string `json:"access_mode"` // "read", "write", "read_write"

	// URI points to the remote NornicDB endpoint when Type == "remote".
	URI string `json:"uri,omitempty"`

	// SecretRef identifies credentials/token material for remote access.
	// The actual secret is resolved outside of metadata persistence.
	SecretRef string `json:"secret_ref,omitempty"`

	// User and Password implement Neo4j-style explicit remote auth:
	// ... AT '<url>' USER <user> PASSWORD '<password>'
	//
	// Password is encrypted before persisting metadata to the system namespace.
	// URI userinfo is normalized into these same fields, retaining forwarding
	// mode so that URI credentials remain a fallback when no token is supplied.
	// At runtime, DatabaseManager decrypts it before invoking RemoteEngineFactory.
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`

	// AuthMode defines remote auth behavior:
	// - "oidc_forwarding": forward caller Authorization header
	// - "user_password": use explicit User/Password for outbound Basic auth
	// Empty is treated as "oidc_forwarding" for remote constituents.
	AuthMode string `json:"auth_mode,omitempty"`
}

// Validate validates a constituent reference.
func (c *ConstituentRef) Validate() error {
	if c.Alias == "" {
		return localizedError(localization.MultidbConstituentAliasRequired(), nil)
	}
	if c.DatabaseName == "" {
		return localizedError(localization.MultidbConstituentDatabaseRequired(), nil)
	}
	if c.Type != "local" && c.Type != "remote" {
		return localizedError(localization.MultidbConstituentTypeInvalid(), nil)
	}
	if c.AccessMode != "read" && c.AccessMode != "write" && c.AccessMode != "read_write" {
		return localizedError(localization.MultidbAccessModeInvalid(), nil)
	}
	if c.Type == "remote" && c.URI == "" {
		return localizedError(localization.MultidbRemoteURIRequired(), nil)
	}
	if c.Type == "remote" {
		mode := strings.ToLower(strings.TrimSpace(c.AuthMode))
		if mode == "" {
			mode = "oidc_forwarding"
		}
		if mode != "oidc_forwarding" && mode != "user_password" {
			return localizedError(localization.MultidbRemoteAuthModeInvalid(), nil)
		}
		if mode == "user_password" {
			if strings.TrimSpace(c.User) == "" {
				return localizedError(localization.MultidbRemoteUserRequired(), nil)
			}
			if strings.TrimSpace(c.Password) == "" {
				return localizedError(localization.MultidbRemotePasswordRequired(), nil)
			}
		}
		if mode == "oidc_forwarding" {
			if strings.TrimSpace(c.User) != "" || strings.TrimSpace(c.Password) != "" {
				return localizedError(localization.MultidbOIDCCredentialsForbidden(), nil)
			}
		}
	}
	return nil
}

// CreateCompositeDatabase creates a new composite database.
//
// A composite database is a virtual database that spans multiple constituent databases.
// Queries against a composite database transparently access data from all constituents.
//
// Parameters:
//   - name: The name of the composite database (must be unique)
//   - constituents: List of constituent database references
//
// Returns ErrDatabaseExists if a database with this name already exists.
// Returns ErrInvalidDatabaseName if the name is invalid.
// Returns an error if any constituent database doesn't exist.
func (m *DatabaseManager) CreateCompositeDatabase(name string, constituents []ConstituentRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate name
	if name == "" {
		return ErrInvalidDatabaseName
	}

	// Check if database already exists
	if _, exists := m.databases[name]; exists {
		return ErrDatabaseExists
	}

	// Validate all constituents
	for i, ref := range constituents {
		if err := ref.Validate(); err != nil {
			return localizedError(localization.MultidbInvalidConstituent(i, err), err)
		}

		if ref.Type == "local" {
			// Check if constituent database exists
			// Resolve alias if needed
			actualName, err := m.resolveDatabaseInternal(ref.DatabaseName)
			if err != nil {
				return localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)
			}

			// Cannot use composite database as constituent (prevent cycles)
			if info, exists := m.databases[actualName]; exists && info.Type == "composite" {
				return localizedError(localization.MultidbCompositeDatabaseAsConstituent(actualName), nil)
			}
		}
	}

	// Check for duplicate aliases
	aliasMap := make(map[string]bool)
	for _, ref := range constituents {
		if aliasMap[ref.Alias] {
			return localizedError(localization.MultidbCompositeDuplicateAlias(ref.Alias), nil)
		}
		aliasMap[ref.Alias] = true
	}

	// Normalize new input through the same owner used by metadata persistence.
	encrypted := make([]ConstituentRef, len(constituents))
	copy(encrypted, constituents)
	for i := range encrypted {
		ref, err := m.normalizeRemoteCredentials(encrypted[i], true)
		if err != nil {
			return err
		}
		encrypted[i] = ref
	}

	// Create composite database info
	m.databases[name] = &DatabaseInfo{
		Name:         name,
		CreatedAt:    time.Now(),
		Status:       "online",
		Type:         "composite",
		IsDefault:    false,
		UpdatedAt:    time.Now(),
		Constituents: encrypted,
	}

	return m.persistMetadata()
}

// DropCompositeDatabase removes a composite database.
//
// This only removes the composite database metadata. The constituent databases
// remain unchanged.
func (m *DatabaseManager) DropCompositeDatabase(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.databases[name]
	if !exists {
		return ErrDatabaseNotFound
	}

	if info.Type != "composite" {
		return localizedError(localization.MultidbCompositeDatabaseNotComposite(name), nil)
	}

	// Remove from metadata
	delete(m.databases, name)
	delete(m.engines, name) // Clear cached engine

	if err := m.persistMetadata(); err != nil {
		// If persistence fails, restore the database to maintain consistency
		// This prevents the database from being dropped in memory but still existing in storage
		m.databases[name] = info
		return localizedError(localization.MultidbCompositePersistMetadataAfterDropFailed(err), err)
	}

	return nil
}

// AddConstituent adds a constituent to an existing composite database.
func (m *DatabaseManager) AddConstituent(compositeName string, constituent ConstituentRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.databases[compositeName]
	if !exists {
		return ErrDatabaseNotFound
	}

	if info.Type != "composite" {
		return localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
	}

	// Validate constituent
	if err := constituent.Validate(); err != nil {
		return err
	}

	// Check if constituent database exists for local constituents
	if constituent.Type == "local" {
		_, err := m.resolveDatabaseInternal(constituent.DatabaseName)
		if err != nil {
			return localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(constituent.DatabaseName, err), err)
		}
	}

	// Check for duplicate alias
	for _, existing := range info.Constituents {
		if existing.Alias == constituent.Alias {
			return localizedError(localization.MultidbCompositeAliasExists(constituent.Alias), nil)
		}
	}

	encrypted, err := m.normalizeRemoteCredentials(constituent, true)
	if err != nil {
		return err
	}

	// Add constituent
	info.Constituents = append(info.Constituents, encrypted)
	info.UpdatedAt = time.Now()

	return m.persistMetadata()
}

// RemoveConstituent removes a constituent from a composite database.
func (m *DatabaseManager) RemoveConstituent(compositeName string, alias string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.databases[compositeName]
	if !exists {
		return ErrDatabaseNotFound
	}

	if info.Type != "composite" {
		return localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
	}

	// Find and remove constituent
	for i, ref := range info.Constituents {
		if ref.Alias == alias {
			info.Constituents = append(info.Constituents[:i], info.Constituents[i+1:]...)
			info.UpdatedAt = time.Now()
			return m.persistMetadata()
		}
	}

	return localizedError(localization.MultidbCompositeAliasNotFound(alias), nil)
}

// GetCompositeConstituents returns the list of constituents for a composite database.
func (m *DatabaseManager) GetCompositeConstituents(compositeName string) ([]ConstituentRef, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[compositeName]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	if info.Type != "composite" {
		return nil, localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
	}

	return snapshotConstituents(info.Constituents), nil
}

// canonicalConstituents is the shared catalog/manager reader of graph aliases.
// Preserve the catalog's last-registration-wins rule for case-folded duplicates.
// Walk backward so a single lookup can stop at its winner without copying the
// catalog. The yielded key has exactly the catalog's case normalization.
func canonicalConstituents(refs []ConstituentRef) iter.Seq2[string, ConstituentRef] {
	return func(yield func(string, ConstituentRef) bool) {
		seen := make(map[string]struct{}, len(refs))
		for i := len(refs) - 1; i >= 0; i-- {
			ref := refs[i]
			key := strings.ToLower(ref.Alias)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			if !yield(key, ref) {
				return
			}
		}
	}
}

// snapshotConstituents returns the same winners in their stored order for
// metadata/catalog callers that need an independent slice.
func snapshotConstituents(refs []ConstituentRef) []ConstituentRef {
	if refs == nil {
		return nil
	}
	result := make([]ConstituentRef, 0, len(refs))
	for _, ref := range canonicalConstituents(refs) {
		result = append(result, ref)
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// ListCompositeDatabases returns all composite databases.
func (m *DatabaseManager) ListCompositeDatabases() []*DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*DatabaseInfo
	for _, info := range m.databases {
		if info.Type == "composite" {
			result = append(result, cloneDatabaseInfo(info))
		}
	}
	return result
}

// IsCompositeDatabase checks if a database is a composite database.
func (m *DatabaseManager) IsCompositeDatabase(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[name]
	return exists && info.Type == "composite"
}

// ExistsOrIsConstituent returns true if the name refers to an existing database
// or a valid composite constituent in dotted "composite.alias" form.
// This is used by protocol entry paths (Bolt/HTTP) to avoid premature
// DB-not-found rejection for constituent graph references.
func (m *DatabaseManager) ExistsOrIsConstituent(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Direct database match.
	if m.databases[name] != nil {
		return true
	}

	// Check alias match.
	for _, info := range m.databases {
		for _, alias := range info.Aliases {
			if alias == name {
				return true
			}
		}
	}

	// Dotted composite.alias form.
	if dotIdx := strings.IndexByte(name, '.'); dotIdx > 0 && dotIdx < len(name)-1 {
		compositeName := name[:dotIdx]
		constituentAlias := name[dotIdx+1:]
		info, exists := m.databases[compositeName]
		if exists && info.Type == "composite" {
			for key := range canonicalConstituents(info.Constituents) {
				if key == strings.ToLower(constituentAlias) {
					return true
				}
			}
		}
	}

	return false
}

// resolveDatabaseInternal resolves a database name or alias to the actual database name.
// Must be called with lock held.
func (m *DatabaseManager) resolveDatabaseInternal(nameOrAlias string) (string, error) {
	// Check if it's an actual database name
	if _, exists := m.databases[nameOrAlias]; exists {
		return nameOrAlias, nil
	}

	// Check if it's an alias
	for dbName, info := range m.databases {
		for _, alias := range info.Aliases {
			if alias == nameOrAlias {
				return dbName, nil
			}
		}
	}

	return "", ErrDatabaseNotFound
}
