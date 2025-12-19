// Package multidb provides multi-database support for NornicDB.
//
// This package implements Neo4j 4.x-style multi-database support, allowing
// multiple logical databases (tenants) to share a single physical storage backend
// while maintaining complete data isolation.
package multidb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// DatabaseManager manages multiple logical databases within a single storage engine.
//
// It provides:
//   - Database creation and deletion
//   - Database metadata tracking
//   - Namespaced storage engine views
//   - Neo4j 4.x multi-database compatibility
//
// Thread-safe: all operations are protected by mutex.
//
// Example:
//
//	// Create manager with shared storage
//	inner := storage.NewBadgerEngine("./data")
//	manager := multidb.NewDatabaseManager(inner, nil)
//
//	// Create databases
//	manager.CreateDatabase("tenant_a")
//	manager.CreateDatabase("tenant_b")
//
//	// Get namespaced storage for a tenant
//	tenantStorage, _ := manager.GetStorage("tenant_a")
//
//	// Use storage (isolated to tenant_a)
//	tenantStorage.CreateNode(&storage.Node{ID: "123"})
type DatabaseManager struct {
	mu sync.RWMutex

	// Shared underlying storage
	inner storage.Engine

	// Database metadata (persisted in "system" namespace)
	databases map[string]*DatabaseInfo

	// Configuration
	config *Config

	// Cached namespaced engines (avoid recreating)
	engines map[string]*storage.NamespacedEngine
}

// DatabaseInfo holds metadata about a database.
type DatabaseInfo struct {
	Name         string           `json:"name"`
	CreatedAt    time.Time        `json:"created_at"`
	CreatedBy    string           `json:"created_by,omitempty"`
	Status       string           `json:"status"` // "online", "offline"
	Type         string           `json:"type"`   // "standard", "system"
	IsDefault    bool             `json:"is_default"`
	NodeCount    int64            `json:"node_count,omitempty"` // Cached, may be stale
	UpdatedAt    time.Time        `json:"updated_at"`
	Aliases      []string         `json:"aliases,omitempty"`      // Database aliases (Neo4j-compatible)
	Limits       *Limits          `json:"limits,omitempty"`       // Resource limits
	Constituents []ConstituentRef `json:"constituents,omitempty"` // Constituent databases (for composite type)
}

// Config holds DatabaseManager configuration.
type Config struct {
	// DefaultDatabase is the database used when none is specified (default: "nornic")
	// This matches Neo4j's behavior where "neo4j" is the default, but NornicDB uses "nornic"
	DefaultDatabase string

	// SystemDatabase stores metadata (default: "system")
	SystemDatabase string

	// MaxDatabases limits total databases (0 = unlimited)
	MaxDatabases int

	// AllowDropDefault allows dropping the default database
	AllowDropDefault bool
}

// DefaultConfig returns default configuration.
// The default database name is "nornic" (NornicDB's equivalent of Neo4j's "neo4j").
func DefaultConfig() *Config {
	return &Config{
		DefaultDatabase:  "nornic",
		SystemDatabase:   "system",
		MaxDatabases:     0, // Unlimited
		AllowDropDefault: false,
	}
}

// NewDatabaseManager creates a new database manager.
//
// Parameters:
//   - inner: The underlying storage engine (shared by all databases)
//   - config: Configuration (nil for defaults)
//
// On creation, initializes:
//   - System database (for metadata)
//   - Default database ("nornic" by default, configurable)
func NewDatabaseManager(inner storage.Engine, config *Config) (*DatabaseManager, error) {
	if config == nil {
		config = DefaultConfig()
	}

	m := &DatabaseManager{
		inner:     inner,
		databases: make(map[string]*DatabaseInfo),
		config:    config,
		engines:   make(map[string]*storage.NamespacedEngine),
	}

	// Load existing databases from system namespace
	if err := m.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load database metadata: %w", err)
	}

	// Ensure system and default databases exist
	if err := m.ensureSystemDatabases(); err != nil {
		return nil, err
	}

	// Migrate existing unprefixed data to default database namespace
	// This handles backwards compatibility for databases created before multi-db support
	if err := m.migrateLegacyData(); err != nil {
		return nil, fmt.Errorf("failed to migrate legacy data: %w", err)
	}

	return m, nil
}

// ensureSystemDatabases creates system and default databases if they don't exist.
func (m *DatabaseManager) ensureSystemDatabases() error {
	// System database
	if _, exists := m.databases[m.config.SystemDatabase]; !exists {
		m.databases[m.config.SystemDatabase] = &DatabaseInfo{
			Name:      m.config.SystemDatabase,
			CreatedAt: time.Now(),
			Status:    "online",
			Type:      "system",
			IsDefault: false,
			UpdatedAt: time.Now(),
		}
	}

	// Default database
	if _, exists := m.databases[m.config.DefaultDatabase]; !exists {
		m.databases[m.config.DefaultDatabase] = &DatabaseInfo{
			Name:      m.config.DefaultDatabase,
			CreatedAt: time.Now(),
			Status:    "online",
			Type:      "standard",
			IsDefault: true,
			UpdatedAt: time.Now(),
		}
	}

	return m.persistMetadata()
}

// CreateDatabase creates a new database.
//
// Parameters:
//   - name: Database name (must be unique, lowercase recommended)
//
// Returns ErrDatabaseExists if database already exists.
// Returns ErrMaxDatabasesReached if limit exceeded.
func (m *DatabaseManager) CreateDatabase(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate name
	if name == "" {
		return ErrInvalidDatabaseName
	}

	// Check if exists
	if _, exists := m.databases[name]; exists {
		return ErrDatabaseExists
	}

	// Check limit
	if m.config.MaxDatabases > 0 && len(m.databases) >= m.config.MaxDatabases {
		return ErrMaxDatabasesReached
	}

	// Create metadata
	m.databases[name] = &DatabaseInfo{
		Name:      name,
		CreatedAt: time.Now(),
		Status:    "online",
		Type:      "standard",
		IsDefault: false,
		UpdatedAt: time.Now(),
	}

	return m.persistMetadata()
}

// DropDatabase removes a database and all its data.
//
// Parameters:
//   - name: Database name to drop
//
// Returns ErrDatabaseNotFound if database doesn't exist.
// Returns ErrCannotDropSystemDB for system/default databases.
func (m *DatabaseManager) DropDatabase(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if exists
	info, exists := m.databases[name]
	if !exists {
		return ErrDatabaseNotFound
	}

	// Prevent dropping system database
	if info.Type == "system" {
		return ErrCannotDropSystemDB
	}

	// Prevent dropping default (unless allowed)
	if info.IsDefault && !m.config.AllowDropDefault {
		return ErrCannotDropDefaultDB
	}

	// Delete all data with this namespace prefix
	prefix := name + ":"
	nodesDeleted, edgesDeleted, err := m.inner.DeleteByPrefix(prefix)
	if err != nil {
		return fmt.Errorf("failed to delete database data: %w", err)
	}

	// Update metadata with deletion info (for logging/debugging)
	_ = nodesDeleted
	_ = edgesDeleted

	// Remove from metadata
	delete(m.databases, name)
	delete(m.engines, name) // Clear cached engine

	return m.persistMetadata()
}

// GetStorage returns a namespaced storage engine for the specified database.
//
// The returned engine is scoped to the database - all operations only
// affect data within that namespace.
func (m *DatabaseManager) GetStorage(name string) (storage.Engine, error) {
	m.mu.RLock()

	// Check cache first
	if engine, exists := m.engines[name]; exists {
		m.mu.RUnlock()
		return engine, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if engine, exists := m.engines[name]; exists {
		return engine, nil
	}

	// Validate database exists
	info, exists := m.databases[name]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	if info.Status != "online" {
		return nil, ErrDatabaseOffline
	}

	// Handle composite databases differently
	if info.Type == "composite" {
		// Build constituent engines map
		constituents := make(map[string]storage.Engine)
		constituentNames := make(map[string]string)
		accessModes := make(map[string]string)

		for _, ref := range info.Constituents {
			// Resolve actual database name (might be an alias)
			actualName, err := m.resolveDatabaseInternal(ref.DatabaseName)
			if err != nil {
				return nil, fmt.Errorf("constituent database '%s' not found: %w", ref.DatabaseName, err)
			}

			// Get storage for constituent
			constituentStorage, err := m.getStorageInternal(actualName)
			if err != nil {
				return nil, fmt.Errorf("failed to get storage for constituent '%s': %w", ref.DatabaseName, err)
			}

			constituents[ref.Alias] = constituentStorage
			constituentNames[ref.Alias] = actualName
			accessModes[ref.Alias] = ref.AccessMode
		}

		// Create composite engine
		compositeEngine := storage.NewCompositeEngine(constituents, constituentNames, accessModes)
		// Note: We don't cache composite engines the same way as they're lightweight wrappers
		return compositeEngine, nil
	}

	// Create namespaced engine for standard databases
	// Note: Limit enforcement is handled separately via LimitChecker
	// which is created on-demand when needed (not stored here)
	engine := storage.NewNamespacedEngine(m.inner, name)
	m.engines[name] = engine

	return engine, nil
}

// getStorageInternal gets storage for a database without resolving aliases.
// Must be called with lock held.
func (m *DatabaseManager) getStorageInternal(name string) (storage.Engine, error) {
	// Check cache first
	if engine, exists := m.engines[name]; exists {
		return engine, nil
	}

	// Validate database exists
	info, exists := m.databases[name]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	if info.Status != "online" {
		return nil, ErrDatabaseOffline
	}

	// Create namespaced engine
	engine := storage.NewNamespacedEngine(m.inner, name)
	m.engines[name] = engine

	return engine, nil
}

// GetDefaultStorage returns storage for the default database.
func (m *DatabaseManager) GetDefaultStorage() (storage.Engine, error) {
	return m.GetStorage(m.config.DefaultDatabase)
}

// ListDatabases returns all database info.
func (m *DatabaseManager) ListDatabases() []*DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*DatabaseInfo, 0, len(m.databases))
	for _, info := range m.databases {
		// Return a copy
		infoCopy := *info
		result = append(result, &infoCopy)
	}
	return result
}

// GetDatabase returns info for a specific database.
func (m *DatabaseManager) GetDatabase(name string) (*DatabaseInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[name]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	infoCopy := *info
	return &infoCopy, nil
}

// Exists checks if a database exists.
func (m *DatabaseManager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.databases[name] != nil
}

// DefaultDatabaseName returns the default database name.
func (m *DatabaseManager) DefaultDatabaseName() string {
	return m.config.DefaultDatabase
}

// SetDatabaseStatus sets a database online/offline.
func (m *DatabaseManager) SetDatabaseStatus(name, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.databases[name]
	if !exists {
		return ErrDatabaseNotFound
	}

	if status != "online" && status != "offline" {
		return fmt.Errorf("invalid status: %s (must be 'online' or 'offline')", status)
	}

	info.Status = status
	info.UpdatedAt = time.Now()

	// Clear cached engine if going offline
	if status == "offline" {
		delete(m.engines, name)
	}

	return m.persistMetadata()
}

// Close releases resources.
func (m *DatabaseManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear all cached engines
	m.engines = make(map[string]*storage.NamespacedEngine)

	// Close the underlying storage
	return m.inner.Close()
}

// ResolveDatabase resolves an alias or database name to the actual database name.
func (m *DatabaseManager) ResolveDatabase(nameOrAlias string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

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

// CreateAlias creates an alias for a database (Neo4j-compatible).
func (m *DatabaseManager) CreateAlias(alias, databaseName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate target database exists
	info, exists := m.databases[databaseName]
	if !exists {
		return ErrDatabaseNotFound
	}

	// Validate alias doesn't conflict with existing database name
	if m.Exists(alias) {
		return ErrAliasConflict
	}

	// Validate alias name
	if err := m.validateAliasName(alias); err != nil {
		return err
	}

	// Check if alias is already used by another database
	for _, dbInfo := range m.databases {
		for _, existingAlias := range dbInfo.Aliases {
			if existingAlias == alias {
				return ErrAliasExists
			}
		}
	}

	// Add alias
	if info.Aliases == nil {
		info.Aliases = []string{}
	}
	info.Aliases = append(info.Aliases, alias)
	info.UpdatedAt = time.Now()

	return m.persistMetadata()
}

// DropAlias removes an alias (Neo4j-compatible).
func (m *DatabaseManager) DropAlias(alias string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find database with this alias
	for _, info := range m.databases {
		for i, existingAlias := range info.Aliases {
			if existingAlias == alias {
				// Remove alias
				info.Aliases = append(info.Aliases[:i], info.Aliases[i+1:]...)
				info.UpdatedAt = time.Now()
				return m.persistMetadata()
			}
		}
	}

	return ErrAliasNotFound
}

// ListAliases returns all aliases for a database, or all aliases if database is empty.
func (m *DatabaseManager) ListAliases(databaseName string) map[string]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]string)

	if databaseName != "" {
		// List aliases for specific database
		if info, exists := m.databases[databaseName]; exists {
			for _, alias := range info.Aliases {
				result[alias] = databaseName
			}
		}
	} else {
		// List all aliases
		for dbName, info := range m.databases {
			for _, alias := range info.Aliases {
				result[alias] = dbName
			}
		}
	}

	return result
}

// SetDatabaseLimits sets resource limits for a database.
func (m *DatabaseManager) SetDatabaseLimits(databaseName string, limits *Limits) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, exists := m.databases[databaseName]
	if !exists {
		return ErrDatabaseNotFound
	}

	info.Limits = limits
	info.UpdatedAt = time.Now()

	return m.persistMetadata()
}

// GetDatabaseLimits returns resource limits for a database.
func (m *DatabaseManager) GetDatabaseLimits(databaseName string) (*Limits, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[databaseName]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	return info.Limits, nil
}

// validateAliasName validates an alias name.
func (m *DatabaseManager) validateAliasName(alias string) error {
	if alias == "" {
		return ErrInvalidAliasName
	}

	// Alias cannot contain whitespace
	if strings.ContainsAny(alias, " \t\n\r") {
		return fmt.Errorf("%w: '%s' (cannot contain whitespace)", ErrInvalidAliasName, alias)
	}

	// Alias cannot be reserved names
	reserved := []string{"system", m.config.DefaultDatabase}
	for _, reservedName := range reserved {
		if alias == reservedName {
			return fmt.Errorf("%w: '%s' (reserved name)", ErrInvalidAliasName, alias)
		}
	}

	return nil
}
