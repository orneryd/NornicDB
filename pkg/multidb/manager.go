// Package multidb provides multi-database support for NornicDB.
//
// This package implements Neo4j 4.x-style multi-database support, allowing
// multiple logical databases (tenants) to share a single physical storage backend
// while maintaining complete data isolation.
package multidb

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
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

	// Cached database-scoped engines (avoid recreating)
	engines map[string]storage.Engine

	// Factory used to create storage engines for remote constituents.
	remoteEngineFactory RemoteEngineFactory

	// Optional encryptor for remote constituent credentials persisted in metadata.
	remoteCredentialCipher *remoteCredentialCipher
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

	// Size tracking (incremental, not recalculated)
	totalSize       int64        // Total storage size in bytes
	nodeSize        int64        // Total size of all nodes in bytes
	edgeSize        int64        // Total size of all edges in bytes
	sizeInitialized bool         // Whether size has been calculated at least once
	sizeMu          sync.RWMutex // Protects size tracking
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

	// RemoteEngineFactory creates a storage engine for a remote constituent.
	// If nil, remote constituents are not executable (metadata may still be stored).
	RemoteEngineFactory RemoteEngineFactory

	// RemoteCredentialEncryptionKey encrypts remote constituent user/password values
	// before metadata persistence. If empty, user_password auth mode is rejected.
	RemoteCredentialEncryptionKey string
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
	// DatabaseManager requires an un-namespaced base engine. It creates NamespacedEngines
	// per database. Passing a NamespacedEngine here would double-prefix IDs and can
	// leak system metadata into the default database (e.g., "nornic:system:...").
	if _, ok := inner.(*storage.NamespacedEngine); ok {
		return nil, fmt.Errorf("multidb: NewDatabaseManager requires base storage (non-namespaced); pass db.GetBaseStorageForManager()")
	}
	// Ensure callers can't accidentally make the system database the default.
	// Neo4j reserves the system database for metadata and administration commands.
	if config.SystemDatabase == "" {
		config.SystemDatabase = "system"
	}
	if config.DefaultDatabase == "" {
		config.DefaultDatabase = "nornic"
	}
	if config.DefaultDatabase == config.SystemDatabase {
		log.Printf("⚠️  multidb: default database %q matches system database; forcing default to %q", config.DefaultDatabase, "nornic")
		config.DefaultDatabase = "nornic"
	}

	m := &DatabaseManager{
		inner:               inner,
		databases:           make(map[string]*DatabaseInfo),
		config:              config,
		engines:             make(map[string]storage.Engine),
		remoteEngineFactory: config.RemoteEngineFactory,
	}
	if key := strings.TrimSpace(config.RemoteCredentialEncryptionKey); key != "" {
		cipher, err := newRemoteCredentialCipher(key)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize remote credential cipher: %w", err)
		}
		m.remoteCredentialCipher = cipher
	}

	// Load existing databases from system namespace
	if err := m.loadMetadata(); err != nil {
		return nil, fmt.Errorf("failed to load database metadata: %w", err)
	}

	// If the underlying engine is read-only (e.g. HA standby), do not attempt
	// to create/migrate metadata. Standby nodes should start quickly and serve
	// reads/UI while replication catches them up.
	readOnly := false
	if leader, ok := inner.(interface{ IsLeader() bool }); ok && !leader.IsLeader() {
		readOnly = true
	}

	// Ensure system and default databases exist (in-memory always; persisted only if writable).
	if readOnly {
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
		log.Printf("ℹ️  multidb: storage is read-only; skipping metadata writes and migrations")
		return m, nil
	}

	if err := m.ensureSystemDatabases(); err != nil {
		return nil, err
	}

	// One-time migration: move any pre-multi-db (unprefixed) data into the default
	// database namespace so the rest of the system can remain strictly namespaced.
	if err := m.migrateLegacyData(); err != nil {
		return nil, fmt.Errorf("failed to migrate legacy data: %w", err)
	}

	// Cleanup: if a previous run mistakenly constructed DatabaseManager with a
	// NamespacedEngine, system metadata nodes may have been stored under the default
	// namespace as "defaultDb:system:<...>". Remove those leaked nodes so normal
	// queries against the default database don't show system internals.
	m.cleanupLeakedSystemNodes()

	return m, nil
}

func (m *DatabaseManager) cleanupLeakedSystemNodes() {
	systemPrefix := m.config.SystemDatabase + ":"
	if m.config.SystemDatabase == "" {
		systemPrefix = "system:"
	}

	// Only target known system-internal nodes (metadata, migration, users) that
	// should never live in user databases.
	isLeak := func(node *storage.Node) bool {
		if node == nil {
			return false
		}
		id := string(node.ID)
		if !strings.HasPrefix(id, systemPrefix) {
			return false
		}
		hasSystemLabel := false
		for _, label := range node.Labels {
			if label == "_System" {
				hasSystemLabel = true
				break
			}
		}
		if !hasSystemLabel {
			return false
		}
		// Restrict deletions to well-known internal namespaces.
		return id == systemPrefix+"databases:metadata" ||
			strings.HasPrefix(id, systemPrefix+"migration:") ||
			strings.HasPrefix(id, systemPrefix+"user:")
	}

	removed := 0
	for dbName, info := range m.databases {
		if info == nil {
			continue
		}
		if dbName == m.config.SystemDatabase {
			continue
		}
		engine := storage.NewNamespacedEngine(m.inner, dbName)
		nodes, err := engine.AllNodes()
		if err != nil {
			continue
		}
		for _, node := range nodes {
			if !isLeak(node) {
				continue
			}
			if err := engine.DeleteNode(node.ID); err == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		log.Printf("🧹 multidb: removed %d leaked system nodes from user databases", removed)
	}
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
		return localizedError(localization.MultidbManagerDeleteDatabaseDataFailed(err), err)
	}

	// Update metadata with deletion info (for logging/debugging)
	_ = nodesDeleted
	_ = edgesDeleted

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

// GetStorage returns a namespaced storage engine for the specified database.
//
// The returned engine is scoped to the database - all operations only
// affect data within that namespace.
func (m *DatabaseManager) GetStorage(name string) (storage.Engine, error) {
	return m.GetStorageWithAuth(name, "")
}

// GetStorageWithAuth returns a storage engine for the specified database and forwards
// authToken to remote constituent factories when composite databases include remotes.
func (m *DatabaseManager) GetStorageWithAuth(name string, authToken string) (storage.Engine, error) {
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

	// Resolve dotted composite constituent references first (e.g. "cmp.tr").
	// This is required for protocol-level USE routing where the effective database
	// name is a constituent graph reference, not a top-level database metadata key.
	if ref, resolvedName, ok := m.resolveCompositeConstituentInternal(name); ok {
		if strings.EqualFold(strings.TrimSpace(ref.Type), "remote") {
			runtimeRef := ref
			if strings.EqualFold(strings.TrimSpace(runtimeRef.AuthMode), "user_password") {
				decrypted, decErr := m.decryptStoredRemotePassword(runtimeRef.Password)
				if decErr != nil {
					return nil, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(runtimeRef.Alias, decErr), decErr)
				}
				runtimeRef.Password = decrypted
			}
			return m.getRemoteStorageInternal(runtimeRef, authToken)
		}
		return m.getStorageInternal(resolvedName)
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
			var constituentStorage storage.Engine
			runtimeRef := ref
			actualName := ref.DatabaseName
			var err error

			switch ref.Type {
			case "remote":
				if strings.EqualFold(strings.TrimSpace(runtimeRef.AuthMode), "user_password") {
					decrypted, decErr := m.decryptStoredRemotePassword(runtimeRef.Password)
					if decErr != nil {
						return nil, localizedError(localization.MultidbManagerResolveRemoteCredentialsFailed(runtimeRef.Alias, decErr), decErr)
					}
					runtimeRef.Password = decrypted
				}
				constituentStorage, err = m.getRemoteStorageInternal(runtimeRef, authToken)
				if err != nil {
					return nil, localizedError(localization.MultidbManagerGetRemoteStorageFailed(ref.Alias, err), err)
				}
			default:
				// Resolve actual database name (might be an alias)
				actualName, err = m.resolveDatabaseInternal(ref.DatabaseName)
				if err != nil {
					return nil, localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)
				}

				// Get storage for constituent
				constituentStorage, err = m.getStorageInternal(actualName)
				if err != nil {
					return nil, localizedError(localization.MultidbManagerGetConstituentStorageFailed(ref.DatabaseName, err), err)
				}
			}

			constituents[ref.Alias] = constituentStorage
			constituentNames[ref.Alias] = actualName
			accessModes[ref.Alias] = ref.AccessMode
		}

		// Create composite engine with intelligent default routing
		// Routing will be auto-configured based on constituent aliases and access modes
		compositeEngine := storage.NewCompositeEngine(constituents, constituentNames, accessModes)
		// Note: We don't cache composite engines the same way as they're lightweight wrappers
		return compositeEngine, nil
	}

	// Create namespaced engine for standard databases
	// Note: Limit enforcement is handled separately via LimitChecker
	// which is created on-demand when needed (not stored here)
	baseEngine := storage.NewNamespacedEngine(m.inner, name)
	engine := newSizeTrackingEngine(baseEngine, m, name)
	m.engines[name] = engine

	return engine, nil
}

// resolveCompositeConstituentInternal resolves dotted graph references in the form
// "<composite>.<alias>" to a constituent reference and resolved backing database name.
// Must be called with lock held.
func (m *DatabaseManager) resolveCompositeConstituentInternal(name string) (ConstituentRef, string, bool) {
	dotIdx := strings.IndexByte(name, '.')
	if dotIdx <= 0 || dotIdx >= len(name)-1 {
		return ConstituentRef{}, "", false
	}

	compositeName := strings.TrimSpace(name[:dotIdx])
	constituentAlias := strings.TrimSpace(name[dotIdx+1:])
	if compositeName == "" || constituentAlias == "" {
		return ConstituentRef{}, "", false
	}

	info, exists := m.databases[compositeName]
	if !exists || info.Type != "composite" {
		return ConstituentRef{}, "", false
	}
	for _, ref := range info.Constituents {
		if !strings.EqualFold(ref.Alias, constituentAlias) {
			continue
		}
		resolvedName := ref.DatabaseName
		if !strings.EqualFold(strings.TrimSpace(ref.Type), "remote") {
			var err error
			resolvedName, err = m.resolveDatabaseInternal(ref.DatabaseName)
			if err != nil {
				return ConstituentRef{}, "", false
			}
		}
		return ref, resolvedName, true
	}
	return ConstituentRef{}, "", false
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

	// Create namespaced engine with storage-size tracking.
	baseEngine := storage.NewNamespacedEngine(m.inner, name)
	engine := newSizeTrackingEngine(baseEngine, m, name)
	m.engines[name] = engine

	return engine, nil
}

// getRemoteStorageInternal creates an engine for a remote constituent.
// Must be called with lock held.
func (m *DatabaseManager) getRemoteStorageInternal(ref ConstituentRef, authToken string) (storage.Engine, error) {
	if m.remoteEngineFactory == nil {
		return nil, localizedError(localization.MultidbManagerRemoteEngineFactoryNotConfigured(ref.Alias), nil)
	}
	engine, err := m.remoteEngineFactory(ref, authToken)
	if err != nil {
		return nil, err
	}
	if engine == nil {
		return nil, localizedError(localization.MultidbManagerRemoteEngineFactoryReturnedNil(ref.Alias), nil)
	}
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
		result = append(result, cloneDatabaseInfo(info))
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

	return cloneDatabaseInfo(info), nil
}

// cloneDatabaseInfo returns a deep-enough, lock-safe snapshot of DatabaseInfo.
// Caller must hold m.mu (read or write) before invoking.
func cloneDatabaseInfo(info *DatabaseInfo) *DatabaseInfo {
	var limitsCopy *Limits
	if info.Limits != nil {
		lc := *info.Limits
		limitsCopy = &lc
	}

	aliases := append([]string(nil), info.Aliases...)
	constituents := append([]ConstituentRef(nil), info.Constituents...)

	info.sizeMu.RLock()
	totalSize := info.totalSize
	nodeSize := info.nodeSize
	edgeSize := info.edgeSize
	sizeInitialized := info.sizeInitialized
	info.sizeMu.RUnlock()

	return &DatabaseInfo{
		Name:            info.Name,
		CreatedAt:       info.CreatedAt,
		CreatedBy:       info.CreatedBy,
		Status:          info.Status,
		Type:            info.Type,
		IsDefault:       info.IsDefault,
		NodeCount:       info.NodeCount,
		UpdatedAt:       info.UpdatedAt,
		Aliases:         aliases,
		Limits:          limitsCopy,
		Constituents:    constituents,
		totalSize:       totalSize,
		nodeSize:        nodeSize,
		edgeSize:        edgeSize,
		sizeInitialized: sizeInitialized,
	}
}

// Exists checks if a database exists.
func (m *DatabaseManager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.databases[name] != nil
}

// DefaultDatabaseName returns the default database name.
func (m *DatabaseManager) DefaultDatabaseName() string {
	// Never advertise the system database as the default.
	if m.config.DefaultDatabase == m.config.SystemDatabase {
		return "nornic"
	}
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
		return localizedError(localization.MultidbManagerInvalidStatus(status), nil)
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
	m.engines = make(map[string]storage.Engine)

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
	// NOTE: do NOT call m.Exists() here — we already hold m.mu.Lock() and
	// Exists() acquires m.mu.RLock(), which deadlocks on a non-re-entrant RWMutex.
	if m.databases[alias] != nil {
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
		return localizedError(localization.MultidbManagerAliasContainsWhitespace(alias, ErrInvalidAliasName), ErrInvalidAliasName)
	}

	// Alias cannot be reserved names
	reserved := []string{"system", m.config.DefaultDatabase}
	for _, reservedName := range reserved {
		if alias == reservedName {
			return localizedError(localization.MultidbManagerAliasReserved(alias, ErrInvalidAliasName), ErrInvalidAliasName)
		}
	}

	return nil
}

// IncrementStorageSize increments the tracked storage size for a database.
//
// This should be called after successful node/edge creation to maintain accurate
// size tracking for MaxBytes limit enforcement. The sizes should be calculated
// using the same gob encoding used by the storage engine.
//
// Parameters:
//   - databaseName: The database to update
//   - nodeSize: Size in bytes of the node that was created (0 if no node created)
//   - edgeSize: Size in bytes of the edge that was created (0 if no edge created)
//
// Example:
//
//	// After successfully creating a node
//	nodeSize, _ := calculateNodeSize(node)
//	manager.IncrementStorageSize("tenant_a", nodeSize, 0)
//
//	// After successfully creating an edge
//	edgeSize, _ := calculateEdgeSize(edge)
//	manager.IncrementStorageSize("tenant_a", 0, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
func (m *DatabaseManager) IncrementStorageSize(databaseName string, nodeSize, edgeSize int64) {
	m.applyStorageSizeDelta(databaseName, nodeSize, edgeSize)
}

// DecrementStorageSize decrements the tracked storage size for a database.
//
// This should be called after successful node/edge deletion to maintain accurate
// size tracking for MaxBytes limit enforcement. The sizes should be the same
// values that were used when the entities were created.
//
// Parameters:
//   - databaseName: The database to update
//   - nodeSize: Size in bytes of the node that was deleted (0 if no node deleted)
//   - edgeSize: Size in bytes of the edge that was deleted (0 if no edge deleted)
//
// Example:
//
//	// After successfully deleting a node (size known from creation)
//	manager.DecrementStorageSize("tenant_a", nodeSize, 0)
//
//	// After successfully deleting an edge (size known from creation)
//	manager.DecrementStorageSize("tenant_a", 0, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
// Defensive: Size is prevented from going negative (resets to 0 if underflow).
func (m *DatabaseManager) DecrementStorageSize(databaseName string, nodeSize, edgeSize int64) {
	m.applyStorageSizeDelta(databaseName, -nodeSize, -edgeSize)
}

// GetStorageSize returns the current tracked storage size for a database.
//
// Returns:
//   - totalSize: Total storage size in bytes (sum of all nodes and edges)
//   - nodeSize: Total size of all nodes in bytes
//   - edgeSize: Total size of all edges in bytes
//
// The size is tracked incrementally and initialized lazily on first access.
// This provides O(1) access for limit checking without recalculating from all entities.
//
// Example:
//
//	totalSize, nodeSize, edgeSize := manager.GetStorageSize("tenant_a")
//	fmt.Printf("Database uses %d bytes (%d from nodes, %d from edges)\n",
//		totalSize, nodeSize, edgeSize)
//
// Thread-safe: This method is safe to call from multiple goroutines.
func (m *DatabaseManager) GetStorageSize(databaseName string) (int64, int64, int64) {
	m.mu.RLock()
	info, exists := m.databases[databaseName]
	var engine storage.Engine
	if exists {
		if cached, ok := m.engines[databaseName]; ok {
			engine = cached
		} else {
			engine = storage.NewNamespacedEngine(m.inner, databaseName)
		}
	}
	m.mu.RUnlock()

	if !exists {
		return 0, 0, 0
	}

	_ = m.ensureStorageSizeInitialized(databaseName, engine)

	info.sizeMu.RLock()
	defer info.sizeMu.RUnlock()
	return info.totalSize, info.nodeSize, info.edgeSize
}
