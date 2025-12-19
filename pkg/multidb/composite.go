// Package multidb provides composite database support for multi-database functionality.
//
// Composite databases are virtual databases that span multiple physical databases,
// allowing queries to transparently access data from multiple constituent databases.
package multidb

import (
	"fmt"
	"time"
)

// ConstituentRef represents a reference to a constituent database within a composite database.
type ConstituentRef struct {
	// Alias is the name used within the composite database to reference this constituent.
	Alias string `json:"alias"`

	// DatabaseName is the actual database name (or alias) that this constituent points to.
	DatabaseName string `json:"database_name"`

	// Type is the type of constituent: "local" (same instance) or "remote" (future: different instance).
	Type string `json:"type"` // "local", "remote"

	// AccessMode controls what operations are allowed: "read", "write", "read_write".
	AccessMode string `json:"access_mode"` // "read", "write", "read_write"
}

// Validate validates a constituent reference.
func (c *ConstituentRef) Validate() error {
	if c.Alias == "" {
		return fmt.Errorf("constituent alias cannot be empty")
	}
	if c.DatabaseName == "" {
		return fmt.Errorf("constituent database name cannot be empty")
	}
	if c.Type != "local" && c.Type != "remote" {
		return fmt.Errorf("constituent type must be 'local' or 'remote'")
	}
	if c.AccessMode != "read" && c.AccessMode != "write" && c.AccessMode != "read_write" {
		return fmt.Errorf("access mode must be 'read', 'write', or 'read_write'")
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
			return fmt.Errorf("invalid constituent at index %d: %w", i, err)
		}

		// Check if constituent database exists
		// Resolve alias if needed
		actualName, err := m.resolveDatabaseInternal(ref.DatabaseName)
		if err != nil {
			return fmt.Errorf("constituent database '%s' not found: %w", ref.DatabaseName, err)
		}

		// Cannot use composite database as constituent (prevent cycles)
		if info, exists := m.databases[actualName]; exists && info.Type == "composite" {
			return fmt.Errorf("cannot use composite database '%s' as constituent", actualName)
		}

		// For now, only support local databases
		if ref.Type != "local" {
			return fmt.Errorf("remote constituents not yet supported")
		}
	}

	// Check for duplicate aliases
	aliasMap := make(map[string]bool)
	for _, ref := range constituents {
		if aliasMap[ref.Alias] {
			return fmt.Errorf("duplicate constituent alias: '%s'", ref.Alias)
		}
		aliasMap[ref.Alias] = true
	}

	// Create composite database info
	m.databases[name] = &DatabaseInfo{
		Name:         name,
		CreatedAt:    time.Now(),
		Status:       "online",
		Type:         "composite",
		IsDefault:    false,
		UpdatedAt:    time.Now(),
		Constituents: constituents,
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
		return fmt.Errorf("database '%s' is not a composite database", name)
	}

	// Remove from metadata
	delete(m.databases, name)
	delete(m.engines, name) // Clear cached engine

	return m.persistMetadata()
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
		return fmt.Errorf("database '%s' is not a composite database", compositeName)
	}

	// Validate constituent
	if err := constituent.Validate(); err != nil {
		return err
	}

	// Check if constituent database exists
	_, err := m.resolveDatabaseInternal(constituent.DatabaseName)
	if err != nil {
		return fmt.Errorf("constituent database '%s' not found: %w", constituent.DatabaseName, err)
	}

	// Check for duplicate alias
	for _, existing := range info.Constituents {
		if existing.Alias == constituent.Alias {
			return fmt.Errorf("constituent alias '%s' already exists", constituent.Alias)
		}
	}

	// Add constituent
	info.Constituents = append(info.Constituents, constituent)
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
		return fmt.Errorf("database '%s' is not a composite database", compositeName)
	}

	// Find and remove constituent
	for i, ref := range info.Constituents {
		if ref.Alias == alias {
			info.Constituents = append(info.Constituents[:i], info.Constituents[i+1:]...)
			info.UpdatedAt = time.Now()
			return m.persistMetadata()
		}
	}

	return fmt.Errorf("constituent alias '%s' not found", alias)
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
		return nil, fmt.Errorf("database '%s' is not a composite database", compositeName)
	}

	// Return a copy
	result := make([]ConstituentRef, len(info.Constituents))
	copy(result, info.Constituents)
	return result, nil
}

// ListCompositeDatabases returns all composite databases.
func (m *DatabaseManager) ListCompositeDatabases() []*DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*DatabaseInfo
	for _, info := range m.databases {
		if info.Type == "composite" {
			// Return a copy
			infoCopy := *info
			result = append(result, &infoCopy)
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
