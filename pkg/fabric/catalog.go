package fabric

import (
	"fmt"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Catalog is the registry of all known graphs and their locations.
// It mirrors Neo4j's Catalog.scala and is populated from multidb.DatabaseManager.
//
// Graph names follow the pattern:
//   - "dbname" — a standard or composite database
//   - "composite.alias" — a constituent within a composite database
//
// Thread-safe: all operations are protected by RWMutex.
type Catalog struct {
	graphs map[string]Location
	mu     sync.RWMutex
}

// NewCatalog creates an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{
		graphs: make(map[string]Location),
	}
}

// Register adds or replaces a graph location in the catalog.
func (c *Catalog) Register(name string, loc Location) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.graphs[strings.ToLower(name)] = loc
}

// Unregister removes a graph from the catalog.
func (c *Catalog) Unregister(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.graphs, strings.ToLower(name))
}

// Resolve looks up a graph location by name.
// Returns an error if the graph is not registered.
func (c *Catalog) Resolve(name string) (Location, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	loc, ok := c.graphs[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("graph '%s' not found in fabric catalog", name)
	}
	return loc, nil
}

// ListGraphs returns all registered graph names.
func (c *Catalog) ListGraphs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.graphs))
	for name := range c.graphs {
		names = append(names, name)
	}
	return names
}

// HasGraphWithPrefix returns true when any registered graph name starts with prefix.
// Names are stored lowercased; callers should pass a lowercased prefix.
func (c *Catalog) HasGraphWithPrefix(prefix string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for name := range c.graphs {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// PopulateFromManager loads graph registrations from a DatabaseManager.
// It registers:
//   - each standard database as a LocationLocal
//   - each composite database as a LocationLocal
//   - each constituent of a composite database as "composite.alias"
//     with LocationLocal for local constituents or LocationRemote for remote ones
//
// Previously registered graphs are cleared and replaced.
func (c *Catalog) PopulateFromManager(mgr *multidb.DatabaseManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing registrations.
	c.graphs = make(map[string]Location)

	databases := mgr.ListDatabases()
	for _, db := range databases {
		dbName := strings.ToLower(db.Name)
		dbType := db.Type

		// Register the database itself.
		c.graphs[dbName] = &LocationLocal{DBName: dbName}

		// Register aliases.
		for _, alias := range db.Aliases {
			c.graphs[strings.ToLower(alias)] = &LocationLocal{DBName: dbName}
		}

		// Register constituents for composite databases.
		if dbType == "composite" {
			constituents, err := mgr.GetCompositeConstituents(db.Name)
			if err != nil {
				return fmt.Errorf("failed to load constituents for composite '%s': %w", db.Name, err)
			}
			for _, ref := range constituents {
				qualifiedName := dbName + "." + strings.ToLower(ref.Alias)
				switch ref.Type {
				case "remote":
					// Catalog lookup is case-insensitive, but the manager's
					// composite key retains its original spelling.
					managerName := db.Name + "." + ref.Alias
					c.graphs[qualifiedName] = &LocationRemote{
						GraphName: qualifiedName,
						DBName:    ref.DatabaseName,
						URI:       ref.URI,
						AuthMode:  effectiveAuthMode(ref.AuthMode),
						ResolveEngine: func(authToken string) (storage.Engine, error) {
							return mgr.GetStorageWithAuth(managerName, authToken)
						},
					}
				default:
					c.graphs[qualifiedName] = &LocationLocal{
						DBName: ref.DatabaseName,
					}
				}
			}
		}
	}

	return nil
}

// effectiveAuthMode returns the canonical auth mode, defaulting empty to "oidc_forwarding".
func effectiveAuthMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return "oidc_forwarding"
	}
	return mode
}
