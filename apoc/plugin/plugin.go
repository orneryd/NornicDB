// Package plugin provides a plugin system for loading APOC function packages.
//
// This package allows APOC function packages to be loaded/unloaded at runtime,
// enabling dynamic extension of Cypher query capabilities.
package plugin

import (
	"fmt"
	"sync"
	
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Plugin represents a loadable APOC function package.
type Plugin interface {
	// Name returns the plugin name (e.g., "coll", "text", "math")
	Name() string
	
	// Version returns the plugin version
	Version() string
	
	// Description returns a description of the plugin
	Description() string
	
	// Register registers all functions in this plugin
	Register(reg *registry.FunctionRegistry) error
	
	// Initialize initializes the plugin with storage
	Initialize(store storage.Storage) error
	
	// Cleanup cleans up plugin resources
	Cleanup() error
}

// PluginManager manages loaded plugins.
type PluginManager struct {
	plugins map[string]Plugin
	storage storage.Storage
	mu      sync.RWMutex
}

// NewPluginManager creates a new plugin manager.
func NewPluginManager(store storage.Storage) *PluginManager {
	return &PluginManager{
		plugins: make(map[string]Plugin),
		storage: store,
	}
}

// Load loads a plugin and registers its functions.
func (pm *PluginManager) Load(plugin Plugin) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	name := plugin.Name()
	
	if _, exists := pm.plugins[name]; exists {
		return fmt.Errorf("plugin %s already loaded", name)
	}
	
	// Initialize plugin with storage
	if err := plugin.Initialize(pm.storage); err != nil {
		return fmt.Errorf("failed to initialize plugin %s: %w", name, err)
	}
	
	// Register functions
	if err := plugin.Register(registry.GetGlobalRegistry()); err != nil {
		return fmt.Errorf("failed to register plugin %s: %w", name, err)
	}
	
	pm.plugins[name] = plugin
	return nil
}

// Unload unloads a plugin and unregisters its functions.
func (pm *PluginManager) Unload(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	plugin, exists := pm.plugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not loaded", name)
	}
	
	// Cleanup plugin
	if err := plugin.Cleanup(); err != nil {
		return fmt.Errorf("failed to cleanup plugin %s: %w", name, err)
	}
	
	// Unregister functions
	reg := registry.GetGlobalRegistry()
	for _, funcName := range reg.ListByCategory(name) {
		reg.Unregister(funcName)
	}
	
	delete(pm.plugins, name)
	return nil
}

// IsLoaded checks if a plugin is loaded.
func (pm *PluginManager) IsLoaded(name string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	_, exists := pm.plugins[name]
	return exists
}

// List returns all loaded plugin names.
func (pm *PluginManager) List() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	names := make([]string, 0, len(pm.plugins))
	for name := range pm.plugins {
		names = append(names, name)
	}
	return names
}

// Get retrieves a loaded plugin.
func (pm *PluginManager) Get(name string) (Plugin, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	plugin, exists := pm.plugins[name]
	return plugin, exists
}

// LoadAll loads multiple plugins.
func (pm *PluginManager) LoadAll(plugins []Plugin) error {
	for _, plugin := range plugins {
		if err := pm.Load(plugin); err != nil {
			return err
		}
	}
	return nil
}

// UnloadAll unloads all plugins.
func (pm *PluginManager) UnloadAll() error {
	pm.mu.Lock()
	names := make([]string, 0, len(pm.plugins))
	for name := range pm.plugins {
		names = append(names, name)
	}
	pm.mu.Unlock()
	
	for _, name := range names {
		if err := pm.Unload(name); err != nil {
			return err
		}
	}
	
	return nil
}

// PluginInfo returns information about a loaded plugin.
type PluginInfo struct {
	Name        string
	Version     string
	Description string
	Functions   []string
}

// Info returns information about a loaded plugin.
func (pm *PluginManager) Info(name string) (*PluginInfo, error) {
	pm.mu.RLock()
	plugin, exists := pm.plugins[name]
	pm.mu.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("plugin %s not loaded", name)
	}
	
	reg := registry.GetGlobalRegistry()
	functions := reg.ListByCategory(name)
	
	return &PluginInfo{
		Name:        plugin.Name(),
		Version:     plugin.Version(),
		Description: plugin.Description(),
		Functions:   functions,
	}, nil
}

// AllInfo returns information about all loaded plugins.
func (pm *PluginManager) AllInfo() []*PluginInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	infos := make([]*PluginInfo, 0, len(pm.plugins))
	for name := range pm.plugins {
		if info, err := pm.Info(name); err == nil {
			infos = append(infos, info)
		}
	}
	
	return infos
}

// Global plugin manager instance
var globalManager *PluginManager
var managerOnce sync.Once

// GetGlobalManager returns the global plugin manager.
func GetGlobalManager(store storage.Storage) *PluginManager {
	managerOnce.Do(func() {
		globalManager = NewPluginManager(store)
	})
	return globalManager
}
