// Package apoc provides a convenient loader for all APOC plugins.
package apoc

import (
	"github.com/orneryd/nornicdb/apoc/plugin"
	"github.com/orneryd/nornicdb/apoc/plugins"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
)

// LoadAll loads all available APOC plugins.
//
// Example:
//
//	store := storage.NewInMemoryStorage()
//	manager, err := apoc.LoadAll(store)
func LoadAll(store storage.Storage) (*plugin.PluginManager, error) {
	manager := plugin.NewPluginManager(store)

	// Load all plugins
	allPlugins := []plugin.Plugin{
		plugins.NewCollPlugin(),
		plugins.NewTextPlugin(),
		// Add more plugins here as they're created
	}

	return manager, manager.LoadAll(allPlugins)
}

// LoadSelective loads only specified plugins.
//
// Example:
//
//	store := storage.NewInMemoryStorage()
//	manager, err := apoc.LoadSelective(store, []string{"coll", "text"})
func LoadSelective(store storage.Storage, pluginNames []string) (*plugin.PluginManager, error) {
	manager := plugin.NewPluginManager(store)

	availablePlugins := map[string]plugin.Plugin{
		"coll": plugins.NewCollPlugin(),
		"text": plugins.NewTextPlugin(),
		// Add more plugins here
	}

	for _, name := range pluginNames {
		if p, ok := availablePlugins[name]; ok {
			if err := manager.Load(p); err != nil {
				return manager, err
			}
		}
	}

	return manager, nil
}

// Note: Call function is defined in apoc.go with more complete implementation
// (includes config checking). Use apoc.Call() directly.

// ListFunctions returns all registered function names.
func ListFunctions() []string {
	return registry.GetGlobalRegistry().List()
}

// ListCategories returns all registered categories.
func ListCategories() []string {
	return registry.GetGlobalRegistry().Categories()
}

// GetFunctionInfo returns information about a specific function.
func GetFunctionInfo(name string) (*registry.FunctionDescriptor, bool) {
	return registry.GetGlobalRegistry().Get(name)
}
