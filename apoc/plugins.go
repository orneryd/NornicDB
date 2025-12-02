// Package apoc plugin loading support
package apoc

import (
	"fmt"
	"path/filepath"
	"plugin"
	"sync"
)

// PluginInterface defines the interface that external plugins must implement.
type PluginInterface interface {
	// Name returns the plugin name (e.g., "ml", "kafka")
	Name() string
	
	// Version returns the plugin version
	Version() string
	
	// Functions returns a map of function name to handler
	Functions() map[string]PluginFunction
}

// PluginFunction represents a function provided by a plugin.
type PluginFunction struct {
	Handler     interface{}
	Description string
	Examples    []string
}

var (
	loadedPlugins map[string]*LoadedPlugin
	pluginMu      sync.RWMutex
)

// LoadedPlugin represents a loaded plugin.
type LoadedPlugin struct {
	Name      string
	Version   string
	Plugin    PluginInterface
	Functions []string
}

func init() {
	loadedPlugins = make(map[string]*LoadedPlugin)
}

// LoadPlugin loads a Go plugin from a .so file.
//
// Example:
//   err := apoc.LoadPlugin("./plugins/apoc-ml.so")
func LoadPlugin(path string) error {
	pluginMu.Lock()
	defer pluginMu.Unlock()
	
	// Open the plugin
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open plugin %s: %w", path, err)
	}
	
	// Look up the exported symbol
	symPlugin, err := p.Lookup("Plugin")
	if err != nil {
		return fmt.Errorf("plugin %s does not export 'Plugin': %w", path, err)
	}
	
	// Assert that it implements PluginInterface
	pluginImpl, ok := symPlugin.(PluginInterface)
	if !ok {
		return fmt.Errorf("plugin %s does not implement PluginInterface", path)
	}
	
	name := pluginImpl.Name()
	
	// Check if already loaded
	if _, exists := loadedPlugins[name]; exists {
		return fmt.Errorf("plugin %s already loaded", name)
	}
	
	// Register all functions from the plugin
	functions := pluginImpl.Functions()
	functionNames := make([]string, 0, len(functions))
	
	mu.Lock()
	for funcName, pluginFunc := range functions {
		fullName := fmt.Sprintf("apoc.%s.%s", name, funcName)
		register(fullName, name, pluginFunc.Handler, pluginFunc.Description, pluginFunc.Examples)
		functionNames = append(functionNames, fullName)
	}
	mu.Unlock()
	
	// Track loaded plugin
	loadedPlugins[name] = &LoadedPlugin{
		Name:      name,
		Version:   pluginImpl.Version(),
		Plugin:    pluginImpl,
		Functions: functionNames,
	}
	
	return nil
}

// LoadPluginsFromDir loads all .so files from a directory.
//
// Example:
//   err := apoc.LoadPluginsFromDir("./plugins")
func LoadPluginsFromDir(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, "*.so"))
	if err != nil {
		return err
	}
	
	var errors []error
	for _, path := range matches {
		if err := LoadPlugin(path); err != nil {
			errors = append(errors, err)
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("failed to load %d plugins: %v", len(errors), errors)
	}
	
	return nil
}

// UnloadPlugin unloads a plugin and unregisters its functions.
//
// Note: Go plugins cannot be truly unloaded due to Go runtime limitations.
// This function removes the plugin from the registry but the .so remains in memory.
func UnloadPlugin(name string) error {
	pluginMu.Lock()
	defer pluginMu.Unlock()
	
	loaded, exists := loadedPlugins[name]
	if !exists {
		return fmt.Errorf("plugin %s not loaded", name)
	}
	
	// Unregister all functions
	mu.Lock()
	for _, funcName := range loaded.Functions {
		delete(functions, funcName)
	}
	mu.Unlock()
	
	delete(loadedPlugins, name)
	return nil
}

// ListPlugins returns information about all loaded plugins.
func ListPlugins() []*LoadedPlugin {
	pluginMu.RLock()
	defer pluginMu.RUnlock()
	
	result := make([]*LoadedPlugin, 0, len(loadedPlugins))
	for _, p := range loadedPlugins {
		result = append(result, p)
	}
	return result
}

// GetPlugin returns information about a specific loaded plugin.
func GetPlugin(name string) (*LoadedPlugin, bool) {
	pluginMu.RLock()
	defer pluginMu.RUnlock()
	
	p, exists := loadedPlugins[name]
	return p, exists
}

// IsPluginLoaded checks if a plugin is loaded.
func IsPluginLoaded(name string) bool {
	pluginMu.RLock()
	defer pluginMu.RUnlock()
	
	_, exists := loadedPlugins[name]
	return exists
}
