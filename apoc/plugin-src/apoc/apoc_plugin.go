//go:build ignore

// APOC Plugin - Full APOC functions for NornicDB
// This is the main APOC plugin containing all 60+ functions.
// Build with: go build -buildmode=plugin -o apoc.so apoc_plugin.go
package main

import (
	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/apoc/storage"
)

// PluginFunction matches the struct expected by the plugin loader
type PluginFunction struct {
	Handler     *registry.Callable
	Description string
	Examples    []string
}

// APOCPlugin provides all APOC functions
type APOCPlugin struct{}

func (p APOCPlugin) Name() string    { return "apoc" }
func (p APOCPlugin) Version() string { return "1.0.0" }
func (p APOCPlugin) Type() string    { return "function" }

// Author: Codex NOR-184 lane. Failed admission prevents plugin publication.
func (p APOCPlugin) Functions() (map[string]PluginFunction, error) {
	// Initialize APOC with nil storage (functions don't need storage)
	if err := apoc.Initialize(storage.NewInMemoryStorage(), nil); err != nil {
		return nil, err
	}

	// The registered functions, without APOC's procedures and without the
	// functions the query engine has built in (apoc.PluginFunctionNames).
	functions := make(map[string]PluginFunction)

	for _, name := range apoc.PluginFunctionNames() {
		info, ok := apoc.GetInfo(name)
		if !ok {
			continue
		}

		// Strip the "apoc." prefix for the function key since
		// the plugin loader will add "apoc.<pluginname>." prefix
		// Actually, keep the full name for direct mapping
		functions[name] = PluginFunction{
			Handler:     info.Handler,
			Description: info.Description,
			Examples:    info.Examples,
		}
	}

	return functions, nil
}

// Plugin is the exported symbol that NornicDB will load
var Plugin APOCPlugin
