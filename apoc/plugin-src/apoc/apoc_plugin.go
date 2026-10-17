//go:build ignore

// APOC Plugin - Full APOC functions for NornicDB
// This is the main APOC plugin containing all 60+ functions.
// Build with: go build -buildmode=plugin -o apoc.so apoc_plugin.go
package main

import (
	"github.com/orneryd/nornicdb/apoc"
	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/auth"
)

// PluginFunction matches the struct expected by the plugin loader
type PluginFunction struct {
	Privilege   auth.OperationClass
	Handler     interface{}
	Description string
	Examples    []string
}

// APOCPlugin provides all APOC functions
type APOCPlugin struct{}

func (p APOCPlugin) Name() string    { return "apoc" }
func (p APOCPlugin) Version() string { return "1.0.0" }
func (p APOCPlugin) Type() string    { return "function" }

func (p APOCPlugin) Functions() map[string]PluginFunction {
	// Initialize APOC with nil storage (functions don't need storage)
	apoc.Initialize(storage.NewInMemoryStorage(), nil)

	// Get all registered functions from APOC
	functions := make(map[string]PluginFunction)

	for _, name := range apoc.List() {
		if isAPOCProcedureName(name) {
			continue
		}
		info, ok := apoc.GetInfo(name)
		if !ok {
			continue
		}

		// Strip the "apoc." prefix for the function key since
		// the plugin loader will add "apoc.<pluginname>." prefix
		// Actually, keep the full name for direct mapping
		functions[name] = PluginFunction{
			Privilege:   info.Privilege,
			Handler:     info.Handler,
			Description: info.Description,
			Examples:    info.Examples,
		}
	}

	return functions
}

// Plugin is the exported symbol that NornicDB will load
var Plugin APOCPlugin

// isAPOCProcedureName excludes confirmed procedure-only names from the scalar
// function plugin. This is not a declaration that their CALL implementation
// exists: see docs/features/apoc-procedure-capabilities.md for the audit.
// Sources: APOC Core 5.26.30 SHOW PROCEDURES and Extended 5.26.4 @Procedure
// declarations. The three historical mixed-case aliases are procedure aliases,
// not additional functions.
func isAPOCProcedureName(name string) bool {
	switch name {
	case "apoc.algo.aStar":
		return true
	case "apoc.algo.cover":
		return true
	case "apoc.algo.dijkstra":
		return true
	case "apoc.atomic.add":
		return true
	case "apoc.atomic.concat":
		return true
	case "apoc.atomic.insert":
		return true
	case "apoc.atomic.remove":
		return true
	case "apoc.atomic.subtract":
		return true
	case "apoc.atomic.update":
		return true
	case "apoc.coll.split":
		return true
	case "apoc.convert.setJsonProperty":
		return true
	case "apoc.convert.toTree":
		return true
	case "apoc.create.addLabels":
		return true
	case "apoc.create.node":
		return true
	case "apoc.create.nodes":
		return true
	case "apoc.create.relationship":
		return true
	case "apoc.create.removeLabels":
		return true
	case "apoc.create.removeProperties":
		return true
	case "apoc.create.removeRelProperties":
		return true
	case "apoc.create.setProperties":
		return true
	case "apoc.create.setProperty":
		return true
	case "apoc.create.setRelProperties":
		return true
	case "apoc.create.setRelProperty":
		return true
	case "apoc.create.uuids":
		return true
	case "apoc.create.vNodes":
		return true
	case "apoc.cypher.doIt":
		return true
	case "apoc.cypher.mapParallel":
		return true
	case "apoc.cypher.parallel":
		return true
	case "apoc.cypher.run":
		return true
	case "apoc.cypher.runFile":
		return true
	case "apoc.cypher.runMany":
		return true
	case "apoc.graph.from":
		return true
	case "apoc.graph.fromCypher":
		return true
	case "apoc.graph.fromData":
		return true
	case "apoc.graph.fromDocument":
		return true
	case "apoc.graph.fromPath":
		return true
	case "apoc.graph.fromPaths":
		return true
	case "apoc.import.csv":
		return true
	case "apoc.import.graphML":
		return true
	case "apoc.import.json":
		return true
	case "apoc.load.csv":
		return true
	case "apoc.load.directory":
		return true
	case "apoc.load.driver":
		return true
	case "apoc.load.html":
		return true
	case "apoc.load.jdbc":
		return true
	case "apoc.load.jdbcUpdate":
		return true
	case "apoc.load.json":
		return true
	case "apoc.load.jsonArray":
		return true
	case "apoc.load.jsonParams":
		return true
	case "apoc.load.ldap":
		return true
	case "apoc.load.parquet":
		return true
	case "apoc.load.xml":
		return true
	case "apoc.lock.all":
		return true
	case "apoc.lock.nodes":
		return true
	case "apoc.log.debug":
		return true
	case "apoc.log.error":
		return true
	case "apoc.log.info":
		return true
	case "apoc.log.stream":
		return true
	case "apoc.log.warn":
		return true
	case "apoc.meta.data":
		return true
	case "apoc.meta.graph":
		return true
	case "apoc.meta.graphSample":
		return true
	case "apoc.meta.nodeTypeProperties":
		return true
	case "apoc.meta.relTypeProperties":
		return true
	case "apoc.meta.schema":
		return true
	case "apoc.meta.stats":
		return true
	case "apoc.meta.subGraph":
		return true
	case "apoc.neighbors.atHop":
		return true
	case "apoc.neighbors.toHop":
		return true
	case "apoc.nodes.collapse":
		return true
	case "apoc.nodes.cycles":
		return true
	case "apoc.nodes.delete":
		return true
	case "apoc.nodes.get":
		return true
	case "apoc.nodes.group":
		return true
	case "apoc.nodes.link":
		return true
	case "apoc.path.expandConfig":
		return true
	case "apoc.path.spanningTree":
		return true
	case "apoc.path.subgraphAll":
		return true
	case "apoc.path.subgraphNodes":
		return true
	case "apoc.periodic.cancel":
		return true
	case "apoc.periodic.commit":
		return true
	case "apoc.periodic.countdown":
		return true
	case "apoc.periodic.iterate":
		return true
	case "apoc.periodic.list":
		return true
	case "apoc.periodic.repeat":
		return true
	case "apoc.periodic.submit":
		return true
	case "apoc.periodic.truncate":
		return true
	case "apoc.refactor.cloneNodes":
		return true
	case "apoc.refactor.cloneSubgraph":
		return true
	case "apoc.refactor.cloneSubgraphFromPaths":
		return true
	case "apoc.refactor.collapseNode":
		return true
	case "apoc.refactor.deleteAndReconnect":
		return true
	case "apoc.refactor.extractNode":
		return true
	case "apoc.refactor.from":
		return true
	case "apoc.refactor.mergeNodes":
		return true
	case "apoc.refactor.mergeRelationships":
		return true
	case "apoc.refactor.normalizeAsBoolean":
		return true
	case "apoc.refactor.setType":
		return true
	case "apoc.schema.assert":
		return true
	case "apoc.schema.nodes":
		return true
	case "apoc.schema.relationships":
		return true
	case "apoc.search.node":
		return true
	case "apoc.search.nodeAll":
		return true
	case "apoc.search.nodeReduced":
		return true
	case "apoc.stats.degrees":
		return true
	case "apoc.text.phoneticDelta":
		return true
	case "apoc.trigger.add":
		return true
	case "apoc.trigger.drop":
		return true
	case "apoc.trigger.install":
		return true
	case "apoc.trigger.list":
		return true
	case "apoc.trigger.pause":
		return true
	case "apoc.trigger.remove":
		return true
	case "apoc.trigger.removeAll":
		return true
	case "apoc.trigger.resume":
		return true
	case "apoc.trigger.show":
		return true
	case "apoc.util.sleep":
		return true
	case "apoc.util.validate":
		return true
	case "apoc.warmup.run":
		return true

	}
	return false
}
