package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

func (e *StorageExecutor) queryPropertyDecayEnabled() bool {
	be := unwrapBadgerEngine(e.storage)
	return be != nil && be.IsDecayEnabled()
}

// queryVisibleValue is a detached projection of entity properties. Internal
// row bindings and mutation owners retain full entities: filtering their maps
// in place would make an unrelated SET persist the removal of hidden fields.
func (e *StorageExecutor) queryVisibleValue(value interface{}) interface{} {
	be := unwrapBadgerEngine(e.storage)
	if be == nil || !be.IsDecayEnabled() {
		return value
	}
	return e.projectVisibleQueryValue(value)
}

func (e *StorageExecutor) projectVisibleQueryValue(value interface{}) interface{} {
	switch v := value.(type) {
	case *storage.Node:
		if v == nil {
			return v
		}
		out := *v
		out.Properties = e.decayVisibleNodeProperties(v)
		return &out
	case *storage.Edge:
		if v == nil {
			return v
		}
		out := *v
		out.Properties = e.decayVisibleEdgeProperties(v)
		return &out
	case *PathResult:
		if v == nil {
			return v
		}
		out := *v
		out.Nodes = make([]*storage.Node, len(v.Nodes))
		for i, n := range v.Nodes {
			out.Nodes[i] = e.projectVisibleQueryValue(n).(*storage.Node)
		}
		out.Relationships = make([]*storage.Edge, len(v.Relationships))
		for i, r := range v.Relationships {
			out.Relationships[i] = e.projectVisibleQueryValue(r).(*storage.Edge)
		}
		return &out
	case []interface{}:
		if v == nil {
			return v
		}
		out := make([]interface{}, len(v))
		for i, x := range v {
			out[i] = e.projectVisibleQueryValue(x)
		}
		return out
	case []*storage.Node:
		if v == nil {
			return v
		}
		out := make([]*storage.Node, len(v))
		for i, n := range v {
			out[i] = e.projectVisibleQueryValue(n).(*storage.Node)
		}
		return out
	case []*storage.Edge:
		if v == nil {
			return v
		}
		out := make([]*storage.Edge, len(v))
		for i, r := range v {
			out[i] = e.projectVisibleQueryValue(r).(*storage.Edge)
		}
		return out
	case map[string]interface{}:
		if v == nil {
			return v
		}
		out := make(map[string]interface{}, len(v))
		for k, x := range v {
			out[k] = e.projectVisibleQueryValue(x)
		}
		return out
	}
	return value
}

func (e *StorageExecutor) queryEntityProperties(value interface{}) map[string]interface{} {
	switch v := e.queryVisibleValue(value).(type) {
	case *storage.Node:
		if v != nil {
			return v.Properties
		}
	case *storage.Edge:
		if v != nil {
			return v.Properties
		}
	}
	return nil
}

func (e *StorageExecutor) projectQueryResultProperties(result *ExecuteResult) {
	be := unwrapBadgerEngine(e.storage)
	if result == nil || be == nil || !be.IsDecayEnabled() {
		return
	}
	for i, row := range result.Rows {
		result.Rows[i] = e.projectVisibleQueryValue(row).([]interface{})
	}
}

func (e *StorageExecutor) projectVisiblePipelineNodeRead(nodes []*storage.Node, items []returnItem, variable string) *ExecuteResult {
	if e.queryPropertyDecayEnabled() {
		nodes = e.projectVisibleQueryValue(nodes).([]*storage.Node)
	}
	return projectPipelineSimpleNodeRead(nodes, items, variable)
}
