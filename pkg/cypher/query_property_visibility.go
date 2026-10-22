package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) propertyViewEngine() *storage.BadgerEngine {
	if e.storedPropertyIdentity {
		return nil
	}
	if e.propertyDecayEngineKnown {
		return e.propertyDecayEngine
	}
	// Internal test literals can bypass the constructors. Do not mutate the
	// shared executor lazily, since independent statements may run concurrently.
	return unwrapBadgerEngine(e.storage)
}

func (e *StorageExecutor) queryPropertyDecayEnabled() bool {
	be := e.propertyViewEngine()
	return be != nil && be.IsDecayEnabled()
}

// queryVisibleValue is a detached projection of entity properties. Internal
// row bindings and mutation owners retain full entities: filtering their maps
// in place would make an unrelated SET persist the removal of hidden fields.
func (e *StorageExecutor) queryVisibleValue(value interface{}) interface{} {
	be := e.propertyViewEngine()
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
		if e.statementPropertyViews != nil {
			return e.statementPropertyViews.node(v)
		}
		out := *v
		out.Properties = e.decayVisibleNodeProperties(v)
		return &out
	case *storage.Edge:
		if v == nil {
			return v
		}
		if e.statementPropertyViews != nil {
			return e.statementPropertyViews.edge(v)
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

func (e *StorageExecutor) projectQueryResultProperties(ctx context.Context, result *ExecuteResult) {
	be := e.propertyViewEngine()
	if result == nil || be == nil || !be.IsDecayEnabled() {
		return
	}
	for batch, batchErr := range materializedBatches(ctx, len(result.Rows)) {
		if batchErr != nil {
			return
		}
		for offset, row := range result.Rows[batch.start:batch.end] {
			i := batch.start + offset
			result.Rows[i] = e.projectVisibleQueryValue(row).([]interface{})
		}
	}
}

func (e *StorageExecutor) projectVisiblePipelineNodeRead(ctx context.Context, nodes []*storage.Node, items []returnItem, variable string) (*ExecuteResult, error) {
	if e.queryPropertyDecayEnabled() {
		visible := make([]*storage.Node, len(nodes))
		for batch, batchErr := range materializedBatches(ctx, len(nodes)) {
			if batchErr != nil {
				return nil, batchErr
			}
			for offset, node := range nodes[batch.start:batch.end] {
				visible[batch.start+offset] = e.projectVisibleQueryValue(node).(*storage.Node)
			}
		}
		nodes = visible
	}
	return projectPipelineSimpleNodeRead(ctx, nodes, items, variable)
}
