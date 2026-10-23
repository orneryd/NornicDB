package cypher

import (
	"context"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
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
func (e *StorageExecutor) queryVisibleValue(walk *materializedRowCheckpoint, value interface{}) interface{} {
	be := e.propertyViewEngine()
	if be == nil || !be.IsDecayEnabled() {
		return value
	}
	return e.projectVisibleQueryValue(walk, value)
}

func (e *StorageExecutor) projectVisibleQueryValue(walk *materializedRowCheckpoint, value interface{}) interface{} {
	return cypherfn.ProjectValue(walk, value, func(value interface{}) (interface{}, bool) {
		return e.projectVisibleQueryEntity(walk, value)
	})
}

// projectVisibleQueryEntity is the visible view of one entity or path value
// (handled), for the value walker's projection; containers are its to walk.
func (e *StorageExecutor) projectVisibleQueryEntity(walk *materializedRowCheckpoint, value interface{}) (interface{}, bool) {
	switch v := value.(type) {
	case storage.RemoteEntityValue, storage.RemotePathValue:
		// These carriers already have their source database's visibility view.
		return value, true
	case storage.Node:
		node, _ := e.projectVisibleQueryEntity(walk, &v)
		return *node.(*storage.Node), true
	case storage.Edge:
		edge, _ := e.projectVisibleQueryEntity(walk, &v)
		return *edge.(*storage.Edge), true
	case *storage.Node:
		if v == nil {
			return v, true
		}
		if e.statementPropertyViews != nil {
			return e.statementPropertyViews.node(v), true
		}
		out := *v
		out.Properties = e.decayVisibleNodeProperties(v)
		return &out, true
	case *storage.Edge:
		if v == nil {
			return v, true
		}
		if e.statementPropertyViews != nil {
			return e.statementPropertyViews.edge(v), true
		}
		out := *v
		out.Properties = e.decayVisibleEdgeProperties(v)
		return &out, true
	case PathResult:
		path, _ := e.projectVisibleQueryEntity(walk, &v)
		return *path.(*PathResult), true
	case *PathResult:
		if v == nil {
			return v, true
		}
		out := *v
		out.Nodes = e.projectVisibleQueryValue(walk, v.Nodes).([]*storage.Node)
		out.Relationships = e.projectVisibleQueryValue(walk, v.Relationships).([]*storage.Edge)
		return &out, true
	}
	return nil, false
}

func (e *StorageExecutor) queryEntityProperties(value interface{}) map[string]interface{} {
	switch v := e.queryVisibleValue(nil, value).(type) {
	case *storage.Node:
		if v != nil {
			return v.Properties
		}
	case *storage.Edge:
		if v != nil {
			return v.Properties
		}
	case storage.Node:
		return v.Properties
	case storage.Edge:
		return v.Properties
	}
	return nil
}

// publishQueryRow is the public row boundary for both materialized results
// and ResultStream.emit. Projection detaches graph values before a consumer
// can observe them; the expression memo has no ownership beyond that row.
func (e *StorageExecutor) publishQueryRow(walk *materializedRowCheckpoint, row []interface{}) []interface{} {
	if e.statementPropertyViews != nil {
		defer e.statementPropertyViews.releaseRow()
	}
	if !e.queryPropertyDecayEnabled() {
		return row
	}
	return e.projectVisibleQueryValue(walk, row).([]interface{})
}

func (e *StorageExecutor) projectQueryResultProperties(ctx context.Context, result *ExecuteResult) {
	be := e.propertyViewEngine()
	if result == nil || be == nil || !be.IsDecayEnabled() {
		return
	}
	walk := valueWalk(ctx)
	for batch, batchErr := range materializedBatches(ctx, len(result.Rows)) {
		if batchErr != nil {
			return
		}
		for offset, row := range result.Rows[batch.start:batch.end] {
			i := batch.start + offset
			result.Rows[i] = e.publishQueryRow(walk, row)
		}
	}
}

func (e *StorageExecutor) projectVisiblePipelineNodeRead(ctx context.Context, nodes []*storage.Node, items []returnItem, variable string) (*ExecuteResult, error) {
	if e.queryPropertyDecayEnabled() {
		walk := valueWalk(ctx)
		visible := make([]*storage.Node, len(nodes))
		for batch, batchErr := range materializedBatches(ctx, len(nodes)) {
			if batchErr != nil {
				return nil, batchErr
			}
			for offset, node := range nodes[batch.start:batch.end] {
				visible[batch.start+offset] = e.projectVisibleQueryValue(walk, node).(*storage.Node)
			}
		}
		nodes = visible
	}
	return projectPipelineSimpleNodeRead(ctx, nodes, items, variable)
}
