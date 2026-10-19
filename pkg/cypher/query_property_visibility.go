package cypher

import (
	"reflect"

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
	case storage.RemoteEntityValue, storage.RemotePathValue:
		// These carriers already have their source database's visibility view.
		return value
	case storage.Node:
		return *e.projectVisibleQueryValue(&v).(*storage.Node)
	case storage.Edge:
		return *e.projectVisibleQueryValue(&v).(*storage.Edge)
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
	case PathResult:
		return *e.projectVisibleQueryValue(&v).(*PathResult)
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
	// The public APIs and encoders also accept typed lists, arrays and maps.
	// Preserve their carrier type while recursing through the same entity owner;
	// adding another container spelling must not introduce another policy switch.
	if value != nil {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.Slice, reflect.Array:
			if v.Kind() == reflect.Slice && v.IsNil() {
				return value
			}
			var out reflect.Value
			if v.Kind() == reflect.Slice {
				out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
			} else {
				out = reflect.New(v.Type()).Elem()
			}
			for i := 0; i < v.Len(); i++ {
				projected := e.projectVisibleQueryValue(v.Index(i).Interface())
				if projected != nil {
					out.Index(i).Set(reflect.ValueOf(projected))
				}
			}
			return out.Interface()
		case reflect.Map:
			if v.IsNil() || v.Type().Key().Kind() != reflect.String {
				return value
			}
			out := reflect.MakeMapWithSize(v.Type(), v.Len())
			entries := v.MapRange()
			for entries.Next() {
				projected := e.projectVisibleQueryValue(entries.Value().Interface())
				entry := reflect.Zero(v.Type().Elem())
				if projected != nil {
					entry = reflect.ValueOf(projected)
				}
				out.SetMapIndex(entries.Key(), entry)
			}
			return out.Interface()
		}
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
func (e *StorageExecutor) publishQueryRow(row []interface{}) []interface{} {
	if e.statementPropertyViews != nil {
		defer e.statementPropertyViews.releaseRow()
	}
	if !e.queryPropertyDecayEnabled() {
		return row
	}
	return e.projectVisibleQueryValue(row).([]interface{})
}

func (e *StorageExecutor) projectQueryResultProperties(result *ExecuteResult) {
	be := e.propertyViewEngine()
	if result == nil || be == nil || !be.IsDecayEnabled() {
		return
	}
	for i, row := range result.Rows {
		result.Rows[i] = e.publishQueryRow(row)
	}
}

func (e *StorageExecutor) projectVisiblePipelineNodeRead(nodes []*storage.Node, items []returnItem, variable string) *ExecuteResult {
	if e.queryPropertyDecayEnabled() {
		nodes = e.projectVisibleQueryValue(nodes).([]*storage.Node)
	}
	return projectPipelineSimpleNodeRead(nodes, items, variable)
}
