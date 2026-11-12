package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// readAlgorithmGraph is the call contract and relationship-type selection shared
// by the six whole-graph APOC extension algorithms. Existing (nodes, config)
// calls keep their configuration; (nodes, types, config) adds the documented
// type filter. Omitted/null types and ["*"] select all types; [] selects no
// relationships.
//
// The reads these algorithms perform are reported by the identity graph reader
// they go through, which owns their record: the node population the call runs
// over, and every adjacency request's entries where that read hands them over,
// before this call's type selection narrows them. A call that selects types
// therefore filters at this boundary, and a call that selects every type keeps
// the graph it was given and allocates no reader.
func (e *StorageExecutor) readAlgorithmGraph(ctx context.Context, procedure string, args []interface{}) (*StorageExecutor, []*storage.Node, map[string]interface{}, error) {
	var config map[string]interface{}
	var types map[string]struct{}
	second := procedureArgument(args, 1)
	if supplied, ok := second.(map[string]interface{}); ok {
		config = supplied
		if len(args) > 2 {
			return nil, nil, nil, procedureArgumentTypeError(procedure, "relationshipTypes", "LIST<STRING> or null before config", second)
		}
	} else if second != nil {
		items, list := cypherListValue(second)
		if !list {
			return nil, nil, nil, procedureArgumentTypeError(procedure, "relationshipTypes", "LIST<STRING> or MAP", second)
		}
		types = make(map[string]struct{}, len(items))
		all := false
		for _, item := range items {
			name, ok := item.(string)
			if !ok {
				return nil, nil, nil, procedureArgumentTypeError(procedure, "relationshipTypes", "LIST<STRING>", second)
			}
			if name == "*" {
				all = true
			}
			types[name] = struct{}{}
		}
		if all {
			types = nil
		}
	}
	if len(args) > 2 && args[2] != nil {
		var ok bool
		config, ok = args[2].(map[string]interface{})
		if !ok {
			return nil, nil, nil, procedureArgumentTypeError(procedure, "config", "MAP", args[2])
		}
	}
	graph, nodes, err := e.algorithmIdentityInput(ctx, procedure, args)
	if err != nil {
		return nil, nil, nil, err
	}
	// A call that selects relationship types filters at this read boundary; an
	// every-type call reads the graph it was given. Neither reports the read:
	// the identity graph reader both of them read through owns that record.
	if types == nil {
		return graph, nodes, config, nil
	}
	return graph.cloneWithStorage(&algorithmRelationshipReader{Engine: graph.storage, types: types}), nodes, config, nil
}

// algorithmRelationshipReader is the whole-graph algorithms' relationship-type
// selection boundary. It keeps the relationship types the call supplies, and
// keeps direction, multiplicity and relationship properties for the consuming
// algorithm. The population of each request is reported by the identity graph
// reader this one wraps, before the selection below narrows it, so one request
// is one record. It never mutates a storage-owned slice or retains a
// graph-sized cross-read identity set.
type algorithmRelationshipReader struct {
	storage.Engine
	types map[string]struct{}
}

// ReadAdjacentEdges reads one adjacency request and keeps the relationships of
// the types the call selected. The read is the wrapped reader's, so the entries
// it returned were reported there, before this selection rejected the others;
// a selection that keeps no type keeps no relationship.
func (r *algorithmRelationshipReader) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	edges, err := r.Engine.ReadAdjacentEdges(ctx, request)
	if err != nil {
		return nil, err
	}
	selected := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		if _, allowed := r.types[edge.Type]; allowed {
			selected = append(selected, edge)
		}
	}
	return selected, nil
}

// SubscribeEvents preserves storage event delivery through the relationship
// filter. Algorithm reads filter adjacency only; storage events remain intact.
func (r *algorithmRelationshipReader) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(r.Engine, handlers)
}

var _ storage.EventSubscriber = (*algorithmRelationshipReader)(nil)
