package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// readAlgorithmGraph is the call contract and relationship reader shared by the
// six whole-graph APOC extension algorithms. Existing (nodes, config) calls keep
// their configuration; (nodes, types, config) adds the documented type filter.
// Omitted/null types and ["*"] select all types; [] selects no relationships.
func (e *StorageExecutor) readAlgorithmGraph(procedure string, args []interface{}) (*StorageExecutor, []*storage.Node, map[string]interface{}, error) {
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
			if name == "*" { all = true }
			types[name] = struct{}{}
		}
		if all { types = nil }
	}
	if len(args) > 2 && args[2] != nil {
		var ok bool
		config, ok = args[2].(map[string]interface{})
		if !ok { return nil, nil, nil, procedureArgumentTypeError(procedure, "config", "MAP", args[2]) }
	}
	nodes, err := e.algorithmNodes(procedure, args)
	if err != nil { return nil, nil, nil, err }
	if types == nil { return e, nodes, config, nil }
	return e.cloneWithStorage(&algorithmRelationshipReader{Engine: e.storage, types: types}), nodes, config, nil
}

// Filter each adjacency read at the shared storage boundary, keeping direction,
// multiplicity and relationship properties for the consuming algorithm. Never
// mutate a storage-owned slice or retain a graph-sized cross-read identity set.
type algorithmRelationshipReader struct {
	storage.Engine
	types map[string]struct{}
}

func (r *algorithmRelationshipReader) selectEdges(edges []*storage.Edge, err error) ([]*storage.Edge, error) {
	if err != nil { return nil, err }
	selected := make([]*storage.Edge, 0, len(edges))
	for _, edge := range edges {
		if _, allowed := r.types[edge.Type]; allowed { selected = append(selected, edge) }
	}
	return selected, nil
}

func (r *algorithmRelationshipReader) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return r.selectEdges(r.Engine.GetOutgoingEdges(id))
}

func (r *algorithmRelationshipReader) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return r.selectEdges(r.Engine.GetIncomingEdges(id))
}
