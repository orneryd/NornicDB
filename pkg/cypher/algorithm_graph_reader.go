package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// readAlgorithmGraph is the call contract and relationship reader shared by the
// six whole-graph APOC extension algorithms. Existing (nodes, config) calls keep
// their configuration; (nodes, types, config) adds the documented type filter.
// Omitted/null types and ["*"] select all types; [] selects no relationships.
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
	if types == nil {
		return graph, nodes, config, nil
	}
	return graph.cloneWithStorage(&algorithmRelationshipReader{Engine: graph.storage, types: types}), nodes, config, nil
}

// Filter each adjacency read at the shared storage boundary, keeping direction,
// multiplicity and relationship properties for the consuming algorithm. Never
// mutate a storage-owned slice or retain a graph-sized cross-read identity set.
type algorithmRelationshipReader struct {
	namespace carriedNamespace
	storage.Engine
	types map[string]struct{}
}

// algorithmRelationshipReader declares every optional method; admission is derived from this policy.
var algorithmRelationshipReaderReadPolicy = storage.BoundaryReadPolicy{
	{Methods: "ReadStored StoredReadPolicy", Disposition: storage.CapabilityRefused, Scope: "Explicit stored modes preserve this view; mandatory facts never use ordinary/full-body fallback."},
	{Methods: "Namespace", Disposition: storage.CapabilityOwned, Scope: "Carried namespace identity; the filtered data view stays owned."},
	{Methods: "SubscribeEvents",
		Disposition: storage.CapabilityOwned, Scope: "Relationship type filtering remains in core Engine reads; scoped event subscription stays on the view."},
}

func (*algorithmRelationshipReader) ReadCapabilityPolicy() storage.BoundaryReadPolicy {
	return algorithmRelationshipReaderReadPolicy.Copy()
}

func (r *algorithmRelationshipReader) selectEdges(edges []*storage.Edge, err error) ([]*storage.Edge, error) {
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

func (r *algorithmRelationshipReader) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return r.selectEdges(r.Engine.GetOutgoingEdges(id))
}

func (r *algorithmRelationshipReader) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return r.selectEdges(r.Engine.GetIncomingEdges(id))
}

// SubscribeEvents preserves storage event delivery through the relationship
// filter. Algorithm reads filter adjacency only; storage events remain intact.
func (r *algorithmRelationshipReader) SubscribeEvents(handlers storage.EventHandlers) error {
	return storage.SubscribeEvents(r.Engine, handlers)
}

var _ storage.EventSubscriber = (*algorithmRelationshipReader)(nil)

func (*algorithmRelationshipReader) EngineCapabilityPolicy() storage.EngineBoundaryPolicy {
	return storage.EngineBoundaryPolicy{Kind: storage.EngineView, ReadState: storage.BindingCarried, Membership: storage.BindingCarried, Controller: storage.BindingRefused, PendingActions: storage.BindingCarried, SchemaPublication: storage.BindingCarried, ReadLease: storage.BindingCarried, StatementMode: storage.BindingCarried, Identity: storage.BindingCarried, Enforcement: storage.BindingCarried, CompoundDependencies: storage.BindingCarried, SameVersionFallback: storage.BindingCarried, Delivery: storage.BindingCarried, ExactEffects: storage.BindingCarried, FullRead: storage.BindingCarried, ObservedNode: storage.BindingRefused, ObservedEdge: storage.BindingRefused}
}

// Stored integrity facts may not bypass this view.
func (r *algorithmRelationshipReader) StoredReadPolicy() storage.StoredReadPolicy {
	return storage.RefusedStoredReadPolicy()
}
func (r *algorithmRelationshipReader) ReadStored(ctx context.Context, _ storage.StoredReadRequest) (storage.StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, storage.ErrNotImplemented
}
