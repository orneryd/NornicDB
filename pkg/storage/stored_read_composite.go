package storage

import (
	"context"
	"strings"
)

func (*CompositeEngine) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }

// ReadStored admits and binds every required constituent before any body read.
// Each child contributes its own captured identity; a composite does not claim
// one physical timestamp across stores or silently discard an unsupported peer.
func (c *CompositeEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.StorageMembership().ReadError(); err != nil {
		return nil, err
	}
	type child struct {
		alias, database string
		engine          Engine
		provider        StoredReadProvider
		request         StoredReadRequest
	}
	catalog := c.constituentReadCatalog()
	aliases, err := c.getConstituentsForReadWithContext(ctx)
	if err != nil {
		return nil, err
	}
	requests := map[string]*StoredReadRequest{}
	route := func(kind, id string, appendID func(*StoredReadRequest, string)) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		selected, local, _, err := catalog.selectEntity(kind, id, aliases)
		if err != nil {
			return err
		}
		for _, alias := range selected {
			request := requests[alias]
			if request == nil {
				copy := r
				copy.Nodes = nil
				copy.Edges = nil
				copy.Adjacency = nil
				request = &copy
				requests[alias] = request
			}
			appendID(request, local)
		}
		return nil
	}
	for _, id := range r.Nodes {
		if err := route("4", string(id), func(request *StoredReadRequest, local string) { request.Nodes = append(request.Nodes, NodeID(local)) }); err != nil {
			return nil, err
		}
	}
	for _, id := range r.Edges {
		if err := route("5", string(id), func(request *StoredReadRequest, local string) { request.Edges = append(request.Edges, EdgeID(local)) }); err != nil {
			return nil, err
		}
	}
	for _, adjacency := range r.Adjacency {
		if err := route("4", string(adjacency.Node), func(request *StoredReadRequest, local string) {
			adjacency.Node = NodeID(local)
			request.Adjacency = append(request.Adjacency, adjacency)
		}); err != nil {
			return nil, err
		}
	}
	children := make([]child, 0, len(requests))
	for _, alias := range aliases {
		request := requests[alias]
		if request == nil {
			continue
		}
		engine, err := c.getConstituent(alias)
		if err != nil {
			return nil, err
		}
		provider, ok := engineCapability[StoredReadProvider](engine, capabilityViewMetadata, nil)
		if !ok || !provider.StoredReadPolicy().Admits(*request) {
			return nil, ErrNotImplemented
		}
		children = append(children, child{alias, catalog.database[alias], engine, provider, *request})
	}
	facts := StoredMetadata{}
	projection := StoredProjection{}
	full := StoredFullRead{}
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := child.provider.ReadStored(ctx, child.request)
		if err != nil {
			return nil, err
		}
		view := func(v StoredReadIdentity) StoredReadIdentity {
			v.Constituent = strings.Clone(child.alias)
			if v.Namespace == "" {
				v.Namespace = strings.Clone(child.database)
			}
			return v
		}
		result, err = transformStoredResult(ctx, result, view, func(f StoredNodeFact) (StoredNodeFact, error) {
			f.View = view(f.View)
			f.ID = NodeID(SourceNodeElementID(child.database, f.ID))
			return f, nil
		}, func(f StoredEdgeFact) (StoredEdgeFact, error) {
			f.View = view(f.View)
			f.ID = EdgeID(SourceRelationshipElementID(child.database, f.ID))
			if f.Exists {
				f.StartNode = NodeID(SourceNodeElementID(child.database, f.StartNode))
				f.EndNode = NodeID(SourceNodeElementID(child.database, f.EndNode))
			}
			return f, nil
		}, func(n *Node) *Node { return c.sourceNode(child.alias, child.engine, n) }, func(e *Edge) *Edge { return c.sourceEdge(child.alias, child.engine, e) })
		if err != nil {
			return nil, err
		}
		f := result.StoredFacts()
		facts.Views = append(facts.Views, f.Views...)
		facts.Nodes = append(facts.Nodes, f.Nodes...)
		facts.Edges = append(facts.Edges, f.Edges...)
		facts.PhysicalNodes = append(facts.PhysicalNodes, f.PhysicalNodes...)
		facts.PhysicalEdges = append(facts.PhysicalEdges, f.PhysicalEdges...)
		switch value := result.(type) {
		case StoredProjection:
			projection.Nodes = append(projection.Nodes, value.Nodes...)
			projection.Edges = append(projection.Edges, value.Edges...)
			projection.PhysicalNodes = append(projection.PhysicalNodes, value.PhysicalNodes...)
			projection.PhysicalEdges = append(projection.PhysicalEdges, value.PhysicalEdges...)
		case StoredFullRead:
			full.Nodes = append(full.Nodes, value.Nodes...)
			full.Edges = append(full.Edges, value.Edges...)
			full.PhysicalNodes = append(full.PhysicalNodes, value.PhysicalNodes...)
			full.PhysicalEdges = append(full.PhysicalEdges, value.PhysicalEdges...)
		}
	}
	switch r.Mode {
	case StoredMetadataOnly:
		return facts, nil
	case StoredSelectedProperties:
		projection.Facts = facts
		return projection, nil
	case StoredFullMaterialization:
		full.Facts = facts
		return full, nil
	}
	return nil, ErrInvalidData
}
