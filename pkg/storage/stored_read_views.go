package storage

import (
	"context"
	"strings"
)

func (t *TracedEngine) StoredReadPolicy() StoredReadPolicy {
	return StoredReadPolicyForEngine(t.Engine)
}
func (t *TracedEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	return ReadStored(ctx, t.Engine, r)
}
func (w *WALEngine) StoredReadPolicy() StoredReadPolicy { return StoredReadPolicyForEngine(w.engine) }
func (w *WALEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	return ReadStored(ctx, w.engine, r)
}
func (*RemoteEngine) StoredReadPolicy() StoredReadPolicy { return RefusedStoredReadPolicy() }
func (*RemoteEngine) ReadStored(ctx context.Context, _ StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotImplemented
}

func (n *NamespacedEngine) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (n *NamespacedEngine) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	return ReadStoredInNamespace(ctx, n.inner, n.namespace, r)
}
func (s *namespacedReadSnapshot) StoredReadPolicy() StoredReadPolicy { return NativeStoredReadPolicy() }
func (s *namespacedReadSnapshot) ReadStored(ctx context.Context, r StoredReadRequest) (StoredReadResult, error) {
	return ReadStoredInNamespace(ctx, s.inner, s.n.namespace, r)
}

// ReadStoredInNamespace is the single namespace transform for Engine, native
// transaction and snapshot adapters. It changes neither membership nor capture.
// Every physical and logical endpoint is validated before a result escapes.
func ReadStoredInNamespace(ctx context.Context, owner any, namespace string, r StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if namespace == "" {
		return ReadStored(ctx, owner, r)
	}
	n := NamespacedEngine{namespace: namespace, scope: namespace + ":"}
	nodes := make([]NodeID, len(r.Nodes))
	for i, id := range r.Nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nodes[i] = n.PrefixNodeID(id)
	}
	r.Nodes = nodes
	edges := make([]EdgeID, len(r.Edges))
	for i, id := range r.Edges {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		edges[i] = n.PrefixEdgeID(id)
	}
	r.Edges = edges
	adjacency := make([]StoredAdjacencyRequest, len(r.Adjacency))
	for i, a := range r.Adjacency {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.Node = n.PrefixNodeID(a.Node)
		adjacency[i] = a
	}
	r.Adjacency = adjacency
	result, err := ReadStored(ctx, owner, r)
	if err != nil {
		return nil, err
	}
	return transformStoredResult(ctx, result, func(v StoredReadIdentity) StoredReadIdentity { v.Namespace = strings.Clone(namespace); return v }, func(f StoredNodeFact) (StoredNodeFact, error) {
		if !n.hasNodePrefix(f.ID) {
			return f, ErrInvalidData
		}
		f.ID = NodeID(strings.TrimPrefix(string(f.ID), n.scope))
		f.View.Namespace = strings.Clone(namespace)
		return f, nil
	}, func(f StoredEdgeFact) (StoredEdgeFact, error) {
		if !n.hasEdgePrefix(f.ID) || f.Exists && (!n.hasNodePrefix(f.StartNode) || !n.hasNodePrefix(f.EndNode)) {
			return f, ErrInvalidData
		}
		f.ID = EdgeID(strings.TrimPrefix(string(f.ID), n.scope))
		f.StartNode = NodeID(strings.TrimPrefix(string(f.StartNode), n.scope))
		f.EndNode = NodeID(strings.TrimPrefix(string(f.EndNode), n.scope))
		f.View.Namespace = strings.Clone(namespace)
		return f, nil
	}, func(node *Node) *Node { return n.toUserNode(node, nodeUserView{owned: true}) }, n.toUserEdge)
}

// transformStoredResult applies one owning view transform to every mode and
// every before-image variant. An invalid member refuses the whole operation.
func transformStoredResult(ctx context.Context, result StoredReadResult, view func(StoredReadIdentity) StoredReadIdentity, node func(StoredNodeFact) (StoredNodeFact, error), edge func(StoredEdgeFact) (StoredEdgeFact, error), nodeBody func(*Node) *Node, edgeBody func(*Edge) *Edge) (StoredReadResult, error) {
	facts := result.StoredFacts()
	for i, v := range facts.Views {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		facts.Views[i] = view(v)
	}
	for _, rows := range [][]StoredNodeFact{facts.Nodes, facts.PhysicalNodes} {
		for i, f := range rows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			mapped, err := node(f)
			if err != nil {
				return nil, err
			}
			rows[i] = mapped
		}
	}
	for _, rows := range [][]StoredEdgeFact{facts.Edges, facts.PhysicalEdges} {
		for i, f := range rows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			mapped, err := edge(f)
			if err != nil {
				return nil, err
			}
			rows[i] = mapped
		}
	}
	switch value := result.(type) {
	case StoredMetadata:
		return facts, nil
	case StoredProjection:
		value.Facts = facts
		for _, rows := range [][]StoredNodeProjection{value.Nodes, value.PhysicalNodes} {
			for i, p := range rows {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				f, err := node(p.Fact)
				if err != nil {
					return nil, err
				}
				rows[i].Fact = f
			}
		}
		for _, rows := range [][]StoredEdgeProjection{value.Edges, value.PhysicalEdges} {
			for i, p := range rows {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				f, err := edge(p.Fact)
				if err != nil {
					return nil, err
				}
				rows[i].Fact = f
			}
		}
		return value, nil
	case StoredFullRead:
		value.Facts = facts
		for _, rows := range [][]*Node{value.Nodes, value.PhysicalNodes} {
			for i, n := range rows {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				rows[i] = nodeBody(n)
				if rows[i] == nil {
					return nil, ErrInvalidData
				}
			}
		}
		for _, rows := range [][]*Edge{value.Edges, value.PhysicalEdges} {
			for i, e := range rows {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				rows[i] = edgeBody(e)
				if rows[i] == nil {
					return nil, ErrInvalidData
				}
			}
		}
		return value, nil
	}
	return nil, ErrInvalidData
}

func (r storedReadOperation) StoredReadPolicy() StoredReadPolicy {
	return StoredReadPolicyForEngine(r.engine)
}
func (r storedReadOperation) ReadStored(ctx context.Context, request StoredReadRequest) (StoredReadResult, error) {
	return ReadStored(ctx, r.engine, request)
}
func (v engineConstraintView) StoredReadPolicy() StoredReadPolicy {
	return StoredReadPolicyForEngine(v.engine)
}
func (v engineConstraintView) ReadStored(ctx context.Context, request StoredReadRequest) (StoredReadResult, error) {
	return ReadStored(ctx, v.engine, request)
}

// The constraint evaluator calls this adapter while holding tx.mu. It exposes
// its existing full-image predicates, not a reentrant metadata capture grant.
func (txConstraintView) StoredReadPolicy() StoredReadPolicy { return RefusedStoredReadPolicy() }
func (txConstraintView) ReadStored(ctx context.Context, _ StoredReadRequest) (StoredReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotImplemented
}
