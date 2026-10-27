package storage

import "github.com/orneryd/nornicdb/internal/jsontext"

// These finite read views preserve the historical JSON field set while giving
// dynamic property leaves exact-number binding. They do not change Node/Edge
// Go APIs, entity encodings or ignored recovery extension fields.
type legacyNode struct{ *Node }
type legacyEdge struct{ *Edge }

func (n *legacyNode) UnmarshalJSON(raw []byte) error {
	if n.Node == nil {
		n.Node = &Node{}
	}
	type Fields Node
	view := struct {
		*Fields
		Properties jsontext.NumberMap `json:"properties"`
	}{Fields: (*Fields)(n.Node), Properties: jsontext.NumberMap(n.Properties)}
	if err := jsontext.Unmarshal(raw, &view, jsontext.ProjectedInput); err != nil {
		return err
	}
	n.Properties = map[string]any(view.Properties)
	return nil
}
func (e *legacyEdge) UnmarshalJSON(raw []byte) error {
	if e.Edge == nil {
		e.Edge = &Edge{}
	}
	type Fields Edge
	view := struct {
		*Fields
		Properties jsontext.NumberMap `json:"properties"`
	}{Fields: (*Fields)(e.Edge), Properties: jsontext.NumberMap(e.Properties)}
	if err := jsontext.Unmarshal(raw, &view, jsontext.ProjectedInput); err != nil {
		return err
	}
	e.Properties = map[string]any(view.Properties)
	return nil
}
func legacyNodeView(node *Node) *legacyNode {
	if node == nil {
		return nil
	}
	return &legacyNode{node}
}
func legacyEdgeView(edge *Edge) *legacyEdge {
	if edge == nil {
		return nil
	}
	return &legacyEdge{edge}
}
func (n *legacyNode) entity() *Node {
	if n == nil {
		return nil
	}
	return n.Node
}
func (e *legacyEdge) entity() *Edge {
	if e == nil {
		return nil
	}
	return e.Edge
}

type legacyOperation struct{ *Operation }

func (o *legacyOperation) UnmarshalJSON(raw []byte) error {
	if o.Operation == nil {
		o.Operation = &Operation{}
	}
	type Fields Operation
	view := struct {
		*Fields
		Node    *legacyNode `json:"node"`
		OldNode *legacyNode `json:"old_node"`
		Edge    *legacyEdge `json:"edge"`
		OldEdge *legacyEdge `json:"old_edge"`
	}{Fields: (*Fields)(o.Operation), Node: legacyNodeView(o.Node), OldNode: legacyNodeView(o.OldNode), Edge: legacyEdgeView(o.Edge), OldEdge: legacyEdgeView(o.OldEdge)}
	if err := jsontext.Unmarshal(raw, &view, jsontext.ProjectedInput); err != nil {
		return err
	}
	o.Node, o.OldNode = view.Node.entity(), view.OldNode.entity()
	o.Edge, o.OldEdge = view.Edge.entity(), view.OldEdge.entity()
	return nil
}

// decodeLegacyRecoveryRecord keeps the finite recovery envelope and its ignored
// Fields unchanged; only dynamic entity properties select exact-number views.
func decodeLegacyRecoveryRecord(raw []byte, target any) error {
	switch target := target.(type) {
	case *Node:
		return jsontext.Unmarshal(raw, &legacyNode{target})
	case *Edge:
		return jsontext.Unmarshal(raw, &legacyEdge{target})
	case *WALNodeData:
		view := struct {
			*WALNodeData
			Node    *legacyNode `json:"node"`
			OldNode *legacyNode `json:"old_node"`
		}{WALNodeData: target}
		view.Node = legacyNodeView(target.Node)
		view.OldNode = legacyNodeView(target.OldNode)
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Node = view.Node.entity()
		target.OldNode = view.OldNode.entity()
		return nil
	case *WALEdgeData:
		view := struct {
			*WALEdgeData
			Edge    *legacyEdge `json:"edge"`
			OldEdge *legacyEdge `json:"old_edge"`
		}{WALEdgeData: target}
		view.Edge = legacyEdgeView(target.Edge)
		view.OldEdge = legacyEdgeView(target.OldEdge)
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Edge = view.Edge.entity()
		target.OldEdge = view.OldEdge.entity()
		return nil
	case *WALDeleteData:
		view := struct {
			*WALDeleteData
			OldNode *legacyNode `json:"old_node"`
			OldEdge *legacyEdge `json:"old_edge"`
		}{WALDeleteData: target}
		view.OldNode = legacyNodeView(target.OldNode)
		view.OldEdge = legacyEdgeView(target.OldEdge)
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.OldNode = view.OldNode.entity()
		target.OldEdge = view.OldEdge.entity()
		return nil
	case *WALBulkNodesData:
		view := struct {
			*WALBulkNodesData
			Nodes []*legacyNode `json:"nodes"`
		}{WALBulkNodesData: target}
		if target.Nodes != nil {
			view.Nodes = make([]*legacyNode, len(target.Nodes))
			for i := range target.Nodes {
				view.Nodes[i] = legacyNodeView(target.Nodes[i])
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Nodes = nil
		if view.Nodes != nil {
			target.Nodes = make([]*Node, len(view.Nodes))
			for i := range view.Nodes {
				target.Nodes[i] = view.Nodes[i].entity()
			}
		}
		return nil
	case *WALBulkEdgesData:
		view := struct {
			*WALBulkEdgesData
			Edges []*legacyEdge `json:"edges"`
		}{WALBulkEdgesData: target}
		if target.Edges != nil {
			view.Edges = make([]*legacyEdge, len(target.Edges))
			for i := range target.Edges {
				view.Edges[i] = legacyEdgeView(target.Edges[i])
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Edges = nil
		if view.Edges != nil {
			target.Edges = make([]*Edge, len(view.Edges))
			for i := range view.Edges {
				target.Edges[i] = view.Edges[i].entity()
			}
		}
		return nil
	case *WALBulkDeleteNodesData:
		view := struct {
			*WALBulkDeleteNodesData
			OldNodes []*legacyNode `json:"old_nodes"`
		}{WALBulkDeleteNodesData: target}
		if target.OldNodes != nil {
			view.OldNodes = make([]*legacyNode, len(target.OldNodes))
			for i := range target.OldNodes {
				view.OldNodes[i] = legacyNodeView(target.OldNodes[i])
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.OldNodes = nil
		if view.OldNodes != nil {
			target.OldNodes = make([]*Node, len(view.OldNodes))
			for i := range view.OldNodes {
				target.OldNodes[i] = view.OldNodes[i].entity()
			}
		}
		return nil
	case *WALBulkDeleteEdgesData:
		view := struct {
			*WALBulkDeleteEdgesData
			OldEdges []*legacyEdge `json:"old_edges"`
		}{WALBulkDeleteEdgesData: target}
		if target.OldEdges != nil {
			view.OldEdges = make([]*legacyEdge, len(target.OldEdges))
			for i := range target.OldEdges {
				view.OldEdges[i] = legacyEdgeView(target.OldEdges[i])
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.OldEdges = nil
		if view.OldEdges != nil {
			target.OldEdges = make([]*Edge, len(view.OldEdges))
			for i := range view.OldEdges {
				target.OldEdges[i] = view.OldEdges[i].entity()
			}
		}
		return nil
	case *Snapshot:
		view := struct {
			*Snapshot
			Nodes []*legacyNode `json:"nodes"`
			Edges []*legacyEdge `json:"edges"`
		}{Snapshot: target}
		if target.Nodes != nil {
			view.Nodes = make([]*legacyNode, len(target.Nodes))
			for i := range target.Nodes {
				view.Nodes[i] = legacyNodeView(target.Nodes[i])
			}
		}
		if target.Edges != nil {
			view.Edges = make([]*legacyEdge, len(target.Edges))
			for i := range target.Edges {
				view.Edges[i] = legacyEdgeView(target.Edges[i])
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Nodes = nil
		if view.Nodes != nil {
			target.Nodes = make([]*Node, len(view.Nodes))
			for i := range view.Nodes {
				target.Nodes[i] = view.Nodes[i].entity()
			}
		}
		target.Edges = nil
		if view.Edges != nil {
			target.Edges = make([]*Edge, len(view.Edges))
			for i := range view.Edges {
				target.Edges[i] = view.Edges[i].entity()
			}
		}
		return nil
	case *WALBufferedCommitData:
		view := struct {
			*WALBufferedCommitData
			Ops []legacyOperation `json:"ops"`
		}{WALBufferedCommitData: target}
		if target.Ops != nil {
			view.Ops = make([]legacyOperation, len(target.Ops))
			for i := range target.Ops {
				view.Ops[i] = legacyOperation{&target.Ops[i]}
			}
		}
		if err := jsontext.Unmarshal(raw, &view); err != nil {
			return err
		}
		target.Ops = nil
		if view.Ops != nil {
			target.Ops = make([]Operation, len(view.Ops))
			for i := range view.Ops {
				target.Ops[i] = *view.Ops[i].Operation
			}
		}
		return nil
	case *any:
		value, err := jsontext.UnmarshalValue(raw)
		if err == nil {
			*target = value
		}
		return err
	case *map[string]any:
		var values jsontext.NumberMap
		if err := jsontext.Unmarshal(raw, &values); err != nil {
			return err
		}
		*target = map[string]any(values)
		return nil
	default:
		return jsontext.Unmarshal(raw, target)
	}
}
