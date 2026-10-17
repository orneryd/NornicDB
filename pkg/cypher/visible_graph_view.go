package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// visibleGraphView is the graph as a restricted principal's statement reads
// it (GraphVisibility): every node and relationship read is filtered, and
// counts and degrees count what is visible. It embeds only storage.Engine,
// so none of the optional interfaces the executor's fast paths look for
// (label counts, projected or streaming readers, adjacency, edge headers) is
// visible through it, and it offers no GetInnerEngine: those fast paths fall
// back to the reads below, and nothing can unwrap it to read around it. A
// fast path added later that type-asserts for another interface also falls
// back, so a missed path fails closed. Writes pass through; a hidden node
// can't be found to be written.
type visibleGraphView struct {
	storage.Engine
	eval *visibilityEvaluator
}

func newVisibleGraphView(inner storage.Engine, eval *visibilityEvaluator) *visibleGraphView {
	if view, ok := inner.(*visibleGraphView); ok {
		return view
	}
	return &visibleGraphView{Engine: inner, eval: eval}
}

// Namespace is the inner engine's database name, which the executor reads
// for element IDs and messages.
func (v *visibleGraphView) Namespace() string {
	if scoped, ok := v.Engine.(interface{ Namespace() string }); ok {
		return scoped.Namespace()
	}
	return ""
}

func (v *visibleGraphView) filterNodes(nodes []*storage.Node) []*storage.Node {
	out := nodes[:0:0]
	for _, node := range nodes {
		if v.eval.nodeVisible(node) {
			out = append(out, node)
		}
	}
	return out
}

func (v *visibleGraphView) filterEdges(edges []*storage.Edge) []*storage.Edge {
	out := edges[:0:0]
	for _, edge := range edges {
		if v.eval.edgeVisible(edge) {
			out = append(out, edge)
		}
	}
	return out
}

func (v *visibleGraphView) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := v.Engine.GetNode(id)
	if err != nil {
		return nil, err
	}
	if !v.eval.nodeVisible(node) {
		return nil, storage.ErrNotFound
	}
	return node, nil
}

func (v *visibleGraphView) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, err := v.Engine.GetEdge(id)
	if err != nil {
		return nil, err
	}
	if !v.eval.edgeVisible(edge) {
		return nil, storage.ErrNotFound
	}
	return edge, nil
}

func (v *visibleGraphView) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := v.Engine.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	return v.filterNodes(nodes), nil
}

func (v *visibleGraphView) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	nodes, err := v.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (v *visibleGraphView) nodeEdges(id storage.NodeID, edges []*storage.Edge, err error) ([]*storage.Edge, error) {
	if err != nil {
		return nil, err
	}
	if !v.eval.nodeIDVisible(id) {
		return nil, nil
	}
	return v.filterEdges(edges), nil
}

func (v *visibleGraphView) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := v.Engine.GetOutgoingEdges(id)
	return v.nodeEdges(id, edges, err)
}

func (v *visibleGraphView) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := v.Engine.GetIncomingEdges(id)
	return v.nodeEdges(id, edges, err)
}

func (v *visibleGraphView) GetEdgesBetween(start, end storage.NodeID) ([]*storage.Edge, error) {
	edges, err := v.Engine.GetEdgesBetween(start, end)
	if err != nil {
		return nil, err
	}
	return v.filterEdges(edges), nil
}

func (v *visibleGraphView) GetEdgeBetween(start, end storage.NodeID, edgeType string) *storage.Edge {
	edge := v.Engine.GetEdgeBetween(start, end, edgeType)
	if edge == nil || !v.eval.edgeVisible(edge) {
		return nil
	}
	return edge
}

func (v *visibleGraphView) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	edges, err := v.Engine.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}
	return v.filterEdges(edges), nil
}

func (v *visibleGraphView) AllNodes() ([]*storage.Node, error) {
	nodes, err := v.Engine.AllNodes()
	if err != nil {
		return nil, err
	}
	return v.filterNodes(nodes), nil
}

func (v *visibleGraphView) AllEdges() ([]*storage.Edge, error) {
	edges, err := v.Engine.AllEdges()
	if err != nil {
		return nil, err
	}
	return v.filterEdges(edges), nil
}

func (v *visibleGraphView) GetAllNodes() []*storage.Node {
	return v.filterNodes(v.Engine.GetAllNodes())
}

func (v *visibleGraphView) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(node *storage.Node) error) error {
	return v.Engine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
		if !v.eval.nodeVisible(node) {
			return nil
		}
		return fn(node)
	})
}

func (v *visibleGraphView) GetInDegree(id storage.NodeID) int {
	edges, _ := v.GetIncomingEdges(id)
	return len(edges)
}

func (v *visibleGraphView) GetOutDegree(id storage.NodeID) int {
	edges, _ := v.GetOutgoingEdges(id)
	return len(edges)
}

func (v *visibleGraphView) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	nodes, err := v.Engine.BatchGetNodes(ids)
	if err != nil {
		return nil, err
	}
	for id, node := range nodes {
		if !v.eval.nodeVisible(node) {
			delete(nodes, id)
		}
	}
	return nodes, nil
}

func (v *visibleGraphView) NodeCount() (int64, error) {
	nodes, err := v.AllNodes()
	return int64(len(nodes)), err
}

func (v *visibleGraphView) EdgeCount() (int64, error) {
	edges, err := v.AllEdges()
	return int64(len(edges)), err
}

func (v *visibleGraphView) EdgeCountByType(edgeType string) (int64, error) {
	edges, err := v.GetEdgesByType(edgeType)
	return int64(len(edges)), err
}

func (v *visibleGraphView) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return v.edgeCountByEndLabel(label, edgeType, true)
}

func (v *visibleGraphView) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return v.edgeCountByEndLabel(label, edgeType, false)
}

func (v *visibleGraphView) edgeCountByEndLabel(label, edgeType string, start bool) (int64, error) {
	edges, err := v.GetEdgesByType(edgeType)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		id := edge.EndNode
		if start {
			id = edge.StartNode
		}
		if node, err := v.Engine.GetNode(id); err == nil && nodeHasLabel(node, label) {
			count++
		}
	}
	return count, nil
}

// CreateNode and BulkCreateNodes record what they create, which stays
// visible to the statement and its transaction (createdNodes).
func (v *visibleGraphView) CreateNode(node *storage.Node) (storage.NodeID, error) {
	id, err := v.Engine.CreateNode(node)
	if err == nil {
		v.eval.created.add(node.ID)
		v.eval.created.add(id)
	}
	return id, err
}

func (v *visibleGraphView) BulkCreateNodes(nodes []*storage.Node) error {
	if err := v.Engine.BulkCreateNodes(nodes); err != nil {
		return err
	}
	for _, node := range nodes {
		v.eval.created.add(node.ID)
	}
	return nil
}

// UpdateNode and UpdateEdge change only what is visible.
func (v *visibleGraphView) UpdateNode(node *storage.Node) error {
	if _, err := v.GetNode(node.ID); err != nil {
		return err
	}
	return v.Engine.UpdateNode(node)
}

func (v *visibleGraphView) UpdateEdge(edge *storage.Edge) error {
	if _, err := v.GetEdge(edge.ID); err != nil {
		return err
	}
	return v.Engine.UpdateEdge(edge)
}

// DeleteNode deletes a visible node that has no relationships the principal
// can't see; DETACH DELETE removes the visible ones first. A node with a
// relationship to a hidden node is refused, as Neo4j refuses deleting a node
// whose relationships the user can't see.
func (v *visibleGraphView) DeleteNode(id storage.NodeID) error {
	if _, err := v.GetNode(id); err != nil {
		return err
	}
	out, err := v.Engine.GetOutgoingEdges(id)
	if err != nil {
		return err
	}
	in, err := v.Engine.GetIncomingEdges(id)
	if err != nil {
		return err
	}
	for _, edge := range append(out, in...) {
		if !v.eval.edgeVisible(edge) {
			return localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminHiddenRelationshipDelete())
		}
	}
	return v.Engine.DeleteNode(id)
}

func (v *visibleGraphView) DeleteEdge(id storage.EdgeID) error {
	if _, err := v.GetEdge(id); err != nil {
		return err
	}
	return v.Engine.DeleteEdge(id)
}

func (v *visibleGraphView) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := v.DeleteNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (v *visibleGraphView) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := v.DeleteEdge(id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteByPrefix deletes a whole database's keys; a restricted principal
// can't.
func (v *visibleGraphView) DeleteByPrefix(string) (int64, int64, error) {
	return 0, 0, localizedStatusError("Neo.ClientError.Security.Forbidden", "Forbidden", localization.CypherAdminHiddenRelationshipDelete())
}
