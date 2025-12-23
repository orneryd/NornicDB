package multidb

import (
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// legacyTestEngine is a minimal in-memory storage.Engine implementation that does NOT
// enforce namespaced IDs. It exists only to simulate pre-multi-db on-disk layouts
// (unprefixed IDs) for migration tests.
//
// Production engines must enforce namespaced IDs at the storage layer.
type legacyTestEngine struct {
	mu sync.RWMutex

	nodes map[storage.NodeID]*storage.Node
	edges map[storage.EdgeID]*storage.Edge

	labelIndex    map[string]map[storage.NodeID]struct{}
	outgoingEdges map[storage.NodeID]map[storage.EdgeID]struct{}
	incomingEdges map[storage.NodeID]map[storage.EdgeID]struct{}
	edgeTypeIndex map[string]map[storage.EdgeID]struct{}

	schema storage.SchemaManager
}

func newLegacyTestEngine() *legacyTestEngine {
	return &legacyTestEngine{
		nodes:         make(map[storage.NodeID]*storage.Node),
		edges:         make(map[storage.EdgeID]*storage.Edge),
		labelIndex:    make(map[string]map[storage.NodeID]struct{}),
		outgoingEdges: make(map[storage.NodeID]map[storage.EdgeID]struct{}),
		incomingEdges: make(map[storage.NodeID]map[storage.EdgeID]struct{}),
		edgeTypeIndex: make(map[string]map[storage.EdgeID]struct{}),
	}
}

func (e *legacyTestEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if node == nil || node.ID == "" {
		return "", storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.nodes[node.ID]; ok {
		return "", storage.ErrAlreadyExists
	}
	c := storage.CopyNode(node)
	e.nodes[node.ID] = c

	for _, label := range c.Labels {
		norm := strings.ToLower(label)
		if e.labelIndex[norm] == nil {
			e.labelIndex[norm] = make(map[storage.NodeID]struct{})
		}
		e.labelIndex[norm][c.ID] = struct{}{}
	}

	return node.ID, nil
}

func (e *legacyTestEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == "" {
		return nil, storage.ErrInvalidID
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	n, ok := e.nodes[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return storage.CopyNode(n), nil
}

func (e *legacyTestEngine) UpdateNode(node *storage.Node) error {
	if node == nil || node.ID == "" {
		return storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.nodes[node.ID]; !ok {
		return storage.ErrNotFound
	}

	// Clear old labels
	old := e.nodes[node.ID]
	for _, label := range old.Labels {
		norm := strings.ToLower(label)
		if idx := e.labelIndex[norm]; idx != nil {
			delete(idx, node.ID)
		}
	}

	c := storage.CopyNode(node)
	e.nodes[node.ID] = c
	for _, label := range c.Labels {
		norm := strings.ToLower(label)
		if e.labelIndex[norm] == nil {
			e.labelIndex[norm] = make(map[storage.NodeID]struct{})
		}
		e.labelIndex[norm][c.ID] = struct{}{}
	}
	return nil
}

func (e *legacyTestEngine) DeleteNode(id storage.NodeID) error {
	if id == "" {
		return storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	node, ok := e.nodes[id]
	if !ok {
		return storage.ErrNotFound
	}
	delete(e.nodes, id)
	for _, label := range node.Labels {
		norm := strings.ToLower(label)
		if idx := e.labelIndex[norm]; idx != nil {
			delete(idx, id)
		}
	}

	// Delete attached edges.
	for edgeID := range e.outgoingEdges[id] {
		e.deleteEdgeLocked(edgeID)
	}
	for edgeID := range e.incomingEdges[id] {
		e.deleteEdgeLocked(edgeID)
	}
	delete(e.outgoingEdges, id)
	delete(e.incomingEdges, id)
	return nil
}

func (e *legacyTestEngine) CreateEdge(edge *storage.Edge) error {
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.edges[edge.ID]; ok {
		return storage.ErrAlreadyExists
	}
	c := storage.CopyEdge(edge)
	e.edges[edge.ID] = c

	if e.outgoingEdges[c.StartNode] == nil {
		e.outgoingEdges[c.StartNode] = make(map[storage.EdgeID]struct{})
	}
	e.outgoingEdges[c.StartNode][c.ID] = struct{}{}
	if e.incomingEdges[c.EndNode] == nil {
		e.incomingEdges[c.EndNode] = make(map[storage.EdgeID]struct{})
	}
	e.incomingEdges[c.EndNode][c.ID] = struct{}{}

	if e.edgeTypeIndex[c.Type] == nil {
		e.edgeTypeIndex[c.Type] = make(map[storage.EdgeID]struct{})
	}
	e.edgeTypeIndex[c.Type][c.ID] = struct{}{}

	return nil
}

func (e *legacyTestEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if id == "" {
		return nil, storage.ErrInvalidID
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	edge, ok := e.edges[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return storage.CopyEdge(edge), nil
}

func (e *legacyTestEngine) UpdateEdge(edge *storage.Edge) error {
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.edges[edge.ID]; !ok {
		return storage.ErrNotFound
	}
	// Simplify: delete and recreate indices.
	e.deleteEdgeLocked(edge.ID)
	c := storage.CopyEdge(edge)
	e.edges[edge.ID] = c
	if e.outgoingEdges[c.StartNode] == nil {
		e.outgoingEdges[c.StartNode] = make(map[storage.EdgeID]struct{})
	}
	e.outgoingEdges[c.StartNode][c.ID] = struct{}{}
	if e.incomingEdges[c.EndNode] == nil {
		e.incomingEdges[c.EndNode] = make(map[storage.EdgeID]struct{})
	}
	e.incomingEdges[c.EndNode][c.ID] = struct{}{}
	if e.edgeTypeIndex[c.Type] == nil {
		e.edgeTypeIndex[c.Type] = make(map[storage.EdgeID]struct{})
	}
	e.edgeTypeIndex[c.Type][c.ID] = struct{}{}
	return nil
}

func (e *legacyTestEngine) DeleteEdge(id storage.EdgeID) error {
	if id == "" {
		return storage.ErrInvalidID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.edges[id]; !ok {
		return storage.ErrNotFound
	}
	e.deleteEdgeLocked(id)
	return nil
}

func (e *legacyTestEngine) deleteEdgeLocked(id storage.EdgeID) {
	edge, ok := e.edges[id]
	if !ok {
		return
	}
	delete(e.edges, id)
	if out := e.outgoingEdges[edge.StartNode]; out != nil {
		delete(out, id)
	}
	if in := e.incomingEdges[edge.EndNode]; in != nil {
		delete(in, id)
	}
	if byType := e.edgeTypeIndex[edge.Type]; byType != nil {
		delete(byType, id)
	}
}

func (e *legacyTestEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	norm := strings.ToLower(label)
	ids := e.labelIndex[norm]
	out := make([]*storage.Node, 0, len(ids))
	for id := range ids {
		if n := e.nodes[id]; n != nil {
			out = append(out, storage.CopyNode(n))
		}
	}
	return out, nil
}

func (e *legacyTestEngine) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	nodes, err := e.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (e *legacyTestEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	outIDs := e.outgoingEdges[nodeID]
	out := make([]*storage.Edge, 0, len(outIDs))
	for id := range outIDs {
		if edge := e.edges[id]; edge != nil {
			out = append(out, storage.CopyEdge(edge))
		}
	}
	return out, nil
}

func (e *legacyTestEngine) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	inIDs := e.incomingEdges[nodeID]
	out := make([]*storage.Edge, 0, len(inIDs))
	for id := range inIDs {
		if edge := e.edges[id]; edge != nil {
			out = append(out, storage.CopyEdge(edge))
		}
	}
	return out, nil
}

func (e *legacyTestEngine) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	out, err := e.GetOutgoingEdges(startID)
	if err != nil {
		return nil, err
	}
	var filtered []*storage.Edge
	for _, edge := range out {
		if edge.EndNode == endID {
			filtered = append(filtered, edge)
		}
	}
	return filtered, nil
}

func (e *legacyTestEngine) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	edges, _ := e.GetEdgesBetween(startID, endID)
	for _, edge := range edges {
		if edge.Type == edgeType {
			return edge
		}
	}
	return nil
}

func (e *legacyTestEngine) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ids := e.edgeTypeIndex[edgeType]
	out := make([]*storage.Edge, 0, len(ids))
	for id := range ids {
		if edge := e.edges[id]; edge != nil {
			out = append(out, storage.CopyEdge(edge))
		}
	}
	return out, nil
}

func (e *legacyTestEngine) AllNodes() ([]*storage.Node, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*storage.Node, 0, len(e.nodes))
	for _, node := range e.nodes {
		out = append(out, storage.CopyNode(node))
	}
	return out, nil
}

func (e *legacyTestEngine) AllEdges() ([]*storage.Edge, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*storage.Edge, 0, len(e.edges))
	for _, edge := range e.edges {
		out = append(out, storage.CopyEdge(edge))
	}
	return out, nil
}

func (e *legacyTestEngine) GetAllNodes() []*storage.Node {
	nodes, _ := e.AllNodes()
	return nodes
}

func (e *legacyTestEngine) GetInDegree(nodeID storage.NodeID) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.incomingEdges[nodeID])
}

func (e *legacyTestEngine) GetOutDegree(nodeID storage.NodeID) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.outgoingEdges[nodeID])
}

func (e *legacyTestEngine) GetSchema() *storage.SchemaManager {
	return &e.schema
}

func (e *legacyTestEngine) BulkCreateNodes(nodes []*storage.Node) error {
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if _, err := e.CreateNode(n); err != nil && err != storage.ErrAlreadyExists {
			return err
		}
	}
	return nil
}

func (e *legacyTestEngine) BulkCreateEdges(edges []*storage.Edge) error {
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if err := e.CreateEdge(edge); err != nil && err != storage.ErrAlreadyExists {
			return err
		}
	}
	return nil
}

func (e *legacyTestEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		_ = e.DeleteNode(id)
	}
	return nil
}

func (e *legacyTestEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		_ = e.DeleteEdge(id)
	}
	return nil
}

func (e *legacyTestEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	result := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		node, err := e.GetNode(id)
		if err == nil {
			result[id] = node
		}
	}
	return result, nil
}

func (e *legacyTestEngine) Close() error { return nil }

func (e *legacyTestEngine) NodeCount() (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return int64(len(e.nodes)), nil
}

func (e *legacyTestEngine) EdgeCount() (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return int64(len(e.edges)), nil
}

func (e *legacyTestEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.nodes {
		if strings.HasPrefix(string(id), prefix) {
			delete(e.nodes, id)
			nodesDeleted++
		}
	}
	for id := range e.edges {
		if strings.HasPrefix(string(id), prefix) {
			delete(e.edges, id)
			edgesDeleted++
		}
	}
	return nodesDeleted, edgesDeleted, nil
}
