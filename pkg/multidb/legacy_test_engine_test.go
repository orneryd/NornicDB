package multidb

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/internal/collection"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Legacy seeds retain the genuinely unprefixed old layout. Every admitted
// prefixed/catalog write uses the real native transaction/receipt owner, so the
// migration fixture exercises the current storage contract without simulating it.
type legacyTestEngine struct {
	*storage.MemoryEngine
	mu    sync.RWMutex
	nodes map[storage.NodeID]*storage.Node
	edges map[storage.EdgeID]*storage.Edge
}

func newLegacyTestEngine(t testing.TB) *legacyTestEngine {
	return &legacyTestEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nodes: map[storage.NodeID]*storage.Node{}, edges: map[storage.EdgeID]*storage.Edge{}}
}
func (e *legacyTestEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }
func (e *legacyTestEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if node == nil || node.ID == "" {
		return "", storage.ErrInvalidID
	}
	if strings.Contains(string(node.ID), ":") {
		return e.MemoryEngine.CreateNode(node)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.nodes[node.ID] != nil {
		return "", storage.ErrAlreadyExists
	}
	e.nodes[node.ID] = storage.CopyNode(node)
	return node.ID, nil
}
func (e *legacyTestEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetNode(id)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	node := e.nodes[id]
	if node == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CopyNode(node), nil
}
func (e *legacyTestEngine) UpdateNode(node *storage.Node) error {
	if node == nil || node.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(node.ID), ":") {
		return e.MemoryEngine.UpdateNode(node)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.nodes[node.ID] == nil {
		return storage.ErrNotFound
	}
	e.nodes[node.ID] = storage.CopyNode(node)
	return nil
}
func (e *legacyTestEngine) DeleteNode(id storage.NodeID) error {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.DeleteNode(id)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.nodes[id] == nil {
		return storage.ErrNotFound
	}
	delete(e.nodes, id)
	for key, edge := range e.edges {
		if edge.StartNode == id || edge.EndNode == id {
			delete(e.edges, key)
		}
	}
	return nil
}
func (e *legacyTestEngine) CreateEdge(edge *storage.Edge) error {
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(edge.ID), ":") {
		return e.MemoryEngine.CreateEdge(edge)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edges[edge.ID] != nil {
		return storage.ErrAlreadyExists
	}
	e.edges[edge.ID] = storage.CopyEdge(edge)
	return nil
}
func (e *legacyTestEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetEdge(id)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	edge := e.edges[id]
	if edge == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CopyEdge(edge), nil
}
func (e *legacyTestEngine) UpdateEdge(edge *storage.Edge) error {
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(edge.ID), ":") {
		return e.MemoryEngine.UpdateEdge(edge)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edges[edge.ID] == nil {
		return storage.ErrNotFound
	}
	e.edges[edge.ID] = storage.CopyEdge(edge)
	return nil
}
func (e *legacyTestEngine) DeleteEdge(id storage.EdgeID) error {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.DeleteEdge(id)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edges[id] == nil {
		return storage.ErrNotFound
	}
	delete(e.edges, id)
	return nil
}
func (e *legacyTestEngine) AllNodes() ([]*storage.Node, error) {
	result, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, node := range e.nodes {
		result = append(result, storage.CopyNode(node))
	}
	return result, nil
}
func (e *legacyTestEngine) AllEdges() ([]*storage.Edge, error) {
	result, err := e.MemoryEngine.AllEdges()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, edge := range e.edges {
		result = append(result, storage.CopyEdge(edge))
	}
	return result, nil
}
func (e *legacyTestEngine) GetAllNodes() []*storage.Node { nodes, _ := e.AllNodes(); return nodes }
func (e *legacyTestEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if visit == nil {
		return storage.ErrInvalidData
	}
	if err := e.MemoryEngine.StreamNodesWithOptions(ctx, opts, visit); err != nil {
		return err
	}
	e.mu.RLock()
	nodes := make([]*storage.Node, 0, len(e.nodes))
	for _, node := range e.nodes {
		nodes = append(nodes, storage.CopyNode(node))
	}
	e.mu.RUnlock()
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if opts.Prefix != "" && !strings.HasPrefix(string(node.ID), opts.Prefix) {
			continue
		}
		if opts.Projection != nil {
			properties := map[string]interface{}{}
			for _, name := range opts.Projection {
				if value, ok := node.Properties[name]; ok {
					properties[name] = value
				}
			}
			node.Properties = properties
		} else if opts.StripEmbeddings || (!opts.WithEmbeddings && !opts.ApplyDecayFilter) {
			node.ChunkEmbeddings = nil
			node.NamedEmbeddings = nil
		}
		if err := visit(node); err != nil {
			if err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}
func (e *legacyTestEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := e.AllNodes()
	if err != nil {
		return nil, err
	}
	var result []*storage.Node
	for _, node := range nodes {
		for _, name := range node.Labels {
			if strings.EqualFold(name, label) {
				result = append(result, node)
				break
			}
		}
	}
	return result, nil
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
func (e *legacyTestEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetOutgoingEdges(id)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []*storage.Edge
	for _, edge := range e.edges {
		if edge.StartNode == id {
			result = append(result, storage.CopyEdge(edge))
		}
	}
	return result, nil
}
func (e *legacyTestEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetIncomingEdges(id)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []*storage.Edge
	for _, edge := range e.edges {
		if edge.EndNode == id {
			result = append(result, storage.CopyEdge(edge))
		}
	}
	return result, nil
}
func (e *legacyTestEngine) GetEdgesBetween(start, end storage.NodeID) ([]*storage.Edge, error) {
	edges, err := e.GetOutgoingEdges(start)
	if err != nil {
		return nil, err
	}
	var result []*storage.Edge
	for _, edge := range edges {
		if edge.EndNode == end {
			result = append(result, edge)
		}
	}
	return result, nil
}
func (e *legacyTestEngine) GetEdgeBetween(start, end storage.NodeID, kind string) *storage.Edge {
	edges, _ := e.GetEdgesBetween(start, end)
	for _, edge := range edges {
		if edge.Type == kind {
			return edge
		}
	}
	return nil
}
func (e *legacyTestEngine) GetEdgesByType(kind string) ([]*storage.Edge, error) {
	edges, err := e.AllEdges()
	if err != nil {
		return nil, err
	}
	var result []*storage.Edge
	for _, edge := range edges {
		if edge.Type == kind {
			result = append(result, edge)
		}
	}
	return result, nil
}
func (e *legacyTestEngine) GetInDegree(id storage.NodeID) int {
	edges, _ := e.GetIncomingEdges(id)
	return len(edges)
}
func (e *legacyTestEngine) GetOutDegree(id storage.NodeID) int {
	edges, _ := e.GetOutgoingEdges(id)
	return len(edges)
}
func (e *legacyTestEngine) BulkCreateNodes(nodes []*storage.Node) error {
	return collection.ApplyInOrder(nodes, func(node *storage.Node) error { _, err := e.CreateNode(node); return err })
}
func (e *legacyTestEngine) BulkCreateEdges(edges []*storage.Edge) error {
	return collection.ApplyInOrder(edges, e.CreateEdge)
}
func (e *legacyTestEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	return collection.ApplyInOrder(ids, e.DeleteNode)
}
func (e *legacyTestEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	return collection.ApplyInOrder(ids, e.DeleteEdge)
}
func (e *legacyTestEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	result := map[storage.NodeID]*storage.Node{}
	for _, id := range ids {
		node, err := e.GetNode(id)
		if err == storage.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		result[id] = node
	}
	return result, nil
}
func (e *legacyTestEngine) NodeCount() (int64, error) {
	nodes, err := e.AllNodes()
	return int64(len(nodes)), err
}
func (e *legacyTestEngine) EdgeCount() (int64, error) {
	edges, err := e.AllEdges()
	return int64(len(edges)), err
}
func (e *legacyTestEngine) EdgeCountByType(kind string) (int64, error) {
	edges, err := e.GetEdgesByType(kind)
	return int64(len(edges)), err
}
func (e *legacyTestEngine) edgeCountByEndpointLabel(label, kind string, start bool) (int64, error) {
	edges, err := e.GetEdgesByType(kind)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		id := edge.EndNode
		if start {
			id = edge.StartNode
		}
		node, err := e.GetNode(id)
		if err != nil {
			continue
		}
		for _, name := range node.Labels {
			if strings.EqualFold(name, label) {
				count++
				break
			}
		}
	}
	return count, nil
}
func (e *legacyTestEngine) EdgeCountByStartLabel(label, kind string) (int64, error) {
	return e.edgeCountByEndpointLabel(label, kind, true)
}
func (e *legacyTestEngine) EdgeCountByEndLabel(label, kind string) (int64, error) {
	return e.edgeCountByEndpointLabel(label, kind, false)
}
func (e *legacyTestEngine) DeleteByPrefix(prefix string) (nodesDeleted, edgesDeleted int64, err error) {
	edges, err := e.AllEdges()
	if err != nil {
		return 0, 0, err
	}
	for _, edge := range edges {
		if strings.HasPrefix(string(edge.ID), prefix) {
			if err = e.DeleteEdge(edge.ID); err != nil {
				return
			}
			edgesDeleted++
		}
	}
	nodes, err := e.AllNodes()
	if err != nil {
		return
	}
	for _, node := range nodes {
		if strings.HasPrefix(string(node.ID), prefix) {
			if err = e.DeleteNode(node.ID); err != nil {
				return
			}
			nodesDeleted++
		}
	}
	return
}
