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
	owner testing.TB
	mu    sync.RWMutex
	nodes map[storage.NodeID]*storage.Node
	edges map[storage.EdgeID]*storage.Edge
}

func newLegacyTestEngine(t testing.TB) *legacyTestEngine {
	return &legacyTestEngine{owner: t, MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nodes: map[storage.NodeID]*storage.Node{}, edges: map[storage.EdgeID]*storage.Edge{}}
}
func (e *legacyTestEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }
func (e *legacyTestEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	if node == nil || node.ID == "" {
		return "", storage.ErrInvalidID
	}
	if strings.Contains(string(node.ID), ":") {
		return e.MemoryEngine.CreateNode(ctx, node)
	}
	if err := writeLegacySeed(e, e.nodes, node.ID, node, legacyInsert, storage.CopyNode); err != nil {
		return "", err
	}
	return node.ID, nil
}
func (e *legacyTestEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetNode(ctx, id)
	}
	return readLegacySeed(e, e.nodes, id, storage.CopyNode)
}
func (e *legacyTestEngine) UpdateNode(ctx context.Context, updateRequest storage.NodeUpdateRequest) error {
	node := updateRequest.After
	if node == nil || node.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(node.ID), ":") {
		return e.MemoryEngine.UpdateNode(ctx, updateRequest)
	}
	return writeLegacySeed(e, e.nodes, node.ID, node, legacyReplace, storage.CopyNode)
}
func (e *legacyTestEngine) DeleteNode(ctx context.Context, id storage.NodeID) error {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.DeleteNode(ctx, id)
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
func (e *legacyTestEngine) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(edge.ID), ":") {
		return e.MemoryEngine.CreateEdge(ctx, edge)
	}
	return writeLegacySeed(e, e.edges, edge.ID, edge, legacyInsert, storage.CopyEdge)
}
func (e *legacyTestEngine) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.GetEdge(ctx, id)
	}
	return readLegacySeed(e, e.edges, id, storage.CopyEdge)
}
func (e *legacyTestEngine) UpdateEdge(ctx context.Context, updateRequest storage.EdgeUpdateRequest) error {
	edge := updateRequest.After
	if edge == nil || edge.ID == "" {
		return storage.ErrInvalidID
	}
	if strings.Contains(string(edge.ID), ":") {
		return e.MemoryEngine.UpdateEdge(ctx, updateRequest)
	}
	return writeLegacySeed(e, e.edges, edge.ID, edge, legacyReplace, storage.CopyEdge)
}
func (e *legacyTestEngine) DeleteEdge(ctx context.Context, id storage.EdgeID) error {
	if strings.Contains(string(id), ":") {
		return e.MemoryEngine.DeleteEdge(ctx, id)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.edges[id] == nil {
		return storage.ErrNotFound
	}
	delete(e.edges, id)
	return nil
}
func (e *legacyTestEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	result, err := e.MemoryEngine.AllNodes(ctx)
	return appendLegacySeeds(e, e.nodes, result, err, storage.CopyNode)
}
func (e *legacyTestEngine) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	result, err := e.MemoryEngine.AllEdges(ctx)
	return appendLegacySeeds(e, e.edges, result, err, storage.CopyEdge)
}
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
func (e *legacyTestEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	nodes, err := e.AllNodes(ctx)
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
func (e *legacyTestEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	nodes, err := e.GetNodesByLabel(ctx, label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}
func (e *legacyTestEngine) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return e.edgesForEndpoint(ctx, id, true)
}
func (e *legacyTestEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return e.edgesForEndpoint(ctx, id, false)
}
func (e *legacyTestEngine) GetEdgesBetween(ctx context.Context, start, end storage.NodeID) ([]*storage.Edge, error) {
	edges, err := e.GetOutgoingEdges(ctx, start)
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
func (e *legacyTestEngine) GetEdgeBetween(ctx context.Context, start, end storage.NodeID, kind string) (*storage.Edge, error) {
	edges, err := e.GetEdgesBetween(ctx, start, end)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if edge.Type == kind {
			return edge, nil
		}
	}
	return nil, nil
}
func (e *legacyTestEngine) GetEdgesByType(ctx context.Context, kind string) ([]*storage.Edge, error) {
	edges, err := e.AllEdges(ctx)
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
func (e *legacyTestEngine) GetInDegree(ctx context.Context, id storage.NodeID) (int, error) {
	edges, err := e.GetIncomingEdges(ctx, id)
	return len(edges), err
}
func (e *legacyTestEngine) GetOutDegree(ctx context.Context, id storage.NodeID) (int, error) {
	edges, err := e.GetOutgoingEdges(ctx, id)
	return len(edges), err
}
func applyLegacyCreates[T any](ctx context.Context, items []*T, create func(context.Context, *T) error) error {
	return collection.ApplyInOrder(items, func(item *T) error {
		if item == nil {
			return nil
		}
		if err := create(ctx, item); err != nil && err != storage.ErrAlreadyExists {
			return err
		}
		return nil
	})
}

func applyLegacyDeletes[T any](ctx context.Context, ids []T, remove func(context.Context, T) error) error {
	return collection.ApplyInOrder(ids, func(id T) error {
		_ = remove(ctx, id)
		return nil
	})
}

func (e *legacyTestEngine) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	return applyLegacyCreates(ctx, nodes, func(ctx context.Context, node *storage.Node) error {
		_, err := e.CreateNode(ctx, node)
		return err
	})
}
func (e *legacyTestEngine) BulkCreateEdges(ctx context.Context, edges []*storage.Edge) error {
	return applyLegacyCreates(ctx, edges, e.CreateEdge)
}
func (e *legacyTestEngine) BulkDeleteNodes(ctx context.Context, ids []storage.NodeID) error {
	return applyLegacyDeletes(ctx, ids, e.DeleteNode)
}
func (e *legacyTestEngine) BulkDeleteEdges(ctx context.Context, ids []storage.EdgeID) error {
	return applyLegacyDeletes(ctx, ids, e.DeleteEdge)
}
func (e *legacyTestEngine) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	result := map[storage.NodeID]*storage.Node{}
	for _, id := range ids {
		node, err := e.GetNode(ctx, id)
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
func (e *legacyTestEngine) NodeCount(ctx context.Context) (int64, error) {
	nodes, err := e.AllNodes(ctx)
	return int64(len(nodes)), err
}
func (e *legacyTestEngine) EdgeCount(ctx context.Context) (int64, error) {
	edges, err := e.AllEdges(ctx)
	return int64(len(edges)), err
}
func (e *legacyTestEngine) EdgeCountByType(ctx context.Context, kind string) (int64, error) {
	edges, err := e.GetEdgesByType(ctx, kind)
	return int64(len(edges)), err
}
func (e *legacyTestEngine) edgeCountByEndpointLabel(ctx context.Context, label, kind string, start bool) (int64, error) {
	edges, err := e.GetEdgesByType(ctx, kind)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, edge := range edges {
		id := edge.EndNode
		if start {
			id = edge.StartNode
		}
		node, err := e.GetNode(ctx, id)
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
func (e *legacyTestEngine) EdgeCountByStartLabel(ctx context.Context, label, kind string) (int64, error) {
	return e.edgeCountByEndpointLabel(ctx, label, kind, true)
}
func (e *legacyTestEngine) EdgeCountByEndLabel(ctx context.Context, label, kind string) (int64, error) {
	return e.edgeCountByEndpointLabel(ctx, label, kind, false)
}
func (e *legacyTestEngine) DeleteByPrefix(prefix string) (nodesDeleted, edgesDeleted int64, err error) {
	edges, err := e.AllEdges(e.owner.Context())
	if err != nil {
		return 0, 0, err
	}
	for _, edge := range edges {
		if strings.HasPrefix(string(edge.ID), prefix) {
			if err = e.DeleteEdge(e.owner.Context(), edge.ID); err != nil {
				return
			}
			edgesDeleted++
		}
	}
	nodes, err := e.AllNodes(e.owner.Context())
	if err != nil {
		return
	}
	for _, node := range nodes {
		if strings.HasPrefix(string(node.ID), prefix) {
			if err = e.DeleteNode(e.owner.Context(), node.ID); err != nil {
				return
			}
			nodesDeleted++
		}
	}
	return
}

type legacySeedWrite uint8

const (
	legacyInsert legacySeedWrite = iota
	legacyReplace
)

// writeLegacySeed owns raw unprefixed fixture publication. Prefixed operations
// retain the real native engine/receipt path in the required Engine adapters.
func writeLegacySeed[K comparable, T any](engine *legacyTestEngine, records map[K]*T, id K, entity *T, mode legacySeedWrite, copyEntity func(*T) *T) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	exists := records[id] != nil
	if mode == legacyInsert && exists {
		return storage.ErrAlreadyExists
	}
	if mode == legacyReplace && !exists {
		return storage.ErrNotFound
	}
	records[id] = copyEntity(entity)
	return nil
}

// Legacy reads share the same locked copy boundary for nodes and edges.
func readLegacySeed[K comparable, T any](engine *legacyTestEngine, records map[K]*T, id K, copyEntity func(*T) *T) (*T, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	entity := records[id]
	if entity == nil {
		return nil, storage.ErrNotFound
	}
	return copyEntity(entity), nil
}

func appendLegacySeeds[K comparable, T any](engine *legacyTestEngine, records map[K]*T, nativeResult []*T, nativeErr error, copyEntity func(*T) *T) ([]*T, error) {
	if nativeErr != nil {
		return nil, nativeErr
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	for _, entity := range records {
		nativeResult = append(nativeResult, copyEntity(entity))
	}
	return nativeResult, nil
}

func (e *legacyTestEngine) edgesForEndpoint(ctx context.Context, id storage.NodeID, outgoing bool) ([]*storage.Edge, error) {
	if strings.Contains(string(id), ":") {
		if outgoing {
			return e.MemoryEngine.GetOutgoingEdges(ctx, id)
		}
		return e.MemoryEngine.GetIncomingEdges(ctx, id)
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []*storage.Edge
	for _, edge := range e.edges {
		var matches bool
		if outgoing {
			matches = edge.StartNode == id
		} else {
			matches = edge.EndNode == id
		}
		if matches {
			result = append(result, storage.CopyEdge(edge))
		}
	}
	return result, nil
}
