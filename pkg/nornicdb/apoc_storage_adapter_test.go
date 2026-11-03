package nornicdb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type apocAdapterTestEngine struct {
	nextNodeID int64
	nextEdgeID int64
	nodes      map[storage.NodeID]*storage.Node
	edges      map[storage.EdgeID]*storage.Edge
	schema     *storage.SchemaManager
}

// StreamNodesWithOptions satisfies the Engine streaming contract over the
// in-memory map with prefix scope, projection and embedding stripping applied.
func (e *apocAdapterTestEngine) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	if fn == nil {
		return storage.ErrInvalidData
	}
	for _, node := range e.nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if node == nil {
			continue
		}
		if opts.Prefix != "" && !strings.HasPrefix(string(node.ID), opts.Prefix) {
			continue
		}
		out := storage.CopyNode(node)
		if opts.Projection != nil {
			out.Properties = make(map[string]interface{}, len(opts.Projection))
			for _, property := range opts.Projection {
				if value, ok := node.Properties[property]; ok {
					out.Properties[property] = value
				}
			}
		} else if opts.StripEmbeddings || (!opts.WithEmbeddings && !opts.ApplyDecayFilter) {
			out.ChunkEmbeddings = nil
			out.NamedEmbeddings = nil
		}
		if err := fn(out); err != nil {
			if err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

type apocCreateErrorEngine struct {
	*apocAdapterTestEngine
	createNodeErr error
	createEdgeErr error
}

func newAPOCAdapterTestEngine() *apocAdapterTestEngine {
	return &apocAdapterTestEngine{
		nextNodeID: 1,
		nextEdgeID: 1,
		nodes:      make(map[storage.NodeID]*storage.Node),
		edges:      make(map[storage.EdgeID]*storage.Edge),
		schema:     storage.NewSchemaManager(),
	}
}

func (e *apocCreateErrorEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	if e.createNodeErr != nil {
		return "", e.createNodeErr
	}
	return e.apocAdapterTestEngine.CreateNode(ctx, node)
}

func (e *apocCreateErrorEngine) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	if e.createEdgeErr != nil {
		return e.createEdgeErr
	}
	return e.apocAdapterTestEngine.CreateEdge(ctx, edge)
}

func cloneNode(n *storage.Node) *storage.Node {
	if n == nil {
		return nil
	}
	labels := append([]string(nil), n.Labels...)
	props := make(map[string]interface{}, len(n.Properties))
	for k, v := range n.Properties {
		props[k] = v
	}
	return &storage.Node{ID: n.ID, Labels: labels, Properties: props}
}

func cloneEdge(e *storage.Edge) *storage.Edge {
	if e == nil {
		return nil
	}
	props := make(map[string]interface{}, len(e.Properties))
	for k, v := range e.Properties {
		props[k] = v
	}
	return &storage.Edge{
		ID:         e.ID,
		StartNode:  e.StartNode,
		EndNode:    e.EndNode,
		Type:       e.Type,
		Properties: props,
	}
}

// collectEdges walks one edge map once, cloning every edge the caller's
// predicate keeps. Nothing is preallocated and the zero-value nil slice is
// returned when no edge matches, so callers keep reporting an empty list rather
// than an allocated one, and results keep the underlying map order.
func collectEdges(edges map[storage.EdgeID]*storage.Edge, keep func(*storage.Edge) bool) []*storage.Edge {
	var out []*storage.Edge
	for _, edge := range edges {
		if keep(edge) {
			out = append(out, cloneEdge(edge))
		}
	}
	return out
}

func (e *apocAdapterTestEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return "", ctxError
	}

	stored := cloneNode(node)
	if stored.ID == "" {
		stored.ID = storage.NodeID(fmt.Sprintf("%d", e.nextNodeID))
		e.nextNodeID++
	}
	if stored.Properties == nil {
		stored.Properties = map[string]interface{}{}
	}
	e.nodes[stored.ID] = stored
	node.ID = stored.ID
	return stored.ID, nil
}

func (e *apocAdapterTestEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	node, ok := e.nodes[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneNode(node), nil
}

func (e *apocAdapterTestEngine) UpdateNode(ctx context.Context, updateRequest storage.NodeUpdateRequest) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	node := updateRequest.After
	if _, ok := e.nodes[node.ID]; !ok {
		return storage.ErrNotFound
	}
	e.nodes[node.ID] = cloneNode(node)
	return nil
}

func (e *apocAdapterTestEngine) DeleteNode(ctx context.Context, id storage.NodeID) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	if _, ok := e.nodes[id]; !ok {
		return storage.ErrNotFound
	}
	delete(e.nodes, id)
	for edgeID, edge := range e.edges {
		if edge.StartNode == id || edge.EndNode == id {
			delete(e.edges, edgeID)
		}
	}
	return nil
}

func (e *apocAdapterTestEngine) CreateEdge(ctx context.Context, edge *storage.Edge) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	stored := cloneEdge(edge)
	if stored.ID == "" {
		stored.ID = storage.EdgeID(fmt.Sprintf("%d", e.nextEdgeID))
		e.nextEdgeID++
	}
	if stored.Properties == nil {
		stored.Properties = map[string]interface{}{}
	}
	e.edges[stored.ID] = stored
	edge.ID = stored.ID
	return nil
}

func (e *apocAdapterTestEngine) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	edge, ok := e.edges[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneEdge(edge), nil
}

func (e *apocAdapterTestEngine) UpdateEdge(ctx context.Context, updateRequest storage.EdgeUpdateRequest) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	edge := updateRequest.After
	if _, ok := e.edges[edge.ID]; !ok {
		return storage.ErrNotFound
	}
	e.edges[edge.ID] = cloneEdge(edge)
	return nil
}

func (e *apocAdapterTestEngine) DeleteEdge(ctx context.Context, id storage.EdgeID) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	if _, ok := e.edges[id]; !ok {
		return storage.ErrNotFound
	}
	delete(e.edges, id)
	return nil
}

func (e *apocAdapterTestEngine) GetNodesByLabel(ctx context.Context, label string) ([]*storage.Node, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	var out []*storage.Node
	for _, node := range e.nodes {
		for _, l := range node.Labels {
			if l == label {
				out = append(out, cloneNode(node))
				break
			}
		}
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetFirstNodeByLabel(ctx context.Context, label string) (*storage.Node, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	nodes, err := e.GetNodesByLabel(ctx, label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (e *apocAdapterTestEngine) GetOutgoingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	return collectEdges(e.edges, func(edge *storage.Edge) bool {
		return edge.StartNode == nodeID
	}), nil
}

func (e *apocAdapterTestEngine) GetIncomingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	return collectEdges(e.edges, func(edge *storage.Edge) bool {
		return edge.EndNode == nodeID
	}), nil
}

func (e *apocAdapterTestEngine) GetEdgesBetween(ctx context.Context, startID, endID storage.NodeID) ([]*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	return collectEdges(e.edges, func(edge *storage.Edge) bool {
		return edge.StartNode == startID && edge.EndNode == endID
	}), nil
}

func (e *apocAdapterTestEngine) GetEdgeBetween(ctx context.Context, startID, endID storage.NodeID, edgeType string) (*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	for _, edge := range e.edges {
		if edge.StartNode == startID && edge.EndNode == endID && edge.Type == edgeType {
			return cloneEdge(edge), nil
		}
	}
	return nil, nil
}

func (e *apocAdapterTestEngine) GetEdgesByType(ctx context.Context, edgeType string) ([]*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	return collectEdges(e.edges, func(edge *storage.Edge) bool {
		return edge.Type == edgeType
	}), nil
}

func (e *apocAdapterTestEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	var out []*storage.Node
	for _, node := range e.nodes {
		out = append(out, cloneNode(node))
	}
	return out, nil
}

func (e *apocAdapterTestEngine) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	var out []*storage.Edge
	for _, edge := range e.edges {
		out = append(out, cloneEdge(edge))
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetInDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	edges, err := e.GetIncomingEdges(ctx, nodeID)
	return len(edges), err
}

func (e *apocAdapterTestEngine) GetOutDegree(ctx context.Context, nodeID storage.NodeID) (int, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	edges, err := e.GetOutgoingEdges(ctx, nodeID)
	return len(edges), err
}

func (e *apocAdapterTestEngine) GetSchema() *storage.SchemaManager { return e.schema }
func (e *apocAdapterTestEngine) BulkCreateNodes(ctx context.Context, nodes []*storage.Node) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	for _, node := range nodes {
		if _, err := e.CreateNode(ctx, node); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkCreateEdges(ctx context.Context, edges []*storage.Edge) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	for _, item := range edges {
		if err := e.CreateEdge(ctx, item); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkDeleteNodes(ctx context.Context, ids []storage.NodeID) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	for _, item := range ids {
		if err := e.DeleteNode(ctx, item); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkDeleteEdges(ctx context.Context, ids []storage.EdgeID) error {
	if ctxError := ctx.Err(); ctxError != nil {
		return ctxError
	}

	for _, item := range ids {
		if err := e.DeleteEdge(ctx, item); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return nil, ctxError
	}

	out := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		if node, ok := e.nodes[id]; ok {
			out[id] = cloneNode(node)
		}
	}
	return out, nil
}
func (e *apocAdapterTestEngine) Close() error { return nil }
func (e *apocAdapterTestEngine) NodeCount(ctx context.Context) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	return int64(len(e.nodes)), nil
}
func (e *apocAdapterTestEngine) EdgeCount(ctx context.Context) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	return int64(len(e.edges)), nil
}

// EdgeCountByType answers from the in-memory edge map (issue #638 Engine
// contract): a typed count never materializes edges on the read path.
func (e *apocAdapterTestEngine) EdgeCountByType(ctx context.Context, edgeType string) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	var count int64
	for _, edge := range e.edges {
		if edge != nil && strings.EqualFold(edge.Type, edgeType) {
			count++
		}
	}
	return count, nil
}

// edgeCountByEndpointLabel counts typed edges whose physical start (or end)
// endpoint node carries the label, over the in-memory maps.
func (e *apocAdapterTestEngine) edgeCountByEndpointLabel(ctx context.Context, label, edgeType string, start bool) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	var count int64
	for _, edge := range e.edges {
		if edge == nil || !strings.EqualFold(edge.Type, edgeType) {
			continue
		}
		nodeID := edge.EndNode
		if start {
			nodeID = edge.StartNode
		}
		node := e.nodes[nodeID]
		if node == nil {
			continue
		}
		for _, nodeLabel := range node.Labels {
			if strings.EqualFold(nodeLabel, label) {
				count++
				break
			}
		}
	}
	return count, nil
}

func (e *apocAdapterTestEngine) EdgeCountByStartLabel(ctx context.Context, label, edgeType string) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	return e.edgeCountByEndpointLabel(ctx, label, edgeType, true)
}

func (e *apocAdapterTestEngine) EdgeCountByEndLabel(ctx context.Context, label, edgeType string) (int64, error) {
	if ctxError := ctx.Err(); ctxError != nil {
		return 0, ctxError
	}

	return e.edgeCountByEndpointLabel(ctx, label, edgeType, false)
}
func (e *apocAdapterTestEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	var nodesDeleted, edgesDeleted int64
	for id := range e.nodes {
		if prefix == "" || strings.HasPrefix(string(id), prefix) {
			delete(e.nodes, id)
			nodesDeleted++
		}
	}
	for id := range e.edges {
		if prefix == "" || strings.HasPrefix(string(id), prefix) {
			delete(e.edges, id)
			edgesDeleted++
		}
	}
	return nodesDeleted, edgesDeleted, nil
}

func TestAPOCStorageAdapter_NodeAndRelationshipCRUD(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)

	node, err := adapter.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{"name": "Alice"})
	require.NoError(t, err)
	require.NotZero(t, node.ID)
	require.Equal(t, "Alice", node.Properties["name"])

	storedNode, err := adapter.GetNode(t.Context(), node.ID)
	require.NoError(t, err)
	require.Equal(t, node.ID, storedNode.ID)

	require.NoError(t, adapter.UpdateNode(t.Context(), node.ID, map[string]interface{}{"age": 30}))
	storedNode, err = adapter.GetNode(t.Context(), node.ID)
	require.NoError(t, err)
	require.Equal(t, 30, storedNode.Properties["age"])

	require.NoError(t, adapter.AddLabels(t.Context(), node.ID, []string{"Employee", "Person"}))
	storedNode, err = adapter.GetNode(t.Context(), node.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person", "Employee"}, storedNode.Labels)

	require.NoError(t, adapter.RemoveLabels(t.Context(), node.ID, []string{"Person"}))
	storedNode, err = adapter.GetNode(t.Context(), node.ID)
	require.NoError(t, err)
	require.Equal(t, []string{"Employee"}, storedNode.Labels)

	other, err := adapter.CreateNode(t.Context(), []string{"Person"}, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	rel, err := adapter.CreateRelationship(t.Context(), node.ID, other.ID, "KNOWS", map[string]interface{}{"since": 2020})
	require.NoError(t, err)
	require.Equal(t, "KNOWS", rel.Type)

	storedRel, err := adapter.GetRelationship(t.Context(), rel.ID)
	require.NoError(t, err)
	require.Equal(t, rel.ID, storedRel.ID)
	require.Equal(t, node.ID, storedRel.StartNode)
	require.Equal(t, other.ID, storedRel.EndNode)

	require.NoError(t, adapter.UpdateRelationship(t.Context(), rel.ID, map[string]interface{}{"weight": 0.5}))
	storedRel, err = adapter.GetRelationship(t.Context(), rel.ID)
	require.NoError(t, err)
	require.Equal(t, 0.5, storedRel.Properties["weight"])

	require.NoError(t, adapter.DeleteRelationship(t.Context(), rel.ID))
	_, err = adapter.GetRelationship(t.Context(), rel.ID)
	require.ErrorIs(t, err, apocstorage.ErrRelationshipNotFound)

	require.NoError(t, adapter.DeleteNode(t.Context(), node.ID))
	_, err = adapter.GetNode(t.Context(), node.ID)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
}

func TestAPOCStorageAdapter_UpdateInitializesNilPropertyMaps(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	engine.nodes["1"] = &storage.Node{ID: "1", Labels: []string{"Person"}}
	engine.edges["1"] = &storage.Edge{ID: "1", StartNode: "1", EndNode: "1", Type: "SELF"}
	adapter := NewAPOCStorageAdapter(engine)

	require.NotPanics(t, func() {
		require.NoError(t, adapter.UpdateNode(t.Context(), 1, map[string]interface{}{"name": "Alice"}))
	})
	require.Equal(t, "Alice", engine.nodes["1"].Properties["name"])

	require.NotPanics(t, func() {
		require.NoError(t, adapter.UpdateRelationship(t.Context(), 1, map[string]interface{}{"weight": 1.0}))
	})
	require.Equal(t, 1.0, engine.edges["1"].Properties["weight"])
}

func TestAPOCStorageAdapter_GraphTraversalAndPaths(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)

	for i := int64(1); i <= 4; i++ {
		engine.nodes[storage.NodeID(fmt.Sprintf("%d", i))] = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("%d", i)),
			Labels:     []string{"Node"},
			Properties: map[string]interface{}{"name": fmt.Sprintf("n%d", i)},
		}
	}
	for _, edge := range []*storage.Edge{
		{ID: "10", StartNode: "1", EndNode: "2", Type: "LINK"},
		{ID: "11", StartNode: "2", EndNode: "4", Type: "LINK"},
		{ID: "12", StartNode: "1", EndNode: "3", Type: "LINK"},
		{ID: "13", StartNode: "3", EndNode: "4", Type: "LINK"},
		{ID: "14", StartNode: "4", EndNode: "1", Type: "BACK"},
	} {
		engine.edges[edge.ID] = cloneEdge(edge)
	}

	outgoing, err := adapter.GetNodeRelationships(t.Context(), 1, "LINK", apocstorage.DirectionOutgoing)
	require.NoError(t, err)
	require.Len(t, outgoing, 2)

	incoming, err := adapter.GetNodeRelationships(t.Context(), 1, "", apocstorage.DirectionIncoming)
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, int64(14), incoming[0].ID)

	neighbors, err := adapter.GetNodeNeighbors(t.Context(), 1, "", apocstorage.DirectionBoth)
	require.NoError(t, err)
	require.Len(t, neighbors, 3)

	degree, err := adapter.GetNodeDegree(t.Context(), 1, "", apocstorage.DirectionBoth)
	require.NoError(t, err)
	require.Equal(t, 3, degree)

	samePath, err := adapter.FindShortestPath(t.Context(), 1, 1, "", 3)
	require.NoError(t, err)
	require.Equal(t, 0, samePath.Length)

	shortest, err := adapter.FindShortestPath(t.Context(), 1, 4, "LINK", 3)
	require.NoError(t, err)
	require.Equal(t, 2, shortest.Length)
	require.Equal(t, int64(1), shortest.Nodes[0].ID)
	require.Equal(t, int64(4), shortest.Nodes[len(shortest.Nodes)-1].ID)

	allPaths, err := adapter.FindAllPaths(t.Context(), 1, 4, "LINK", 3)
	require.NoError(t, err)
	require.Len(t, allPaths, 2)

	var bfsOrder []int64
	require.NoError(t, adapter.BFS(t.Context(), 1, "", 2, func(node *apocstorage.Node) bool {
		bfsOrder = append(bfsOrder, node.ID)
		return node.ID != 2
	}))
	require.NotEmpty(t, bfsOrder)
	require.Equal(t, int64(1), bfsOrder[0])
	require.Contains(t, bfsOrder, int64(2))

	var dfsOrder []int64
	require.NoError(t, adapter.DFS(t.Context(), 1, "", 3, func(node *apocstorage.Node) bool {
		dfsOrder = append(dfsOrder, node.ID)
		return node.ID != 4
	}))
	require.Contains(t, dfsOrder, int64(4))

	_, err = adapter.FindShortestPath(t.Context(), 2, 3, "BACK", 1)
	require.ErrorIs(t, err, apocstorage.ErrPathNotFound)
}

func TestAPOCStorageAdapter_TraversalMissingNodeBranches(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)
	engine.nodes["1"] = &storage.Node{ID: "1", Labels: []string{"Node"}}
	engine.nodes["3"] = &storage.Node{ID: "3", Labels: []string{"Node"}}
	engine.edges["10"] = &storage.Edge{ID: "10", StartNode: "1", EndNode: "2", Type: "LINK"}
	engine.edges["11"] = &storage.Edge{ID: "11", StartNode: "1", EndNode: "3", Type: "LINK"}

	_, err := adapter.FindShortestPath(t.Context(), 99, 99, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	_, err = adapter.FindShortestPath(t.Context(), 99, 3, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)

	path, err := adapter.FindShortestPath(t.Context(), 1, 3, "LINK", 2)
	require.NoError(t, err)
	require.Equal(t, int64(3), path.Nodes[len(path.Nodes)-1].ID)
	_, err = adapter.FindShortestPath(t.Context(), 1, 3, "LINK", 0)
	require.ErrorIs(t, err, apocstorage.ErrPathNotFound)
	// The only DEAD relationship leads to a missing node, so no path exists.
	engine.edges["12"] = &storage.Edge{ID: "12", StartNode: "1", EndNode: "2", Type: "DEAD"}
	_, err = adapter.FindShortestPath(t.Context(), 1, 3, "DEAD", 2)
	require.ErrorIs(t, err, apocstorage.ErrPathNotFound)
	delete(engine.edges, "12")

	_, err = adapter.FindAllPaths(t.Context(), 99, 3, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	paths, err := adapter.FindAllPaths(t.Context(), 1, 3, "LINK", 2)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	paths, err = adapter.FindAllPaths(t.Context(), 1, 3, "LINK", 0)
	require.NoError(t, err)
	require.Empty(t, paths)

	var bfsVisited []int64
	require.NoError(t, adapter.BFS(t.Context(), 99, "", 2, func(node *apocstorage.Node) bool {
		bfsVisited = append(bfsVisited, node.ID)
		return true
	}))
	require.Empty(t, bfsVisited)
	require.NoError(t, adapter.BFS(t.Context(), 1, "", 0, func(node *apocstorage.Node) bool {
		bfsVisited = append(bfsVisited, node.ID)
		return true
	}))
	require.Equal(t, []int64{1}, bfsVisited)

	var dfsVisited []int64
	require.NoError(t, adapter.DFS(t.Context(), 99, "", 2, func(node *apocstorage.Node) bool {
		dfsVisited = append(dfsVisited, node.ID)
		return true
	}))
	require.Empty(t, dfsVisited)
	require.NoError(t, adapter.DFS(t.Context(), 1, "", 0, func(node *apocstorage.Node) bool {
		dfsVisited = append(dfsVisited, node.ID)
		return true
	}))
	require.Equal(t, []int64{1}, dfsVisited)
}

func TestAPOCStorageAdapter_ErrorMappings(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)

	_, err := adapter.GetNode(t.Context(), 99)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.UpdateNode(t.Context(), 99, map[string]interface{}{"x": 1}), apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.AddLabels(t.Context(), 99, []string{"X"}), apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.RemoveLabels(t.Context(), 99, []string{"X"}), apocstorage.ErrNodeNotFound)

	_, err = adapter.GetRelationship(t.Context(), 99)
	require.ErrorIs(t, err, apocstorage.ErrRelationshipNotFound)
	require.ErrorIs(t, adapter.UpdateRelationship(t.Context(), 99, map[string]interface{}{"x": 1}), apocstorage.ErrRelationshipNotFound)

	node := adapter.convertNode(&storage.Node{ID: "abc", Labels: []string{"X"}, Properties: map[string]interface{}{"k": "v"}})
	assert.Equal(t, int64(0), node.ID)
	rel := adapter.convertRelationship(&storage.Edge{ID: "xyz", StartNode: "a", EndNode: "b", Type: "REL", Properties: map[string]interface{}{"k": "v"}})
	assert.Equal(t, int64(0), rel.ID)
	assert.Equal(t, int64(0), rel.StartNode)
	assert.Equal(t, int64(0), rel.EndNode)
}

func TestAPOCStorageAdapter_CreateErrorsAndDirectionalDegree(t *testing.T) {
	t.Run("create node and relationship propagate storage errors", func(t *testing.T) {
		engine := &apocCreateErrorEngine{
			apocAdapterTestEngine: newAPOCAdapterTestEngine(),
			createNodeErr:         fmt.Errorf("create node boom"),
		}
		adapter := NewAPOCStorageAdapter(engine)

		_, err := adapter.CreateNode(t.Context(), []string{"X"}, map[string]interface{}{"k": "v"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "create node boom")

		engine.createNodeErr = nil
		engine.createEdgeErr = fmt.Errorf("create edge boom")
		_, _ = adapter.CreateNode(t.Context(), []string{"X"}, nil)
		_, _ = adapter.CreateNode(t.Context(), []string{"X"}, nil)
		_, err = adapter.CreateRelationship(t.Context(), 1, 2, "REL", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "create edge boom")
	})

	t.Run("degree respects direction and relationship type filter", func(t *testing.T) {
		engine := newAPOCAdapterTestEngine()
		adapter := NewAPOCStorageAdapter(engine)
		for _, id := range []string{"1", "2", "3"} {
			engine.nodes[storage.NodeID(id)] = &storage.Node{ID: storage.NodeID(id), Labels: []string{"Node"}, Properties: map[string]interface{}{}}
		}
		engine.edges["10"] = &storage.Edge{ID: "10", StartNode: "1", EndNode: "2", Type: "KNOWS"}
		engine.edges["11"] = &storage.Edge{ID: "11", StartNode: "3", EndNode: "1", Type: "LIKES"}
		engine.edges["12"] = &storage.Edge{ID: "12", StartNode: "1", EndNode: "3", Type: "LIKES"}

		outgoingLikes, err := adapter.GetNodeDegree(t.Context(), 1, "LIKES", apocstorage.DirectionOutgoing)
		require.NoError(t, err)
		require.Equal(t, 1, outgoingLikes)

		incomingLikes, err := adapter.GetNodeDegree(t.Context(), 1, "LIKES", apocstorage.DirectionIncoming)
		require.NoError(t, err)
		require.Equal(t, 1, incomingLikes)

		allRels, err := adapter.GetNodeDegree(t.Context(), 1, "", apocstorage.DirectionBoth)
		require.NoError(t, err)
		require.Equal(t, 3, allRels)
	})
}

func TestAPOCStorageAdapterNativeDeleteCascadesIncidentRelationships(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	native := storage.NewNamespacedEngine(base, "apoc_delete_parity")
	for _, id := range []storage.NodeID{"1", "2", "3"} {
		_, err := native.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "out", StartNode: "1", EndNode: "2", Type: "R"},
		{ID: "in", StartNode: "3", EndNode: "1", Type: "R"},
		{ID: "self", StartNode: "1", EndNode: "1", Type: "R"},
	} {
		require.NoError(t, native.CreateEdge(t.Context(), edge))
	}
	adapter := NewAPOCStorageAdapter(native)
	require.NoError(t, adapter.DeleteNode(t.Context(), 1))
	relationships, err := native.AllEdges(t.Context())
	require.NoError(t, err)
	require.Empty(t, relationships)
	nodes, err := native.AllNodes(t.Context())
	require.NoError(t, err)
	require.Len(t, nodes, 2)
}

func TestAPOCStorageAdapterPreservesNativePointFailure(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	adapter := NewAPOCStorageAdapter(storage.NewNamespacedEngine(base, "apoc_native_failure"))
	require.NoError(t, base.Close())
	_, err := adapter.GetNode(t.Context(), 1)
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	_, err = adapter.GetRelationship(t.Context(), 1)
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	_, err = adapter.GetNodeRelationships(t.Context(), 1, "", apocstorage.DirectionBoth)
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	require.ErrorIs(t, adapter.UpdateNode(t.Context(), 1, nil), storage.ErrStorageClosed)
	require.ErrorIs(t, adapter.AddLabels(t.Context(), 1, []string{"L"}), storage.ErrStorageClosed)
	require.ErrorIs(t, adapter.RemoveLabels(t.Context(), 1, []string{"L"}), storage.ErrStorageClosed)
	require.ErrorIs(t, adapter.UpdateRelationship(t.Context(), 1, nil), storage.ErrStorageClosed)
}

func TestAPOCStorageAdapterNativeTraversalVisitorCancellation(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	native := storage.NewNamespacedEngine(base, "apoc_visitor_cancel")
	for _, id := range []storage.NodeID{"1", "2"} {
		_, err := native.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, native.CreateEdge(t.Context(), &storage.Edge{ID: "r", StartNode: "1", EndNode: "2", Type: "R"}))
	adapter := NewAPOCStorageAdapter(native)
	for _, traversal := range []string{"BFS", "DFS"} {
		for _, keepGoing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%t", traversal, keepGoing), func(t *testing.T) {
				request, cancel := context.WithCancel(t.Context())
				defer cancel()
				visits := 0
				visitor := func(*apocstorage.Node) bool { visits++; cancel(); return keepGoing }
				var err error
				if traversal == "BFS" {
					err = adapter.BFS(request, 1, "R", 0, visitor)
				} else {
					err = adapter.DFS(request, 1, "R", 0, visitor)
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, visits)
				visits = 0
				healthy := func(*apocstorage.Node) bool { visits++; return true }
				if traversal == "BFS" {
					err = adapter.BFS(t.Context(), 1, "R", 1, healthy)
				} else {
					err = adapter.DFS(t.Context(), 1, "R", 1, healthy)
				}
				require.NoError(t, err)
				require.Equal(t, 2, visits)
			})
		}
	}
}
