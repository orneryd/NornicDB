package nornicdb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
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

func (e *apocCreateErrorEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.createNodeErr != nil {
		return "", e.createNodeErr
	}
	return e.apocAdapterTestEngine.CreateNode(node)
}

func (e *apocCreateErrorEngine) CreateEdge(edge *storage.Edge) error {
	if e.createEdgeErr != nil {
		return e.createEdgeErr
	}
	return e.apocAdapterTestEngine.CreateEdge(edge)
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

func (e *apocAdapterTestEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
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

func (e *apocAdapterTestEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, ok := e.nodes[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneNode(node), nil
}

func (e *apocAdapterTestEngine) UpdateNode(node *storage.Node) error {
	if _, ok := e.nodes[node.ID]; !ok {
		return storage.ErrNotFound
	}
	e.nodes[node.ID] = cloneNode(node)
	return nil
}

func (e *apocAdapterTestEngine) DeleteNode(id storage.NodeID) error {
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

func (e *apocAdapterTestEngine) CreateEdge(edge *storage.Edge) error {
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

func (e *apocAdapterTestEngine) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, ok := e.edges[id]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return cloneEdge(edge), nil
}

func (e *apocAdapterTestEngine) UpdateEdge(edge *storage.Edge) error {
	if _, ok := e.edges[edge.ID]; !ok {
		return storage.ErrNotFound
	}
	e.edges[edge.ID] = cloneEdge(edge)
	return nil
}

func (e *apocAdapterTestEngine) DeleteEdge(id storage.EdgeID) error {
	if _, ok := e.edges[id]; !ok {
		return storage.ErrNotFound
	}
	delete(e.edges, id)
	return nil
}

func (e *apocAdapterTestEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
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

func (e *apocAdapterTestEngine) GetFirstNodeByLabel(label string) (*storage.Node, error) {
	nodes, _ := e.GetNodesByLabel(label)
	if len(nodes) == 0 {
		return nil, storage.ErrNotFound
	}
	return nodes[0], nil
}

func (e *apocAdapterTestEngine) GetOutgoingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	var out []*storage.Edge
	for _, edge := range e.edges {
		if edge.StartNode == nodeID {
			out = append(out, cloneEdge(edge))
		}
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetIncomingEdges(nodeID storage.NodeID) ([]*storage.Edge, error) {
	var out []*storage.Edge
	for _, edge := range e.edges {
		if edge.EndNode == nodeID {
			out = append(out, cloneEdge(edge))
		}
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	var out []*storage.Edge
	for _, edge := range e.edges {
		if edge.StartNode == startID && edge.EndNode == endID {
			out = append(out, cloneEdge(edge))
		}
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetEdgeBetween(startID, endID storage.NodeID, edgeType string) *storage.Edge {
	for _, edge := range e.edges {
		if edge.StartNode == startID && edge.EndNode == endID && edge.Type == edgeType {
			return cloneEdge(edge)
		}
	}
	return nil
}

func (e *apocAdapterTestEngine) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	var out []*storage.Edge
	for _, edge := range e.edges {
		if edge.Type == edgeType {
			out = append(out, cloneEdge(edge))
		}
	}
	return out, nil
}

func (e *apocAdapterTestEngine) AllNodes() ([]*storage.Node, error) {
	var out []*storage.Node
	for _, node := range e.nodes {
		out = append(out, cloneNode(node))
	}
	return out, nil
}

func (e *apocAdapterTestEngine) AllEdges() ([]*storage.Edge, error) {
	var out []*storage.Edge
	for _, edge := range e.edges {
		out = append(out, cloneEdge(edge))
	}
	return out, nil
}

func (e *apocAdapterTestEngine) GetAllNodes() []*storage.Node {
	nodes, _ := e.AllNodes()
	return nodes
}

func (e *apocAdapterTestEngine) GetInDegree(nodeID storage.NodeID) int {
	edges, _ := e.GetIncomingEdges(nodeID)
	return len(edges)
}

func (e *apocAdapterTestEngine) GetOutDegree(nodeID storage.NodeID) int {
	edges, _ := e.GetOutgoingEdges(nodeID)
	return len(edges)
}

func (e *apocAdapterTestEngine) GetSchema() *storage.SchemaManager { return e.schema }
func (e *apocAdapterTestEngine) BulkCreateNodes(nodes []*storage.Node) error {
	for _, node := range nodes {
		if _, err := e.CreateNode(node); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkCreateEdges(edges []*storage.Edge) error {
	for _, edge := range edges {
		if err := e.CreateEdge(edge); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := e.DeleteNode(id); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := e.DeleteEdge(id); err != nil {
			return err
		}
	}
	return nil
}
func (e *apocAdapterTestEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	out := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		if node, ok := e.nodes[id]; ok {
			out[id] = cloneNode(node)
		}
	}
	return out, nil
}
func (e *apocAdapterTestEngine) Close() error { return nil }
func (e *apocAdapterTestEngine) NodeCount() (int64, error) {
	return int64(len(e.nodes)), nil
}
func (e *apocAdapterTestEngine) EdgeCount() (int64, error) {
	return int64(len(e.edges)), nil
}

// EdgeCountByType answers from the in-memory edge map (issue #638 Engine
// contract): a typed count never materializes edges on the read path.
func (e *apocAdapterTestEngine) EdgeCountByType(edgeType string) (int64, error) {
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
func (e *apocAdapterTestEngine) edgeCountByEndpointLabel(label, edgeType string, start bool) int64 {
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
	return count
}

func (e *apocAdapterTestEngine) EdgeCountByStartLabel(label, edgeType string) (int64, error) {
	return e.edgeCountByEndpointLabel(label, edgeType, true), nil
}

func (e *apocAdapterTestEngine) EdgeCountByEndLabel(label, edgeType string) (int64, error) {
	return e.edgeCountByEndpointLabel(label, edgeType, false), nil
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

	node, err := adapter.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Alice"})
	require.NoError(t, err)
	require.NotZero(t, node.ID)
	require.Equal(t, "Alice", node.Properties["name"])

	storedNode, err := adapter.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, node.ID, storedNode.ID)

	require.NoError(t, adapter.UpdateNode(node.ID, map[string]interface{}{"age": 30}))
	storedNode, err = adapter.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, 30, storedNode.Properties["age"])

	require.NoError(t, adapter.AddLabels(node.ID, []string{"Employee", "Person"}))
	storedNode, err = adapter.GetNode(node.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Person", "Employee"}, storedNode.Labels)

	require.NoError(t, adapter.RemoveLabels(node.ID, []string{"Person"}))
	storedNode, err = adapter.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, []string{"Employee"}, storedNode.Labels)

	other, err := adapter.CreateNode([]string{"Person"}, map[string]interface{}{"name": "Bob"})
	require.NoError(t, err)
	rel, err := adapter.CreateRelationship(node.ID, other.ID, "KNOWS", map[string]interface{}{"since": 2020})
	require.NoError(t, err)
	require.Equal(t, "KNOWS", rel.Type)

	storedRel, err := adapter.GetRelationship(rel.ID)
	require.NoError(t, err)
	require.Equal(t, rel.ID, storedRel.ID)
	require.Equal(t, node.ID, storedRel.StartNode)
	require.Equal(t, other.ID, storedRel.EndNode)

	require.NoError(t, adapter.UpdateRelationship(rel.ID, map[string]interface{}{"weight": 0.5}))
	storedRel, err = adapter.GetRelationship(rel.ID)
	require.NoError(t, err)
	require.Equal(t, 0.5, storedRel.Properties["weight"])

	require.NoError(t, adapter.DeleteRelationship(rel.ID))
	_, err = adapter.GetRelationship(rel.ID)
	require.ErrorIs(t, err, apocstorage.ErrRelationshipNotFound)

	require.NoError(t, adapter.DeleteNode(node.ID))
	_, err = adapter.GetNode(node.ID)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
}

func TestAPOCStorageAdapter_UpdateInitializesNilPropertyMaps(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	engine.nodes["1"] = &storage.Node{ID: "1", Labels: []string{"Person"}}
	engine.edges["1"] = &storage.Edge{ID: "1", StartNode: "1", EndNode: "1", Type: "SELF"}
	adapter := NewAPOCStorageAdapter(engine)

	require.NotPanics(t, func() {
		require.NoError(t, adapter.UpdateNode(1, map[string]interface{}{"name": "Alice"}))
	})
	require.Equal(t, "Alice", engine.nodes["1"].Properties["name"])

	require.NotPanics(t, func() {
		require.NoError(t, adapter.UpdateRelationship(1, map[string]interface{}{"weight": 1.0}))
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

	outgoing, err := adapter.GetNodeRelationships(1, "LINK", apocstorage.DirectionOutgoing)
	require.NoError(t, err)
	require.Len(t, outgoing, 2)

	incoming, err := adapter.GetNodeRelationships(1, "", apocstorage.DirectionIncoming)
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, int64(14), incoming[0].ID)

	neighbors, err := adapter.GetNodeNeighbors(1, "", apocstorage.DirectionBoth)
	require.NoError(t, err)
	require.Len(t, neighbors, 3)

	degree, err := adapter.GetNodeDegree(1, "", apocstorage.DirectionBoth)
	require.NoError(t, err)
	require.Equal(t, 3, degree)

	samePath, err := adapter.FindShortestPath(1, 1, "", 3)
	require.NoError(t, err)
	require.Equal(t, 0, samePath.Length)

	shortest, err := adapter.FindShortestPath(1, 4, "LINK", 3)
	require.NoError(t, err)
	require.Equal(t, 2, shortest.Length)
	require.Equal(t, int64(1), shortest.Nodes[0].ID)
	require.Equal(t, int64(4), shortest.Nodes[len(shortest.Nodes)-1].ID)

	allPaths, err := adapter.FindAllPaths(1, 4, "LINK", 3)
	require.NoError(t, err)
	require.Len(t, allPaths, 2)

	var bfsOrder []int64
	require.NoError(t, adapter.BFS(1, "", 2, func(node *apocstorage.Node) bool {
		bfsOrder = append(bfsOrder, node.ID)
		return node.ID != 2
	}))
	require.NotEmpty(t, bfsOrder)
	require.Equal(t, int64(1), bfsOrder[0])
	require.Contains(t, bfsOrder, int64(2))

	var dfsOrder []int64
	require.NoError(t, adapter.DFS(1, "", 3, func(node *apocstorage.Node) bool {
		dfsOrder = append(dfsOrder, node.ID)
		return node.ID != 4
	}))
	require.Contains(t, dfsOrder, int64(4))

	_, err = adapter.FindShortestPath(2, 3, "BACK", 1)
	require.ErrorIs(t, err, apocstorage.ErrPathNotFound)
}

func TestAPOCStorageAdapter_TraversalMissingNodeBranches(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)
	engine.nodes["1"] = &storage.Node{ID: "1", Labels: []string{"Node"}}
	engine.nodes["3"] = &storage.Node{ID: "3", Labels: []string{"Node"}}
	engine.edges["10"] = &storage.Edge{ID: "10", StartNode: "1", EndNode: "2", Type: "LINK"}
	engine.edges["11"] = &storage.Edge{ID: "11", StartNode: "1", EndNode: "3", Type: "LINK"}

	_, err := adapter.FindShortestPath(99, 99, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	_, err = adapter.FindShortestPath(99, 3, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)

	path, err := adapter.FindShortestPath(1, 3, "LINK", 2)
	require.NoError(t, err)
	require.Equal(t, int64(3), path.Nodes[len(path.Nodes)-1].ID)
	_, err = adapter.FindShortestPath(1, 3, "LINK", 0)
	require.ErrorIs(t, err, apocstorage.ErrPathNotFound)

	_, err = adapter.FindAllPaths(99, 3, "", 1)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	paths, err := adapter.FindAllPaths(1, 3, "LINK", 2)
	require.NoError(t, err)
	require.Len(t, paths, 1)
	paths, err = adapter.FindAllPaths(1, 3, "LINK", 0)
	require.NoError(t, err)
	require.Empty(t, paths)

	var bfsVisited []int64
	require.NoError(t, adapter.BFS(99, "", 2, func(node *apocstorage.Node) bool {
		bfsVisited = append(bfsVisited, node.ID)
		return true
	}))
	require.Empty(t, bfsVisited)
	require.NoError(t, adapter.BFS(1, "", 0, func(node *apocstorage.Node) bool {
		bfsVisited = append(bfsVisited, node.ID)
		return true
	}))
	require.Equal(t, []int64{1}, bfsVisited)

	var dfsVisited []int64
	require.NoError(t, adapter.DFS(99, "", 2, func(node *apocstorage.Node) bool {
		dfsVisited = append(dfsVisited, node.ID)
		return true
	}))
	require.Empty(t, dfsVisited)
	require.NoError(t, adapter.DFS(1, "", 0, func(node *apocstorage.Node) bool {
		dfsVisited = append(dfsVisited, node.ID)
		return true
	}))
	require.Equal(t, []int64{1}, dfsVisited)
}

func TestAPOCStorageAdapter_ErrorMappings(t *testing.T) {
	engine := newAPOCAdapterTestEngine()
	adapter := NewAPOCStorageAdapter(engine)

	_, err := adapter.GetNode(99)
	require.ErrorIs(t, err, apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.UpdateNode(99, map[string]interface{}{"x": 1}), apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.AddLabels(99, []string{"X"}), apocstorage.ErrNodeNotFound)
	require.ErrorIs(t, adapter.RemoveLabels(99, []string{"X"}), apocstorage.ErrNodeNotFound)

	_, err = adapter.GetRelationship(99)
	require.ErrorIs(t, err, apocstorage.ErrRelationshipNotFound)
	require.ErrorIs(t, adapter.UpdateRelationship(99, map[string]interface{}{"x": 1}), apocstorage.ErrRelationshipNotFound)

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

		_, err := adapter.CreateNode([]string{"X"}, map[string]interface{}{"k": "v"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "create node boom")

		engine.createNodeErr = nil
		engine.createEdgeErr = fmt.Errorf("create edge boom")
		_, _ = adapter.CreateNode([]string{"X"}, nil)
		_, _ = adapter.CreateNode([]string{"X"}, nil)
		_, err = adapter.CreateRelationship(1, 2, "REL", nil)
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

		outgoingLikes, err := adapter.GetNodeDegree(1, "LIKES", apocstorage.DirectionOutgoing)
		require.NoError(t, err)
		require.Equal(t, 1, outgoingLikes)

		incomingLikes, err := adapter.GetNodeDegree(1, "LIKES", apocstorage.DirectionIncoming)
		require.NoError(t, err)
		require.Equal(t, 1, incomingLikes)

		allRels, err := adapter.GetNodeDegree(1, "", apocstorage.DirectionBoth)
		require.NoError(t, err)
		require.Equal(t, 3, allRels)
	})
}
