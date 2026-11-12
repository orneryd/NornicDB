package mcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mcpRelatedProbeEngine records the related-node read path of one MCP discover
// call and can hide nodes or cancel the caller's context mid-traversal.
type mcpRelatedProbeEngine struct {
	storage.Engine

	outgoing []string
	incoming []string
	nodes    []string
	missing  map[string]bool
	onNode   func(loaded int)
	outErr   error
	inErr    error
}

func (e *mcpRelatedProbeEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		e.incoming = append(e.incoming, string(request.Node))
		rows, err := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node})
		if e.inErr != nil {
			return rows, e.inErr
		}
		return rows, err
	}
	if request.Direction == storage.AdjacentBoth {
		incoming, err := e.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		outgoing, err := e.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		combined := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
		combined = append(combined, outgoing...)
		for _, edge := range incoming {
			if edge.StartNode == edge.EndNode {
				continue
			}
			combined = append(combined, edge)
		}
		return combined, nil
	}
	e.outgoing = append(e.outgoing, string(request.Node))
	rows, err := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: request.Node})
	if e.outErr != nil {
		return rows, e.outErr
	}
	return rows, err
}

func (e *mcpRelatedProbeEngine) GetIncomingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.incoming = append(e.incoming, string(nodeID))
	rows, err := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: nodeID})
	if e.inErr != nil {
		return rows, e.inErr
	}
	return rows, err
}

func (e *mcpRelatedProbeEngine) GetNode(ctx context.Context, nodeID storage.NodeID) (*storage.Node, error) {
	e.nodes = append(e.nodes, string(nodeID))
	if e.onNode != nil {
		e.onNode(len(e.nodes))
	}
	if e.missing[string(nodeID)] {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(ctx, nodeID)
}

func newMCPRelatedProbe(t *testing.T) (*mcpRelatedProbeEngine, storage.Engine) {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	return &mcpRelatedProbeEngine{Engine: base, missing: map[string]bool{}}, base
}

func createMCPRelatedNode(t *testing.T, engine storage.Engine, id storage.NodeID, props map[string]interface{}, labels ...string) {
	t.Helper()
	if len(labels) == 0 {
		labels = []string{"Doc"}
	}
	_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: labels, Properties: props})
	require.NoError(t, err)
}

func newMCPRelatedServer() *Server {
	server := NewServer(nil, nil)
	return server
}

func TestMCPRelatedNodesDepthBoundaryAndProjection(t *testing.T) {
	_, engine := newMCPRelatedProbe(t)
	createMCPRelatedNode(t, engine, "tenant:root", map[string]interface{}{"title": "Root"})
	createMCPRelatedNode(t, engine, "tenant:a", map[string]interface{}{"title": "A"}, "Child")
	createMCPRelatedNode(t, engine, "tenant:b", map[string]interface{}{"title": "B"})
	createMCPRelatedNode(t, engine, "tenant:c", map[string]interface{}{"name": "C Name"}, "Inbound")
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-ra", StartNode: "tenant:root", EndNode: "tenant:a", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-ab", StartNode: "tenant:a", EndNode: "tenant:b", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-cr", StartNode: "tenant:c", EndNode: "tenant:root", Type: "REFERENCES"}))

	server := newMCPRelatedServer()
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return engine, nil })
	ctx := ContextWithDatabase(context.Background(), "tenant")

	require.Nil(t, server.getRelatedNodes(ctx, "tenant:root", 1), "depth 1 means no related nodes")

	direct := server.getRelatedNodes(ctx, "tenant:root", 2)
	require.Len(t, direct, 2)
	assert.Equal(t, "tenant:a", localNodeIDFromAny(direct[0].ID), "MCP publishes element ids for stored ids")
	assert.Equal(t, "Child", direct[0].Type)
	assert.Equal(t, "A", direct[0].Title)
	assert.Equal(t, 1, direct[0].Distance)
	assert.Equal(t, "outgoing", direct[0].Direction)
	assert.Equal(t, "LINKS", direct[0].Relationship)
	assert.Equal(t, []string{server.nodeElementID(ctx, "tenant:root"), server.nodeElementID(ctx, "tenant:a")}, direct[0].Path)
	assert.Equal(t, "incoming", direct[1].Direction)
	assert.Equal(t, "REFERENCES", direct[1].Relationship)
	assert.Equal(t, "C Name", direct[1].Title, "MCP falls back from title to name")

	deeper := server.getRelatedNodes(ctx, "tenant:root", 3)
	require.Len(t, deeper, 3)
	assert.Equal(t, []int{1, 1, 2}, []int{deeper[0].Distance, deeper[1].Distance, deeper[2].Distance})
	assert.Equal(t, "tenant:b", localNodeIDFromAny(deeper[2].ID))
	assert.Equal(t, []string{
		server.nodeElementID(ctx, "tenant:root"),
		server.nodeElementID(ctx, "tenant:a"),
		server.nodeElementID(ctx, "tenant:b"),
	}, deeper[2].Path)
}

func TestMCPRelatedNodesEmitsAndExpandsMissingNode(t *testing.T) {
	probe, engine := newMCPRelatedProbe(t)
	createMCPRelatedNode(t, engine, "tenant:root", map[string]interface{}{"title": "Root"})
	createMCPRelatedNode(t, engine, "tenant:gone", map[string]interface{}{"title": "Gone"})
	createMCPRelatedNode(t, engine, "tenant:child", map[string]interface{}{"title": "Child"})
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-rg", StartNode: "tenant:root", EndNode: "tenant:gone", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-gc", StartNode: "tenant:gone", EndNode: "tenant:child", Type: "LINKS"}))
	probe.missing["tenant:gone"] = true

	server := newMCPRelatedServer()
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return probe, nil })
	ctx := ContextWithDatabase(context.Background(), "tenant")

	related := server.getRelatedNodes(ctx, "tenant:root", 3)
	require.Len(t, related, 2)
	assert.Equal(t, "tenant:gone", localNodeIDFromAny(related[0].ID))
	assert.Equal(t, "Node", related[0].Type, "MCP publishes a placeholder type for an unreadable node")
	assert.Empty(t, related[0].Title)
	assert.Equal(t, "tenant:child", localNodeIDFromAny(related[1].ID))
	assert.Equal(t, 2, related[1].Distance)
	assert.Contains(t, probe.outgoing, "tenant:gone", "MCP still expands a published missing node")
}

// TestMCPRelatedNodesKeepsRowsAlongsideAdjacencyReadErrors preserves the MCP
// contract this walk replaced: both directions were walked and the rows an
// errored read returned were published, with the MCP path and placeholder rules.
func TestMCPRelatedNodesKeepsRowsAlongsideAdjacencyReadErrors(t *testing.T) {
	probe, engine := newMCPRelatedProbe(t)
	createMCPRelatedNode(t, engine, "tenant:root", map[string]interface{}{"title": "Root"})
	createMCPRelatedNode(t, engine, "tenant:a", map[string]interface{}{"title": "A"})
	createMCPRelatedNode(t, engine, "tenant:c", map[string]interface{}{"title": "C"})
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-ra", StartNode: "tenant:root", EndNode: "tenant:a", Type: "LINKS"}))
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "tenant:e-cr", StartNode: "tenant:c", EndNode: "tenant:root", Type: "REFERENCES"}))
	probe.outErr = errors.New("outgoing adjacency read failed")
	probe.inErr = errors.New("incoming adjacency read failed")

	server := newMCPRelatedServer()
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return probe, nil })
	ctx := ContextWithDatabase(context.Background(), "tenant")

	related := server.getRelatedNodes(ctx, "tenant:root", 2)
	require.Len(t, related, 2, "both errored lists keep their rows")
	assert.Equal(t, "tenant:a", localNodeIDFromAny(related[0].ID))
	assert.Equal(t, "LINKS", related[0].Relationship)
	assert.Equal(t, "tenant:c", localNodeIDFromAny(related[1].ID))
	assert.Equal(t, "REFERENCES", related[1].Relationship)
	assert.Equal(t, []string{server.nodeElementID(ctx, "tenant:root"), server.nodeElementID(ctx, "tenant:a")}, related[0].Path)
	assert.Equal(t, []string{"tenant:root"}, probe.outgoing)
	assert.Equal(t, []string{"tenant:root"}, probe.incoming, "an incoming read happens even when the outgoing read errored")
}

// TestMCPRelatedNodesCapInsideOneAdjacency is a healthy control, not a repaired
// defect: the pre-merge MCP walk already enforced the 50-record cap inside both
// adjacency loops (see the server and query-executor tests for the consumers that
// only checked it between adjacency reads). It stays here so the converged walk
// cannot regress the cap MCP always had.
func TestMCPRelatedNodesCapInsideOneAdjacency(t *testing.T) {
	probe, engine := newMCPRelatedProbe(t)
	createMCPRelatedNode(t, engine, "tenant:root", map[string]interface{}{"title": "Root"})
	for i := 0; i < 60; i++ {
		id := storage.NodeID(fmt.Sprintf("tenant:n-%02d", i))
		createMCPRelatedNode(t, engine, id, map[string]interface{}{"title": string(id)})
		require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("tenant:e-%02d", i)), StartNode: "tenant:root", EndNode: id, Type: "LINKS"}))
	}

	server := newMCPRelatedServer()
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return probe, nil })
	ctx := ContextWithDatabase(context.Background(), "tenant")

	related := server.getRelatedNodes(ctx, "tenant:root", 2)
	require.Len(t, related, 50)
	assert.Equal(t, "tenant:n-00", localNodeIDFromAny(related[0].ID))
	assert.Equal(t, "tenant:n-49", localNodeIDFromAny(related[49].ID))
	assert.Len(t, probe.nodes, 50, "no node is loaded after the cap")
}

func TestMCPRelatedNodesCancellationPublishesNothing(t *testing.T) {
	probe, engine := newMCPRelatedProbe(t)
	createMCPRelatedNode(t, engine, "tenant:root", map[string]interface{}{"title": "Root"})
	for i := 0; i < 20; i++ {
		id := storage.NodeID(fmt.Sprintf("tenant:n-%02d", i))
		createMCPRelatedNode(t, engine, id, map[string]interface{}{"title": string(id)})
		require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("tenant:e-%02d", i)), StartNode: "tenant:root", EndNode: id, Type: "LINKS"}))
	}

	ctx, cancel := context.WithCancel(ContextWithDatabase(context.Background(), "tenant"))
	defer cancel()
	probe.onNode = func(loaded int) {
		if loaded == 5 {
			cancel()
		}
	}

	server := newMCPRelatedServer()
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return probe, nil })

	assert.Nil(t, server.getRelatedNodes(ctx, "tenant:root", 2), "a canceled walk must not publish a partial collection")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// mcpDiscoverNodeSeed is one node of a discover graph: its label and the
// properties the cypher seed writes.
type mcpDiscoverNodeSeed struct {
	label string
	props map[string]interface{}
}

// mcpDiscoverEdgeSeed is one edge of a discover graph; it names its endpoints by
// their index in the node seed list.
type mcpDiscoverEdgeSeed struct {
	from    int
	to      int
	relType string
}

// mcpDiscoverCreateNodeStatement builds the seed CREATE for one node, writing
// each supplied property under its own parameter. The keys are sorted so the
// statement text is deterministic.
func mcpDiscoverCreateNodeStatement(label string, props map[string]interface{}) string {
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	assignments := make([]string, 0, len(keys))
	for _, key := range keys {
		assignments = append(assignments, key+": $"+key)
	}
	if len(assignments) == 0 {
		return fmt.Sprintf("CREATE (n:%s) RETURN elementId(n) AS id", label)
	}
	return fmt.Sprintf("CREATE (n:%s {%s}) RETURN elementId(n) AS id", label, strings.Join(assignments, ", "))
}

// seedMCPDiscoverGraph opens one real database, seeds the supplied nodes and
// edges through its cypher executor and builds the search indexes the discover
// tool reads. It returns the store and the element ids in node seed order, and
// closes the store with the test that seeded it.
func seedMCPDiscoverGraph(t *testing.T, nodes []mcpDiscoverNodeSeed, edges []mcpDiscoverEdgeSeed) (*nornicdb.DB, []string) {
	t.Helper()
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	exec := db.GetCypherExecutor()
	require.NotNil(t, exec)
	ctx := context.Background()

	ids := make([]string, len(nodes))
	for index, node := range nodes {
		result, err := exec.Execute(ctx, mcpDiscoverCreateNodeStatement(node.label, node.props), node.props)
		require.NoError(t, err)
		id, _ := result.Rows[0][0].(string)
		ids[index] = id
	}
	for _, edge := range edges {
		_, err := exec.Execute(ctx,
			fmt.Sprintf("MATCH (a), (b) WHERE elementId(a) = $from AND elementId(b) = $to CREATE (a)-[:%s]->(b)", edge.relType),
			map[string]interface{}{"from": ids[edge.from], "to": ids[edge.to]},
		)
		require.NoError(t, err)
	}
	require.NoError(t, db.BuildSearchIndexes(ctx))
	return db, ids
}

// runMCPDiscoverDepth routes one discover invocation over a seeded graph and
// returns the decoded result. Both discover cases share the server
// construction and argument shape; each keeps its own query, depth and
// assertions.
func runMCPDiscoverDepth(t *testing.T, db *nornicdb.DB, query string, depth int) DiscoverResult {
	t.Helper()
	result, err := NewServer(db, nil).handleDiscover(context.Background(), map[string]interface{}{
		"query": query,
		"depth": depth,
		"limit": 5,
	})
	require.NoError(t, err)
	discover, ok := result.(DiscoverResult)
	require.True(t, ok, "handleDiscover must return a DiscoverResult")
	return discover
}

// TestHandleDiscoverRelatedNodesDepthAndIdentity drives the public discover tool
// and asserts the related entries, their distances and their paths against the
// search result identity.
func TestHandleDiscoverRelatedNodesDepthAndIdentity(t *testing.T) {
	db, ids := seedMCPDiscoverGraph(t, []mcpDiscoverNodeSeed{
		{label: "Memory", props: map[string]interface{}{"title": "Canonical Root", "content": "canonical related root document"}},
		{label: "Task", props: map[string]interface{}{"title": "Canonical Child", "content": "supporting neighbor"}},
		{label: "Task", props: map[string]interface{}{"name": "Canonical Grandchild", "content": "second hop"}},
	}, []mcpDiscoverEdgeSeed{
		{from: 0, to: 1, relType: "RELATES_TO"},
		{from: 1, to: 2, relType: "NEXT"},
	})
	require.Len(t, ids, 3, "this case keeps its own three-node, two-hop population")

	discover := runMCPDiscoverDepth(t, db, "canonical related root", 3)
	require.NotEmpty(t, discover.Results)

	source := discover.Results[0]
	require.Equal(t, "Canonical Root", source.Title)
	require.Len(t, source.Related, 2)
	assert.Equal(t, 1, source.Related[0].Distance)
	assert.Equal(t, "outgoing", source.Related[0].Direction)
	assert.Equal(t, "RELATES_TO", source.Related[0].Relationship)
	assert.Equal(t, "Task", source.Related[0].Type)
	assert.Equal(t, "Canonical Child", source.Related[0].Title)
	assert.Equal(t, []string{source.ID, source.Related[0].ID}, source.Related[0].Path)

	assert.Equal(t, 2, source.Related[1].Distance)
	assert.Equal(t, "NEXT", source.Related[1].Relationship)
	assert.Equal(t, "Canonical Grandchild", source.Related[1].Title, "MCP falls back from title to name")
	assert.Equal(t, []string{source.ID, source.Related[0].ID, source.Related[1].ID}, source.Related[1].Path)
	assert.True(t, strings.HasPrefix(source.Related[0].ID, "4:"), "MCP publishes element ids")
}
