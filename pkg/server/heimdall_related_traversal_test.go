package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relatedProbeEngine records the read path of the related-node walk and can hide
// nodes or cancel the caller's context mid-traversal.
type relatedProbeEngine struct {
	storage.Engine

	mu       sync.Mutex
	outgoing []string
	incoming []string
	nodes    []string
	missing  map[string]bool
	onNode   func(loaded int)
}

func (e *relatedProbeEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		e.mu.Lock()
		e.incoming = append(e.incoming, string(request.Node))
		e.mu.Unlock()
		return e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node})
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
	e.mu.Lock()
	e.outgoing = append(e.outgoing, string(request.Node))
	e.mu.Unlock()
	return e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: request.Node})
}

func (e *relatedProbeEngine) GetIncomingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.mu.Lock()
	e.incoming = append(e.incoming, string(nodeID))
	e.mu.Unlock()
	return e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: nodeID})
}

func (e *relatedProbeEngine) GetNode(ctx context.Context, nodeID storage.NodeID) (*storage.Node, error) {
	e.mu.Lock()
	e.nodes = append(e.nodes, string(nodeID))
	hook := e.onNode
	missing := e.missing[string(nodeID)]
	loaded := len(e.nodes)
	e.mu.Unlock()
	if hook != nil {
		hook(loaded)
	}
	if missing {
		return nil, storage.ErrNotFound
	}
	return e.Engine.GetNode(ctx, nodeID)
}

func (e *relatedProbeEngine) snapshot() (outgoing, incoming, nodes []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string{}, e.outgoing...), append([]string{}, e.incoming...), append([]string{}, e.nodes...)
}

func newRelatedProbe(t *testing.T) (*relatedProbeEngine, storage.Engine) {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "related")
	probe := &relatedProbeEngine{Engine: engine, missing: map[string]bool{}}
	return probe, engine
}

func createRelatedNode(t *testing.T, engine storage.Engine, id storage.NodeID, labels ...string) {
	t.Helper()
	if len(labels) == 0 {
		labels = []string{"Doc"}
	}
	_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: labels, Properties: map[string]interface{}{"title": string(id)}})
	require.NoError(t, err)
}

func createRelatedEdge(t *testing.T, engine storage.Engine, id storage.EdgeID, from, to storage.NodeID) {
	t.Helper()
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: id, StartNode: from, EndNode: to, Type: "LINK"}))
}

func TestHeimdallRelatedNodesDepthOrderingAndCycles(t *testing.T) {
	probe, engine := newRelatedProbe(t)
	createRelatedNode(t, engine, "root")
	for _, id := range []storage.NodeID{"a", "b", "d"} {
		createRelatedNode(t, engine, id)
	}
	createRelatedNode(t, engine, "c", "Inbound")
	createRelatedEdge(t, engine, "e-ra", "root", "a")
	createRelatedEdge(t, engine, "e-rb", "root", "b")
	createRelatedEdge(t, engine, "e-a-root", "a", "root") // cycle
	createRelatedEdge(t, engine, "e-a-d", "a", "d")
	createRelatedEdge(t, engine, "e-c-root", "c", "root")

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(context.Background(), probe, "root", 2)

	require.Len(t, related, 4)
	assert.Equal(t, []string{"a", "b", "c", "d"}, relatedIDs(related))
	assert.Equal(t, []string{"outgoing", "outgoing", "incoming", "outgoing"}, relatedDirections(related))
	assert.Equal(t, []int{1, 1, 1, 2}, relatedDistances(related))
	assert.Equal(t, "Inbound", related[2].Type)
	assert.Equal(t, "c", related[2].Title)

	outgoing, incoming, nodes := probe.snapshot()
	// Outgoing edges are read before incoming edges for each expanded node, and
	// nodes at distance == depth are not expanded.
	assert.Equal(t, []string{"root", "a", "b", "c"}, outgoing)
	assert.Equal(t, []string{"root", "a", "b", "c"}, incoming)
	assert.Equal(t, []string{"a", "b", "c", "d"}, nodes)
}

func TestHeimdallRelatedNodesSkipsAndDoesNotExpandMissingNodes(t *testing.T) {
	probe, engine := newRelatedProbe(t)
	createRelatedNode(t, engine, "root")
	createRelatedNode(t, engine, "gone")
	createRelatedNode(t, engine, "child")
	createRelatedNode(t, engine, "kept")
	createRelatedEdge(t, engine, "e-r-gone", "root", "gone")
	createRelatedEdge(t, engine, "e-gone-child", "gone", "child")
	createRelatedEdge(t, engine, "e-r-kept", "root", "kept")
	probe.missing["gone"] = true

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(context.Background(), probe, "root", 3)

	require.Len(t, related, 1)
	assert.Equal(t, "kept", related[0].ID)
	outgoing, _, _ := probe.snapshot()
	assert.NotContains(t, outgoing, "gone", "a missing node is not expanded")
}

func TestHeimdallRelatedNodesCapStopsReadsInsideOneAdjacency(t *testing.T) {
	probe, engine := newRelatedProbe(t)
	createRelatedNode(t, engine, "root")
	for i := 0; i < 60; i++ {
		id := storage.NodeID(fmt.Sprintf("out-%02d", i))
		createRelatedNode(t, engine, id)
		createRelatedEdge(t, engine, storage.EdgeID(fmt.Sprintf("e-out-%02d", i)), "root", id)
		createRelatedEdge(t, engine, storage.EdgeID(fmt.Sprintf("e-in-%02d", i)), id, "root")
	}

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(context.Background(), probe, "root", 1)

	require.Len(t, related, 50)
	assert.Equal(t, "out-00", related[0].ID)
	assert.Equal(t, "out-49", related[49].ID)
	outgoing, incoming, nodes := probe.snapshot()
	assert.Equal(t, []string{"root"}, outgoing)
	assert.Empty(t, incoming, "the cap is reached before the incoming adjacency is read")
	assert.Len(t, nodes, 50, "no node is loaded after the cap")
}

func TestHeimdallRelatedNodesCancellationPublishesNothing(t *testing.T) {
	probe, engine := newRelatedProbe(t)
	createRelatedNode(t, engine, "root")
	for i := 0; i < 20; i++ {
		id := storage.NodeID(fmt.Sprintf("n-%02d", i))
		createRelatedNode(t, engine, id)
		createRelatedEdge(t, engine, storage.EdgeID(fmt.Sprintf("e-%02d", i)), "root", id)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe.onNode = func(loaded int) {
		if loaded == 5 {
			cancel()
		}
	}

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(ctx, probe, "root", 2)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Nil(t, related, "a canceled walk must not publish a partial collection")
}

func TestHeimdallRelatedNodesDeadlinePublishesNothing(t *testing.T) {
	probe, engine := newRelatedProbe(t)
	createRelatedNode(t, engine, "root")
	createRelatedNode(t, engine, "a")
	createRelatedEdge(t, engine, "e-ra", "root", "a")

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	router := &heimdallDBRouter{}
	assert.Nil(t, router.getRelatedNodes(ctx, probe, "root", 2))
}

// TestHeimdallDiscoverRelatedNodesComeFromCanonicalWalk drives the exported
// DatabaseRouter surface with a real search index and asserts that the related
// entries carry the stored node identity at the documented distance.
func TestHeimdallDiscoverRelatedNodesComeFromCanonicalWalk(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	ctx := heimdallAuthDisabled()

	dbName, err := router.ResolveDatabase("")
	require.NoError(t, err)
	engine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)

	rootID := storage.NodeID("nornic:related-root")
	childID := storage.NodeID("nornic:related-child")
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: rootID, Labels: []string{"Document"}, Properties: map[string]interface{}{
		"title":   "Related Root",
		"content": "canonical walk root document",
	}})
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: childID, Labels: []string{"Document"}, Properties: map[string]interface{}{
		"title":   "Related Child",
		"content": "neighbor document",
	}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "nornic:related-edge", StartNode: rootID, EndNode: childID, Type: "LINKS"}))
	require.NoError(t, server.db.BuildSearchIndexes(context.Background()))

	result, err := router.Discover(ctx, dbName, "canonical walk root", []string{"Document"}, 5, 1)
	require.NoError(t, err)
	require.NotNil(t, result)

	// The search service publishes local ids for the selected database, so the
	// related-node walk must stay in that same id space and report the stored
	// neighbor, not a re-derived identifier.
	var found *heimdall.SearchResult
	for i := range result.Results {
		if result.Results[i].ID == "related-root" {
			found = &result.Results[i]
			break
		}
	}
	require.NotNil(t, found, "the seeded root document must be the search hit")
	require.Len(t, found.Related, 1)
	assert.Equal(t, "related-child", found.Related[0].ID)
	assert.Equal(t, 1, found.Related[0].Distance)
	assert.Equal(t, "outgoing", found.Related[0].Direction)
	assert.Equal(t, "LINKS", found.Related[0].Relationship)
	assert.Equal(t, "Document", found.Related[0].Type)
	assert.Equal(t, "Related Child", found.Related[0].Title)
}

// relatedPartialErrorEngine returns the real adjacency rows of a node together
// with an injected read error, which is what the pre-merge server and MCP
// readers observed from a partially failing storage read.
type relatedPartialErrorEngine struct {
	storage.Engine
	outErr   error
	inErr    error
	outCalls int
	inCalls  int
}

func (e *relatedPartialErrorEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		e.inCalls++
		rows, _ := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node})
		return rows, e.inErr
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
	e.outCalls++
	rows, _ := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: request.Node})
	return rows, e.outErr
}

func (e *relatedPartialErrorEngine) GetIncomingEdges(ctx context.Context, nodeID storage.NodeID) ([]*storage.Edge, error) {
	e.inCalls++
	rows, _ := e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: nodeID})
	return rows, e.inErr
}

// TestHeimdallRelatedNodesKeepsRowsAlongsideAdjacencyReadErrors preserves the
// server contract this walk replaced: outgoing and incoming rows were walked even
// when the engine reported an error for that list, so a partially failing read
// keeps the neighbors it did return.
func TestHeimdallRelatedNodesKeepsRowsAlongsideAdjacencyReadErrors(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "related")
	createRelatedNode(t, engine, "root")
	createRelatedNode(t, engine, "a")
	createRelatedNode(t, engine, "c", "Inbound")
	createRelatedEdge(t, engine, "e-ra", "root", "a")
	createRelatedEdge(t, engine, "e-cr", "c", "root")
	probe := &relatedPartialErrorEngine{
		Engine: engine,
		outErr: errors.New("outgoing adjacency read failed"),
		inErr:  errors.New("incoming adjacency read failed"),
	}

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(context.Background(), probe, "root", 1)

	require.Len(t, related, 2, "both errored lists keep their rows")
	assert.Equal(t, []string{"a", "c"}, relatedIDs(related))
	assert.Equal(t, []string{"outgoing", "incoming"}, relatedDirections(related))
	assert.Equal(t, 1, probe.outCalls)
	assert.Equal(t, 1, probe.inCalls, "an outgoing read error does not skip the incoming read")
}

// relatedWideEngine serves one synthetic wide outgoing adjacency and loads every
// neighbor from the wrapped engine, which does not hold them: every neighbor is
// skipped, so nothing is published and the 50-record cap cannot bound the walk.
type relatedWideEngine struct {
	storage.Engine
	out      []*storage.Edge
	nodes    int
	onNode   func(loaded int)
	outCalls int
	inCalls  int
}

func (e *relatedWideEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		e.inCalls++
		return nil, nil
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
	e.outCalls++
	return e.out, nil
}

func (e *relatedWideEngine) GetNode(ctx context.Context, nodeID storage.NodeID) (*storage.Node, error) {
	e.nodes++
	if e.onNode != nil {
		e.onNode(e.nodes)
	}
	return e.Engine.GetNode(ctx, nodeID)
}

// TestHeimdallRelatedNodesCancellationBoundsWideAdjacency is the router's
// fail-first case: missing neighbors are neither published nor expanded, so the
// cap never applies and only the amortized poll bounds the reads after the
// cancellation.
func TestHeimdallRelatedNodesCancellationBoundsWideAdjacency(t *testing.T) {
	const (
		width    = 20480
		cancelAt = 100
		bounds   = 256 // relatedwalk's documented amortized poll interval
	)
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	probe := &relatedWideEngine{Engine: base, out: make([]*storage.Edge, 0, width)}
	for i := 0; i < width; i++ {
		probe.out = append(probe.out, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("e-%05d", i)),
			StartNode: "root",
			EndNode:   storage.NodeID(fmt.Sprintf("missing-%05d", i)),
			Type:      "LINK",
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe.onNode = func(loaded int) {
		if loaded == cancelAt {
			cancel()
		}
	}

	router := &heimdallDBRouter{}
	related := router.getRelatedNodes(ctx, probe, "root", 2)

	assert.Equal(t, 1, probe.outCalls, "no adjacency is read after the cancellation")
	assert.Zero(t, probe.inCalls)
	require.GreaterOrEqual(t, probe.nodes, cancelAt)
	assert.LessOrEqual(t, probe.nodes, cancelAt+bounds,
		"post-cancellation node loads must stay inside the documented poll interval")
	assert.Nil(t, related, "a canceled walk must not publish a partial collection")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func relatedIDs(related []heimdall.RelatedNode) []string {
	ids := make([]string, 0, len(related))
	for _, node := range related {
		ids = append(ids, node.ID)
	}
	return ids
}

func relatedDirections(related []heimdall.RelatedNode) []string {
	directions := make([]string, 0, len(related))
	for _, node := range related {
		directions = append(directions, node.Direction)
	}
	return directions
}

func relatedDistances(related []heimdall.RelatedNode) []int {
	distances := make([]int, 0, len(related))
	for _, node := range related {
		distances = append(distances, node.Distance)
	}
	return distances
}
