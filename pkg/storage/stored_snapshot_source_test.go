package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// The pinned stored snapshot source is the read side of a snapshot: a caller
// that owns the barrier/cut opens it, then enumerates the node and relationship
// population it pinned. These tests pin a populated store, mutate it after the
// pin, and read the pin: the pinned population must stay the one storage held,
// hidden entities and sidecar vectors included.

func pinnedStateOf(properties map[string]any) string {
	state, _ := properties["state"].(string)
	return state
}

// pinnedNodeStates reads one node stream into ID -> state.
func pinnedNodeStates(t *testing.T, ctx context.Context, stream func(context.Context, func(*Node) error) error) map[NodeID]string {
	t.Helper()
	states := map[NodeID]string{}
	require.NoError(t, stream(ctx, func(node *Node) error {
		states[node.ID] = pinnedStateOf(node.Properties)
		return nil
	}))
	return states
}

// pinnedVisibleNodeStates reads the engine's user-facing whole-node stream.
func pinnedVisibleNodeStates(t *testing.T, engine Engine) map[NodeID]string {
	t.Helper()
	states := map[NodeID]string{}
	require.NoError(t, engine.StreamNodesWithOptions(context.Background(), fullNodeStreamOptions(""), func(node *Node) error {
		states[node.ID] = pinnedStateOf(node.Properties)
		return nil
	}))
	return states
}

// pinnedSnapshotEdgeRecord is one relationship record a snapshot write emits:
// the stored-body transcoder's borrowed view, or a decoded edge when the body
// was not proved to round-trip.
type pinnedSnapshotEdgeRecord struct {
	id         EdgeID
	start, end NodeID
	edgeType   string
	properties map[string]any
	transcoded bool
}

func pinnedSnapshotProperty(t *testing.T, raw []byte) any {
	t.Helper()
	var value any
	require.NoError(t, msgpack.Unmarshal(raw, &value))
	return value
}

// pinnedSnapshotEdgeRecords reads one snapshot relationship stream.
func pinnedSnapshotEdgeRecords(t *testing.T, ctx context.Context, stream func(context.Context, func(byte, any) error) error) map[EdgeID]pinnedSnapshotEdgeRecord {
	t.Helper()
	records := map[EdgeID]pinnedSnapshotEdgeRecord{}
	require.NoError(t, stream(ctx, func(frameType byte, value any) error {
		require.Equal(t, streamSnapshotEdge, frameType)
		var record pinnedSnapshotEdgeRecord
		switch typed := value.(type) {
		case *snapshotEdgeTranscoder:
			record.transcoded = true
			record.id, record.start, record.end = typed.edge.ID, typed.edge.StartNode, typed.edge.EndNode
			record.edgeType = typed.edge.Type
			record.properties = make(map[string]any, len(typed.properties))
			for _, property := range typed.properties {
				record.properties[property.name] = pinnedSnapshotProperty(t, property.value)
			}
		case *Edge:
			record.id, record.start, record.end = typed.ID, typed.StartNode, typed.EndNode
			record.edgeType = typed.Type
			record.properties = typed.Properties
		default:
			t.Fatalf("unexpected snapshot relationship record %T", value)
		}
		records[record.id] = record
		return nil
	}))
	return records
}

func pinnedEdgeStates(records map[EdgeID]pinnedSnapshotEdgeRecord) map[EdgeID]string {
	states := make(map[EdgeID]string, len(records))
	for id, record := range records {
		states[id] = pinnedStateOf(record.properties)
	}
	return states
}

func TestStoredSnapshotSourcePinsStoredNodesBeforeMutations(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	for _, id := range []NodeID{"app:ordinary", "app:hidden", "app:vector"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"state": "original"}})
		require.NoError(t, err)
	}
	vectorNode, err := engine.GetNode(ctx, "app:vector")
	require.NoError(t, err)
	vectorNode.ChunkEmbeddings = [][]float32{{1, 2, 3}}
	vectorNode.EmbedMeta = map[string]any{"status": "original", "chunk_count": int64(1)}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(ctx, vectorNode))
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "app:hidden", true))

	// The visibility layer hides the suppressed node from user reads.
	require.Equal(t, map[NodeID]string{"app:ordinary": "original", "app:vector": "original"}, pinnedVisibleNodeStates(t, engine))

	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.Close()

	// Mutate the store after the pin: update, delete, replace vectors, create.
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "app:ordinary", Labels: []string{"Doc"}, Properties: map[string]any{"state": "mutated"}}}))
	require.NoError(t, engine.DeleteNode(ctx, "app:hidden"))
	replaced, err := engine.GetNode(ctx, "app:vector")
	require.NoError(t, err)
	replaced.ChunkEmbeddings = [][]float32{{9, 9, 9}}
	replaced.EmbedMeta = map[string]any{"status": "mutated", "chunk_count": int64(1)}
	require.NoError(t, engine.UpdateNodeEmbeddingSidecar(ctx, replaced))
	_, err = engine.CreateNode(ctx, &Node{ID: "app:created", Labels: []string{"Doc"}, Properties: map[string]any{"state": "created"}})
	require.NoError(t, err)

	// The live store holds the mutations.
	live, err := engine.GetNode(ctx, "app:ordinary")
	require.NoError(t, err)
	require.Equal(t, "mutated", pinnedStateOf(live.Properties))
	_, err = engine.GetNode(ctx, "app:hidden")
	require.ErrorIs(t, err, ErrNotFound)
	liveVector, err := engine.GetNode(ctx, "app:vector")
	require.NoError(t, err)
	require.Equal(t, [][]float32{{9, 9, 9}}, liveVector.ChunkEmbeddings)
	require.Equal(t, "mutated", liveVector.EmbedMeta["status"])
	allStored := map[NodeID]string{}
	require.NoError(t, engine.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		allStored[node.ID] = pinnedStateOf(node.Properties)
		return nil
	}))
	require.Equal(t, map[NodeID]string{
		"app:ordinary": "mutated",
		"app:vector":   "original",
		"app:created":  "created",
	}, allStored)

	// The pin still holds what storage held, hidden node and stored vectors
	// included.
	expectPinned := func() {
		pinnedNodes := map[NodeID]*Node{}
		require.NoError(t, source.StreamNodes(ctx, func(node *Node) error {
			pinnedNodes[node.ID] = node
			return nil
		}))
		require.Len(t, pinnedNodes, 3)
		require.Equal(t, "original", pinnedStateOf(pinnedNodes["app:ordinary"].Properties))
		require.Equal(t, "original", pinnedStateOf(pinnedNodes["app:hidden"].Properties))
		require.Equal(t, [][]float32{{1, 2, 3}}, pinnedNodes["app:vector"].ChunkEmbeddings)
		require.Equal(t, "original", pinnedNodes["app:vector"].EmbedMeta["status"])
		require.NotContains(t, pinnedNodes, NodeID("app:created"))
	}
	expectPinned()
	// The pin is a read point, not a batch: a second scan reads the same one.
	expectPinned()
}

func TestStoredSnapshotSourcePinsStoredEdgesBeforeMutations(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	for _, id := range []NodeID{"app:a", "app:b"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "app:active_rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "original"}},
		{ID: "app:hidden_rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "original"}},
	} {
		require.NoError(t, engine.CreateEdge(ctx, edge))
	}
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityEdgeKind, "app:hidden_rel", true))
	var visible []EdgeID
	require.NoError(t, engine.StreamEdgesByType(ctx, "", func(edge *Edge) error {
		visible = append(visible, edge.ID)
		return nil
	}))
	require.Equal(t, []EdgeID{"app:active_rel"}, visible)

	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.Close()

	// Mutate the store after the pin.
	require.NoError(t, engine.UpdateEdge(ctx, EdgeUpdateRequest{After: &Edge{ID: "app:active_rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "mutated"}}}))
	require.NoError(t, engine.DeleteEdge(ctx, "app:hidden_rel"))
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "app:late_rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "created"}}))

	live := pinnedSnapshotEdgeRecords(t, ctx, func(ctx context.Context, write func(byte, any) error) error {
		return writeSnapshotEdges(ctx, engine, write)
	})
	require.Equal(t, map[EdgeID]string{"app:active_rel": "mutated", "app:late_rel": "created"}, pinnedEdgeStates(live))

	pinned := pinnedSnapshotEdgeRecords(t, ctx, func(ctx context.Context, write func(byte, any) error) error {
		return source.StreamEdges(ctx, write)
	})
	require.Equal(t, map[EdgeID]string{"app:active_rel": "original", "app:hidden_rel": "original"}, pinnedEdgeStates(pinned))
	// The stored-body transcoder still owns the pinned record.
	require.True(t, pinned["app:active_rel"].transcoded)
	require.Equal(t, "REL", pinned["app:active_rel"].edgeType)
	require.Equal(t, NodeID("app:a"), pinned["app:hidden_rel"].start)
	require.Equal(t, NodeID("app:b"), pinned["app:hidden_rel"].end)

	// A second pinned scan reads the same point.
	require.Equal(t, map[EdgeID]string{"app:active_rel": "original", "app:hidden_rel": "original"}, pinnedEdgeStates(pinnedSnapshotEdgeRecords(t, ctx, func(ctx context.Context, write func(byte, any) error) error {
		return source.StreamEdges(ctx, write)
	})))
}

func TestStoredSnapshotSourcePinsNamespaceEntities(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	documents := NewNamespacedEngine(engine, "documents")
	other := NewNamespacedEngine(engine, "other")
	for _, node := range []*Node{
		{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]any{"state": "original"}},
		{ID: "hid", Labels: []string{"Doc"}, Properties: map[string]any{"state": "original"}},
	} {
		_, err := documents.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	_, err := other.CreateNode(ctx, &Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]any{"state": "other"}})
	require.NoError(t, err)
	require.NoError(t, documents.CreateEdge(ctx, &Edge{ID: "r1", StartNode: "n1", EndNode: "hid", Type: "REL", Properties: map[string]any{"state": "original"}}))
	require.NoError(t, other.CreateEdge(ctx, &Edge{ID: "r1", StartNode: "n1", EndNode: "n1", Type: "REL", Properties: map[string]any{"state": "other"}}))
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "documents:hid", true))

	require.Equal(t, map[NodeID]string{"n1": "original"}, pinnedVisibleNodeStates(t, documents))

	source, err := openStoredSnapshotSource(ctx, documents)
	require.NoError(t, err)
	defer source.Close()

	require.NoError(t, documents.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "n1", Labels: []string{"Doc"}, Properties: map[string]any{"state": "mutated"}}}))

	// This database's stored population, hidden node included, in user IDs.
	require.Equal(t, map[NodeID]string{"n1": "original", "hid": "original"}, pinnedNodeStates(t, ctx, source.StreamNodes))

	pinnedEdges := pinnedSnapshotEdgeRecords(t, ctx, func(ctx context.Context, write func(byte, any) error) error {
		return source.StreamEdges(ctx, write)
	})
	require.Equal(t, map[EdgeID]string{"r1": "original"}, pinnedEdgeStates(pinnedEdges))
	require.Equal(t, NodeID("n1"), pinnedEdges["r1"].start)
	require.Equal(t, NodeID("hid"), pinnedEdges["r1"].end)
	require.True(t, pinnedEdges["r1"].transcoded)

	// The live store holds the mutation; the pin does not.
	live, err := documents.GetNode(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, "mutated", pinnedStateOf(live.Properties))
	require.Equal(t, "original", pinnedNodeStates(t, ctx, source.StreamNodes)["n1"])
}

func TestStoredSnapshotSourcePinsBufferedWriteBehindEntities(t *testing.T) {
	ctx := context.Background()
	engine := newWriteBehindTestEngine(t, time.Hour)
	engine.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	bufferedCreate(t, engine, &Node{ID: "test:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"state": "original"}})

	source, err := openStoredSnapshotSource(ctx, engine)
	require.NoError(t, err)
	defer source.Close()

	// The acknowledged write reaches the store after the pin.
	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, engine.writeBehind.PendingOps())
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"state": "mutated"}}}))

	live, err := engine.GetNode(ctx, "test:buffered")
	require.NoError(t, err)
	require.Equal(t, "mutated", pinnedStateOf(live.Properties))
	require.Equal(t, map[NodeID]string{"test:buffered": "original"}, pinnedNodeStates(t, ctx, source.StreamNodes))
}

// overriddenStoredEdgeEngine is a decorator whose stored relationship read is
// its own enumeration: the pinned source must not reach past it.
type overriddenStoredEdgeEngine struct {
	*BadgerEngine
}

func (e *overriddenStoredEdgeEngine) StreamStoredEdgesByType(_ context.Context, edgeType string, visit func(*Edge) error) error {
	if edgeType != "" {
		return nil
	}
	return visit(&Edge{ID: "app:override", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "override"}})
}

func TestStoredSnapshotSourceReportsUnsupportedStoredReaderOverride(t *testing.T) {
	ctx := context.Background()
	engine := newTestEngine(t)
	for _, id := range []NodeID{"app:a", "app:b"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "app:inner_rel", StartNode: "app:a", EndNode: "app:b", Type: "REL", Properties: map[string]any{"state": "inner"}}))
	decorated := &overriddenStoredEdgeEngine{BadgerEngine: engine}

	source, err := openStoredSnapshotSource(ctx, decorated)
	require.Nil(t, source)
	require.ErrorIs(t, err, errStoredSnapshotUnsupported)

	// The override keeps the enumeration it declares on the live snapshot write.
	records := pinnedSnapshotEdgeRecords(t, ctx, func(ctx context.Context, write func(byte, any) error) error {
		return writeSnapshotEdges(ctx, decorated, write)
	})
	require.Equal(t, map[EdgeID]string{"app:override": "override"}, pinnedEdgeStates(records))
}

var _ Engine = (*overriddenStoredEdgeEngine)(nil)
var _ StoredEntityReader = (*overriddenStoredEdgeEngine)(nil)
