package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// txReadFixture builds a small graph that exercises every committed
// read path on *BadgerTransaction:
//
//	test:alice -KNOWS->   test:bob
//	test:alice -KNOWS->   test:carol     (second alice→other KNOWS edge)
//	test:bob   -FOLLOWS-> test:carol
//	test:dave  -REVIEWS-> test:doc       (separate "other" namespace)
//
// Labels: alice/bob/carol/dave are People; alice is also Engineer; doc
// is Document. The mix of types, multi-edges, and shared endpoints is
// deliberate so the assertions below distinguish "the right edge" from
// "any edge with these endpoints."
//
// Each namespace is committed in its own transaction because the storage
// layer pins every transaction to a single namespace; the cross-namespace
// REVIEWS edge that previously linked the two roots is omitted so the
// fixture remains within the supported invariants.
//
// Returns the engine; tests open a fresh transaction to layer pending
// writes onto the committed state.
func txReadFixture(t *testing.T) *MemoryEngine {
	t.Helper()
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	commitNamespace := func(nodes []*Node, edges []*Edge) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		for _, n := range nodes {
			_, err := tx.CreateNode(n)
			require.NoError(t, err, "CreateNode(%q)", n.ID)
		}
		for _, e := range edges {
			require.NoError(t, tx.CreateEdge(e), "CreateEdge(%q)", e.ID)
		}
		require.NoError(t, tx.Commit())
	}

	commitNamespace(
		[]*Node{
			{ID: "test:alice", Labels: []string{"Person", "Engineer"}, Properties: map[string]any{"name": "Alice"}},
			{ID: "test:bob", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}},
			{ID: "test:carol", Labels: []string{"Person"}, Properties: map[string]any{"name": "Carol"}},
			{ID: "test:dave", Labels: []string{"Person"}, Properties: map[string]any{"name": "Dave"}},
		},
		[]*Edge{
			{ID: "test:e-knows-1", StartNode: "test:alice", EndNode: "test:bob", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}},
			{ID: "test:e-knows-2", StartNode: "test:alice", EndNode: "test:carol", Type: "KNOWS", Properties: map[string]any{"since": int64(2021)}},
			{ID: "test:e-follows", StartNode: "test:bob", EndNode: "test:carol", Type: "FOLLOWS", Properties: map[string]any{"strength": "weak"}},
		},
	)
	commitNamespace(
		[]*Node{
			{ID: "other:doc", Labels: []string{"Document"}, Properties: map[string]any{"title": "Spec"}},
		},
		nil,
	)
	return engine
}

// edgeIDs is a deterministic helper for asserting on collected edge IDs
// regardless of iteration order.
func edgeIDs(edges []*Edge) []string {
	out := make([]string, 0, len(edges))
	for _, e := range edges {
		out = append(out, string(e.ID))
	}
	sort.Strings(out)
	return out
}

// nodeIDs is the node-side analogue.
func nodeIDs(nodes []*Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, string(n.ID))
	}
	sort.Strings(out)
	return out
}

func TestTxReads_StreamNodesByLabelProjected_MergesSnapshotAndPendingWrites(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	bob, err := tx.GetNode("test:bob")
	require.NoError(t, err)
	bob.Properties["name"] = "Robert"
	bob.Properties["secret"] = "not projected"
	require.NoError(t, tx.UpdateNode(bob))
	require.NoError(t, tx.DeleteNode("test:carol"))
	_, err = tx.CreateNode(&Node{
		ID: "test:erin", Labels: []string{"Person"},
		Properties: map[string]any{"name": "Erin", "secret": "not projected"},
	})
	require.NoError(t, err)

	var nodes []*Node
	err = tx.StreamNodesByLabelProjected("Person", []string{"name"}, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"test:alice", "test:bob", "test:dave", "test:erin"}, nodeIDs(nodes))
	for _, node := range nodes {
		require.NotContains(t, node.Properties, "secret")
	}
	for _, node := range nodes {
		if node.ID == "test:bob" {
			require.Equal(t, "Robert", node.Properties["name"])
		}
	}
}

func TestTxReads_StreamNodesByLabelProjected_StopsWithoutMaterializingRemainder(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	visited := 0
	err = tx.StreamNodesByLabelProjected("Person", []string{"name"}, func(*Node) error {
		visited++
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Equal(t, 1, visited)
	require.Empty(t, tx.snapshotProjectedLabelNodes, "an incomplete stream must not be cached")
}

func TestTxReads_StreamNodesByLabelProjected_ReplaysBoundedPrefixAndResumes(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })

	visited := 0
	err = tx.StreamNodesByLabelProjected("Person", []string{"name"}, func(*Node) error {
		visited++
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Equal(t, 1, visited)
	require.Len(t, tx.snapshotLabelPrefixNodes, 1, "an early stop should retain only its bounded prefix")
	require.Empty(t, tx.snapshotProjectedLabelNodes, "an incomplete prefix must not be treated as a complete stream")

	alice, err := tx.GetNode("test:alice")
	require.NoError(t, err)
	alice.Properties["name"] = "Alicia"
	require.NoError(t, tx.UpdateNode(alice))
	require.NoError(t, tx.DeleteNode("test:bob"))
	_, err = tx.CreateNode(&Node{ID: "test:erin", Labels: []string{"Person"}, Properties: map[string]any{"name": "Erin"}})
	require.NoError(t, err)

	var nodes []*Node
	err = tx.StreamNodesByLabelProjected("Person", []string{"name"}, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"test:alice", "test:carol", "test:dave", "test:erin"}, nodeIDs(nodes))
	require.Equal(t, "Alicia", nodes[0].Properties["name"], "the cached prefix must apply pending updates")
	require.Len(t, tx.snapshotProjectedLabelNodes, 1, "resuming to completion should promote the result to a complete snapshot cache")
	require.Empty(t, tx.snapshotLabelPrefixNodes, "completed replay should release the partial prefix")
}

func TestTxReads_StreamNodesByLabel_UnprojectedPrefixAndLargePayloadGuard(t *testing.T) {
	engine := txReadFixture(t)
	alice, err := engine.GetNode("test:alice")
	require.NoError(t, err)
	alice.Properties["large"] = strings.Repeat("x", maxSnapshotLabelPrefixNodeBytes+1)
	require.NoError(t, engine.UpdateNode(alice))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })

	err = tx.StreamNodesByLabelProjected("Person", nil, func(*Node) error {
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Empty(t, tx.snapshotLabelPrefixNodes, "large full-node results must not be retained as a partial prefix")

	largeID := NodeID("test:large")
	_, err = tx.CreateNode(&Node{ID: largeID, Labels: []string{"Person"}, Properties: map[string]any{"name": "Large"}})
	require.NoError(t, err)
	seen := make([]NodeID, 0, 5)
	err = tx.StreamNodesByLabelProjected("Person", nil, func(node *Node) error {
		seen = append(seen, node.ID)
		return nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:alice", "test:bob", "test:carol", "test:dave", largeID}, seen)
	require.Len(t, tx.snapshotLabelNodes, 1, "full replay should populate only the complete cache")
}

func TestTxReads_CommittedNodeCanUseCachedLabelPrefix(t *testing.T) {
	cached := &Node{ID: "test:cached", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
	tx := &BadgerTransaction{snapshotPrefixNodeByID: map[NodeID]*Node{cached.ID: cached}}

	got, err := tx.getCommittedNodeLocked(cached.ID)
	require.NoError(t, err)
	require.Equal(t, cached, got)
	require.NotSame(t, cached, got, "cached snapshot nodes must be copied before returning them to callers")
	got.Properties["name"] = "mutated"
	require.Equal(t, "Alice", cached.Properties["name"], "caller mutation must not alter the retained snapshot prefix")
}

func TestTxReads_EndpointPrefixNodeCacheIsBounded(t *testing.T) {
	tx := &BadgerTransaction{}
	for index := 0; index < maxSnapshotPrefixNodeCacheNodes+5; index++ {
		node := &Node{ID: NodeID(fmt.Sprintf("test:cached-%03d", index)), Labels: []string{"Person"}}
		tx.cacheSnapshotPrefixNodeByIDLocked(node)
	}
	require.Len(t, tx.snapshotPrefixNodeByID, maxSnapshotPrefixNodeCacheNodes)
	require.NotContains(t, tx.snapshotPrefixNodeByID, NodeID("test:cached-000"), "oldest entries should be evicted first")
	require.Contains(t, tx.snapshotPrefixNodeByID, NodeID(fmt.Sprintf("test:cached-%03d", maxSnapshotPrefixNodeCacheNodes+4)))
	require.LessOrEqual(t, tx.snapshotPrefixNodeBytes, maxSnapshotPrefixNodeCacheBytes)
	require.Len(t, tx.snapshotPrefixNodeOrder, maxSnapshotPrefixNodeCacheNodes)
}

func TestTxReads_GetNodeUsesUnprojectedSnapshotPrefix(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	t.Cleanup(func() { _ = tx.Rollback() })

	err = tx.StreamNodesByLabelProjected("Person", nil, func(*Node) error {
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Len(t, tx.snapshotPrefixNodeByID, 1)

	first := tx.snapshotLabelPrefixNodes["person"][0]
	got, err := tx.GetNode(first.ID)
	require.NoError(t, err)
	require.Equal(t, first, got)
	require.NotSame(t, first, got)
	got.Properties["name"] = "caller mutation"
	gotAgain, err := tx.GetNode(first.ID)
	require.NoError(t, err)
	require.Equal(t, first.Properties["name"], gotAgain.Properties["name"])

	gotAgain.Properties["name"] = "pending update"
	require.NoError(t, tx.UpdateNode(gotAgain))
	pending, err := tx.GetNode(first.ID)
	require.NoError(t, err)
	require.Equal(t, "pending update", pending.Properties["name"], "pending state must take priority over the committed prefix cache")
	require.NoError(t, tx.DeleteNode(first.ID))
	_, err = tx.GetNode(first.ID)
	require.ErrorIs(t, err, ErrNotFound, "deleted state must take priority over the committed prefix cache")
}

func TestTxReads_StreamNodesByLabelProjected_CachedReplayKeepsBeginSnapshot(t *testing.T) {
	engine := txReadFixture(t)
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, reader.SetNamespace("test"))
	t.Cleanup(func() { _ = reader.Rollback() })

	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, writer.SetNamespace("test"))
	bob, err := writer.GetNode("test:bob")
	require.NoError(t, err)
	bob.Properties["name"] = "Changed after reader began"
	require.NoError(t, writer.UpdateNode(bob))
	require.NoError(t, writer.Commit())

	readNames := func() map[NodeID]string {
		t.Helper()
		names := make(map[NodeID]string)
		err := reader.StreamNodesByLabelProjected("Person", []string{"name"}, func(node *Node) error {
			names[node.ID] = node.Properties["name"].(string)
			return nil
		})
		require.NoError(t, err)
		return names
	}
	require.Equal(t, "Bob", readNames()["test:bob"])
	require.Len(t, reader.snapshotProjectedLabelNodes, 1, "a completed projection should be reusable")
	require.Equal(t, "Bob", readNames()["test:bob"], "cached replay must retain the reader's begin snapshot")
	require.Len(t, reader.snapshotProjectedLabelNodes, 1, "an equivalent projection should reuse its cache entry")
}

func TestTxReads_GetEdge_Committed(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	got, err := tx.GetEdge("test:e-knows-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, EdgeID("test:e-knows-1"), got.ID)
	require.Equal(t, NodeID("test:alice"), got.StartNode)
	require.Equal(t, NodeID("test:bob"), got.EndNode)
	require.Equal(t, "KNOWS", got.Type)
	require.Equal(t, int64(2020), got.Properties["since"])
}

func TestTxReads_GetEdge_PendingWriteVisible(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Stage a brand-new edge inside the open tx; GetEdge must read it
	// back via the pendingEdges fast path, not engine state.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-pending", StartNode: "test:alice", EndNode: "test:dave",
		Type: "MENTIONS", Properties: map[string]any{"weight": 0.7},
	}))

	got, err := tx.GetEdge("test:e-pending")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "MENTIONS", got.Type)
	require.Equal(t, 0.7, got.Properties["weight"])

	// Engine must NOT see it yet — the tx hasn't committed.
	_, err = engine.GetEdge("test:e-pending")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTxReads_GetEdge_DeletedReturnsNotFound(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.DeleteEdge("test:e-knows-1"))

	_, err = tx.GetEdge("test:e-knows-1")
	require.ErrorIs(t, err, ErrNotFound)

	// Deleted edge is still visible to the engine until commit.
	committed, err := engine.GetEdge("test:e-knows-1")
	require.NoError(t, err)
	require.NotNil(t, committed)
}

func TestTxReads_GetOutgoingEdges(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	out, err := tx.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-1", "test:e-knows-2"}, edgeIDs(out))
	out[0].Type = "CALLER_MUTATION"
	out, err = tx.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	require.Equal(t, "KNOWS", out[0].Type, "mutating returned edge data must not corrupt the transaction snapshot cache")

	updated, err := tx.GetEdge("test:e-knows-2")
	require.NoError(t, err)
	updated.EndNode = "test:dave"
	updated.Type = "MENTIONS"
	require.NoError(t, tx.UpdateEdge(updated))
	out, err = tx.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	var foundUpdated *Edge
	for _, edge := range out {
		if edge.ID == "test:e-knows-2" {
			foundUpdated = edge
		}
	}
	require.NotNil(t, foundUpdated)
	require.Equal(t, NodeID("test:dave"), foundUpdated.EndNode, "pending edge update must override the cached committed edge")
	require.Equal(t, "MENTIONS", foundUpdated.Type)

	// Layer a pending edge from alice — must merge in.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-new", StartNode: "test:alice", EndNode: "test:dave",
		Type: "MENTIONS", Properties: map[string]any{},
	}))
	require.NoError(t, tx.DeleteEdge("test:e-knows-1"))
	out, err = tx.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-2", "test:e-new"}, edgeIDs(out))

	// Pending edge from a different node must NOT show up under alice.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-other", StartNode: "test:bob", EndNode: "test:dave",
		Type: "REFERS",
	}))
	out, err = tx.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-2", "test:e-new"}, edgeIDs(out),
		"alice's outgoing must not include bob→dave")
}

func TestTxReads_GetIncomingEdges(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	in, err := tx.GetIncomingEdges("test:carol")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-follows", "test:e-knows-2"}, edgeIDs(in))

	// alice has no incoming edges.
	in, err = tx.GetIncomingEdges("test:alice")
	require.NoError(t, err)
	require.Empty(t, in)

	// Pending edge into alice — must show up in subsequent read.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-incoming", StartNode: "test:dave", EndNode: "test:alice",
		Type: "MENTIONS",
	}))
	in, err = tx.GetIncomingEdges("test:alice")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-incoming"}, edgeIDs(in))
}

func TestTxReads_GetEdgesBetween(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// alice→bob has exactly one edge.
	got, err := tx.GetEdgesBetween("test:alice", "test:bob")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-1"}, edgeIDs(got))

	// bob→alice has none (directed).
	got, err = tx.GetEdgesBetween("test:bob", "test:alice")
	require.NoError(t, err)
	require.Empty(t, got)

	// Add a second alice→bob edge as pending; the result must
	// include the committed one + the pending one but no dupes.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-knows-3", StartNode: "test:alice", EndNode: "test:bob",
		Type: "RIVALS",
	}))
	got, err = tx.GetEdgesBetween("test:alice", "test:bob")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-1", "test:e-knows-3"}, edgeIDs(got))
}

func TestTxReads_GetEdgeBetween(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Type-filtered single lookup hits the committed KNOWS edge.
	got := tx.GetEdgeBetween("test:alice", "test:bob", "KNOWS")
	require.NotNil(t, got)
	require.Equal(t, EdgeID("test:e-knows-1"), got.ID)

	// Empty type matches anything between the pair (first hit wins);
	// we can only assert it returns SOME alice→bob edge.
	got = tx.GetEdgeBetween("test:alice", "test:bob", "")
	require.NotNil(t, got)
	require.Equal(t, NodeID("test:alice"), got.StartNode)
	require.Equal(t, NodeID("test:bob"), got.EndNode)

	// Wrong type → nil (not error).
	got = tx.GetEdgeBetween("test:alice", "test:bob", "MENTIONS")
	require.Nil(t, got)

	// Unconnected pair → nil.
	got = tx.GetEdgeBetween("test:alice", "other:doc", "KNOWS")
	require.Nil(t, got)
}

func TestTxReads_GetEdgesByType(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Two committed KNOWS edges, no pending.
	knows, err := tx.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-knows-1", "test:e-knows-2"}, edgeIDs(knows))

	// FOLLOWS has exactly one.
	follows, err := tx.GetEdgesByType("FOLLOWS")
	require.NoError(t, err)
	require.Equal(t, []string{"test:e-follows"}, edgeIDs(follows))

	// Layer a pending KNOWS edge — must merge in.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-knows-pending", StartNode: "test:bob", EndNode: "test:dave",
		Type: "KNOWS",
	}))
	knows, err = tx.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Equal(t, []string{
		"test:e-knows-1", "test:e-knows-2", "test:e-knows-pending",
	}, edgeIDs(knows))

	// Pending edge of a DIFFERENT type must NOT contaminate.
	require.NoError(t, tx.CreateEdge(&Edge{
		ID: "test:e-mentions", StartNode: "test:alice", EndNode: "test:dave",
		Type: "MENTIONS",
	}))
	knows, err = tx.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.NotContains(t, edgeIDs(knows), "test:e-mentions")

	// Empty type returns nothing (no edge has empty type).
	none, err := tx.GetEdgesByType("NEVER")
	require.NoError(t, err)
	require.Empty(t, none)
}

func TestTxReads_GetNodesByLabel(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	people, err := tx.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, []string{
		"test:alice", "test:bob", "test:carol", "test:dave",
	}, nodeIDs(people))

	engineers, err := tx.GetNodesByLabel("Engineer")
	require.NoError(t, err)
	require.Equal(t, []string{"test:alice"}, nodeIDs(engineers))
}

// Separate case: pending nodes must merge into label scans, deleted
// committed nodes must drop out, unknown labels return empty.
func TestTxReads_GetNodesByLabel_PendingNodeMerges(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	_, err = tx.CreateNode(&Node{
		ID: "test:eve", Labels: []string{"Person", "Designer"},
		Properties: map[string]any{},
	})
	require.NoError(t, err)

	people, err := tx.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, []string{
		"test:alice", "test:bob", "test:carol", "test:dave", "test:eve",
	}, nodeIDs(people))

	designers, err := tx.GetNodesByLabel("Designer")
	require.NoError(t, err)
	require.Equal(t, []string{"test:eve"}, nodeIDs(designers))

	// Deleted committed node must drop out.
	require.NoError(t, tx.DeleteNode("test:bob"))
	people, err = tx.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.NotContains(t, nodeIDs(people), "test:bob")

	// Unknown label returns empty, not error.
	none, err := tx.GetNodesByLabel("NoSuchLabel")
	require.NoError(t, err)
	require.Empty(t, none)
}

func TestTxReads_GetFirstNodeByLabel(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	got, err := tx.GetFirstNodeByLabel("Engineer")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, NodeID("test:alice"), got.ID)

	// Multi-match: returns SOMETHING valid (first hit, not specified).
	got, err = tx.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, got.Labels, "Person")

	// No match → ErrNotFound, not silent nil.
	got, err = tx.GetFirstNodeByLabel("NoSuchLabel")
	require.ErrorIs(t, err, ErrNotFound)
	require.Nil(t, got)
}

func TestTxReads_AllNodesAndGetAllNodes(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	all, err := tx.AllNodes()
	require.NoError(t, err)
	require.Equal(t, []string{
		"other:doc", "test:alice", "test:bob", "test:carol", "test:dave",
	}, nodeIDs(all))

	// GetAllNodes wraps AllNodes and swallows errors.
	require.Equal(t, nodeIDs(all), nodeIDs(tx.GetAllNodes()))

	// Pending node added, deleted node removed, both reflected.
	_, err = tx.CreateNode(&Node{
		ID: "test:eve", Labels: []string{"Person"}, Properties: map[string]any{},
	})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode("test:dave"))

	all, err = tx.AllNodes()
	require.NoError(t, err)
	require.Equal(t, []string{
		"other:doc", "test:alice", "test:bob", "test:carol", "test:eve",
	}, nodeIDs(all))
}

func TestTxReads_MergePendingNodesLocked(t *testing.T) {
	committedNode := &Node{ID: "test:alice", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}}
	deletedNode := &Node{ID: "test:bob", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}}
	otherNode := &Node{ID: "test:doc", Labels: []string{"Document"}, Properties: map[string]any{"title": "Spec"}}
	pendingReplacement := &Node{ID: "test:alice", Labels: []string{"Person", "Engineer"}, Properties: map[string]any{"name": "Alice v2"}}
	pendingIncluded := &Node{ID: "test:carol", Labels: []string{"Person"}, Properties: map[string]any{"name": "Carol"}}
	pendingExcluded := &Node{ID: "test:draft", Labels: []string{"Draft"}, Properties: map[string]any{"name": "Draft"}}

	tx := &BadgerTransaction{
		Status: TxStatusActive,
		pendingNodes: map[NodeID]*Node{
			pendingReplacement.ID: pendingReplacement,
			pendingIncluded.ID:    pendingIncluded,
			pendingExcluded.ID:    pendingExcluded,
		},
		deletedNodes: map[NodeID]struct{}{
			deletedNode.ID: {},
		},
	}

	merged := tx.mergePendingNodesLocked([]*Node{nil, committedNode, deletedNode, otherNode}, func(node *Node) bool {
		if node == nil {
			return false
		}
		for _, label := range node.Labels {
			if label == "Person" {
				return true
			}
		}
		return false
	})

	require.Equal(t, []string{"test:alice", "test:carol"}, nodeIDs(merged))
	require.Equal(t, []string{"Person", "Engineer"}, merged[0].Labels)
	require.Equal(t, "Alice v2", merged[0].Properties["name"])

	merged[0].Labels[0] = "Mutated"
	merged[0].Properties["name"] = "mutated"
	require.Equal(t, []string{"Person", "Engineer"}, pendingReplacement.Labels)
	require.Equal(t, "Alice v2", pendingReplacement.Properties["name"])
	require.Equal(t, []string{"Person"}, committedNode.Labels)
	require.Equal(t, "Alice", committedNode.Properties["name"])
}

func TestTxReads_MergePendingEdgesLocked(t *testing.T) {
	committedEdge := &Edge{ID: "test:e1", StartNode: "test:alice", EndNode: "test:bob", Type: "KNOWS", Properties: map[string]any{"since": int64(2020)}}
	deletedEdge := &Edge{ID: "test:e2", StartNode: "test:alice", EndNode: "test:carol", Type: "KNOWS", Properties: map[string]any{"since": int64(2021)}}
	pendingReplacement := &Edge{ID: "test:e1", StartNode: "test:alice", EndNode: "test:bob", Type: "KNOWS", Properties: map[string]any{"since": int64(2024)}}
	pendingIncluded := &Edge{ID: "test:e3", StartNode: "test:bob", EndNode: "test:dave", Type: "KNOWS", Properties: map[string]any{"since": int64(2025)}}
	pendingExcluded := &Edge{ID: "test:e4", StartNode: "test:bob", EndNode: "test:dave", Type: "FOLLOWS", Properties: map[string]any{"since": int64(2026)}}

	tx := &BadgerTransaction{
		Status: TxStatusActive,
		pendingEdges: map[EdgeID]*Edge{
			pendingReplacement.ID: pendingReplacement,
			pendingIncluded.ID:    pendingIncluded,
			pendingExcluded.ID:    pendingExcluded,
		},
		deletedEdges: map[EdgeID]struct{}{
			deletedEdge.ID: {},
		},
	}

	merged := tx.mergePendingEdgesLocked([]*Edge{nil, committedEdge, deletedEdge}, func(edge *Edge) bool {
		return edge != nil && edge.Type == "KNOWS"
	})

	require.Equal(t, []string{"test:e1", "test:e3"}, edgeIDs(merged))
	require.Equal(t, int64(2024), merged[0].Properties["since"])

	merged[0].Properties["since"] = int64(9999)
	require.Equal(t, int64(2024), pendingReplacement.Properties["since"])
	require.Equal(t, int64(2020), committedEdge.Properties["since"])
}

func TestTxReads_GetAllNodes_ClosedTransactionReturnsNil(t *testing.T) {
	tx := &BadgerTransaction{Status: TxStatusCommitted}
	require.Nil(t, tx.GetAllNodes())
}

func BenchmarkTxReads_MergePendingNodesLocked_NoOverlay(b *testing.B) {
	committed := make([]*Node, 512)
	for i := range committed {
		committed[i] = &Node{ID: NodeID("test:n" + strconv.Itoa(i)), Labels: []string{"Person"}}
	}
	tx := &BadgerTransaction{Status: TxStatusActive}
	include := func(node *Node) bool { return node != nil && len(node.Labels) > 0 && node.Labels[0] == "Person" }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := tx.mergePendingNodesLocked(committed, include); len(got) != 512 {
			b.Fatalf("expected 512 nodes, got %d", len(got))
		}
	}
}

func BenchmarkTxReads_MergePendingNodesLocked_WithOverlay(b *testing.B) {
	committed := make([]*Node, 512)
	pending := make(map[NodeID]*Node, 64)
	for i := range committed {
		committed[i] = &Node{ID: NodeID("test:n" + strconv.Itoa(i)), Labels: []string{"Person"}}
	}
	for i := 0; i < 64; i++ {
		id := NodeID("test:n" + strconv.Itoa(i))
		pending[id] = &Node{ID: id, Labels: []string{"Person", "Engineer"}}
	}
	tx := &BadgerTransaction{Status: TxStatusActive, pendingNodes: pending}
	include := func(node *Node) bool { return node != nil && len(node.Labels) > 0 && node.Labels[0] == "Person" }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := tx.mergePendingNodesLocked(committed, include); len(got) != 512 {
			b.Fatalf("expected 512 nodes, got %d", len(got))
		}
	}
}

func BenchmarkTxReads_MergePendingEdgesLocked_WithOverlay(b *testing.B) {
	committed := make([]*Edge, 512)
	pending := make(map[EdgeID]*Edge, 64)
	for i := range committed {
		committed[i] = &Edge{ID: EdgeID("test:e" + strconv.Itoa(i)), StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}
	}
	for i := 0; i < 64; i++ {
		id := EdgeID("test:e" + strconv.Itoa(i))
		pending[id] = &Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}
	}
	tx := &BadgerTransaction{Status: TxStatusActive, pendingEdges: pending}
	include := func(edge *Edge) bool { return edge != nil && edge.Type == "KNOWS" }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := tx.mergePendingEdgesLocked(committed, include); len(got) != 512 {
			b.Fatalf("expected 512 edges, got %d", len(got))
		}
	}
}

func TestTxReads_BulkCreateEdges_Success(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Mix: one committed-endpoint pair, one with a pending start node,
	// one with a pending end node. The shared existence cache must
	// handle all three.
	_, err = tx.CreateNode(&Node{
		ID: "test:eve", Labels: []string{"Person"}, Properties: map[string]any{},
	})
	require.NoError(t, err)

	require.NoError(t, tx.BulkCreateEdges([]*Edge{
		{ID: "test:e-bulk-1", StartNode: "test:alice", EndNode: "test:bob", Type: "BULK"},
		{ID: "test:e-bulk-2", StartNode: "test:eve", EndNode: "test:bob", Type: "BULK"},
		{ID: "test:e-bulk-3", StartNode: "test:alice", EndNode: "test:eve", Type: "BULK"},
	}))

	got, err := tx.GetEdgesByType("BULK")
	require.NoError(t, err)
	require.Equal(t, []string{
		"test:e-bulk-1", "test:e-bulk-2", "test:e-bulk-3",
	}, edgeIDs(got))

	// Each one round-trips.
	for _, id := range []EdgeID{"test:e-bulk-1", "test:e-bulk-2", "test:e-bulk-3"} {
		e, err := tx.GetEdge(id)
		require.NoError(t, err)
		require.Equal(t, "BULK", e.Type)
	}

	// Empty input is a no-op (not an error).
	require.NoError(t, tx.BulkCreateEdges(nil))
	require.NoError(t, tx.BulkCreateEdges([]*Edge{}))
}

func TestTxReads_BulkCreateEdges_FailsOnMissingEndpoint(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	err = tx.BulkCreateEdges([]*Edge{
		{ID: "test:e-bulk-bad", StartNode: "test:alice", EndNode: "test:nobody", Type: "BULK"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "test:nobody")
	require.Contains(t, err.Error(), "does not exist")

	err = tx.BulkCreateEdges([]*Edge{
		{ID: "test:e-bulk-bad-2", StartNode: "test:nobody", EndNode: "test:alice", Type: "BULK"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "test:nobody")
}

func TestTxReads_BulkCreateEdges_RejectsDuplicate(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Same ID staged twice in the same call.
	err = tx.BulkCreateEdges([]*Edge{
		{ID: "test:e-dup", StartNode: "test:alice", EndNode: "test:bob", Type: "DUP"},
		{ID: "test:e-dup", StartNode: "test:alice", EndNode: "test:bob", Type: "DUP"},
	})
	require.ErrorIs(t, err, ErrAlreadyExists)
}

func TestTxReads_BulkCreateEdges_RejectsInvalidInput(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.ErrorIs(t,
		tx.BulkCreateEdges([]*Edge{nil}),
		ErrInvalidData,
	)
	require.ErrorIs(t,
		tx.BulkCreateEdges([]*Edge{{StartNode: "test:alice", EndNode: "test:bob", Type: "X"}}),
		ErrInvalidID,
	)
}

func TestTxReads_SetImplicit(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, tx.SetImplicit(true))
	require.True(t, tx.implicit, "SetImplicit(true) must flip the flag")

	require.NoError(t, tx.SetImplicit(false))
	require.False(t, tx.implicit, "SetImplicit(false) must clear the flag")

	// Closed transaction rejects further config changes.
	require.NoError(t, tx.Rollback())
	err = tx.SetImplicit(true)
	require.Error(t, err, "SetImplicit on closed tx must fail")
}

// TestTxReads_CreateEdgeSingleAndBulkAgree pins that a transaction creates
// edges one at a time and in bulk with the same checks (#683, #547): the same
// error for a nil edge and an empty ID, the lifecycle check even for an empty
// batch, and a rejected edge leaves the batch's earlier edges unbuffered.
func TestTxReads_CreateEdgeSingleAndBulkAgree(t *testing.T) {
	engine := txReadFixture(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	require.ErrorIs(t, tx.CreateEdge(nil), ErrInvalidData)
	require.ErrorIs(t, tx.BulkCreateEdges([]*Edge{nil}), ErrInvalidData)
	require.ErrorIs(t, tx.CreateEdge(&Edge{StartNode: "test:alice", EndNode: "test:bob", Type: "X"}), ErrInvalidID)

	// The second edge's missing endpoint rejects the batch: the first edge
	// is not buffered.
	err = tx.BulkCreateEdges([]*Edge{
		{ID: "test:e-ok", StartNode: "test:alice", EndNode: "test:bob", Type: "OK"},
		{ID: "test:e-bad", StartNode: "test:alice", EndNode: "test:nobody", Type: "BAD"},
	})
	require.Error(t, err)
	_, err = tx.GetEdge("test:e-ok")
	require.ErrorIs(t, err, ErrNotFound)

	// After the transaction ends, an empty batch reports it like any other.
	require.NoError(t, tx.Rollback())
	require.Error(t, tx.BulkCreateEdges(nil))
}
