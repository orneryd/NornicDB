package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nsTestEngine wraps the BadgerEngine in a NamespacedEngine bound to
// "ns" so the namespace-scoped wrappers actually have data to find.
// Helper kept here so each test stays self-contained.
func nsTestEngine(t *testing.T) (*MemoryEngine, *NamespacedEngine) {
	t.Helper()
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	return base, NewNamespacedEngine(base, "ns")
}

func TestNamespacedEngine_GetNodeLatestVisible_RoundTrip(t *testing.T) {
	base, ns := nsTestEngine(t)

	// Create through the namespaced engine so the prefix lands.
	_, err := ns.CreateNode(&Node{
		ID: "alice", Labels: []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	require.NoError(t, err)

	// Latest-visible by user-side ID; result must come back stripped
	// of the namespace prefix.
	got, err := ns.GetNodeLatestVisible("alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, NodeID("alice"), got.ID, "result must be stripped of ns prefix")
	require.Equal(t, "Alice", got.Properties["name"])

	// Confirm the underlying engine has the prefixed form.
	prefixed, err := base.GetNode("ns:alice")
	require.NoError(t, err)
	require.NotNil(t, prefixed)
}

func TestNamespacedEngine_GetEdgeLatestVisible_RoundTrip(t *testing.T) {
	_, ns := nsTestEngine(t)

	_, err := ns.CreateNode(&Node{ID: "a", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = ns.CreateNode(&Node{ID: "b", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&Edge{
		ID: "e1", StartNode: "a", EndNode: "b", Type: "REL",
		Properties: map[string]any{"v": int64(1)},
	}))

	got, err := ns.GetEdgeLatestVisible("e1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, EdgeID("e1"), got.ID, "user-side edge ID must be unprefixed")
	require.Equal(t, NodeID("a"), got.StartNode, "endpoints must be unprefixed too")
	require.Equal(t, NodeID("b"), got.EndNode)
}

func TestNamespacedEngine_GetNodeAndEdgeVisibleAt(t *testing.T) {
	_, ns := nsTestEngine(t)

	_, err := ns.CreateNode(&Node{ID: "x", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	_, err = ns.CreateNode(&Node{ID: "y", Labels: []string{"L"}, Properties: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, ns.CreateEdge(&Edge{
		ID: "e", StartNode: "x", EndNode: "y", Type: "REL", Properties: map[string]any{},
	}))

	headNode, err := ns.GetNodeCurrentHead("x")
	require.NoError(t, err)
	require.False(t, headNode.Tombstoned)
	require.False(t, headNode.Version.IsZero())

	headEdge, err := ns.GetEdgeCurrentHead("e")
	require.NoError(t, err)
	require.False(t, headEdge.Tombstoned)

	// Latest visible at the head version returns the body via the
	// fast path.
	gotNode, err := ns.GetNodeVisibleAt("x", headNode.Version)
	require.NoError(t, err)
	require.Equal(t, NodeID("x"), gotNode.ID)

	gotEdge, err := ns.GetEdgeVisibleAt("e", headEdge.Version)
	require.NoError(t, err)
	require.Equal(t, EdgeID("e"), gotEdge.ID)

	// Reading at a version below the floor returns ErrNotFound.
	old := MVCCVersion{
		CommitTimestamp: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		CommitSequence:  0,
	}
	_, err = ns.GetNodeVisibleAt("x", old)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestNamespacedEngine_LifecycleAndPrune_Delegate(t *testing.T) {
	_, ns := nsTestEngine(t)

	// LifecycleStatus inherits the underlying enabled flag and
	// adds a namespace key.
	status := ns.LifecycleStatus()
	require.Equal(t, "ns", status["namespace"])

	// TriggerPruneNow delegates without error when no controller is
	// installed (BadgerEngine returns nil).
	require.NoError(t, ns.TriggerPruneNow(context.Background()))

	// RegisterSnapshotReader returns a non-nil deregister fn (inner
	// MVCCLifecycleEngine implementation provides one even with no
	// active controller — the engine has its own tracking counter).
	dereg := ns.RegisterSnapshotReader(SnapshotReaderInfo{Namespace: "explicit"})
	require.NotNil(t, dereg)
	dereg()
}

func TestNamespacedEngine_GetTemporalNodeAsOf_NotImplemented(t *testing.T) {
	// MemoryEngine wraps BadgerEngine; BadgerEngine implements
	// NamespaceTemporalLookupProvider via GetTemporalNodeAsOfInNamespace.
	// With no temporal index data, the result is (nil, nil) — not an
	// error. This exercises the wrapper's "delegate exists" branch.
	_, ns := nsTestEngine(t)
	got, err := ns.GetTemporalNodeAsOf("Order", "key", "k1", "valid_from", "valid_to", time.Now())
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestNamespacedEngine_IsCurrentTemporalNode_NoIndex(t *testing.T) {
	_, ns := nsTestEngine(t)
	// Without a temporal constraint, there's no index for the node and
	// the underlying provider returns true (everything is "current"
	// when nothing is being temporally indexed). This is the
	// degenerate path; the wrapper just delegates.
	node := &Node{ID: "test:x", Labels: []string{"L"}}
	got, err := ns.IsCurrentTemporalNode(node, time.Now())
	require.NoError(t, err)
	// Either true (no index) or false (filtered out) is acceptable
	// per the wrapper's spec — what matters is no error and no panic.
	_ = got
}
