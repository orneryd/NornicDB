package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// These adapters expose the supported capabilities of older engines without
// replacing their storage behavior with a test implementation.
type scopedLabelBasicAdapter struct{ Engine }
type scopedLabelIDAdapter struct {
	Engine
	LabelNodeIDLookupEngine
}

func TestScopedLabelIDWrapperFallbackContracts(t *testing.T) {
	for _, capability := range []string{"full_nodes", "unscoped_ids", "traced_full_nodes"} {
		t.Run(capability, func(t *testing.T) {
			engine := scopedLabelOrderFixture(t)
			var wrapped Engine = scopedLabelBasicAdapter{Engine: engine}
			if capability == "unscoped_ids" {
				wrapped = scopedLabelIDAdapter{Engine: engine, LabelNodeIDLookupEngine: engine}
			} else if capability == "traced_full_nodes" {
				wrapped = NewTracedEngine(wrapped)
			}
			namespace := NewNamespacedEngine(wrapped, "a")
			require.NoError(t, namespace.ForEachNodeIDByLabel(context.Background(), "Person", nil))
			var ids []NodeID
			require.NoError(t, namespace.ForEachNodeIDByLabel(context.Background(), "Person", func(id NodeID) bool {
				ids = append(ids, id)
				return true
			}))
			require.Equal(t, []NodeID{"z", "a", "m"}, ids)
			visits := 0
			require.NoError(t, namespace.ForEachNodeIDByLabel(context.Background(), "Person", func(id NodeID) bool {
				visits++
				require.Equal(t, NodeID("z"), id)
				return false
			}))
			require.Equal(t, 1, visits)
			require.NoError(t, engine.Close())
			require.ErrorIs(t, namespace.ForEachNodeIDByLabel(context.Background(), "Person", func(NodeID) bool {
				t.Fatal("closed engine delivered an ID")
				return false
			}), ErrStorageClosed)
		})
	}
}

func TestScopedLabelTracingPreservesReaderCapabilities(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	traced := NewTracedEngine(engine)
	for _, reader := range []ScopedLabelNodeReader{traced, NewTracedEngine(scopedLabelBasicAdapter{Engine: engine})} {
		// The adapter intentionally supports only the ordinary Engine methods.
		// Node filtering still belongs to the public namespace wrapper.
		namespace := NewNamespacedEngine(reader.(Engine), "a")
		nodes, err := namespace.GetNodesByLabel(context.Background(), "Person")
		require.NoError(t, err)
		require.Equal(t, []NodeID{"z", "a", "m"}, testNodeIDs(nodes))
		first, err := namespace.GetFirstNodeByLabel(t.Context(), "Person")
		require.NoError(t, err)
		require.Equal(t, NodeID("z"), first.ID)
	}
	var ids []NodeID
	require.NoError(t, traced.ForEachNodeIDByLabel(context.Background(), "Person", func(id NodeID) bool {
		ids = append(ids, id)
		return true
	}))
	require.Equal(t, []NodeID{"a:z", "b:z", "a:a", "a:m", "ab:foreign"}, ids)
	require.NoError(t, traced.ForEachNodeIDByLabelInScope(context.Background(), "a:", "Person", nil))
	stop := errors.New("caller stopped")
	visits := 0
	require.ErrorIs(t, traced.StreamNodesByLabelProjectedInScope(context.Background(), "a:", "Person", []string{"id"}, func(node *Node) error {
		visits++
		require.Equal(t, NodeID("a:z"), node.ID)
		require.Equal(t, map[string]any{"id": "a:z"}, node.Properties)
		return stop
	}), stop)
	require.Equal(t, 1, visits)
	nodes, err := traced.GetNodesByLabelVisibleAtInScope(context.Background(), "a:", "Person", engine.currentMVCCReadVersion("a"))
	require.NoError(t, err)
	require.Equal(t, []NodeID{"a:z", "a:a", "a:m"}, testNodeIDs(nodes))
	// The adapter hides the optional scoped and projected readers, so the
	// projected stream is answered by the mandatory stream kernel inside the
	// engine's own view. The kernel takes Label as an index hint and visits
	// every node the scope covers, so the fallback settles the read's label
	// membership: the scope's other label ("person" on a:lower, #862) is not a
	// row of this read.
	legacy := NewTracedEngine(scopedLabelBasicAdapter{Engine: engine})
	var legacyIDs []NodeID
	require.NoError(t, legacy.StreamNodesByLabelProjectedInScope(context.Background(), "a:", "Person", []string{"id"}, func(node *Node) error {
		legacyIDs = append(legacyIDs, node.ID)
		require.Equal(t, map[string]any{"id": string(node.ID)}, node.Properties)
		return nil
	}))
	require.ElementsMatch(t, []NodeID{"a:z", "a:a", "a:m"}, legacyIDs)
	legacyVisits := 0
	require.ErrorIs(t, legacy.StreamNodesByLabelProjectedInScope(context.Background(), "a:", "Person", nil, func(node *Node) error {
		legacyVisits++
		require.True(t, hasLabel(node.Labels, "Person"), "the fallback visited a node of another label")
		require.Equal(t, map[string]any{"id": string(node.ID), "extra": true}, node.Properties)
		return stop
	}), stop)
	require.Equal(t, 1, legacyVisits)
	// The snapshot-visible label read has no such fallback: an engine without
	// MVCCIndexedVisibilityEngine cannot answer it and reports that.
	_, err = legacy.GetNodesByLabelVisibleAtInScope(context.Background(), "a:", "Person", engine.currentMVCCReadVersion("a"))
	require.ErrorIs(t, err, ErrNotImplemented)
}

// TestScopedLabelProjectedFallbackStaysInsideItsLabel proves the public
// namespaced projected read over an engine that exposes only the mandatory
// Engine methods: every row is the label's and the scope's, so a consumer that
// trusts the storage read (the vector index population, for one) never sees
// another label's node.
func TestScopedLabelProjectedFallbackStaysInsideItsLabel(t *testing.T) {
	engine := scopedLabelOrderFixture(t)
	namespace := NewNamespacedEngine(NewTracedEngine(scopedLabelBasicAdapter{Engine: engine}), "a")
	require.ErrorIs(t, namespace.StreamNodesByLabelProjected(context.Background(), "Person", nil, nil), ErrInvalidData)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, namespace.StreamNodesByLabelProjected(canceled, "Person", nil, func(*Node) error {
		t.Fatal("a canceled read delivered a node")
		return nil
	}), context.Canceled)
	var ids []NodeID
	require.NoError(t, namespace.StreamNodesByLabelProjected(context.Background(), "Person", []string{"id"}, func(node *Node) error {
		ids = append(ids, node.ID)
		require.Equal(t, map[string]any{"id": "a:" + string(node.ID)}, node.Properties)
		return nil
	}))
	require.ElementsMatch(t, []NodeID{"z", "a", "m"}, ids)
	stop := errors.New("caller stopped")
	visits := 0
	require.ErrorIs(t, namespace.StreamNodesByLabelProjected(context.Background(), "Person", nil, func(*Node) error {
		visits++
		return stop
	}), stop)
	require.Equal(t, 1, visits)
}
