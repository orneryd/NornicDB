package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func setupVisibilityTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)

	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"episode_decay": {
			Name:                "episode_decay",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     3600,
			VisibilityThreshold: 0.10,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"bind_episode": {
			Name:         "bind_episode",
			ProfileRef:   "episode_decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
	}

	bt, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sm := eng.GetSchemaForNamespace("nornic")
	sm.SetBindingTable(bt)
	return eng
}

func TestVisibilitySuppressed_FastPathNode(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		runtime:    &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}},
		ID:         "nornic:ep1",
		Labels:     []string{"MemoryEpisode"},
		Properties: map[string]interface{}{"name": "test"},
		CreatedAt:  time.Now().Add(-720 * time.Hour),
		UpdatedAt:  time.Now().Add(-720 * time.Hour),
	}

	nowNanos := DecayScoringTime()
	if !eng.filterEntityByDecay(visibleRead, node, nowNanos) {
		t.Error("expected suppressed node to be filtered by fast path")
	}
}

func TestVisibilitySuppressed_RevealAllBypass(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		runtime:    &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}},
		ID:         "nornic:ep1",
		Labels:     []string{"MemoryEpisode"},
		Properties: map[string]interface{}{"name": "test"},
		CreatedAt:  time.Now().Add(-720 * time.Hour),
		UpdatedAt:  time.Now().Add(-720 * time.Hour),
	}

	eng.SetRevealAll(true)
	defer eng.SetRevealAll(false)

	nowNanos := DecayScoringTime()
	if eng.filterEntityByDecay(visibleRead, node, nowNanos) {
		t.Error("revealAll should bypass VisibilitySuppressed")
	}
}

func TestVisibilitySuppressed_FastPathEdge(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	edge := &Edge{
		ID:                   "nornic:edge1",
		Type:                 "RELATES_TO",
		StartNode:            "nornic:a",
		EndNode:              "nornic:b",
		CreatedAt:            time.Now().Add(-720 * time.Hour),
		VisibilitySuppressed: true,
	}

	nowNanos := DecayScoringTime()
	if !eng.filterEntityByDecay(visibleRead, edge, nowNanos) {
		t.Error("expected suppressed edge to be filtered by fast path")
	}
}

func TestVisibilitySuppressed_NotSuppressedPassesThrough(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		ID:         "nornic:recent",
		Labels:     []string{"MemoryEpisode"},
		Properties: map[string]interface{}{"name": "test"},
		CreatedAt:  time.Now().Add(-5 * time.Minute),
		UpdatedAt:  time.Now().Add(-5 * time.Minute),
	}

	nowNanos := DecayScoringTime()
	if eng.filterEntityByDecay(visibleRead, node, nowNanos) {
		t.Error("recent non-suppressed node should not be filtered")
	}
}

func TestVisibilitySuppressed_ScoringRunsBeforeStaleFlagForReverseFloor(t *testing.T) {
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)

	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"reverse_floor": {
			Name:                "reverse_floor",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     -3600,
			VisibilityThreshold: 0.10,
			ScoreFloor:          0.20,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"bind_reverse_floor": {
			Name:         "bind_reverse_floor",
			ProfileRef:   "reverse_floor",
			TargetLabels: []string{"ReverseMemory"},
		},
	}
	bt, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	eng.GetSchemaForNamespace("nornic").SetBindingTable(bt)

	now := time.Now()
	node := &Node{
		runtime:    &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}},
		ID:         "nornic:reverse-floor",
		Labels:     []string{"ReverseMemory"},
		Properties: map[string]interface{}{"name": "test"},
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if eng.filterEntityByDecay(visibleRead, node, now.UnixNano()) {
		t.Fatal("lightweight scoring must run before stale VisibilitySuppressed flag; scoreFloor clears the gate")
	}
}

func TestStreamNodesByLabelProjected_ScoresBeforeStaleVisibility(t *testing.T) {
	eng := newTestEngine(t)
	eng.SetDecayEnabled(true)
	bundles := map[string]*knowledgepolicy.DecayProfileBundle{
		"reverse_floor": {
			Name:                "reverse_floor",
			Scope:               knowledgepolicy.ScopeNode,
			Function:            knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds:     -3600,
			VisibilityThreshold: 0.10,
			ScoreFloor:          0.20,
			ScoreFrom:           knowledgepolicy.ScoreFromCreated,
			Enabled:             true,
			DecayEnabled:        true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"bind_reverse_floor": {
			Name:         "bind_reverse_floor",
			ProfileRef:   "reverse_floor",
			TargetLabels: []string{"ReverseMemory"},
		},
	}
	bindingTable, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, nil)
	require.NoError(t, err)
	eng.GetSchemaForNamespace("nornic").SetBindingTable(bindingTable)

	now := time.Now()
	node := &Node{
		runtime:    &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}},
		ID:         "nornic:reverse-floor-stream",
		Labels:     []string{"ReverseMemory"},
		Properties: map[string]interface{}{"name": "test"},
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_, err = eng.CreateNode(node)
	require.NoError(t, err)
	eng.cacheStoreNode(node)

	var visited []NodeID
	err = eng.StreamNodesByLabelProjected("ReverseMemory", []string{"name"}, func(projected *Node) error {
		visited = append(visited, projected.ID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{node.ID}, visited, "decay scoring must clear a stale suppression flag before the stream hides the node")
}
