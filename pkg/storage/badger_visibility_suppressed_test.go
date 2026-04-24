package storage

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
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
		ID:                   "nornic:ep1",
		Labels:               []string{"MemoryEpisode"},
		Properties:           map[string]interface{}{"name": "test"},
		CreatedAt:            time.Now().Add(-720 * time.Hour),
		UpdatedAt:            time.Now().Add(-720 * time.Hour),
		VisibilitySuppressed: true,
	}

	nowNanos := DecayScoringTime()
	if !eng.filterNodeByDecay(node, nowNanos) {
		t.Error("expected suppressed node to be filtered by fast path")
	}
}

func TestVisibilitySuppressed_RevealAllBypass(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		ID:                   "nornic:ep1",
		Labels:               []string{"MemoryEpisode"},
		Properties:           map[string]interface{}{"name": "test"},
		CreatedAt:            time.Now().Add(-720 * time.Hour),
		UpdatedAt:            time.Now().Add(-720 * time.Hour),
		VisibilitySuppressed: true,
	}

	eng.SetRevealAll(true)
	defer eng.SetRevealAll(false)

	nowNanos := DecayScoringTime()
	if eng.filterNodeByDecay(node, nowNanos) {
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
	if !eng.filterEdgeByDecay(edge, nowNanos) {
		t.Error("expected suppressed edge to be filtered by fast path")
	}
}

func TestVisibilitySuppressed_NotSuppressedPassesThrough(t *testing.T) {
	eng := setupVisibilityTestEngine(t)

	node := &Node{
		ID:                   "nornic:recent",
		Labels:               []string{"MemoryEpisode"},
		Properties:           map[string]interface{}{"name": "test"},
		CreatedAt:            time.Now().Add(-5 * time.Minute),
		UpdatedAt:            time.Now().Add(-5 * time.Minute),
		VisibilitySuppressed: false,
	}

	nowNanos := DecayScoringTime()
	if eng.filterNodeByDecay(node, nowNanos) {
		t.Error("recent non-suppressed node should not be filtered")
	}
}
