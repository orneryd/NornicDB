package storage

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

func setupDecayTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

func setupDecayBindings(t *testing.T, engine *BadgerEngine, namespace string) {
	t.Helper()
	sm := engine.GetSchemaForNamespace(namespace)

	err := sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "episode_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "bind_episode",
		ProfileRef:   "episode_decay",
		TargetLabels: []string{"MemoryEpisode"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createTestNode(t *testing.T, engine *BadgerEngine, id string, labels []string, createdAt time.Time) {
	t.Helper()
	node := &Node{
		ID:         NodeID(id),
		Labels:     labels,
		Properties: map[string]any{"name": id},
		CreatedAt:  createdAt,
		UpdatedAt:  createdAt,
	}
	if _, err := engine.CreateNode(node); err != nil {
		t.Fatal(err)
	}
}

func TestDecayFilter_DisabledByDefault(t *testing.T) {
	engine := setupDecayTestEngine(t)
	createTestNode(t, engine, "nornic:n1", []string{"MemoryEpisode"}, time.Now().Add(-48*time.Hour))

	nodes, err := engine.GetNodesByLabel("MemoryEpisode")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Errorf("expected 1 node when decay disabled, got %d", len(nodes))
	}
}

func TestDecayFilter_SuppressesDecayedNode(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	setupDecayBindings(t, engine, ns)
	engine.SetDecayEnabled(true)

	nodeID := ns + ":old_episode"
	createTestNode(t, engine, nodeID, []string{"MemoryEpisode"}, time.Now().Add(-720*time.Hour)) // 30 days old, ~8 half-lives

	nodes, err := engine.GetNodesByLabel("MemoryEpisode")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			t.Error("old node should be suppressed by decay")
		}
	}
}

func TestDecayFilter_KeepsRecentNode(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	setupDecayBindings(t, engine, ns)
	engine.SetDecayEnabled(true)

	nodeID := ns + ":recent_episode"
	createTestNode(t, engine, nodeID, []string{"MemoryEpisode"}, time.Now().Add(-5*time.Minute))

	nodes, err := engine.GetNodesByLabel("MemoryEpisode")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			found = true
		}
	}
	if !found {
		t.Error("recent node should not be suppressed")
	}
}

func TestDecayFilter_NoDecayBindingAlwaysReturns(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	sm := engine.GetSchemaForNamespace(ns)
	err := sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:      "fact_nodecay",
		Scope:     knowledgepolicy.ScopeNode,
		Function:  knowledgepolicy.DecayFunctionNone,
		ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "bind_fact",
		ProfileRef:   "fact_nodecay",
		TargetLabels: []string{"KnowledgeFact"},
		NoDecay:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine.SetDecayEnabled(true)

	nodeID := ns + ":old_fact"
	createTestNode(t, engine, nodeID, []string{"KnowledgeFact"}, time.Now().Add(-8760*time.Hour)) // 1 year old

	nodes, err := engine.GetNodesByLabel("KnowledgeFact")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			found = true
		}
	}
	if !found {
		t.Error("no-decay node should always be returned regardless of age")
	}
}

func TestDecayFilter_RevealAllBypasses(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	setupDecayBindings(t, engine, ns)
	engine.SetDecayEnabled(true)

	nodeID := ns + ":old_episode"
	createTestNode(t, engine, nodeID, []string{"MemoryEpisode"}, time.Now().Add(-720*time.Hour))

	engine.SetRevealAll(true)
	defer engine.SetRevealAll(false)

	nodes, err := engine.GetNodesByLabel("MemoryEpisode")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			found = true
		}
	}
	if !found {
		t.Error("reveal should bypass decay suppression")
	}
}

func TestDecayFilter_UnmatchedLabelNotSuppressed(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	setupDecayBindings(t, engine, ns)
	engine.SetDecayEnabled(true)

	nodeID := ns + ":custom_node"
	createTestNode(t, engine, nodeID, []string{"CustomLabel"}, time.Now().Add(-720*time.Hour))

	nodes, err := engine.GetNodesByLabel("CustomLabel")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range nodes {
		if string(n.ID) == nodeID {
			found = true
		}
	}
	if !found {
		t.Error("node with no matching binding should not be suppressed")
	}
}

func TestDecayFilter_BindingTableRebuiltOnDDL(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"
	sm := engine.GetSchemaForNamespace(ns)

	// Initially no binding table
	if bt := sm.GetBindingTable(); bt == nil {
		// expected
	}

	// Create a bundle
	err := sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:                "test_decay",
		Scope:               knowledgepolicy.ScopeNode,
		Function:            knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds:     3600,
		VisibilityThreshold: 0.10,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a binding — this should trigger BindingTable rebuild
	err = sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{
		Name:         "test_bind",
		ProfileRef:   "test_decay",
		TargetLabels: []string{"TestNode"},
	})
	if err != nil {
		t.Fatal(err)
	}

	bt := sm.GetBindingTable()
	if bt == nil {
		t.Fatal("BindingTable should be rebuilt after DDL")
	}

	labels := []string{"TestNode"}
	sort.Strings(labels)
	key := strings.Join(labels, "\x00")
	if cb := bt.LookupNode(key); cb == nil {
		t.Error("BindingTable should contain the TestNode binding")
	}
}

func TestDecayFilter_AllNodesFiltered(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	setupDecayBindings(t, engine, ns)
	engine.SetDecayEnabled(true)

	recentID := ns + ":recent"
	createTestNode(t, engine, recentID, []string{"MemoryEpisode"}, time.Now().Add(-5*time.Minute))

	oldID := ns + ":old"
	createTestNode(t, engine, oldID, []string{"MemoryEpisode"}, time.Now().Add(-720*time.Hour))

	otherID := ns + ":other"
	createTestNode(t, engine, otherID, []string{"OtherLabel"}, time.Now().Add(-720*time.Hour))

	nodes := engine.GetAllNodes()
	for _, n := range nodes {
		if string(n.ID) == oldID {
			t.Error("AllNodes should filter decayed MemoryEpisode")
		}
	}

	foundRecent := false
	foundOther := false
	for _, n := range nodes {
		if string(n.ID) == recentID {
			foundRecent = true
		}
		if string(n.ID) == otherID {
			foundOther = true
		}
	}
	if !foundRecent {
		t.Error("AllNodes should include recent MemoryEpisode")
	}
	if !foundOther {
		t.Error("AllNodes should include nodes without decay bindings")
	}
}
