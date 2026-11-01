package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

func setupDecayTestEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
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
		DecayEnabled:        true,
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

func TestDecayFilter_NoDecayBindingAlwaysReturns(t *testing.T) {
	engine := setupDecayTestEngine(t)
	ns := "testns"

	sm := engine.GetSchemaForNamespace(ns)
	err := sm.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name:         "fact_nodecay",
		Scope:        knowledgepolicy.ScopeNode,
		Function:     knowledgepolicy.DecayFunctionNone,
		ScoreFrom:    knowledgepolicy.ScoreFromCreated,
		Enabled:      true,
		DecayEnabled: false,
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

	nodes, err := engine.GetNodesByLabel(context.Background(), "KnowledgeFact")
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
		DecayEnabled:        true,
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

	nodes, err := engine.AllNodes(context.Background())
	if err != nil {
		t.Fatalf("AllNodes failed: %v", err)
	}
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

func TestDecayFilter_Visibility(t *testing.T) {
	for _, test := range []struct {
		name, id, label                          string
		age                                      time.Duration
		configured, enabled, reveal, wantVisible bool
	}{
		{name: "TestDecayFilter_DisabledByDefault", id: "nornic:n1", label: "MemoryEpisode", age: 48 * time.Hour, wantVisible: true},
		{name: "TestDecayFilter_SuppressesDecayedNode", id: "testns:old_episode", label: "MemoryEpisode", age: 720 * time.Hour, configured: true, enabled: true},
		{name: "TestDecayFilter_KeepsRecentNode", id: "testns:recent_episode", label: "MemoryEpisode", age: 5 * time.Minute, configured: true, enabled: true, wantVisible: true},
		{name: "TestDecayFilter_RevealAllBypasses", id: "testns:old_episode", label: "MemoryEpisode", age: 720 * time.Hour, configured: true, enabled: true, reveal: true, wantVisible: true},
		{name: "TestDecayFilter_UnmatchedLabelNotSuppressed", id: "testns:custom_node", label: "CustomLabel", age: 720 * time.Hour, configured: true, enabled: true, wantVisible: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := setupDecayTestEngine(t)
			if test.configured {
				setupDecayBindings(t, engine, "testns")
			}
			if test.enabled {
				engine.SetDecayEnabled(true)
			}
			createTestNode(t, engine, test.id, []string{test.label}, time.Now().Add(-test.age))
			if test.reveal {
				engine.SetRevealAll(true)
				defer engine.SetRevealAll(false)
			}
			nodes, err := engine.GetNodesByLabel(context.Background(), test.label)
			if err != nil {
				t.Fatal(err)
			}
			if !test.wantVisible {
				if len(nodes) != 0 {
					t.Fatalf("decayed node must be suppressed: %+v", nodes)
				}
				return
			}
			if len(nodes) != 1 || string(nodes[0].ID) != test.id {
				t.Fatalf("expected visible node %s, got %+v", test.id, nodes)
			}
		})
	}
}
