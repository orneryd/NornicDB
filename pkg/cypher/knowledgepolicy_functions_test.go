package cypher

import (
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupDecayEngine(t *testing.T) (*storage.BadgerEngine, *StorageExecutor) {
	t.Helper()
	eng, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

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
		"fact_nodecay": {
			Name:      "fact_nodecay",
			Scope:     knowledgepolicy.ScopeNode,
			Function:  knowledgepolicy.DecayFunctionNone,
			ScoreFrom: knowledgepolicy.ScoreFromCreated,
			Enabled:   true,
		},
	}
	bindings := map[string]*knowledgepolicy.DecayProfileBinding{
		"bind_episode": {
			Name:         "bind_episode",
			ProfileRef:   "episode_decay",
			TargetLabels: []string{"MemoryEpisode"},
		},
		"bind_fact": {
			Name:         "bind_fact",
			ProfileRef:   "fact_nodecay",
			TargetLabels: []string{"KnowledgeFact"},
			NoDecay:      true,
		},
	}

	bt, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	sm := eng.GetSchemaForNamespace("nornic")
	sm.SetBindingTable(bt)

	exec := NewStorageExecutor(eng)
	return eng, exec
}

func makeNode(id string, labels []string, age time.Duration) *storage.Node {
	now := time.Now()
	return &storage.Node{
		ID:        storage.NodeID(id),
		Labels:    labels,
		CreatedAt: now.Add(-age),
		UpdatedAt: now.Add(-age),
		Properties: map[string]interface{}{
			"name": id,
		},
	}
}

func TestDecayScore_DecayedNode(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:old_episode", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n)", nodes, rels)
	score, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if score >= 0.10 {
		t.Errorf("expected score < 0.10 for 30-day-old episode, got %.6f", score)
	}
}

func TestDecayScore_RecentNode(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:recent_episode", []string{"MemoryEpisode"}, 5*time.Minute)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n)", nodes, rels)
	score, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if score < 0.90 {
		t.Errorf("expected score > 0.90 for 5-minute-old episode, got %.6f", score)
	}
}

func TestDecayScore_NoDecayFact(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:old_fact", []string{"KnowledgeFact"}, 8760*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n)", nodes, rels)
	score, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if score != 1.0 {
		t.Errorf("expected score 1.0 for no-decay fact, got %.6f", score)
	}
}

func TestDecayScore_UnmatchedLabel(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:custom", []string{"CustomLabel"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n)", nodes, rels)
	score, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if score != 1.0 {
		t.Errorf("expected score 1.0 for unmatched label, got %.6f", score)
	}
}

func TestDecay_ReturnsMap(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecay("decay(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	score, _ := m["score"].(float64)
	if score <= 0 || score >= 1.0 {
		t.Errorf("expected decayed score (0,1), got %.6f", score)
	}

	applies, _ := m["applies"].(bool)
	if !applies {
		t.Error("expected applies=true for MemoryEpisode")
	}
}

func TestDecay_NoDecayReason(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:fact", []string{"KnowledgeFact"}, 8760*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecay("decay(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	reason, _ := m["reason"].(string)
	if reason != "no decay" {
		t.Errorf("expected reason 'no decay', got %q", reason)
	}

	score, _ := m["score"].(float64)
	if score != 1.0 {
		t.Errorf("expected score 1.0, got %.6f", score)
	}
}

func TestDecay_ScoreMatchesDecayScore(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep2", []string{"MemoryEpisode"}, 4*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	dsResult := exec.evalDecayScore("decayScore(n)", nodes, rels)
	dResult := exec.evalDecay("decay(n)", nodes, rels)

	dsScore, _ := dsResult.(float64)
	dMap, _ := dResult.(map[string]interface{})
	dScore, _ := dMap["score"].(float64)

	if math.Abs(dsScore-dScore) > 1e-9 {
		t.Errorf("decayScore(n)=%.9f != decay(n).score=%.9f", dsScore, dScore)
	}
}

func TestPolicy_WithAccessMeta(t *testing.T) {
	eng, exec := setupDecayEngine(t)

	meta := &knowledgepolicy.AccessMetaEntry{
		TargetID:    "nornic:tracked",
		TargetScope: knowledgepolicy.ScopeNode,
		Fixed: knowledgepolicy.AccessMetaFixedFields{
			AccessCount:    42,
			LastAccessedAt: time.Now().Add(-1 * time.Hour).UnixNano(),
		},
	}
	if err := eng.PutAccessMeta("nornic:tracked", meta); err != nil {
		t.Fatal(err)
	}

	node := makeNode("nornic:tracked", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalPolicy("policy(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	ac, _ := m["accessCount"].(int64)
	if ac != 42 {
		t.Errorf("expected accessCount=42, got %d", ac)
	}

	tid, _ := m["targetId"].(string)
	if tid != "nornic:tracked" {
		t.Errorf("expected targetId=nornic:tracked, got %q", tid)
	}
}

func TestPolicy_NoAccessMeta(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:untracked", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalPolicy("policy(n)", nodes, rels)
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", result)
	}

	tid, _ := m["targetId"].(string)
	if tid != "nornic:untracked" {
		t.Errorf("expected targetId=nornic:untracked, got %q", tid)
	}
	scope, _ := m["targetScope"].(string)
	if scope != "NODE" {
		t.Errorf("expected targetScope=NODE, got %q", scope)
	}
	if _, hasCount := m["accessCount"]; hasCount {
		t.Error("expected no accessCount key when no AccessMeta")
	}
}

func TestDecayScore_UnknownOptionKey(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	result := exec.evalDecayScore("decayScore(n, {badKey: 'x'})", nodes, rels)
	if result != nil {
		t.Errorf("expected nil for unknown option key, got %v", result)
	}
}
