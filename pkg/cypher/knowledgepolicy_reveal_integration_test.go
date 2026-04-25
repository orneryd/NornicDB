package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRevealDispatcher_InReturn(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep_reveal", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	// reveal(n) should pass through — the fn.Register builtin evaluates reveal(n) → n.
	// Here we only verify the dispatcher does NOT intercept "reveal" as a decay function.
	v, handled := exec.evaluateKnowledgePolicyFunction("reveal(n)", "reveal(n)", nodes, rels)
	if handled {
		t.Errorf("reveal(n) should NOT be handled by knowledgepolicy dispatcher, got %v", v)
	}
}

func TestRevealDispatcher_DecayScoreNotIntercepted(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	v, handled := exec.evaluateKnowledgePolicyFunction("decayScore(n)", "decayscore(n)", nodes, rels)
	if !handled {
		t.Error("decayScore(n) should be handled by knowledgepolicy dispatcher")
	}
	if _, ok := v.(float64); !ok {
		t.Errorf("expected float64 from decayScore, got %T", v)
	}
}

func TestRevealDispatcher_DecayWithScoreNamedIdentifierHandled(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:score-node", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"scoreNode": node}
	rels := map[string]*storage.Edge{}

	v, handled := exec.evaluateKnowledgePolicyFunction("decay(scoreNode)", "decay(scorenode)", nodes, rels)
	if !handled {
		t.Fatal("decay(scoreNode) should be handled by knowledgepolicy dispatcher")
	}
	if _, ok := v.(map[string]interface{}); !ok {
		t.Errorf("expected map from decay(scoreNode), got %T", v)
	}
}

func TestRevealDispatcher_PolicyHandled(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}

	v, handled := exec.evaluateKnowledgePolicyFunction("policy(n)", "policy(n)", nodes, rels)
	if !handled {
		t.Error("policy(n) should be handled by knowledgepolicy dispatcher")
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", v)
	}
	if _, has := m["targetId"]; !has {
		t.Error("expected targetId in policy result")
	}
}
