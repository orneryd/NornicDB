package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRevealDispatcher_InReturn(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep_reveal", []string{"MemoryEpisode"}, 720*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}
	ctx := context.Background()

	// reveal(n) should pass through — the fn.Register builtin evaluates reveal(n) → n.
	// Here we only verify the dispatcher does NOT intercept "reveal" as a decay function.
	fixtureAdmission1260Context, fixtureAdmission1260 := expressionTestContext(ctx, "reveal(n)")
	_ = fixtureAdmission1260Context
	v, handled := exec.evaluateKnowledgePolicyFunction(fixtureAdmission1260, fixtureAdmission1260Context, "reveal(n)", "reveal(n)", nodes, rels)
	if handled {
		t.Errorf("reveal(n) should NOT be handled by knowledgepolicy dispatcher, got %v", v)
	}
}

func TestRevealDispatcher_DecayScoreNotIntercepted(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep", []string{"MemoryEpisode"}, 2*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}
	ctx := context.Background()

	fixtureAdmission1261Context, fixtureAdmission1261 := expressionTestContext(ctx, "decayScore(n)")
	_ = fixtureAdmission1261Context
	v, handled := exec.evaluateKnowledgePolicyFunction(fixtureAdmission1261, fixtureAdmission1261Context, "decayScore(n)", "decayscore(n)", nodes, rels)
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
	ctx := context.Background()

	fixtureAdmission1262Context, fixtureAdmission1262 := expressionTestContext(ctx, "decay(scoreNode)")
	_ = fixtureAdmission1262Context
	v, handled := exec.evaluateKnowledgePolicyFunction(fixtureAdmission1262, fixtureAdmission1262Context, "decay(scoreNode)", "decay(scorenode)", nodes, rels)
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
	ctx := context.Background()

	fixtureAdmission1263Context, fixtureAdmission1263 := expressionTestContext(ctx, "policy(n)")
	_ = fixtureAdmission1263Context
	v, handled := exec.evaluateKnowledgePolicyFunction(fixtureAdmission1263, fixtureAdmission1263Context, "policy(n)", "policy(n)", nodes, rels)
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
