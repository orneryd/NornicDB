package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupDecayEngine(t *testing.T, enabled ...bool) (*storage.BadgerEngine, *StorageExecutor) {
	t.Helper()
	eng, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if eng != nil {
		fixtureowner.Own(t, eng)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	eng.SetDecayEnabled(len(enabled) == 0 || enabled[0])

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

// decayScore(n) and decay(n) score at the statement's clock, so within one
// statement they agree exactly however long apart they run (#866).
func TestDecay_ScoreMatchesDecayScore(t *testing.T) {
	_, exec := setupDecayEngine(t)

	node := makeNode("nornic:ep2", []string{"MemoryEpisode"}, 4*time.Hour)
	nodes := map[string]*storage.Node{"n": node}
	rels := map[string]*storage.Edge{}
	statement := time.Now().UTC()
	ctx := context.WithValue(context.Background(), temporalStatementTimeKey{}, statement)

	dsResult, policyHandled := exec.evaluateKnowledgePolicyFunction(ctx, "decayScore(n)", "", nodes, rels)
	if !policyHandled {
		t.Fatal("knowledge-policy expression was not handled")
	}
	time.Sleep(10 * time.Millisecond)
	dResult, policyHandled := exec.evaluateKnowledgePolicyFunction(ctx, "decay(n)", "", nodes, rels)
	if !policyHandled {
		t.Fatal("knowledge-policy expression was not handled")
	}

	dsScore, _ := dsResult.(float64)
	dMap, _ := dResult.(map[string]interface{})
	dScore, _ := dMap["score"].(float64)
	if dsScore != dScore {
		t.Errorf("decayScore(n)=%.12f != decay(n).score=%.12f", dsScore, dScore)
	}
	// Four half-lives (one hour each) before the statement's clock.
	if want := math.Pow(0.5, float64(statement.Sub(node.CreatedAt))/float64(time.Hour)); math.Abs(dsScore-want) > 1e-9 {
		t.Errorf("decayScore(n)=%.12f, want %.12f", dsScore, want)
	}
}

// The documented functions run in statements (#871), and one statement reads
// one scoring instant for every decay function, in RETURN and in WHERE (#866).
func TestDecay_OneStatementScoresAtOneInstant(t *testing.T) {
	eng, exec := setupDecayEngine(t)
	_, err := eng.CreateNode(makeNode("nornic:ep3", []string{"MemoryEpisode"}, 2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Execute(context.Background(), `MATCH (n:MemoryEpisode)
		RETURN decayScore(n) = decay(n).score AND decayScore(n) = decayScore(n) AS same, decayScore(n) AS score`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != true {
		t.Fatalf("rows = %v", res.Rows)
	}
	if score, _ := res.Rows[0][1].(float64); score <= 0.2 || score >= 0.3 {
		t.Errorf("score = %v, want about 0.25", res.Rows[0][1])
	}
	res, err = exec.Execute(context.Background(), `MATCH (n) WHERE decayScore(n) > 0.2 AND decayScore(n) = decay(n).score
		RETURN decay(n).policy AS profile, policy(n).targetId AS target ORDER BY decayScore(n) DESC`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "episode_decay" || res.Rows[0][1] != "nornic:ep3" {
		t.Fatalf("rows = %v", res.Rows)
	}
}
