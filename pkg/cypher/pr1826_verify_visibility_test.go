package cypher

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPR1826VerifyDecayPolicyProjection(t *testing.T) {
	store := newTestBadgerNamespacedStore(t)
	engine := unwrapBadgerEngine(store)
	if engine == nil { t.Fatal("expected Badger engine") }
	schema := engine.GetSchemaForNamespace(storage.DatabaseOf(store))
	if err := schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
		Name: "verify_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential,
		HalfLifeSeconds: 3600, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated,
		Enabled: true, DecayEnabled: true,
	}); err != nil { t.Fatalf("decay profile: %v", err) }
	if err := schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name:"verify_binding", ProfileRef:"verify_decay", TargetLabels:[]string{"VFact"}}); err != nil { t.Fatalf("decay binding: %v", err) }
	if err := schema.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{Name:"verify_keep", Scope:knowledgepolicy.ScopeNode, Multiplier:1, ScoreFloor:1, ScoreCap:1, Enabled:true}); err != nil { t.Fatalf("promotion profile: %v", err) }
	if err := schema.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{Name:"verify_policy", TargetLabels:[]string{"VFact"}, Enabled:true, WhenClauses:[]knowledgepolicy.PromotionPolicyWhenClause{{Predicate:"coalesce(n.hidden, 0) = 0", ProfileRef:"verify_keep", Order:1}}}); err != nil { t.Fatalf("promotion policy: %v", err) }
	engine.SetDecayEnabled(true)
	old := time.Now().Add(-72 * time.Hour)
	_, err := store.CreateNode(t.Context(), &storage.Node{ID:"verify:fact", Labels:[]string{"VFact"}, CreatedAt:old, UpdatedAt:old, Properties:map[string]any{"display":"secret", "hidden":int64(1), "body":"secret body"}})
	if err != nil { t.Fatalf("seed: %v", err) }
	exec := NewStorageExecutor(store)
	if _, err := exec.Execute(t.Context(), "CREATE CONSTRAINT verify_fact IF NOT EXISTS FOR (n:VFact) REQUIRE n.display IS UNIQUE", nil); err != nil { t.Fatalf("unique constraint: %v", err) }
	result, err := exec.Execute(t.Context(), "MATCH (n:VFact {display:'secret'}) RETURN n.display, n.body", nil)
	t.Logf("OBS r1_f5 projected_rows=%#v err=%v", result, err)
	whole, wholeErr := exec.Execute(t.Context(), "MATCH (n:VFact {display:'secret'}) RETURN n", nil)
	t.Logf("OBS r1_f5 whole_rows=%#v err=%v", whole, wholeErr)
}
