package storage_test

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: #1548 implementation lane (Codex). Every case enters through the
// public schema owner, changes the caller's containers after successful
// admission, and compares the live definition with the persisted/reloaded one.
func TestResidualSchemaIngressOwnership(t *testing.T) {
	type admission func(*testing.T, *storage.SchemaManager) func()
	cases := map[string]admission{}
	for _, kind := range []string{"property", "composite", "fulltext-node", "fulltext-edge", "range", "typed"} {
		cases[kind] = func(t *testing.T, sm *storage.SchemaManager) func() {
			properties, targets := []string{"before"}, []string{"Before"}
			var err error
			switch kind {
			case "property":
				err = sm.AddPropertyIndex("idx", "L", properties)
			case "composite":
				properties = append(properties, "peer")
				err = sm.AddCompositeIndex("idx", "L", properties)
			case "fulltext-node":
				err = sm.AddFulltextIndex("idx", targets, properties)
			case "fulltext-edge":
				err = sm.AddFulltextRelationshipIndex("idx", targets, properties)
			case "range":
				err = sm.AddRangeIndexForEntity("idx", "L", properties, storage.ConstraintEntityNode)
			case "typed":
				err = sm.AddTypedIndexForEntity(storage.IndexKindText, "idx", "L", properties, storage.ConstraintEntityNode)
			}
			require.NoError(t, err)
			return func() { properties[0], targets[0] = "changed", "Changed" }
		}
	}
	for _, bundle := range []bool{false, true} {
		name := "constraint"
		if bundle {
			name = "contract-bundle"
		}
		cases[name] = func(t *testing.T, sm *storage.SchemaManager) func() {
			nested := map[string]any{"value": []any{"before"}}
			constraint := storage.Constraint{Name: "domain", Type: storage.ConstraintDomain, Label: "L", Properties: []string{"before"}, AllowedValues: []any{nested}}
			contract := storage.ConstraintContract{Name: "contract", TargetEntityType: "NODE", TargetLabelOrType: "L", Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindPrimitiveNode, Properties: []string{"before"}}}}
			compiled := []storage.Constraint{constraint}
			if bundle {
				require.NoError(t, sm.AddConstraintContractBundle(contract, compiled, nil, false))
			} else {
				require.NoError(t, sm.AddConstraint(constraint))
			}
			return func() {
				constraint.Properties[0] = "changed"
				nested["value"].([]any)[0] = "changed"
				contract.Entries[0].Properties[0] = "changed"
				compiled[0].Name = "changed"
			}
		}
	}
	for _, alter := range []bool{false, true} {
		name := "decay-create"
		if alter {
			name = "decay-replace"
		}
		cases[name] = func(t *testing.T, sm *storage.SchemaManager) func() {
			visibility := 0.25
			binding := knowledgepolicy.DecayProfileBinding{Name: "binding", TargetLabels: []string{"Before"}, VisibilityThreshold: &visibility, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "before", NoDecay: true}}}
			if alter {
				require.NoError(t, sm.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: binding.Name, TargetLabels: []string{"Initial"}}))
				require.NoError(t, sm.AlterDecayProfileBinding(binding.Name, binding))
			} else {
				require.NoError(t, sm.CreateDecayProfileBinding(binding))
			}
			return func() {
				binding.TargetLabels[0] = "Changed"
				binding.PropertyRules[0].PropertyPath = "changed"
				visibility = 0.75
			}
		}
		name = "promotion-create"
		if alter {
			name = "promotion-replace"
		}
		cases[name] = func(t *testing.T, sm *storage.SchemaManager) func() {
			policy := knowledgepolicy.PromotionPolicyDef{Name: "policy", TargetLabels: []string{"Before"}, WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{{Predicate: "before"}}, OnAccess: &knowledgepolicy.PromotionPolicyOnAccess{Mutations: []knowledgepolicy.OnAccessMutation{{Expression: "before", Kalman: &knowledgepolicy.KalmanConfig{Q: 1}}}}}
			if alter {
				require.NoError(t, sm.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{Name: policy.Name, TargetLabels: []string{"Initial"}}))
				require.NoError(t, sm.AlterPromotionPolicyDefinition(policy.Name, policy))
			} else {
				require.NoError(t, sm.CreatePromotionPolicy(policy))
			}
			return func() {
				policy.TargetLabels[0] = "Changed"
				policy.WhenClauses[0].Predicate = "changed"
				policy.OnAccess.Mutations[0].Expression = "changed"
				policy.OnAccess.Mutations[0].Kalman.Q = 2
			}
		}
	}
	cases["tokens"] = func(t *testing.T, sm *storage.SchemaManager) func() {
		labels, relationships := []string{"Before"}, []string{"BEFORE"}
		require.NoError(t, sm.RegisterTokens(labels, relationships))
		return func() { labels[0], relationships[0] = "Changed", "CHANGED" }
	}
	for name, admit := range cases {
		t.Run(name, func(t *testing.T) {
			sm := storage.NewSchemaManager()
			var persisted []byte
			sm.SetPersister(func(def *storage.SchemaDefinition) error {
				var err error
				persisted, err = json.Marshal(def)
				return err
			})
			mutate := admit(t, sm)
			require.NotEmpty(t, persisted)
			mutate()
			live, err := json.Marshal(sm.ExportDefinition())
			require.NoError(t, err)
			require.JSONEq(t, string(persisted), string(live), "caller mutation changed committed schema")
			var def storage.SchemaDefinition
			require.NoError(t, json.Unmarshal(persisted, &def))
			reloaded := storage.NewSchemaManager()
			require.NoError(t, reloaded.ReplaceFromDefinition(&def))
			// Mutate the imported image too: reload admission must own it.
			mutateResidualSchemaDefinition(&def)
			restored, err := json.Marshal(reloaded.ExportDefinition())
			require.NoError(t, err)
			require.JSONEq(t, string(persisted), string(restored), "reload retained its input")
			exported := reloaded.ExportDefinition()
			mutateResidualSchemaDefinition(exported)
			restored, err = json.Marshal(reloaded.ExportDefinition())
			require.NoError(t, err)
			require.JSONEq(t, string(persisted), string(restored), "export exposed committed containers")
		})
	}
}

func mutateResidualSchemaDefinition(def *storage.SchemaDefinition) {
	for _, c := range def.Constraints {
		c.Properties[0] = "changed"
		if len(c.AllowedValues) > 0 {
			c.AllowedValues[0].(map[string]any)["value"].([]any)[0] = "changed"
		}
	}
	for _, c := range def.ConstraintContracts {
		c.Entries[0].Properties[0] = "changed"
	}
	for _, idx := range def.PropertyIndexes {
		idx.Properties[0] = "changed"
	}
	for _, idx := range def.CompositeIndexes {
		idx.Properties[0] = "changed"
	}
	for _, idx := range def.FulltextIndexes {
		idx.Properties[0] = "changed"
	}
	for _, idx := range def.RangeIndexes {
		idx.Properties[0] = "changed"
	}
	for _, b := range def.DecayProfileBindings {
		b.TargetLabels[0] = "Changed"
		b.PropertyRules[0].PropertyPath = "changed"
		*b.VisibilityThreshold = 0.75
	}
	for _, p := range def.PromotionPolicies {
		p.TargetLabels[0] = "Changed"
		p.WhenClauses[0].Predicate = "changed"
		p.OnAccess.Mutations[0].Expression = "changed"
		p.OnAccess.Mutations[0].Kalman.Q = 2
	}
	if len(def.LabelTokens) > 0 {
		def.LabelTokens[0] = "Changed"
	}
	if len(def.RelationshipTypeTokens) > 0 {
		def.RelationshipTypeTokens[0] = "CHANGED"
	}
}
