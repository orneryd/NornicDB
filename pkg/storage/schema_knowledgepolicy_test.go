package storage

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validBundle returns a fully-valid DecayProfileBundle for use as a test fixture.
func validBundle(name string) knowledgepolicy.DecayProfileBundle {
	return knowledgepolicy.DecayProfileBundle{
		Name:                name,
		HalfLifeSeconds:     604800,
		VisibilityThreshold: 0.10,
		ScoreFloor:          0.0,
		Function:            knowledgepolicy.DecayFunctionExponential,
		Scope:               knowledgepolicy.ScopeNode,
		DecayEnabled:        true,
		ScoreFrom:           knowledgepolicy.ScoreFromCreated,
		Enabled:             true,
	}
}

// validPromoProfile returns a fully-valid PromotionProfileDef.
func validPromoProfile(name string) knowledgepolicy.PromotionProfileDef {
	return knowledgepolicy.PromotionProfileDef{
		Name:       name,
		Scope:      knowledgepolicy.ScopeNode,
		Multiplier: 1.5,
		ScoreFloor: 0.0,
		ScoreCap:   1.0,
		Enabled:    true,
	}
}

// TestDecayProfileBundle_Create tests creating a valid bundle and verifying it via ShowDecayProfiles.
func TestDecayProfileBundle_Create(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.CreateDecayProfileBundle(validBundle("test_bundle"))
	require.NoError(t, err)

	bundles, bindings := sm.ShowDecayProfiles()
	assert.Len(t, bundles, 1)
	assert.Len(t, bindings, 0)
	assert.Equal(t, "test_bundle", bundles[0].Name)
	assert.Equal(t, knowledgepolicy.DecayFunctionExponential, bundles[0].Function)
	assert.Equal(t, 0.10, bundles[0].VisibilityThreshold)
}

// TestDecayProfileBundle_Duplicate tests that creating a same-name bundle returns an error,
// but passes with ifNotExists=true.
func TestDecayProfileBundle_Duplicate(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("dup_bundle")))

	t.Run("without ifNotExists", func(t *testing.T) {
		err := sm.CreateDecayProfileBundle(validBundle("dup_bundle"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("with ifNotExists", func(t *testing.T) {
		err := sm.CreateDecayProfileBundle(validBundle("dup_bundle"), true)
		assert.NoError(t, err)
	})
}

// TestDecayProfileBundle_Validation tests that invalid bundle fields are rejected.
func TestDecayProfileBundle_Validation(t *testing.T) {
	sm := NewSchemaManager()

	t.Run("empty name", func(t *testing.T) {
		b := validBundle("")
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "name is required")
	})

	t.Run("invalid function", func(t *testing.T) {
		b := validBundle("bad_fn")
		b.Function = "bogus"
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid decay function")
	})

	t.Run("invalid scope", func(t *testing.T) {
		b := validBundle("bad_scope")
		b.Scope = "UNIVERSE"
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid scope type")
	})

	t.Run("invalid scoreFrom", func(t *testing.T) {
		b := validBundle("bad_score_from")
		b.ScoreFrom = "NEVER"
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid score-from mode")
	})

	t.Run("visibilityThreshold below 0", func(t *testing.T) {
		b := validBundle("bad_vt_low")
		b.VisibilityThreshold = -0.1
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "visibilityThreshold")
	})

	t.Run("visibilityThreshold above 1", func(t *testing.T) {
		b := validBundle("bad_vt_high")
		b.VisibilityThreshold = 1.5
		err := sm.CreateDecayProfileBundle(b)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "visibilityThreshold")
	})
}

func TestKnowledgePolicyMutationErrorsDoNotLeakSchemaLock(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.CreateDecayProfileBundle(validBundle(""))
	require.Error(t, err)
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("after_error_bundle")))

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("dup_profile")))
	err = sm.CreatePromotionProfile(validPromoProfile("dup_profile"))
	require.Error(t, err)
	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("after_error_profile")))
}

// TestDecayProfileBinding_Create tests creating a binding that references an existing bundle.
func TestDecayProfileBinding_Create(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("my_bundle")))

	binding := knowledgepolicy.DecayProfileBinding{
		Name:         "user_binding",
		TargetLabels: []string{"User"},
		ProfileRef:   "my_bundle",
	}
	err := sm.CreateDecayProfileBinding(binding)
	require.NoError(t, err)

	bundles, bindings := sm.ShowDecayProfiles()
	assert.Len(t, bundles, 1)
	assert.Len(t, bindings, 1)
	assert.Equal(t, "user_binding", bindings[0].Name)
	assert.Equal(t, "my_bundle", bindings[0].ProfileRef)
}

// TestDecayProfileBinding_MissingBundle tests that referencing a non-existent bundle returns an error.
// The bundles map must be initialised (by creating at least one bundle) before the ref-check is active.
func TestDecayProfileBinding_MissingBundle(t *testing.T) {
	sm := NewSchemaManager()

	// Initialise the bundles map by creating a different, unrelated bundle.
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("unrelated_bundle")))

	binding := knowledgepolicy.DecayProfileBinding{
		Name:         "orphan_binding",
		TargetLabels: []string{"Post"},
		ProfileRef:   "nonexistent_bundle",
	}
	err := sm.CreateDecayProfileBinding(binding)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestDecayProfileBinding_DuplicateTarget tests that two bindings for the same label set return an error.
func TestDecayProfileBinding_DuplicateTarget(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("bundle_a")))

	first := knowledgepolicy.DecayProfileBinding{
		Name:         "binding_first",
		TargetLabels: []string{"Article"},
		ProfileRef:   "bundle_a",
	}
	require.NoError(t, sm.CreateDecayProfileBinding(first))

	second := knowledgepolicy.DecayProfileBinding{
		Name:         "binding_second",
		TargetLabels: []string{"Article"},
		ProfileRef:   "bundle_a",
	}
	err := sm.CreateDecayProfileBinding(second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already has a decay profile binding")
}

// TestDropDecayProfile_Bundle tests that dropping a bundle works and
// that dropping a referenced bundle returns an error.
func TestDropDecayProfile_Bundle(t *testing.T) {
	t.Run("drop unreferenced bundle", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.CreateDecayProfileBundle(validBundle("free_bundle")))

		err := sm.DropDecayProfile("free_bundle")
		require.NoError(t, err)

		bundles, _ := sm.ShowDecayProfiles()
		assert.Len(t, bundles, 0)
	})

	t.Run("drop referenced bundle returns error", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.CreateDecayProfileBundle(validBundle("locked_bundle")))

		binding := knowledgepolicy.DecayProfileBinding{
			Name:         "lock_binding",
			TargetLabels: []string{"Comment"},
			ProfileRef:   "locked_bundle",
		}
		require.NoError(t, sm.CreateDecayProfileBinding(binding))

		err := sm.DropDecayProfile("locked_bundle")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "referenced by active binding")
	})
}

// TestDropDecayProfile_Binding tests that dropping a binding works.
func TestDropDecayProfile_Binding(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("bnd_bundle")))

	binding := knowledgepolicy.DecayProfileBinding{
		Name:         "drop_me_binding",
		TargetLabels: []string{"Tag"},
		ProfileRef:   "bnd_bundle",
	}
	require.NoError(t, sm.CreateDecayProfileBinding(binding))

	err := sm.DropDecayProfile("drop_me_binding")
	require.NoError(t, err)

	_, bindings := sm.ShowDecayProfiles()
	assert.Len(t, bindings, 0)
}

// TestDropDecayProfile_IfExists tests that dropping a non-existent profile with ifExists=true returns nil.
func TestDropDecayProfile_IfExists(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.DropDecayProfile("does_not_exist", true)
	assert.NoError(t, err)

	// Without ifExists, should return an error.
	err = sm.DropDecayProfile("also_missing")
	assert.Error(t, err)
}

// TestAlterDecayProfile tests that AlterDecayProfile updates halfLifeSeconds.
func TestAlterDecayProfile(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("alter_bundle")))

	err := sm.AlterDecayProfile("alter_bundle", map[string]interface{}{
		"halfLifeSeconds": int64(1209600),
	})
	require.NoError(t, err)

	bundles, _ := sm.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	assert.Equal(t, int64(1209600), bundles[0].HalfLifeSeconds)
}

// TestAlterDecayProfile_UnknownOption tests that passing an unrecognised option key returns an error.
func TestAlterDecayProfile_UnknownOption(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("alter_unknown")))

	err := sm.AlterDecayProfile("alter_unknown", map[string]interface{}{
		"notAField": "value",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown option")
}

// TestShowDecayProfiles_Sorted verifies that ShowDecayProfiles returns bundles and bindings in name order.
func TestShowDecayProfiles_Sorted(t *testing.T) {
	sm := NewSchemaManager()

	for _, name := range []string{"zebra", "alpha", "mango"} {
		require.NoError(t, sm.CreateDecayProfileBundle(validBundle(name)))
	}

	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("ref_bundle")))
	for _, name := range []string{"z_bind", "a_bind", "m_bind"} {
		b := knowledgepolicy.DecayProfileBinding{
			Name:         name,
			TargetLabels: []string{name + "_label"},
			ProfileRef:   "ref_bundle",
		}
		require.NoError(t, sm.CreateDecayProfileBinding(b))
	}

	bundles, bindings := sm.ShowDecayProfiles()

	for i := 1; i < len(bundles); i++ {
		assert.LessOrEqual(t, bundles[i-1].Name, bundles[i].Name, "bundles not sorted")
	}
	for i := 1; i < len(bindings); i++ {
		assert.LessOrEqual(t, bindings[i-1].Name, bindings[i].Name, "bindings not sorted")
	}
}

// TestCreatePromotionProfile tests creating a valid promotion profile.
func TestCreatePromotionProfile(t *testing.T) {
	sm := NewSchemaManager()

	err := sm.CreatePromotionProfile(validPromoProfile("boost_profile"))
	require.NoError(t, err)

	profiles := sm.ShowPromotionProfiles()
	assert.Len(t, profiles, 1)
	assert.Equal(t, "boost_profile", profiles[0].Name)
	assert.Equal(t, 1.5, profiles[0].Multiplier)
}

// TestCreatePromotionProfile_Duplicate tests duplicate behaviour.
func TestCreatePromotionProfile_Duplicate(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("dup_profile")))

	t.Run("without ifNotExists", func(t *testing.T) {
		err := sm.CreatePromotionProfile(validPromoProfile("dup_profile"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("with ifNotExists", func(t *testing.T) {
		err := sm.CreatePromotionProfile(validPromoProfile("dup_profile"), true)
		assert.NoError(t, err)
	})
}

// TestCreatePromotionProfile_Validation tests that invalid fields are rejected.
func TestCreatePromotionProfile_Validation(t *testing.T) {
	sm := NewSchemaManager()

	t.Run("invalid scope", func(t *testing.T) {
		p := validPromoProfile("bad_scope")
		p.Scope = "GALAXY"
		err := sm.CreatePromotionProfile(p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid scope type")
	})

	t.Run("negative multiplier", func(t *testing.T) {
		p := validPromoProfile("neg_mult")
		p.Multiplier = -0.5
		err := sm.CreatePromotionProfile(p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "multiplier")
	})

	t.Run("scoreCap above 1", func(t *testing.T) {
		p := validPromoProfile("cap_high")
		p.ScoreCap = 1.1
		err := sm.CreatePromotionProfile(p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scoreCap")
	})

	t.Run("scoreCap below 0", func(t *testing.T) {
		p := validPromoProfile("cap_low")
		p.ScoreCap = -0.1
		err := sm.CreatePromotionProfile(p)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "scoreCap")
	})
}

// TestDropPromotionProfile tests dropping a standalone profile and rejecting drop of a referenced one.
func TestDropPromotionProfile(t *testing.T) {
	t.Run("drop unreferenced", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("free_profile")))

		err := sm.DropPromotionProfile("free_profile")
		require.NoError(t, err)

		profiles := sm.ShowPromotionProfiles()
		assert.Len(t, profiles, 0)
	})

	t.Run("drop referenced profile returns error", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("locked_profile")))

		policy := knowledgepolicy.PromotionPolicyDef{
			Name:         "ref_policy",
			TargetLabels: []string{"User"},
			Enabled:      true,
			WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
				{ProfileRef: "locked_profile", Predicate: "accessCount > 5"},
			},
		}
		require.NoError(t, sm.CreatePromotionPolicy(policy))

		err := sm.DropPromotionProfile("locked_profile")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "referenced by active promotion policy")
	})
}

// TestAlterPromotionProfile tests changing the multiplier on an existing profile.
func TestAlterPromotionProfile(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("tweak_profile")))

	err := sm.AlterPromotionProfile("tweak_profile", map[string]interface{}{
		"multiplier": 3.0,
	})
	require.NoError(t, err)

	profiles := sm.ShowPromotionProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, 3.0, profiles[0].Multiplier)
}

// TestCreatePromotionPolicy tests creating a policy with WHEN clauses referencing an existing profile.
func TestCreatePromotionPolicy(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("signal_profile")))

	policy := knowledgepolicy.PromotionPolicyDef{
		Name:         "access_policy",
		TargetLabels: []string{"KnowledgeFact"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{
				ProfileRef: "signal_profile",
				Predicate:  "mutationCount > 3",
				Order:      1,
			},
		},
	}
	err := sm.CreatePromotionPolicy(policy)
	require.NoError(t, err)

	policies := sm.ShowPromotionPolicies()
	assert.Len(t, policies, 1)
	assert.Equal(t, "access_policy", policies[0].Name)
}

// TestCreatePromotionPolicy_MissingProfileRef tests that a WHEN clause referencing a missing profile returns an error.
func TestCreatePromotionPolicy_MissingProfileRef(t *testing.T) {
	sm := NewSchemaManager()

	policy := knowledgepolicy.PromotionPolicyDef{
		Name:         "bad_ref_policy",
		TargetLabels: []string{"Episode"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{ProfileRef: "ghost_profile", Predicate: "score < 0.5"},
		},
	}
	err := sm.CreatePromotionPolicy(policy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestDropPromotionPolicy tests that a policy can be dropped successfully.
func TestDropPromotionPolicy(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("pp_profile")))

	policy := knowledgepolicy.PromotionPolicyDef{
		Name:         "drop_policy",
		TargetLabels: []string{"Fact"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{ProfileRef: "pp_profile", Predicate: "accessCount > 1"},
		},
	}
	require.NoError(t, sm.CreatePromotionPolicy(policy))

	err := sm.DropPromotionPolicy("drop_policy")
	require.NoError(t, err)

	policies := sm.ShowPromotionPolicies()
	assert.Len(t, policies, 0)

	// After dropping the policy the referenced profile should be droppable.
	err = sm.DropPromotionProfile("pp_profile")
	assert.NoError(t, err)
}

// TestShowPromotionPolicies_Sorted verifies alphabetical ordering.
func TestShowPromotionPolicies_Sorted(t *testing.T) {
	sm := NewSchemaManager()

	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("sort_profile")))

	for _, name := range []string{"zebra_pol", "alpha_pol", "mango_pol"} {
		p := knowledgepolicy.PromotionPolicyDef{
			Name:         name,
			TargetLabels: []string{name + "_label"},
			Enabled:      true,
			WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
				{ProfileRef: "sort_profile", Predicate: "score > 0"},
			},
		}
		require.NoError(t, sm.CreatePromotionPolicy(p))
	}

	policies := sm.ShowPromotionPolicies()
	for i := 1; i < len(policies); i++ {
		assert.LessOrEqual(t, policies[i-1].Name, policies[i].Name, "policies not sorted")
	}
}

// TestSchemaPersistenceRoundTrip verifies that ExportDefinition / ReplaceFromDefinition
// faithfully restores all knowledge-policy objects.
func TestSchemaPersistenceRoundTrip(t *testing.T) {
	sm := NewSchemaManager()

	// Create two bundles.
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("rtt_bundle_a")))
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("rtt_bundle_b")))

	// Create a binding.
	binding := knowledgepolicy.DecayProfileBinding{
		Name:         "rtt_binding",
		TargetLabels: []string{"Doc"},
		ProfileRef:   "rtt_bundle_a",
	}
	require.NoError(t, sm.CreateDecayProfileBinding(binding))

	// Create a promotion profile.
	require.NoError(t, sm.CreatePromotionProfile(validPromoProfile("rtt_promo")))

	// Create a promotion policy.
	policy := knowledgepolicy.PromotionPolicyDef{
		Name:         "rtt_policy",
		TargetLabels: []string{"Note"},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{ProfileRef: "rtt_promo", Predicate: "accessCount > 2", Order: 1},
		},
	}
	require.NoError(t, sm.CreatePromotionPolicy(policy))

	// Export.
	def := sm.ExportDefinition()
	require.NotNil(t, def)

	// Restore into a fresh SchemaManager.
	sm2 := NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(def))

	// Verify bundles.
	bundles2, bindings2 := sm2.ShowDecayProfiles()
	assert.Len(t, bundles2, 2)
	assert.Len(t, bindings2, 1)
	assert.Equal(t, "rtt_binding", bindings2[0].Name)
	assert.Equal(t, "rtt_bundle_a", bindings2[0].ProfileRef)

	// Verify promotion profiles.
	profiles2 := sm2.ShowPromotionProfiles()
	assert.Len(t, profiles2, 1)
	assert.Equal(t, "rtt_promo", profiles2[0].Name)

	// Verify promotion policies.
	policies2 := sm2.ShowPromotionPolicies()
	assert.Len(t, policies2, 1)
	assert.Equal(t, "rtt_policy", policies2[0].Name)
	assert.Len(t, policies2[0].WhenClauses, 1)
	assert.Equal(t, "rtt_promo", policies2[0].WhenClauses[0].ProfileRef)
}

// TestBindingTarget_IndexedPropertyRejected verifies that a binding PropertyRule targeting
// a property that is part of a structural index is rejected.
func TestBindingTarget_IndexedPropertyRejected(t *testing.T) {
	sm := NewSchemaManager()

	// Register a property index on User.email.
	require.NoError(t, sm.AddPropertyIndex("idx_user_email", "User", []string{"email"}))

	// Create a bundle to reference.
	require.NoError(t, sm.CreateDecayProfileBundle(validBundle("idx_bundle")))

	// Attempt to create a binding with a PropertyRule that targets the indexed property.
	threshold := 0.10
	binding := knowledgepolicy.DecayProfileBinding{
		Name:                "idx_binding",
		TargetLabels:        []string{"User"},
		ProfileRef:          "idx_bundle",
		VisibilityThreshold: &threshold,
		PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{
			{
				PropertyPath:    "email",
				HalfLifeSeconds: 86400,
				Order:           1,
			},
		},
	}
	err := sm.CreateDecayProfileBinding(binding)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "structural index")
}
