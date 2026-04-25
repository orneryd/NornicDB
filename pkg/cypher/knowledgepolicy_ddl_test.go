package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── Non-knowledge-policy statements ─────────────────────────────────────────

func TestParseDDL_NonKnowledgePolicyStatement(t *testing.T) {
	for _, stmt := range []string{
		"MATCH (n) RETURN n",
		"CREATE (n:Person {name: 'Alice'})",
		"CREATE INDEX ON :Person(name)",
		"",
		"   ",
	} {
		cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
		assert.Nil(t, cmd, "stmt=%q", stmt)
		assert.False(t, ok, "stmt=%q", stmt)
		assert.NoError(t, err, "stmt=%q", stmt)
	}
}

// ── CREATE DECAY PROFILE … OPTIONS (bundle) ────────────────────────────────

func TestParseDDL_CreateDecayProfileBundle(t *testing.T) {
	stmt := `CREATE DECAY PROFILE slow_decay OPTIONS {
		halfLifeSeconds: 604800,
		visibilityThreshold: 0.10,
		scoreFloor: 0.05,
		function: 'exponential',
		scope: 'NODE',
		decayEnabled: true,
		scoreFrom: 'CREATED',
		enabled: true
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*CreateDecayProfileBundleCmd)
	require.True(t, ok, "expected *CreateDecayProfileBundleCmd, got %T", cmd)

	assert.Equal(t, "slow_decay", c.Bundle.Name)
	assert.Equal(t, int64(604800), c.Bundle.HalfLifeSeconds)
	assert.Equal(t, 0.10, c.Bundle.VisibilityThreshold)
	assert.Equal(t, 0.05, c.Bundle.ScoreFloor)
	assert.Equal(t, knowledgepolicy.DecayFunctionExponential, c.Bundle.Function)
	assert.Equal(t, knowledgepolicy.ScopeNode, c.Bundle.Scope)
	assert.True(t, c.Bundle.DecayEnabled)
	assert.Equal(t, knowledgepolicy.ScoreFromCreated, c.Bundle.ScoreFrom)
	assert.True(t, c.Bundle.Enabled)
}

func TestParseDDL_CreateDecayProfileBundle_QuotedName(t *testing.T) {
	stmt := `CREATE DECAY PROFILE 'my-profile' OPTIONS {
		halfLifeSeconds: 3600,
		function: 'linear',
		scope: 'EDGE',
		scoreFrom: 'VERSION'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, "my-profile", c.Bundle.Name)
	assert.Equal(t, int64(3600), c.Bundle.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.DecayFunctionLinear, c.Bundle.Function)
	assert.Equal(t, knowledgepolicy.ScopeEdge, c.Bundle.Scope)
	assert.Equal(t, knowledgepolicy.ScoreFromVersion, c.Bundle.ScoreFrom)
}

func TestParseDDL_CreateDecayProfileBundle_CustomScoreFrom(t *testing.T) {
	stmt := `CREATE DECAY PROFILE custom_anchor OPTIONS {
		halfLifeSeconds: 86400,
		function: 'step',
		scope: 'NODE',
		scoreFrom: 'CUSTOM',
		scoreFromProperty: 'myTimestamp'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, knowledgepolicy.ScoreFromCustom, c.Bundle.ScoreFrom)
	assert.Equal(t, "myTimestamp", c.Bundle.ScoreFromProperty)
}

func TestParseDDL_CreateDecayProfileBundle_AllFunctions(t *testing.T) {
	for _, fn := range []string{"exponential", "linear", "step", "none"} {
		stmt := `CREATE DECAY PROFILE fn_test OPTIONS {
			halfLifeSeconds: 100,
			function: '` + fn + `',
			scope: 'NODE',
			scoreFrom: 'CREATED'
		}`
		cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
		require.NoError(t, err, "function=%s", fn)
		require.True(t, ok)
		c := cmd.(*CreateDecayProfileBundleCmd)
		assert.Equal(t, knowledgepolicy.DecayFunction(fn), c.Bundle.Function)
	}
}

func TestParseDDL_CreateDecayProfileBundle_InvalidFunction(t *testing.T) {
	stmt := `CREATE DECAY PROFILE bad_fn OPTIONS {
		function: 'quadratic',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid decay function")
}

func TestParseDDL_CreateDecayProfileBundle_InvalidScope(t *testing.T) {
	stmt := `CREATE DECAY PROFILE bad_scope OPTIONS {
		function: 'exponential',
		scope: 'UNIVERSE',
		scoreFrom: 'CREATED'
	}`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid scope")
}

func TestParseDDL_CreateDecayProfileBundle_InvalidScoreFrom(t *testing.T) {
	stmt := `CREATE DECAY PROFILE bad_sf OPTIONS {
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'NEVER'
	}`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid scoreFrom")
}

func TestParseDDL_CreateDecayProfileBundle_UnknownOption(t *testing.T) {
	stmt := `CREATE DECAY PROFILE bad_opt OPTIONS {
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'CREATED',
		bogusField: 42
	}`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown option")
}

func TestParseDDL_CreateDecayProfileBundle_MissingName(t *testing.T) {
	stmt := `CREATE DECAY PROFILE OPTIONS { function: 'exponential' }`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected")
}

func TestParseDDL_CreateDecayProfileBundle_EnabledDefaultsTrue(t *testing.T) {
	stmt := `CREATE DECAY PROFILE minimal OPTIONS {
		halfLifeSeconds: 100,
		function: 'none',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`
	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.True(t, c.Bundle.Enabled, "enabled should default to true")
}

func TestParseDDL_CreateDecayProfileBundle_CaseInsensitiveKeywords(t *testing.T) {
	stmt := `create decay profile ci_test options {
		halfLifeSeconds: 100,
		function: 'exponential',
		scope: 'NODE',
		scoreFrom: 'CREATED'
	}`
	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, "ci_test", c.Bundle.Name)
}

// ── CREATE DECAY PROFILE … FOR (binding) ───────────────────────────────────

func TestParseDDL_CreateDecayProfileBinding_NodeLabel(t *testing.T) {
	stmt := `CREATE DECAY PROFILE fact_binding FOR (n:KnowledgeFact) APPLY {
		DECAY PROFILE 'slow_decay'
		DECAY VISIBILITY THRESHOLD 0.10
		n.content NO DECAY
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*CreateDecayProfileBindingCmd)
	require.True(t, ok, "expected *CreateDecayProfileBindingCmd, got %T", cmd)

	assert.Equal(t, "fact_binding", c.Binding.Name)
	assert.Equal(t, []string{"KnowledgeFact"}, c.Binding.TargetLabels)
	assert.False(t, c.Binding.IsEdge)
	assert.False(t, c.Binding.IsWildcard)
	assert.Equal(t, "slow_decay", c.Binding.ProfileRef)
	require.NotNil(t, c.Binding.VisibilityThreshold)
	assert.Equal(t, 0.10, *c.Binding.VisibilityThreshold)
	require.Len(t, c.Binding.PropertyRules, 1)
	assert.Equal(t, "content", c.Binding.PropertyRules[0].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[0].NoDecay)
}

func TestParseDDL_CreateDecayProfileBinding_MultiLabel(t *testing.T) {
	stmt := `CREATE DECAY PROFILE multi_bind FOR (n:KnowledgeFact:MemoryEpisode) APPLY {
		DECAY PROFILE 'slow_decay'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, []string{"KnowledgeFact", "MemoryEpisode"}, c.Binding.TargetLabels)
}

func TestParseDDL_CreateDecayProfileBinding_Wildcard(t *testing.T) {
	stmt := `CREATE DECAY PROFILE wildcard_bind FOR () APPLY {
		NO DECAY
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.True(t, c.Binding.IsWildcard)
	assert.True(t, c.Binding.NoDecay)
}

func TestParseDDL_CreateDecayProfileBinding_EdgePattern(t *testing.T) {
	stmt := `CREATE DECAY PROFILE edge_bind FOR ()-[r:RELATES_TO]-() APPLY {
		DECAY PROFILE 'edge_profile'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.True(t, c.Binding.IsEdge)
	assert.Equal(t, "RELATES_TO", c.Binding.TargetEdgeType)
	assert.Equal(t, "edge_profile", c.Binding.ProfileRef)
}

func TestParseDDL_CreateDecayProfileBinding_PropertyRuleWithProfileRef(t *testing.T) {
	stmt := `CREATE DECAY PROFILE prop_rule_bind FOR (n:User) APPLY {
		DECAY PROFILE 'base_decay'
		n.score DECAY PROFILE 'slow_decay'
		n.score DECAY HALF LIFE 3600
		n.score DECAY FLOOR 0.01
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	require.Len(t, c.Binding.PropertyRules, 1)
	rule := c.Binding.PropertyRules[0]
	assert.Equal(t, "score", rule.PropertyPath)
	assert.Equal(t, "slow_decay", rule.ProfileRef)
	assert.Equal(t, int64(3600), rule.HalfLifeSeconds)
	assert.Equal(t, 0.01, rule.ScoreFloor)
	assert.Equal(t, 0, rule.Order)
}

func TestParseDDL_CreateDecayProfileBinding_MultiplePropertyRules(t *testing.T) {
	stmt := `CREATE DECAY PROFILE multi_prop FOR (n:Doc) APPLY {
		DECAY PROFILE 'base'
		n.metadata NO DECAY
		n.confidence DECAY HALF LIFE 7200
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	require.Len(t, c.Binding.PropertyRules, 2)
	assert.Equal(t, "metadata", c.Binding.PropertyRules[0].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[0].NoDecay)
	assert.Equal(t, 0, c.Binding.PropertyRules[0].Order)
	assert.Equal(t, "confidence", c.Binding.PropertyRules[1].PropertyPath)
	assert.Equal(t, int64(7200), c.Binding.PropertyRules[1].HalfLifeSeconds)
	assert.Equal(t, 1, c.Binding.PropertyRules[1].Order)
}

func TestParseDDL_CreateDecayProfile_MissingOptionsOrFor(t *testing.T) {
	stmt := `CREATE DECAY PROFILE orphan_name SOMETHING_ELSE`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected OPTIONS or FOR")
}

// ── ALTER DECAY PROFILE ─────────────────────────────────────────────────────

func TestParseDDL_AlterDecayProfile(t *testing.T) {
	stmt := `ALTER DECAY PROFILE slow_decay SET OPTIONS {
		halfLifeSeconds: 1209600,
		visibilityThreshold: 0.05
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*AlterDecayProfileCmd)
	require.True(t, ok, "expected *AlterDecayProfileCmd, got %T", cmd)

	assert.Equal(t, "slow_decay", c.Name)
	assert.Equal(t, int64(1209600), c.Updates["halfLifeSeconds"])
	assert.Equal(t, 0.05, c.Updates["visibilityThreshold"])
}

func TestParseDDL_AlterDecayProfile_QuotedName(t *testing.T) {
	stmt := `ALTER DECAY PROFILE 'my-profile' SET OPTIONS { enabled: false }`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*AlterDecayProfileCmd)
	assert.Equal(t, "my-profile", c.Name)
	assert.Equal(t, false, c.Updates["enabled"])
}

func TestParseDDL_AlterDecayProfile_MissingSetOptions(t *testing.T) {
	stmt := `ALTER DECAY PROFILE slow_decay SOMETHING_ELSE`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected SET OPTIONS")
}

func TestParseDDL_AlterDecayProfile_MissingName(t *testing.T) {
	stmt := `ALTER DECAY PROFILE SET OPTIONS { enabled: false }`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
}

// ── DROP DECAY PROFILE ──────────────────────────────────────────────────────

func TestParseDDL_DropDecayProfile(t *testing.T) {
	stmt := `DROP DECAY PROFILE slow_decay`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*DropDecayProfileCmd)
	require.True(t, ok, "expected *DropDecayProfileCmd, got %T", cmd)

	assert.Equal(t, "slow_decay", c.Name)
	assert.False(t, c.IfExists)
}

func TestParseDDL_DropDecayProfile_IfExists(t *testing.T) {
	stmt := `DROP DECAY PROFILE IF EXISTS slow_decay`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*DropDecayProfileCmd)
	assert.Equal(t, "slow_decay", c.Name)
	assert.True(t, c.IfExists)
}

func TestParseDDL_DropDecayProfile_QuotedName(t *testing.T) {
	stmt := `DROP DECAY PROFILE 'my-profile'`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*DropDecayProfileCmd)
	assert.Equal(t, "my-profile", c.Name)
}

func TestParseDDL_DropDecayProfile_MissingName(t *testing.T) {
	_, _, err := ParseKnowledgePolicyDDL(`DROP DECAY PROFILE`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected profile name")
}

// ── SHOW DECAY PROFILES ─────────────────────────────────────────────────────

func TestParseDDL_ShowDecayProfiles(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW DECAY PROFILES`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowDecayProfilesCmd)
	assert.True(t, ok, "expected *ShowDecayProfilesCmd, got %T", cmd)
}

func TestParseDDL_ShowDecayProfiles_CaseInsensitive(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`show decay profiles`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowDecayProfilesCmd)
	assert.True(t, ok)
}

// ── CREATE PROMOTION PROFILE ────────────────────────────────────────────────

func TestParseDDL_CreatePromotionProfile(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE boost OPTIONS {
		scope: 'NODE',
		multiplier: 2.0,
		scoreFloor: 0.3,
		scoreCap: 1.0,
		enabled: true
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*CreatePromotionProfileCmd)
	require.True(t, ok, "expected *CreatePromotionProfileCmd, got %T", cmd)

	assert.Equal(t, "boost", c.Profile.Name)
	assert.Equal(t, knowledgepolicy.ScopeNode, c.Profile.Scope)
	assert.Equal(t, 2.0, c.Profile.Multiplier)
	assert.Equal(t, 0.3, c.Profile.ScoreFloor)
	assert.Equal(t, 1.0, c.Profile.ScoreCap)
	assert.True(t, c.Profile.Enabled)
}

func TestParseDDL_CreatePromotionProfile_QuotedName(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE 'heavy-boost' OPTIONS {
		scope: 'EDGE',
		multiplier: 3.0,
		scoreCap: 1.0
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionProfileCmd)
	assert.Equal(t, "heavy-boost", c.Profile.Name)
	assert.Equal(t, knowledgepolicy.ScopeEdge, c.Profile.Scope)
}

func TestParseDDL_CreatePromotionProfile_InvalidScope(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE bad OPTIONS { scope: 'GALAXY', multiplier: 1.0, scoreCap: 1.0 }`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid scope")
}

func TestParseDDL_CreatePromotionProfile_UnknownOption(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE bad OPTIONS { scope: 'NODE', foo: 123 }`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown option")
}

func TestParseDDL_CreatePromotionProfile_MissingOptions(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE bare_name`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected OPTIONS")
}

func TestParseDDL_CreatePromotionProfile_MissingName(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE OPTIONS { scope: 'NODE' }`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
}

func TestParseDDL_CreatePromotionProfile_EnabledDefaultsTrue(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE default_enabled OPTIONS {
		scope: 'NODE',
		multiplier: 1.5,
		scoreCap: 1.0
	}`
	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreatePromotionProfileCmd)
	assert.True(t, c.Profile.Enabled, "enabled should default to true")
}

// ── ALTER PROMOTION PROFILE ─────────────────────────────────────────────────

func TestParseDDL_AlterPromotionProfile(t *testing.T) {
	stmt := `ALTER PROMOTION PROFILE boost SET OPTIONS {
		multiplier: 3.0,
		scoreCap: 0.95
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*AlterPromotionProfileCmd)
	require.True(t, ok, "expected *AlterPromotionProfileCmd, got %T", cmd)

	assert.Equal(t, "boost", c.Name)
	assert.Equal(t, 3.0, c.Updates["multiplier"])
	assert.Equal(t, 0.95, c.Updates["scoreCap"])
}

func TestParseDDL_AlterPromotionProfile_MissingSetOptions(t *testing.T) {
	stmt := `ALTER PROMOTION PROFILE boost SOMETHING`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected SET OPTIONS")
}

// ── DROP PROMOTION PROFILE ──────────────────────────────────────────────────

func TestParseDDL_DropPromotionProfile(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION PROFILE boost`)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*DropPromotionProfileCmd)
	require.True(t, ok)
	assert.Equal(t, "boost", c.Name)
	assert.False(t, c.IfExists)
}

func TestParseDDL_DropPromotionProfile_IfExists(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION PROFILE IF EXISTS boost`)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*DropPromotionProfileCmd)
	assert.Equal(t, "boost", c.Name)
	assert.True(t, c.IfExists)
}

func TestParseDDL_DropPromotionProfile_MissingName(t *testing.T) {
	_, _, err := ParseKnowledgePolicyDDL(`DROP PROMOTION PROFILE`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected profile name")
}

// ── SHOW PROMOTION PROFILES ─────────────────────────────────────────────────

func TestParseDDL_ShowPromotionProfiles(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW PROMOTION PROFILES`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowPromotionProfilesCmd)
	assert.True(t, ok)
}

// ── CREATE PROMOTION POLICY ─────────────────────────────────────────────────

func TestParseDDL_CreatePromotionPolicy_WithOnAccess(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY fact_promo FOR (n:KnowledgeFact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*CreatePromotionPolicyCmd)
	require.True(t, ok, "expected *CreatePromotionPolicyCmd, got %T", cmd)

	assert.Equal(t, "fact_promo", c.Policy.Name)
	assert.Equal(t, []string{"KnowledgeFact"}, c.Policy.TargetLabels)
	assert.False(t, c.Policy.IsEdge)
	assert.False(t, c.Policy.IsWildcard)
	assert.True(t, c.Policy.Enabled)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 1)
	assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)
	assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)
}

func TestParseDDL_CreatePromotionPolicy_WithWhenClauses(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY when_promo FOR (n:KnowledgeFact) APPLY {
		WHEN n.accessCount > 10 APPLY PROFILE boost
		WHEN n.mutationCount > 50 APPLY PROFILE heavy_boost
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.Len(t, c.Policy.WhenClauses, 2)
	assert.Equal(t, "n.accessCount > 10", c.Policy.WhenClauses[0].Predicate)
	assert.Equal(t, "boost", c.Policy.WhenClauses[0].ProfileRef)
	assert.Equal(t, 0, c.Policy.WhenClauses[0].Order)
	assert.Equal(t, "n.mutationCount > 50", c.Policy.WhenClauses[1].Predicate)
	assert.Equal(t, "heavy_boost", c.Policy.WhenClauses[1].ProfileRef)
	assert.Equal(t, 1, c.Policy.WhenClauses[1].Order)
}

func TestParseDDL_CreatePromotionPolicy_WithWhenClause_QuotedProfileRef(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY qref_promo FOR (n:User) APPLY {
		WHEN n.score > 0.5 APPLY PROFILE 'my-boost'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.Len(t, c.Policy.WhenClauses, 1)
	assert.Equal(t, "my-boost", c.Policy.WhenClauses[0].ProfileRef)
}

func TestParseDDL_CreatePromotionPolicy_EdgeTarget(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY edge_promo FOR ()-[r:SUPERSEDES]-() APPLY {
		ON ACCESS {
			SET r.traversalCount = r.traversalCount + 1
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	assert.True(t, c.Policy.IsEdge)
	assert.Equal(t, "SUPERSEDES", c.Policy.TargetEdgeType)
}

func TestParseDDL_CreatePromotionPolicy_WildcardTarget(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY wild_promo FOR () APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	assert.True(t, c.Policy.IsWildcard)
}

func TestParseDDL_CreatePromotionPolicy_MissingName(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected policy name")
}

// ── WITH KALMAN in ON ACCESS blocks ─────────────────────────────────────────

func TestParseDDL_OnAccess_PlainSetNoKalman(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY plain_set FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 1)
	assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)
	assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman, "plain SET should have nil Kalman")
}

func TestParseDDL_OnAccess_WithKalmanAutoDefaults(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY kalman_auto FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN SET n.confidenceScore = $evaluatedConfidence
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 1)

	mut := c.Policy.OnAccess.Mutations[0]
	assert.Equal(t, "n.confidenceScore = $evaluatedConfidence", mut.Expression)
	require.NotNil(t, mut.Kalman, "WITH KALMAN should produce non-nil KalmanConfig")
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, mut.Kalman.Mode)
	assert.Equal(t, 0.1, mut.Kalman.Q)
	assert.Equal(t, 88.0, mut.Kalman.R)
	assert.Equal(t, 10.0, mut.Kalman.VarianceScale)
	assert.Equal(t, 32, mut.Kalman.WindowSize)
}

func TestParseDDL_OnAccess_WithKalmanManualMode(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY kalman_manual FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.1, r: 88.0} SET n.score = $rawScore
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	mut := c.Policy.OnAccess.Mutations[0]
	assert.Equal(t, "n.score = $rawScore", mut.Expression)
	require.NotNil(t, mut.Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, mut.Kalman.Mode)
	assert.Equal(t, 0.1, mut.Kalman.Q)
	assert.Equal(t, 88.0, mut.Kalman.R)
}

func TestParseDDL_OnAccess_WithKalmanAutoCustomQ(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY kalman_auto_q FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.05} SET n.confidence = $conf
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	mut := c.Policy.OnAccess.Mutations[0]
	require.NotNil(t, mut.Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, mut.Kalman.Mode, "no r provided, should be auto")
	assert.Equal(t, 0.05, mut.Kalman.Q)
	assert.Equal(t, 88.0, mut.Kalman.R, "R should keep default for auto mode")
	assert.Equal(t, 10.0, mut.Kalman.VarianceScale)
	assert.Equal(t, 32, mut.Kalman.WindowSize)
}

func TestParseDDL_OnAccess_WithKalmanAllConfigKeys(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY kalman_full FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.05, r: 50.0, varianceScale: 5.0, windowSize: 64} SET n.metric = $val
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	mut := c.Policy.OnAccess.Mutations[0]
	require.NotNil(t, mut.Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, mut.Kalman.Mode)
	assert.Equal(t, 0.05, mut.Kalman.Q)
	assert.Equal(t, 50.0, mut.Kalman.R)
	assert.Equal(t, 5.0, mut.Kalman.VarianceScale)
	assert.Equal(t, 64, mut.Kalman.WindowSize)
}

func TestParseDDL_OnAccess_WithKalmanUnknownKey(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY kalman_bad FOR (n:Fact) APPLY {
		ON ACCESS {
			WITH KALMAN{q: 0.1, bogusKey: 42} SET n.score = $val
		}
	}`

	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown Kalman config key")
}

func TestParseDDL_OnAccess_MultipleMutations(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY multi_mut FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
			WITH KALMAN SET n.confidence = $conf
			WITH KALMAN{q: 0.05, r: 50.0} SET n.score = $rawScore
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 3)

	assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)
	assert.Equal(t, "n.accessCount = n.accessCount + 1", c.Policy.OnAccess.Mutations[0].Expression)

	require.NotNil(t, c.Policy.OnAccess.Mutations[1].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[1].Kalman.Mode)

	require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, c.Policy.OnAccess.Mutations[2].Kalman.Mode)
}

func TestParseDDL_OnAccess_MixedOnAccessAndWhen(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY mixed FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.accessCount = n.accessCount + 1
		}
		WHEN n.accessCount > 10 APPLY PROFILE boost
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 1)
	require.Len(t, c.Policy.WhenClauses, 1)
}

func TestParseDDL_OnAccess_QueryContextVarsInExpression(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY ctx_vars FOR (n:Fact) APPLY {
		ON ACCESS {
			SET n.lastSessionId = $_session
			SET n.lastAgent = $_agent
			WITH KALMAN SET n.confidence = CASE WHEN n._lastSessionId <> $_session THEN $evaluatedConfidence ELSE n.confidence END
		}
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 3)

	assert.Contains(t, c.Policy.OnAccess.Mutations[0].Expression, "$_session")
	assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)

	assert.Contains(t, c.Policy.OnAccess.Mutations[1].Expression, "$_agent")
	assert.Nil(t, c.Policy.OnAccess.Mutations[1].Kalman)

	assert.Contains(t, c.Policy.OnAccess.Mutations[2].Expression, "$_session")
	require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[2].Kalman.Mode)
}

// ── ALTER PROMOTION POLICY ──────────────────────────────────────────────────

func TestParseDDL_AlterPromotionPolicy_Enable(t *testing.T) {
	stmt := `ALTER PROMOTION POLICY fact_promo ENABLE`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*AlterPromotionPolicyCmd)
	require.True(t, ok)
	assert.Equal(t, "fact_promo", c.Name)
	assert.Equal(t, true, c.Updates["enabled"])
}

func TestParseDDL_AlterPromotionPolicy_Disable(t *testing.T) {
	stmt := `ALTER PROMOTION POLICY fact_promo DISABLE`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*AlterPromotionPolicyCmd)
	assert.Equal(t, "fact_promo", c.Name)
	assert.Equal(t, false, c.Updates["enabled"])
}

func TestParseDDL_AlterPromotionPolicy_MissingName(t *testing.T) {
	stmt := `ALTER PROMOTION POLICY ENABLE`
	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*AlterPromotionPolicyCmd)
	assert.Equal(t, "ENABLE", c.Name)
}

// ── DROP PROMOTION POLICY ───────────────────────────────────────────────────

func TestParseDDL_DropPromotionPolicy(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION POLICY fact_promo`)
	require.NoError(t, err)
	require.True(t, ok)

	c, ok := cmd.(*DropPromotionPolicyCmd)
	require.True(t, ok)
	assert.Equal(t, "fact_promo", c.Name)
	assert.False(t, c.IfExists)
}

func TestParseDDL_DropPromotionPolicy_IfExists(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION POLICY IF EXISTS fact_promo`)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*DropPromotionPolicyCmd)
	assert.Equal(t, "fact_promo", c.Name)
	assert.True(t, c.IfExists)
}

func TestParseDDL_DropPromotionPolicy_MissingName(t *testing.T) {
	_, _, err := ParseKnowledgePolicyDDL(`DROP PROMOTION POLICY`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected policy name")
}

// ── SHOW PROMOTION POLICIES ─────────────────────────────────────────────────

func TestParseDDL_ShowPromotionPolicies(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW PROMOTION POLICIES`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowPromotionPoliciesCmd)
	assert.True(t, ok)
}

// ── Edge cases and error handling ───────────────────────────────────────────

func TestParseDDL_ExtraWhitespace(t *testing.T) {
	stmt := `   CREATE   DECAY   PROFILE   ws_test   OPTIONS   {
		halfLifeSeconds:   100  ,
		function:   'exponential'  ,
		scope:  'NODE'  ,
		scoreFrom:  'CREATED'
	}  `

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, "ws_test", c.Bundle.Name)
}

func TestParseDDL_EqualsSignSeparator(t *testing.T) {
	stmt := `CREATE DECAY PROFILE eq_test OPTIONS {
		halfLifeSeconds = 200,
		function = 'linear',
		scope = 'NODE',
		scoreFrom = 'CREATED'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, int64(200), c.Bundle.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.DecayFunctionLinear, c.Bundle.Function)
}

func TestParseDDL_SemicolonSeparator(t *testing.T) {
	stmt := `CREATE DECAY PROFILE semi_test OPTIONS {
		halfLifeSeconds: 300;
		function: 'step';
		scope: 'NODE';
		scoreFrom: 'CREATED'
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, int64(300), c.Bundle.HalfLifeSeconds)
}

func TestParseDDL_UnrelatedCreateStatement(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`CREATE INDEX ON :Person(name)`)
	assert.Nil(t, cmd)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestParseDDL_UnrelatedAlterStatement(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`ALTER INDEX foo`)
	assert.Nil(t, cmd)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestParseDDL_UnrelatedDropStatement(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP INDEX foo`)
	assert.Nil(t, cmd)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestParseDDL_UnrelatedShowStatement(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW INDEXES`)
	assert.Nil(t, cmd)
	assert.False(t, ok)
	assert.NoError(t, err)
}

func TestParseDDL_CreateDecayProfileBinding_MissingApplyBraces(t *testing.T) {
	stmt := `CREATE DECAY PROFILE broken_bind FOR (n:Fact) APPLY`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected {")
}

func TestParseDDL_OnAccess_MissingBraces(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY broken_oa FOR (n:Fact) APPLY {
		ON ACCESS
	}`
	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected {")
}

func TestParseDDL_WhenClause_MissingApplyProfile(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY bad_when FOR (n:Fact) APPLY {
		WHEN n.accessCount > 10
	}`

	_, _, err := ParseKnowledgePolicyDDL(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected APPLY")
}

// ── Scanner helper edge cases ───────────────────────────────────────────────

func TestKpScanNumber_Negative(t *testing.T) {
	f, j, ok := kpScanNumber("-3.14", 0)
	require.True(t, ok)
	assert.Equal(t, -3.14, f)
	assert.Equal(t, 5, j)
}

func TestKpScanNumber_Integer(t *testing.T) {
	f, j, ok := kpScanNumber("42", 0)
	require.True(t, ok)
	assert.Equal(t, 42.0, f)
	assert.Equal(t, 2, j)
}

func TestKpScanNumber_InvalidInput(t *testing.T) {
	_, _, ok := kpScanNumber("abc", 0)
	assert.False(t, ok)
}

func TestKpScanInt_Valid(t *testing.T) {
	n, j, ok := kpScanInt("12345", 0)
	require.True(t, ok)
	assert.Equal(t, int64(12345), n)
	assert.Equal(t, 5, j)
}

func TestKpScanInt_Negative(t *testing.T) {
	n, j, ok := kpScanInt("-99", 0)
	require.True(t, ok)
	assert.Equal(t, int64(-99), n)
	assert.Equal(t, 3, j)
}

func TestKpScanBool_True(t *testing.T) {
	b, j, ok := kpScanBool("TRUE rest", 0)
	require.True(t, ok)
	assert.True(t, b)
	assert.Equal(t, 4, j)
}

func TestKpScanBool_False(t *testing.T) {
	b, j, ok := kpScanBool("FALSE rest", 0)
	require.True(t, ok)
	assert.False(t, b)
	assert.Equal(t, 5, j)
}

func TestKpScanBool_Invalid(t *testing.T) {
	_, _, ok := kpScanBool("MAYBE", 0)
	assert.False(t, ok)
}

func TestKpScanQuotedString_SingleQuote(t *testing.T) {
	val, j := kpScanQuotedString("'hello world'", 0)
	assert.Equal(t, "hello world", val)
	assert.Equal(t, 13, j)
}

func TestKpScanQuotedString_DoubleQuote(t *testing.T) {
	val, j := kpScanQuotedString(`"hello"`, 0)
	assert.Equal(t, "hello", val)
	assert.Equal(t, 7, j)
}

func TestKpScanQuotedString_Unterminated(t *testing.T) {
	_, j := kpScanQuotedString("'unterminated", 0)
	assert.Equal(t, -1, j)
}

func TestKpScanBraceBlock_Balanced(t *testing.T) {
	inner, j := kpScanBraceBlock("{ a: 1, b: { c: 2 } }", 0)
	assert.Equal(t, " a: 1, b: { c: 2 } ", inner)
	assert.Equal(t, 21, j)
}

func TestKpScanBraceBlock_NoTrailingWhitespace(t *testing.T) {
	inner, j := kpScanBraceBlock("{abc}", 0)
	assert.Equal(t, "abc", inner)
	assert.Equal(t, 5, j)
}

func TestKpScanBraceBlock_Unbalanced(t *testing.T) {
	_, j := kpScanBraceBlock("{ a: 1, b: { c: 2 }", 0)
	assert.Equal(t, -1, j)
}

func TestKpMatchKeywordAt_CaseInsensitive(t *testing.T) {
	assert.Greater(t, kpMatchKeywordAt("create", 0, "CREATE"), 0)
	assert.Greater(t, kpMatchKeywordAt("CREATE", 0, "create"), 0)
	assert.Greater(t, kpMatchKeywordAt("CrEaTe", 0, "CREATE"), 0)
}

func TestKpMatchKeywordAt_NoPartialMatch(t *testing.T) {
	assert.Equal(t, -1, kpMatchKeywordAt("CREATING", 0, "CREATE"))
}

func TestKpScanIdent_ValidIdent(t *testing.T) {
	name, j := kpScanIdent("my_ident123 rest", 0)
	assert.Equal(t, "my_ident123", name)
	assert.Equal(t, 11, j)
}

func TestKpScanIdent_StartsWithDigit(t *testing.T) {
	name, j := kpScanIdent("123abc", 0)
	assert.Equal(t, "", name)
	assert.Equal(t, 0, j)
}

func TestKpScanIdent_Empty(t *testing.T) {
	name, j := kpScanIdent("", 0)
	assert.Equal(t, "", name)
	assert.Equal(t, 0, j)
}

// ── Plan-syntax DDL examples ──────────────────────────────────────────────────
// Each test below corresponds to a DDL example from the plan / user guide docs.

func TestPlanDDL_WorkingMemoryBundle(t *testing.T) {
	stmt := `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10,
		scoreFloor: 0.01
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBundleCmd)
	assert.Equal(t, "working_memory", c.Bundle.Name)
	assert.Equal(t, int64(604800), c.Bundle.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.DecayFunctionExponential, c.Bundle.Function)
	assert.Equal(t, 0.10, c.Bundle.VisibilityThreshold)
	assert.Equal(t, 0.01, c.Bundle.ScoreFloor)
}

func TestPlanDDL_SessionRecordRetention(t *testing.T) {
	stmt := `CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, "session_record_retention", c.Binding.Name)
	assert.Equal(t, []string{"SessionRecord"}, c.Binding.TargetLabels)
	assert.False(t, c.Binding.IsEdge)
	assert.Equal(t, "working_memory", c.Binding.ProfileRef)
	require.NotNil(t, c.Binding.VisibilityThreshold)
	assert.Equal(t, 0.10, *c.Binding.VisibilityThreshold)

	require.Len(t, c.Binding.PropertyRules, 3)
	assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, "session_summary", c.Binding.PropertyRules[0].ProfileRef)

	assert.Equal(t, "lastConversationSummary", c.Binding.PropertyRules[1].PropertyPath)
	assert.Equal(t, int64(2592000), c.Binding.PropertyRules[1].HalfLifeSeconds)

	assert.Equal(t, "tenantId", c.Binding.PropertyRules[2].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[2].NoDecay)
}

func TestPlanDDL_CoaccessRetention(t *testing.T) {
	stmt := `CREATE DECAY PROFILE coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY HALF LIFE 1209600
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, "coaccess_retention", c.Binding.Name)
	assert.True(t, c.Binding.IsEdge)
	assert.Equal(t, "CO_ACCESSED", c.Binding.TargetEdgeType)
	assert.Equal(t, int64(1209600), c.Binding.HalfLifeSeconds)

	require.Len(t, c.Binding.PropertyRules, 2)
	assert.Equal(t, "signalScore", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)
	assert.Equal(t, 0.15, c.Binding.PropertyRules[0].ScoreFloor)

	assert.Equal(t, "externalId", c.Binding.PropertyRules[1].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[1].NoDecay)
}

func TestPlanDDL_CanonicalLinkRetention(t *testing.T) {
	stmt := `CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, "canonical_link_retention", c.Binding.Name)
	assert.True(t, c.Binding.IsEdge)
	assert.Equal(t, "CANONICAL_LINK", c.Binding.TargetEdgeType)
	assert.True(t, c.Binding.NoDecay)

	require.Len(t, c.Binding.PropertyRules, 2)
	assert.Equal(t, "externalId", c.Binding.PropertyRules[0].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[0].NoDecay)
	assert.Equal(t, "sourceSystem", c.Binding.PropertyRules[1].PropertyPath)
	assert.True(t, c.Binding.PropertyRules[1].NoDecay)
}

func TestPlanDDL_ReviewLinkRetention(t *testing.T) {
	stmt := `CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY HALF LIFE 604800
  r.confidence DECAY HALF LIFE 86400
  r.confidence DECAY FLOOR 0.25
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, "review_link_retention", c.Binding.Name)
	assert.True(t, c.Binding.IsEdge)
	assert.Equal(t, "REVIEWED_WITH", c.Binding.TargetEdgeType)
	assert.Equal(t, int64(604800), c.Binding.HalfLifeSeconds)

	require.Len(t, c.Binding.PropertyRules, 1)
	assert.Equal(t, "confidence", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(86400), c.Binding.PropertyRules[0].HalfLifeSeconds)
	assert.Equal(t, 0.25, c.Binding.PropertyRules[0].ScoreFloor)
}

func TestPlanDDL_ReinforcedTierPromotionProfile(t *testing.T) {
	stmt := `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionProfileCmd)
	assert.Equal(t, "reinforced_tier", c.Profile.Name)
	assert.Equal(t, 1.5, c.Profile.Multiplier)
	assert.Equal(t, 0.3, c.Profile.ScoreFloor)
	assert.Equal(t, 1.0, c.Profile.ScoreCap)
}

func TestPlanDDL_SessionRecordTiering(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROFILE 'canonical_tier'
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	assert.Equal(t, "session_record_tiering", c.Policy.Name)
	assert.Equal(t, []string{"SessionRecord"}, c.Policy.TargetLabels)
	assert.False(t, c.Policy.IsEdge)

	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 2)
	assert.Equal(t, "n.accessCount = coalesce(n.accessCount, 0) + 1", c.Policy.OnAccess.Mutations[0].Expression)
	assert.Equal(t, "n.lastAccessedAt = timestamp()", c.Policy.OnAccess.Mutations[1].Expression)

	require.Len(t, c.Policy.WhenClauses, 2)
	assert.Equal(t, "n.accessCount >= 3", c.Policy.WhenClauses[0].Predicate)
	assert.Equal(t, "reinforced_tier", c.Policy.WhenClauses[0].ProfileRef)
	assert.Contains(t, c.Policy.WhenClauses[1].Predicate, "n.accessCount >= 5")
	assert.Equal(t, "canonical_tier", c.Policy.WhenClauses[1].ProfileRef)
}

func TestPlanDDL_EpisodicRecallQuality(t *testing.T) {
	stmt := `CREATE PROMOTION POLICY episodic_recall_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
    SET n._lastAgentId = $_agent
  }

  WHEN n.accessCount >= 5 AND n.confidenceScore >= 0.8
    APPLY PROFILE 'high_confidence_tier'

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreatePromotionPolicyCmd)
	assert.Equal(t, "episodic_recall_quality", c.Policy.Name)
	assert.Equal(t, []string{"MemoryEpisode"}, c.Policy.TargetLabels)

	require.NotNil(t, c.Policy.OnAccess)
	require.Len(t, c.Policy.OnAccess.Mutations, 6)

	assert.Nil(t, c.Policy.OnAccess.Mutations[0].Kalman)
	assert.Nil(t, c.Policy.OnAccess.Mutations[1].Kalman)

	require.NotNil(t, c.Policy.OnAccess.Mutations[2].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, c.Policy.OnAccess.Mutations[2].Kalman.Mode)
	assert.Equal(t, 0.05, c.Policy.OnAccess.Mutations[2].Kalman.Q)
	assert.Equal(t, 50.0, c.Policy.OnAccess.Mutations[2].Kalman.R)
	assert.Contains(t, c.Policy.OnAccess.Mutations[2].Expression, "$evaluatedConfidence")

	require.NotNil(t, c.Policy.OnAccess.Mutations[3].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, c.Policy.OnAccess.Mutations[3].Kalman.Mode)

	assert.Contains(t, c.Policy.OnAccess.Mutations[4].Expression, "$_session")
	assert.Nil(t, c.Policy.OnAccess.Mutations[4].Kalman)

	assert.Contains(t, c.Policy.OnAccess.Mutations[5].Expression, "$_agent")
	assert.Nil(t, c.Policy.OnAccess.Mutations[5].Kalman)

	require.Len(t, c.Policy.WhenClauses, 2)
	assert.Equal(t, "high_confidence_tier", c.Policy.WhenClauses[0].ProfileRef)
	assert.Equal(t, "reinforced_tier", c.Policy.WhenClauses[1].ProfileRef)
}

func TestPlanDDL_ShowDecayProfiles(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW DECAY PROFILES;`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowDecayProfilesCmd)
	assert.True(t, ok)
}

func TestPlanDDL_ShowPromotionPolicies(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`SHOW PROMOTION POLICIES;`)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok = cmd.(*ShowPromotionPoliciesCmd)
	assert.True(t, ok)
}

func TestPlanDDL_DropDecayProfileByName(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP DECAY PROFILE session_record_retention;`)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*DropDecayProfileCmd)
	assert.Equal(t, "session_record_retention", c.Name)
}

func TestPlanDDL_DropPromotionPolicy(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION POLICY session_record_tiering;`)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*DropPromotionPolicyCmd)
	assert.Equal(t, "session_record_tiering", c.Name)
}

func TestPlanDDL_DropPromotionProfile(t *testing.T) {
	cmd, ok, err := ParseKnowledgePolicyDDL(`DROP PROMOTION PROFILE reinforced_tier;`)
	require.NoError(t, err)
	require.True(t, ok)
	c := cmd.(*DropPromotionProfileCmd)
	assert.Equal(t, "reinforced_tier", c.Name)
}

func TestPlanDDL_MultiLabelTarget(t *testing.T) {
	stmt := `CREATE DECAY PROFILE multi_label
FOR (n:SessionRecord:MemoryEpisode)
APPLY {
  DECAY PROFILE 'working_memory'
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, []string{"SessionRecord", "MemoryEpisode"}, c.Binding.TargetLabels)
}

func TestPlanDDL_EntityLevelDecayHalfLife(t *testing.T) {
	stmt := `CREATE DECAY PROFILE short_lived
FOR (n:Temp)
APPLY {
  DECAY HALF LIFE 86400
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, int64(86400), c.Binding.HalfLifeSeconds)
}

func TestPlanDDL_EntityLevelDecayFloor(t *testing.T) {
	stmt := `CREATE DECAY PROFILE floored
FOR (n:Archive)
APPLY {
  DECAY HALF LIFE 604800
  DECAY FLOOR 0.05
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	assert.Equal(t, int64(604800), c.Binding.HalfLifeSeconds)
	assert.Equal(t, 0.05, c.Binding.ScoreFloor)
}

func TestPlanDDL_PropertyDecayProfile(t *testing.T) {
	stmt := `CREATE DECAY PROFILE prop_ref
FOR (n:Doc)
APPLY {
  n.summary DECAY PROFILE 'session_summary'
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	require.Len(t, c.Binding.PropertyRules, 1)
	assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, "session_summary", c.Binding.PropertyRules[0].ProfileRef)
}

func TestPlanDDL_PropertyDecayHalfLife(t *testing.T) {
	stmt := `CREATE DECAY PROFILE prop_hl
FOR (n:Doc)
APPLY {
  n.content DECAY HALF LIFE 1209600
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	require.Len(t, c.Binding.PropertyRules, 1)
	assert.Equal(t, "content", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)
}

func TestPlanDDL_PropertyDecayFloor(t *testing.T) {
	stmt := `CREATE DECAY PROFILE prop_floor
FOR (n:Doc)
APPLY {
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
}`

	cmd, ok, err := ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err)
	require.True(t, ok)

	c := cmd.(*CreateDecayProfileBindingCmd)
	require.Len(t, c.Binding.PropertyRules, 1)
	assert.Equal(t, "summary", c.Binding.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(1209600), c.Binding.PropertyRules[0].HalfLifeSeconds)
	assert.Equal(t, 0.10, c.Binding.PropertyRules[0].ScoreFloor)
}
