package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseDDLAndApply parses a DDL statement and applies it to the SchemaManager.
func parseDDLAndApply(t *testing.T, sm *storage.SchemaManager, stmt string) {
	t.Helper()
	cmd, ok, err := cypher.ParseKnowledgePolicyDDL(stmt)
	require.NoError(t, err, "parse error for: %s", stmt)
	require.True(t, ok, "not recognized as DDL: %s", stmt)

	switch c := cmd.(type) {
	case *cypher.CreateDecayProfileBundleCmd:
		require.NoError(t, sm.CreateDecayProfileBundle(c.Bundle))
	case *cypher.CreateDecayProfileBindingCmd:
		require.NoError(t, sm.CreateDecayProfileBinding(c.Binding))
	case *cypher.CreatePromotionProfileCmd:
		require.NoError(t, sm.CreatePromotionProfile(c.Profile))
	case *cypher.CreatePromotionPolicyCmd:
		require.NoError(t, sm.CreatePromotionPolicy(c.Policy))
	case *cypher.DropDecayProfileCmd:
		require.NoError(t, sm.DropDecayProfile(c.Name, c.IfExists))
	case *cypher.DropPromotionProfileCmd:
		require.NoError(t, sm.DropPromotionProfile(c.Name, c.IfExists))
	case *cypher.DropPromotionPolicyCmd:
		require.NoError(t, sm.DropPromotionPolicy(c.Name, c.IfExists))
	case *cypher.AlterDecayProfileCmd:
		require.NoError(t, sm.AlterDecayProfile(c.Name, c.Updates))
	case *cypher.AlterPromotionProfileCmd:
		require.NoError(t, sm.AlterPromotionProfile(c.Name, c.Updates))
	case *cypher.AlterPromotionPolicyCmd:
		require.NoError(t, sm.AlterPromotionPolicy(c.Name, c.Updates))
	default:
		t.Fatalf("unexpected DDL command type: %T", cmd)
	}
}

// TestE2E_WorkingMemoryBundle parses the working_memory parameter bundle from
// docs, persists it, retrieves it, and deeply asserts every field.
func TestE2E_WorkingMemoryBundle(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10,
		scoreFloor: 0.01
	}`)

	bundles, bindings := sm.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	assert.Len(t, bindings, 0)

	b := bundles[0]
	assert.Equal(t, "working_memory", b.Name)
	assert.Equal(t, int64(604800), b.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.DecayFunctionExponential, b.Function)
	assert.Equal(t, 0.10, b.VisibilityThreshold)
	assert.Equal(t, 0.01, b.ScoreFloor)
	assert.True(t, b.Enabled)
}

// TestE2E_SessionRecordRetention parses the session_record_retention binding
// from the plan, persists it, and deeply asserts the binding, profile ref,
// visibility threshold, and all three property rules.
func TestE2E_SessionRecordRetention(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10,
		scoreFloor: 0.01
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE session_summary OPTIONS {
		halfLifeSeconds: 1209600,
		function: 'exponential',
		visibilityThreshold: 0.10
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}`)

	_, bindings := sm.ShowDecayProfiles()
	require.Len(t, bindings, 1)

	bind := bindings[0]
	assert.Equal(t, "session_record_retention", bind.Name)
	assert.Equal(t, []string{"SessionRecord"}, bind.TargetLabels)
	assert.False(t, bind.IsEdge)
	assert.False(t, bind.IsWildcard)
	assert.Equal(t, "working_memory", bind.ProfileRef)
	require.NotNil(t, bind.VisibilityThreshold)
	assert.Equal(t, 0.10, *bind.VisibilityThreshold)

	require.Len(t, bind.PropertyRules, 3)

	assert.Equal(t, "summary", bind.PropertyRules[0].PropertyPath)
	assert.Equal(t, "session_summary", bind.PropertyRules[0].ProfileRef)
	assert.False(t, bind.PropertyRules[0].NoDecay)
	assert.Equal(t, 0, bind.PropertyRules[0].Order)

	assert.Equal(t, "lastConversationSummary", bind.PropertyRules[1].PropertyPath)
	assert.Equal(t, int64(2592000), bind.PropertyRules[1].HalfLifeSeconds)
	assert.False(t, bind.PropertyRules[1].NoDecay)
	assert.Equal(t, 1, bind.PropertyRules[1].Order)

	assert.Equal(t, "tenantId", bind.PropertyRules[2].PropertyPath)
	assert.True(t, bind.PropertyRules[2].NoDecay)
	assert.Equal(t, 2, bind.PropertyRules[2].Order)
}

// TestE2E_CoaccessRetention parses the edge-level coaccess_retention binding
// with entity-level DECAY HALF LIFE and property rules with merged DECAY FLOOR.
func TestE2E_CoaccessRetention(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY HALF LIFE 1209600
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}`)

	_, bindings := sm.ShowDecayProfiles()
	require.Len(t, bindings, 1)

	bind := bindings[0]
	assert.Equal(t, "coaccess_retention", bind.Name)
	assert.True(t, bind.IsEdge)
	assert.Equal(t, "CO_ACCESSED", bind.TargetEdgeType)
	assert.Equal(t, int64(1209600), bind.HalfLifeSeconds)

	require.Len(t, bind.PropertyRules, 2)

	assert.Equal(t, "signalScore", bind.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(1209600), bind.PropertyRules[0].HalfLifeSeconds)
	assert.Equal(t, 0.15, bind.PropertyRules[0].ScoreFloor)
	assert.Equal(t, 0, bind.PropertyRules[0].Order)

	assert.Equal(t, "externalId", bind.PropertyRules[1].PropertyPath)
	assert.True(t, bind.PropertyRules[1].NoDecay)
	assert.Equal(t, 1, bind.PropertyRules[1].Order)
}

// TestE2E_CanonicalLinkRetention parses the no-decay edge binding.
func TestE2E_CanonicalLinkRetention(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}`)

	_, bindings := sm.ShowDecayProfiles()
	require.Len(t, bindings, 1)

	bind := bindings[0]
	assert.Equal(t, "canonical_link_retention", bind.Name)
	assert.True(t, bind.IsEdge)
	assert.Equal(t, "CANONICAL_LINK", bind.TargetEdgeType)
	assert.True(t, bind.NoDecay)

	require.Len(t, bind.PropertyRules, 2)
	assert.Equal(t, "externalId", bind.PropertyRules[0].PropertyPath)
	assert.True(t, bind.PropertyRules[0].NoDecay)
	assert.Equal(t, "sourceSystem", bind.PropertyRules[1].PropertyPath)
	assert.True(t, bind.PropertyRules[1].NoDecay)
}

// TestE2E_ReviewLinkRetention parses the edge binding with entity-level half
// life and a property rule merging DECAY HALF LIFE + DECAY FLOOR on the same property.
func TestE2E_ReviewLinkRetention(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY HALF LIFE 604800
  r.confidence DECAY HALF LIFE 86400
  r.confidence DECAY FLOOR 0.25
}`)

	_, bindings := sm.ShowDecayProfiles()
	require.Len(t, bindings, 1)

	bind := bindings[0]
	assert.Equal(t, "review_link_retention", bind.Name)
	assert.True(t, bind.IsEdge)
	assert.Equal(t, "REVIEWED_WITH", bind.TargetEdgeType)
	assert.Equal(t, int64(604800), bind.HalfLifeSeconds)

	require.Len(t, bind.PropertyRules, 1)
	assert.Equal(t, "confidence", bind.PropertyRules[0].PropertyPath)
	assert.Equal(t, int64(86400), bind.PropertyRules[0].HalfLifeSeconds)
	assert.Equal(t, 0.25, bind.PropertyRules[0].ScoreFloor)
}

// TestE2E_ReinforcedTierPromotionProfile parses the reinforced_tier promotion
// profile and asserts all fields.
func TestE2E_ReinforcedTierPromotionProfile(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	profiles := sm.ShowPromotionProfiles()
	require.Len(t, profiles, 1)

	p := profiles[0]
	assert.Equal(t, "reinforced_tier", p.Name)
	assert.Equal(t, 1.5, p.Multiplier)
	assert.Equal(t, 0.3, p.ScoreFloor)
	assert.Equal(t, 1.0, p.ScoreCap)
	assert.True(t, p.Enabled)
}

// TestE2E_SessionRecordTiering parses the session_record_tiering promotion
// policy with ON ACCESS mutations and WHEN clauses, persists, retrieves, and
// deeply asserts the full structure.
func TestE2E_SessionRecordTiering(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE canonical_tier OPTIONS {
		multiplier: 2.0,
		scoreFloor: 0.5,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY session_record_tiering
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
}`)

	policies := sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)

	pol := policies[0]
	assert.Equal(t, "session_record_tiering", pol.Name)
	assert.Equal(t, []string{"SessionRecord"}, pol.TargetLabels)
	assert.False(t, pol.IsEdge)
	assert.False(t, pol.IsWildcard)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 2)
	assert.Equal(t, "n.accessCount = coalesce(n.accessCount, 0) + 1", pol.OnAccess.Mutations[0].Expression)
	assert.Nil(t, pol.OnAccess.Mutations[0].Kalman)
	assert.Equal(t, "n.lastAccessedAt = timestamp()", pol.OnAccess.Mutations[1].Expression)
	assert.Nil(t, pol.OnAccess.Mutations[1].Kalman)

	require.Len(t, pol.WhenClauses, 2)
	assert.Equal(t, "n.accessCount >= 3", pol.WhenClauses[0].Predicate)
	assert.Equal(t, "reinforced_tier", pol.WhenClauses[0].ProfileRef)
	assert.Equal(t, 0, pol.WhenClauses[0].Order)

	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.accessCount >= 5")
	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.sourceAgreement >= 0.95")
	assert.Equal(t, "canonical_tier", pol.WhenClauses[1].ProfileRef)
	assert.Equal(t, 1, pol.WhenClauses[1].Order)
}

// TestE2E_EpisodicRecallQuality parses the Kalman-filtered promotion policy
// from the plan, persists it, and deeply asserts all mutations (plain SET,
// WITH KALMAN manual, WITH KALMAN auto) and WHEN clauses.
func TestE2E_EpisodicRecallQuality(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE high_confidence_tier OPTIONS {
		multiplier: 2.0,
		scoreFloor: 0.5,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY episodic_recall_quality
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
}`)

	policies := sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)

	pol := policies[0]
	assert.Equal(t, "episodic_recall_quality", pol.Name)
	assert.Equal(t, []string{"MemoryEpisode"}, pol.TargetLabels)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 6)

	// Mutation 0: plain SET accessCount
	assert.Equal(t, "n.accessCount = coalesce(n.accessCount, 0) + 1", pol.OnAccess.Mutations[0].Expression)
	assert.Nil(t, pol.OnAccess.Mutations[0].Kalman)

	// Mutation 1: plain SET lastAccessedAt
	assert.Equal(t, "n.lastAccessedAt = timestamp()", pol.OnAccess.Mutations[1].Expression)
	assert.Nil(t, pol.OnAccess.Mutations[1].Kalman)

	// Mutation 2: WITH KALMAN{q: 0.05, r: 50.0} SET confidenceScore
	assert.Contains(t, pol.OnAccess.Mutations[2].Expression, "$evaluatedConfidence")
	require.NotNil(t, pol.OnAccess.Mutations[2].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, pol.OnAccess.Mutations[2].Kalman.Mode)
	assert.Equal(t, 0.05, pol.OnAccess.Mutations[2].Kalman.Q)
	assert.Equal(t, 50.0, pol.OnAccess.Mutations[2].Kalman.R)

	// Mutation 3: WITH KALMAN (auto) SET crossSessionAccessRate
	require.NotNil(t, pol.OnAccess.Mutations[3].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, pol.OnAccess.Mutations[3].Kalman.Mode)
	assert.Equal(t, 0.1, pol.OnAccess.Mutations[3].Kalman.Q)
	assert.Equal(t, 88.0, pol.OnAccess.Mutations[3].Kalman.R)

	// Mutation 4: plain SET _lastSessionId
	assert.Contains(t, pol.OnAccess.Mutations[4].Expression, "$_session")
	assert.Nil(t, pol.OnAccess.Mutations[4].Kalman)

	// Mutation 5: plain SET _lastAgentId
	assert.Contains(t, pol.OnAccess.Mutations[5].Expression, "$_agent")
	assert.Nil(t, pol.OnAccess.Mutations[5].Kalman)

	// WHEN clauses
	require.Len(t, pol.WhenClauses, 2)
	assert.Contains(t, pol.WhenClauses[0].Predicate, "n.accessCount >= 5")
	assert.Contains(t, pol.WhenClauses[0].Predicate, "n.confidenceScore >= 0.8")
	assert.Equal(t, "high_confidence_tier", pol.WhenClauses[0].ProfileRef)

	assert.Equal(t, "n.accessCount >= 3", pol.WhenClauses[1].Predicate)
	assert.Equal(t, "reinforced_tier", pol.WhenClauses[1].ProfileRef)
}

// TestE2E_NoisyCorroborationPromotionLayers verifies a realistic ON ACCESS
// policy where noisy corroboration signals are smoothed before promotion tiers
// advance from reinforced evidence to canonical evidence.
func TestE2E_NoisyCorroborationPromotionLayers(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_evidence OPTIONS {
		multiplier: 1.25,
		scoreFloor: 0.25,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE canonical_evidence OPTIONS {
		multiplier: 1.60,
		scoreFloor: 0.45,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY corroboration_escalation
FOR (n:KnowledgeFact)
APPLY {
  ON ACCESS {
    SET n.evidenceCount = coalesce(n.evidenceCount, 0) + 1
    WITH KALMAN{q: 0.05, r: 50.0} SET n.sourceAgreement = $corroborationScore
    WITH KALMAN SET n.crossSessionSupport =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionSupport, 0) + 1
        ELSE n.crossSessionSupport
      END
    SET n._lastSessionId = $_session
  }

  WHEN n.evidenceCount >= 3 AND n.sourceAgreement >= 0.75
    APPLY PROFILE 'reinforced_evidence'

  WHEN n.evidenceCount >= 8 AND n.sourceAgreement >= 0.90 AND n.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_evidence'
}`)

	policies := sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)

	pol := policies[0]
	assert.Equal(t, "corroboration_escalation", pol.Name)
	assert.Equal(t, []string{"KnowledgeFact"}, pol.TargetLabels)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 4)
	assert.Contains(t, pol.OnAccess.Mutations[0].Expression, "evidenceCount")
	assert.Nil(t, pol.OnAccess.Mutations[0].Kalman)
	assert.Contains(t, pol.OnAccess.Mutations[1].Expression, "$corroborationScore")
	require.NotNil(t, pol.OnAccess.Mutations[1].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeManual, pol.OnAccess.Mutations[1].Kalman.Mode)
	assert.Contains(t, pol.OnAccess.Mutations[2].Expression, "crossSessionSupport")
	require.NotNil(t, pol.OnAccess.Mutations[2].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, pol.OnAccess.Mutations[2].Kalman.Mode)
	assert.Contains(t, pol.OnAccess.Mutations[3].Expression, "$_session")
	assert.Nil(t, pol.OnAccess.Mutations[3].Kalman)

	require.Len(t, pol.WhenClauses, 2)
	assert.Contains(t, pol.WhenClauses[0].Predicate, "n.evidenceCount >= 3")
	assert.Contains(t, pol.WhenClauses[0].Predicate, "n.sourceAgreement >= 0.75")
	assert.Equal(t, "reinforced_evidence", pol.WhenClauses[0].ProfileRef)
	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.evidenceCount >= 8")
	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.crossSessionSupport >= 3")
	assert.Equal(t, "canonical_evidence", pol.WhenClauses[1].ProfileRef)
}

// TestE2E_DropDecayProfile exercises the DROP DECAY PROFILE DDL from docs.
func TestE2E_DropDecayProfile(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10
	}`)

	bundles, _ := sm.ShowDecayProfiles()
	require.Len(t, bundles, 1)

	parseDDLAndApply(t, sm, `DROP DECAY PROFILE working_memory`)

	bundles, _ = sm.ShowDecayProfiles()
	assert.Len(t, bundles, 0)
}

func TestE2E_AlterDecayProfile(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE doc_decay OPTIONS {
		halfLifeSeconds: 3600,
		function: 'exponential',
		visibilityThreshold: 0.10,
		decayEnabled: true
	}`)

	parseDDLAndApply(t, sm, `ALTER DECAY PROFILE doc_decay SET OPTIONS {
		decayEnabled: false,
		visibilityThreshold: 0.0
	}`)

	bundles, _ := sm.ShowDecayProfiles()
	require.Len(t, bundles, 1)
	assert.Equal(t, "doc_decay", bundles[0].Name)
	assert.False(t, bundles[0].DecayEnabled)
	assert.Equal(t, 0.0, bundles[0].VisibilityThreshold)
}

func TestE2E_AlterPromotionProfile(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE lifeline OPTIONS {
		multiplier: 2.0,
		scoreFloor: 0.2,
		scoreCap: 1.0,
		enabled: false
	}`)

	parseDDLAndApply(t, sm, `ALTER PROMOTION PROFILE lifeline SET OPTIONS {
		multiplier: 3.5,
		enabled: true
	}`)

	profiles := sm.ShowPromotionProfiles()
	require.Len(t, profiles, 1)
	assert.Equal(t, "lifeline", profiles[0].Name)
	assert.Equal(t, 3.5, profiles[0].Multiplier)
	assert.True(t, profiles[0].Enabled)
}

func TestE2E_AlterPromotionPolicy(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE lifeline OPTIONS {
		multiplier: 2.0,
		scoreFloor: 0.2,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY fact_policy
FOR (n:Fact)
APPLY {
  WHEN n.promote = true
    APPLY PROFILE 'lifeline'
}`)

	parseDDLAndApply(t, sm, `ALTER PROMOTION POLICY fact_policy DISABLE`)

	policies := sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)
	assert.Equal(t, "fact_policy", policies[0].Name)
	assert.False(t, policies[0].Enabled)

	parseDDLAndApply(t, sm, `ALTER PROMOTION POLICY fact_policy ENABLE`)
	policies = sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)
	assert.True(t, policies[0].Enabled)
}

// TestE2E_DropPromotionPolicy exercises the DROP PROMOTION POLICY DDL from docs.
func TestE2E_DropPromotionPolicy(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
  }
  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}`)

	policies := sm.ShowPromotionPolicies()
	require.Len(t, policies, 1)

	parseDDLAndApply(t, sm, `DROP PROMOTION POLICY session_record_tiering`)

	policies = sm.ShowPromotionPolicies()
	assert.Len(t, policies, 0)
}

// TestE2E_DropPromotionProfile exercises the DROP PROMOTION PROFILE DDL from docs.
func TestE2E_DropPromotionProfile(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	profiles := sm.ShowPromotionProfiles()
	require.Len(t, profiles, 1)

	parseDDLAndApply(t, sm, `DROP PROMOTION PROFILE reinforced_tier`)

	profiles = sm.ShowPromotionProfiles()
	assert.Len(t, profiles, 0)
}

// TestE2E_ShowDecayProfiles exercises the SHOW DECAY PROFILES DDL from docs.
func TestE2E_ShowDecayProfiles(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE bundle_a OPTIONS {
		halfLifeSeconds: 100,
		function: 'linear',
		visibilityThreshold: 0.05
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE bundle_b OPTIONS {
		halfLifeSeconds: 200,
		function: 'step',
		visibilityThreshold: 0.10
	}`)

	cmd, ok, err := cypher.ParseKnowledgePolicyDDL(`SHOW DECAY PROFILES`)
	require.NoError(t, err)
	require.True(t, ok)
	_, isShow := cmd.(*cypher.ShowDecayProfilesCmd)
	assert.True(t, isShow)

	bundles, _ := sm.ShowDecayProfiles()
	assert.Len(t, bundles, 2)
	assert.Equal(t, "bundle_a", bundles[0].Name)
	assert.Equal(t, "bundle_b", bundles[1].Name)
}

// TestE2E_ShowPromotionPolicies exercises the SHOW PROMOTION POLICIES DDL from docs.
func TestE2E_ShowPromotionPolicies(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE p1 OPTIONS {
		multiplier: 1.0,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY pol_a
FOR (n:TypeA)
APPLY {
  WHEN n.score > 0 APPLY PROFILE 'p1'
}`)

	cmd, ok, err := cypher.ParseKnowledgePolicyDDL(`SHOW PROMOTION POLICIES`)
	require.NoError(t, err)
	require.True(t, ok)
	_, isShow := cmd.(*cypher.ShowPromotionPoliciesCmd)
	assert.True(t, isShow)

	policies := sm.ShowPromotionPolicies()
	assert.Len(t, policies, 1)
	assert.Equal(t, "pol_a", policies[0].Name)
}

// TestE2E_MultiLabelBinding tests a multi-label node target from the docs.
func TestE2E_MultiLabelBinding(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE base OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE multi_label
FOR (n:SessionRecord:MemoryEpisode)
APPLY {
  DECAY PROFILE 'base'
}`)

	_, bindings := sm.ShowDecayProfiles()
	require.Len(t, bindings, 1)

	bind := bindings[0]
	assert.Equal(t, []string{"MemoryEpisode", "SessionRecord"}, bind.TargetLabels)
	assert.Equal(t, "base", bind.ProfileRef)
}

// TestE2E_PersistenceRoundTrip creates all doc examples, exports, reimports
// into a fresh SchemaManager, and verifies the full structure survives.
func TestE2E_PersistenceRoundTrip(t *testing.T) {
	sm := storage.NewSchemaManager()

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE working_memory OPTIONS {
		halfLifeSeconds: 604800,
		function: 'exponential',
		visibilityThreshold: 0.10,
		scoreFloor: 0.01
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE session_summary OPTIONS {
		halfLifeSeconds: 1209600,
		function: 'exponential',
		visibilityThreshold: 0.10
	}`)

	parseDDLAndApply(t, sm, `CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.tenantId NO DECAY
}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION PROFILE reinforced_tier OPTIONS {
		multiplier: 1.5,
		scoreFloor: 0.3,
		scoreCap: 1.0
	}`)

	parseDDLAndApply(t, sm, `CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
  }
  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'
}`)

	def := sm.ExportDefinition()
	require.NotNil(t, def)

	sm2 := storage.NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(def))

	bundles2, bindings2 := sm2.ShowDecayProfiles()
	assert.Len(t, bundles2, 2)
	assert.Len(t, bindings2, 1)

	bind := bindings2[0]
	assert.Equal(t, "session_record_retention", bind.Name)
	assert.Equal(t, "working_memory", bind.ProfileRef)
	require.NotNil(t, bind.VisibilityThreshold)
	assert.Equal(t, 0.10, *bind.VisibilityThreshold)
	require.Len(t, bind.PropertyRules, 2)
	assert.Equal(t, "summary", bind.PropertyRules[0].PropertyPath)
	assert.Equal(t, "session_summary", bind.PropertyRules[0].ProfileRef)
	assert.Equal(t, "tenantId", bind.PropertyRules[1].PropertyPath)
	assert.True(t, bind.PropertyRules[1].NoDecay)

	profiles2 := sm2.ShowPromotionProfiles()
	assert.Len(t, profiles2, 1)
	assert.Equal(t, "reinforced_tier", profiles2[0].Name)
	assert.Equal(t, 1.5, profiles2[0].Multiplier)

	policies2 := sm2.ShowPromotionPolicies()
	assert.Len(t, policies2, 1)
	assert.Equal(t, "session_record_tiering", policies2[0].Name)
	require.NotNil(t, policies2[0].OnAccess)
	require.Len(t, policies2[0].OnAccess.Mutations, 1)
	require.Len(t, policies2[0].WhenClauses, 1)
	assert.Equal(t, "reinforced_tier", policies2[0].WhenClauses[0].ProfileRef)
}
