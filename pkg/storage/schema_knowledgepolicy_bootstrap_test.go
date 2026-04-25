package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bootstrapParseDDLAndApply(t *testing.T, sm *storage.SchemaManager, stmt string) {
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
	default:
		t.Fatalf("unexpected DDL command type: %T", cmd)
	}
}

func allBootstrapDDL() []string {
	return []string{
		// Step 1: Bundles
		`CREATE DECAY PROFILE knowledge_fact_retention OPTIONS {
			decayEnabled: false,
			visibilityThreshold: 0.0,
			function: 'none',
			scoreFrom: 'CREATED'
		}`,
		`CREATE DECAY PROFILE memory_episode_retention OPTIONS {
			halfLifeSeconds: 604800,
			function: 'exponential',
			visibilityThreshold: 0.10,
			scoreFrom: 'VERSION'
		}`,
		`CREATE DECAY PROFILE session_summary OPTIONS {
			halfLifeSeconds: 1209600,
			function: 'exponential',
			visibilityThreshold: 0.10,
			scoreFloor: 0.10
		}`,
		`CREATE DECAY PROFILE wisdom_directive_retention OPTIONS {
			decayEnabled: false,
			visibilityThreshold: 0.0,
			function: 'none',
			scoreFrom: 'CREATED'
		}`,
		`CREATE DECAY PROFILE evidence_decay OPTIONS {
			halfLifeSeconds: 2592000,
			function: 'exponential',
			visibilityThreshold: 0.10,
			scoreFrom: 'CREATED'
		}`,
		// Step 2: Bindings
		`CREATE DECAY PROFILE memory_episode_retention_binding
FOR (n:MemoryEpisode)
APPLY {
  DECAY PROFILE 'memory_episode_retention'
  DECAY VISIBILITY THRESHOLD 0.10
  n.tenantId NO DECAY
  n.agentId NO DECAY
  n.sessionId NO DECAY
  n.system_created_at NO DECAY
  n.system_expired_at NO DECAY
  n.valid_from NO DECAY
  n.valid_to NO DECAY
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
  n.ephemeralContext DECAY HALF LIFE 86400
}`,
		`CREATE DECAY PROFILE knowledge_fact_retention_binding
FOR (n:KnowledgeFact)
APPLY {
  DECAY PROFILE 'knowledge_fact_retention'
}`,
		`CREATE DECAY PROFILE wisdom_directive_retention_binding
FOR (n:WisdomDirective)
APPLY {
  DECAY PROFILE 'wisdom_directive_retention'
}`,
		`CREATE DECAY PROFILE evidence_edge_retention_binding
FOR ()-[r:EVIDENCES]-()
APPLY {
  DECAY PROFILE 'evidence_decay'
  DECAY VISIBILITY THRESHOLD 0.10
  r.sourceId NO DECAY
}`,
		`CREATE DECAY PROFILE supersession_edge_retention
FOR ()-[r:SUPERSEDES]-()
APPLY {
  NO DECAY
}`,
		`CREATE DECAY PROFILE consolidation_edge_retention
FOR ()-[r:CONSOLIDATES_TO]-()
APPLY {
  NO DECAY
}`,
		`CREATE DECAY PROFILE revision_edge_retention
FOR ()-[r:REVISES]-()
APPLY {
  NO DECAY
}`,
		`CREATE DECAY PROFILE derivation_edge_retention
FOR ()-[r:DERIVED_FROM]-()
APPLY {
  NO DECAY
}`,
		// Step 3: Promotion profiles
		`CREATE PROMOTION PROFILE memory_reinforced OPTIONS {
			multiplier: 1.25,
			scoreFloor: 0.0,
			scoreCap: 1.0
		}`,
		`CREATE PROMOTION PROFILE consolidation_candidate OPTIONS {
			multiplier: 1.50,
			scoreFloor: 0.80,
			scoreCap: 1.0
		}`,
		`CREATE PROMOTION PROFILE wisdom_provisional OPTIONS {
			multiplier: 1.0,
			scoreFloor: 0.0,
			scoreCap: 1.0
		}`,
		`CREATE PROMOTION PROFILE wisdom_established OPTIONS {
			multiplier: 1.0,
			scoreFloor: 0.50,
			scoreCap: 1.0
		}`,
		`CREATE PROMOTION PROFILE wisdom_canonical OPTIONS {
			multiplier: 1.0,
			scoreFloor: 0.90,
			scoreCap: 1.0
		}`,
		`CREATE PROMOTION PROFILE reinforced_evidence OPTIONS {
			multiplier: 1.20,
			scoreFloor: 0.0,
			scoreCap: 1.0
		}`,
		// Step 4: Promotion policies
		`CREATE PROMOTION POLICY memory_episode_consolidation
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    SET n.accessIntervals = coalesce(n.accessIntervals, '') + ',' + toString(timestamp())
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'memory_reinforced'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.80
    APPLY PROFILE 'consolidation_candidate'
}`,
		`CREATE PROMOTION POLICY wisdom_directive_stability
FOR (n:WisdomDirective)
APPLY {
  ON ACCESS {
    SET n.evaluationCount = coalesce(n.evaluationCount, 0) + 1
    SET n.lastEvaluatedAt = timestamp()
  }

  WHEN n.evidenceCount < 3
    APPLY PROFILE 'wisdom_provisional'

  WHEN n.evidenceCount >= 3 AND n.contradictionRate < 0.20
    APPLY PROFILE 'wisdom_established'

  WHEN n.evidenceCount >= 10 AND n.contradictionRate < 0.05 AND n.crossSessionSupport >= 3
    APPLY PROFILE 'wisdom_canonical'
}`,
		`CREATE PROMOTION POLICY evidence_traversal_tiering
FOR ()-[r:EVIDENCES]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()
  }

  WHEN r.traversalCount >= 5
    APPLY PROFILE 'reinforced_evidence'
}`,
	}
}

func TestBootstrap_AllDDLParses(t *testing.T) {
	sm := storage.NewSchemaManager()
	for i, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
		if t.Failed() {
			t.Fatalf("failed at DDL statement %d", i)
		}
	}
}

func TestBootstrap_Counts(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, bindings := sm.ShowDecayProfiles()
	assert.Len(t, bundles, 5, "expected 5 decay profile bundles")
	assert.Len(t, bindings, 8, "expected 8 decay profile bindings")

	promoProfiles := sm.ShowPromotionProfiles()
	assert.Len(t, promoProfiles, 6, "expected 6 promotion profiles")

	promoPolicies := sm.ShowPromotionPolicies()
	assert.Len(t, promoPolicies, 3, "expected 3 promotion policies")
}

func TestBootstrap_KnowledgeFactBundle(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, _ := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBundle
	for i := range bundles {
		if bundles[i].Name == "knowledge_fact_retention" {
			b = &bundles[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.False(t, b.DecayEnabled)
	assert.Equal(t, 0.0, b.VisibilityThreshold)
	assert.Equal(t, knowledgepolicy.DecayFunctionNone, b.Function)
	assert.Equal(t, knowledgepolicy.ScoreFromCreated, b.ScoreFrom)
}

func TestBootstrap_MemoryEpisodeBundle(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, _ := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBundle
	for i := range bundles {
		if bundles[i].Name == "memory_episode_retention" {
			b = &bundles[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, int64(604800), b.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.DecayFunctionExponential, b.Function)
	assert.Equal(t, 0.10, b.VisibilityThreshold)
	assert.Equal(t, knowledgepolicy.ScoreFromVersion, b.ScoreFrom)
	assert.True(t, b.Enabled)
}

func TestBootstrap_SessionSummaryBundle(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, _ := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBundle
	for i := range bundles {
		if bundles[i].Name == "session_summary" {
			b = &bundles[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, int64(1209600), b.HalfLifeSeconds)
	assert.Equal(t, 0.10, b.ScoreFloor)
}

func TestBootstrap_WisdomBundle(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, _ := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBundle
	for i := range bundles {
		if bundles[i].Name == "wisdom_directive_retention" {
			b = &bundles[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.False(t, b.DecayEnabled)
	assert.Equal(t, knowledgepolicy.DecayFunctionNone, b.Function)
}

func TestBootstrap_EvidenceDecayBundle(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	bundles, _ := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBundle
	for i := range bundles {
		if bundles[i].Name == "evidence_decay" {
			b = &bundles[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, int64(2592000), b.HalfLifeSeconds)
	assert.Equal(t, knowledgepolicy.ScoreFromCreated, b.ScoreFrom)
}

func TestBootstrap_MemoryEpisodeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "memory_episode_retention_binding" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, []string{"MemoryEpisode"}, b.TargetLabels)
	assert.False(t, b.IsEdge)
	assert.Equal(t, "memory_episode_retention", b.ProfileRef)
	require.NotNil(t, b.VisibilityThreshold)
	assert.Equal(t, 0.10, *b.VisibilityThreshold)
	assert.False(t, b.NoDecay)

	require.Len(t, b.PropertyRules, 9)

	noDecayProps := []string{"tenantId", "agentId", "sessionId", "system_created_at",
		"system_expired_at", "valid_from", "valid_to"}
	for i, name := range noDecayProps {
		assert.Equal(t, name, b.PropertyRules[i].PropertyPath)
		assert.True(t, b.PropertyRules[i].NoDecay, "%s should be NoDecay", name)
	}

	// summary: merged DECAY HALF LIFE + DECAY FLOOR
	assert.Equal(t, "summary", b.PropertyRules[7].PropertyPath)
	assert.Equal(t, int64(1209600), b.PropertyRules[7].HalfLifeSeconds)
	assert.Equal(t, 0.10, b.PropertyRules[7].ScoreFloor)
	assert.False(t, b.PropertyRules[7].NoDecay)

	// ephemeralContext: 1-day half-life
	assert.Equal(t, "ephemeralContext", b.PropertyRules[8].PropertyPath)
	assert.Equal(t, int64(86400), b.PropertyRules[8].HalfLifeSeconds)
	assert.False(t, b.PropertyRules[8].NoDecay)
}

func TestBootstrap_KnowledgeFactBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "knowledge_fact_retention_binding" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, []string{"KnowledgeFact"}, b.TargetLabels)
	assert.Equal(t, "knowledge_fact_retention", b.ProfileRef)
	assert.Len(t, b.PropertyRules, 0, "no-decay node should not have redundant property rules")
}

func TestBootstrap_WisdomDirectiveBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "wisdom_directive_retention_binding" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.Equal(t, []string{"WisdomDirective"}, b.TargetLabels)
	assert.Equal(t, "wisdom_directive_retention", b.ProfileRef)
	assert.Len(t, b.PropertyRules, 0, "no-decay node should not have redundant property rules")
}

func TestBootstrap_EvidenceEdgeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "evidence_edge_retention_binding" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.True(t, b.IsEdge)
	assert.Equal(t, "EVIDENCES", b.TargetEdgeType)
	assert.Equal(t, "evidence_decay", b.ProfileRef)
	assert.False(t, b.NoDecay)
	require.NotNil(t, b.VisibilityThreshold)
	assert.Equal(t, 0.10, *b.VisibilityThreshold)

	require.Len(t, b.PropertyRules, 1)
	assert.Equal(t, "sourceId", b.PropertyRules[0].PropertyPath)
	assert.True(t, b.PropertyRules[0].NoDecay)
}

func TestBootstrap_SupersessionEdgeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "supersession_edge_retention" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.True(t, b.IsEdge)
	assert.Equal(t, "SUPERSEDES", b.TargetEdgeType)
	assert.True(t, b.NoDecay)
	assert.Len(t, b.PropertyRules, 0, "no-decay edge should not have redundant property rules")
}

func TestBootstrap_ConsolidationEdgeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "consolidation_edge_retention" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.True(t, b.IsEdge)
	assert.Equal(t, "CONSOLIDATES_TO", b.TargetEdgeType)
	assert.True(t, b.NoDecay)
	assert.Len(t, b.PropertyRules, 0, "no-decay edge should not have redundant property rules")
}

func TestBootstrap_RevisionEdgeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "revision_edge_retention" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.True(t, b.IsEdge)
	assert.Equal(t, "REVISES", b.TargetEdgeType)
	assert.True(t, b.NoDecay)
	assert.Len(t, b.PropertyRules, 0, "no-decay edge should not have redundant property rules")
}

func TestBootstrap_DerivationEdgeBinding(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	_, bindings := sm.ShowDecayProfiles()
	var b *knowledgepolicy.DecayProfileBinding
	for i := range bindings {
		if bindings[i].Name == "derivation_edge_retention" {
			b = &bindings[i]
			break
		}
	}
	require.NotNil(t, b)
	assert.True(t, b.IsEdge)
	assert.Equal(t, "DERIVED_FROM", b.TargetEdgeType)
	assert.True(t, b.NoDecay)
	assert.Len(t, b.PropertyRules, 0, "no-decay edge should not have redundant property rules")
}

func TestBootstrap_PromotionProfiles(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	profiles := sm.ShowPromotionProfiles()
	pMap := make(map[string]*knowledgepolicy.PromotionProfileDef)
	for i := range profiles {
		pMap[profiles[i].Name] = &profiles[i]
	}

	mr := pMap["memory_reinforced"]
	require.NotNil(t, mr)
	assert.Equal(t, 1.25, mr.Multiplier)
	assert.Equal(t, 0.0, mr.ScoreFloor)
	assert.Equal(t, 1.0, mr.ScoreCap)

	cc := pMap["consolidation_candidate"]
	require.NotNil(t, cc)
	assert.Equal(t, 1.50, cc.Multiplier)
	assert.Equal(t, 0.80, cc.ScoreFloor)
	assert.Equal(t, 1.0, cc.ScoreCap)

	wp := pMap["wisdom_provisional"]
	require.NotNil(t, wp)
	assert.Equal(t, 1.0, wp.Multiplier)
	assert.Equal(t, 0.0, wp.ScoreFloor)

	we := pMap["wisdom_established"]
	require.NotNil(t, we)
	assert.Equal(t, 0.50, we.ScoreFloor)

	wc := pMap["wisdom_canonical"]
	require.NotNil(t, wc)
	assert.Equal(t, 0.90, wc.ScoreFloor)

	re := pMap["reinforced_evidence"]
	require.NotNil(t, re)
	assert.Equal(t, 1.20, re.Multiplier)
	assert.Equal(t, 0.0, re.ScoreFloor)
}

func TestBootstrap_MemoryConsolidationPolicy(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	policies := sm.ShowPromotionPolicies()
	var pol *knowledgepolicy.PromotionPolicyDef
	for i := range policies {
		if policies[i].Name == "memory_episode_consolidation" {
			pol = &policies[i]
			break
		}
	}
	require.NotNil(t, pol)
	assert.Equal(t, []string{"MemoryEpisode"}, pol.TargetLabels)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 5)

	assert.Contains(t, pol.OnAccess.Mutations[0].Expression, "accessCount")
	assert.Nil(t, pol.OnAccess.Mutations[0].Kalman)

	assert.Contains(t, pol.OnAccess.Mutations[1].Expression, "lastAccessedAt")
	assert.Nil(t, pol.OnAccess.Mutations[1].Kalman)

	assert.Contains(t, pol.OnAccess.Mutations[2].Expression, "accessIntervals")
	assert.Nil(t, pol.OnAccess.Mutations[2].Kalman)

	require.NotNil(t, pol.OnAccess.Mutations[3].Kalman)
	assert.Equal(t, knowledgepolicy.KalmanModeAuto, pol.OnAccess.Mutations[3].Kalman.Mode)
	assert.Contains(t, pol.OnAccess.Mutations[3].Expression, "crossSessionAccessRate")

	assert.Contains(t, pol.OnAccess.Mutations[4].Expression, "$_session")
	assert.Nil(t, pol.OnAccess.Mutations[4].Kalman)

	require.Len(t, pol.WhenClauses, 2)
	assert.Equal(t, "n.accessCount >= 3", pol.WhenClauses[0].Predicate)
	assert.Equal(t, "memory_reinforced", pol.WhenClauses[0].ProfileRef)

	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.accessCount >= 5")
	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.sourceAgreement >= 0.80")
	assert.Equal(t, "consolidation_candidate", pol.WhenClauses[1].ProfileRef)
}

func TestBootstrap_WisdomStabilityPolicy(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	policies := sm.ShowPromotionPolicies()
	var pol *knowledgepolicy.PromotionPolicyDef
	for i := range policies {
		if policies[i].Name == "wisdom_directive_stability" {
			pol = &policies[i]
			break
		}
	}
	require.NotNil(t, pol)
	assert.Equal(t, []string{"WisdomDirective"}, pol.TargetLabels)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 2)

	assert.Contains(t, pol.OnAccess.Mutations[0].Expression, "evaluationCount")
	assert.Contains(t, pol.OnAccess.Mutations[1].Expression, "lastEvaluatedAt")

	require.Len(t, pol.WhenClauses, 3)
	assert.Equal(t, "n.evidenceCount < 3", pol.WhenClauses[0].Predicate)
	assert.Equal(t, "wisdom_provisional", pol.WhenClauses[0].ProfileRef)

	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.evidenceCount >= 3")
	assert.Contains(t, pol.WhenClauses[1].Predicate, "n.contradictionRate < 0.20")
	assert.Equal(t, "wisdom_established", pol.WhenClauses[1].ProfileRef)

	assert.Contains(t, pol.WhenClauses[2].Predicate, "n.evidenceCount >= 10")
	assert.Contains(t, pol.WhenClauses[2].Predicate, "n.contradictionRate < 0.05")
	assert.Contains(t, pol.WhenClauses[2].Predicate, "n.crossSessionSupport >= 3")
	assert.Equal(t, "wisdom_canonical", pol.WhenClauses[2].ProfileRef)
}

func TestBootstrap_EvidenceTraversalPolicy(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	policies := sm.ShowPromotionPolicies()
	var pol *knowledgepolicy.PromotionPolicyDef
	for i := range policies {
		if policies[i].Name == "evidence_traversal_tiering" {
			pol = &policies[i]
			break
		}
	}
	require.NotNil(t, pol)
	assert.True(t, pol.IsEdge)
	assert.Equal(t, "EVIDENCES", pol.TargetEdgeType)
	assert.True(t, pol.Enabled)

	require.NotNil(t, pol.OnAccess)
	require.Len(t, pol.OnAccess.Mutations, 2)
	assert.Contains(t, pol.OnAccess.Mutations[0].Expression, "traversalCount")
	assert.Contains(t, pol.OnAccess.Mutations[1].Expression, "lastTraversedAt")

	require.Len(t, pol.WhenClauses, 1)
	assert.Equal(t, "r.traversalCount >= 5", pol.WhenClauses[0].Predicate)
	assert.Equal(t, "reinforced_evidence", pol.WhenClauses[0].ProfileRef)
}

func TestBootstrap_PersistenceRoundTrip(t *testing.T) {
	sm := storage.NewSchemaManager()
	for _, stmt := range allBootstrapDDL() {
		bootstrapParseDDLAndApply(t, sm, stmt)
	}

	def := sm.ExportDefinition()
	require.NotNil(t, def)

	sm2 := storage.NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(def))

	bundles2, bindings2 := sm2.ShowDecayProfiles()
	assert.Len(t, bundles2, 5)
	assert.Len(t, bindings2, 8)

	profiles2 := sm2.ShowPromotionProfiles()
	assert.Len(t, profiles2, 6)

	policies2 := sm2.ShowPromotionPolicies()
	assert.Len(t, policies2, 3)

	// Verify deep field survives round-trip
	var memBind *knowledgepolicy.DecayProfileBinding
	for i := range bindings2 {
		if bindings2[i].Name == "memory_episode_retention_binding" {
			memBind = &bindings2[i]
			break
		}
	}
	require.NotNil(t, memBind)
	assert.Equal(t, "memory_episode_retention", memBind.ProfileRef)
	require.Len(t, memBind.PropertyRules, 9)

	// summary has merged DECAY HALF LIFE + DECAY FLOOR
	assert.Equal(t, "summary", memBind.PropertyRules[7].PropertyPath)
	assert.Equal(t, int64(1209600), memBind.PropertyRules[7].HalfLifeSeconds)
	assert.Equal(t, 0.10, memBind.PropertyRules[7].ScoreFloor)

	// Verify consolidation policy survives
	var consolPol *knowledgepolicy.PromotionPolicyDef
	for i := range policies2 {
		if policies2[i].Name == "memory_episode_consolidation" {
			consolPol = &policies2[i]
			break
		}
	}
	require.NotNil(t, consolPol)
	require.Len(t, consolPol.OnAccess.Mutations, 5)
	require.Len(t, consolPol.WhenClauses, 2)
	assert.Equal(t, "consolidation_candidate", consolPol.WhenClauses[1].ProfileRef)

	// Verify evidence traversal policy survives
	var evidPol *knowledgepolicy.PromotionPolicyDef
	for i := range policies2 {
		if policies2[i].Name == "evidence_traversal_tiering" {
			evidPol = &policies2[i]
			break
		}
	}
	require.NotNil(t, evidPol)
	assert.True(t, evidPol.IsEdge)
	require.Len(t, evidPol.WhenClauses, 1)
	assert.Equal(t, "reinforced_evidence", evidPol.WhenClauses[0].ProfileRef)
}
