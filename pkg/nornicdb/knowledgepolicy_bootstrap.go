package nornicdb

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func maybeBootstrapDefaultKnowledgePolicy(base storage.Engine, namespace string) error {
	if base == nil {
		return nil
	}
	if namespace == "" {
		namespace = "nornic"
	}
	provider, ok := base.(storage.NamespaceSchemaProvider)
	if !ok {
		return nil
	}

	schema := provider.GetSchemaForNamespace(namespace)
	if schema == nil || !knowledgePolicySchemaEmpty(schema) {
		return nil
	}

	for _, stmt := range defaultKnowledgePolicyBootstrapDDL() {
		if err := applyKnowledgePolicyDDL(schema, stmt); err != nil {
			return fmt.Errorf("bootstrap default knowledge policy for namespace %q: %w", namespace, err)
		}
	}
	return nil
}

func knowledgePolicySchemaEmpty(schema *storage.SchemaManager) bool {
	bundles, bindings := schema.ShowDecayProfiles()
	profiles := schema.ShowPromotionProfiles()
	policies := schema.ShowPromotionPolicies()
	return len(bundles) == 0 && len(bindings) == 0 && len(profiles) == 0 && len(policies) == 0
}

func applyKnowledgePolicyDDL(schema *storage.SchemaManager, stmt string) error {
	cmd, ok, err := cypher.ParseKnowledgePolicyDDL(stmt)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("not a knowledge-policy DDL statement")
	}

	switch c := cmd.(type) {
	case *cypher.CreateDecayProfileBundleCmd:
		return schema.CreateDecayProfileBundle(c.Bundle)
	case *cypher.CreateDecayProfileBindingCmd:
		return schema.CreateDecayProfileBinding(c.Binding)
	case *cypher.CreatePromotionProfileCmd:
		return schema.CreatePromotionProfile(c.Profile)
	case *cypher.CreatePromotionPolicyCmd:
		return schema.CreatePromotionPolicy(c.Policy)
	default:
		return fmt.Errorf("unsupported bootstrap DDL command %T", cmd)
	}
}

func defaultKnowledgePolicyBootstrapDDL() []string {
	return []string{
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
		`CREATE PROMOTION POLICY memory_episode_consolidation
FOR (n:MemoryEpisode)
ON ACCESS {
  SET n.accessCount = coalesce(n.accessCount, 0) + 1
  SET n.lastAccessedAt = timestamp()
  SET n.accessIntervals = CASE
    WHEN n.lastAccessedAt IS NULL THEN []
    ELSE coalesce(n.accessIntervals, []) + [timestamp() - n.lastAccessedAt]
  END
  SET n.crossSessionAccessRate = CASE
    WHEN n.lastSessionId IS NULL OR n.lastSessionId <> $_session THEN coalesce(n.crossSessionAccessRate, 0)
    ELSE coalesce(n.crossSessionAccessRate, 0)
  END WITH KALMAN AUTO
  SET n.lastSessionId = $_session
}
APPLY {
  WHEN n.accessCount >= 3 THEN PROFILE 'memory_reinforced'
  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.80 THEN PROFILE 'consolidation_candidate'
}`,
		`CREATE PROMOTION POLICY wisdom_directive_stability
FOR (n:WisdomDirective)
ON ACCESS {
  SET n.evaluationCount = coalesce(n.evaluationCount, 0) + 1
  SET n.lastEvaluatedAt = timestamp()
}
APPLY {
  WHEN n.evidenceCount < 3 THEN PROFILE 'wisdom_provisional'
  WHEN n.evidenceCount >= 3 AND n.contradictionRate < 0.20 THEN PROFILE 'wisdom_established'
  WHEN n.evidenceCount >= 10 AND n.contradictionRate < 0.05 AND n.crossSessionSupport >= 3 THEN PROFILE 'wisdom_canonical'
}`,
		`CREATE PROMOTION POLICY evidence_traversal_tiering
FOR ()-[r:EVIDENCES]-()
ON ACCESS {
  SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
  SET r.lastTraversedAt = timestamp()
}
APPLY {
  WHEN r.traversalCount >= 5 THEN PROFILE 'reinforced_evidence'
}`,
	}
}
