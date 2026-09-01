package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeKnowledgePolicyDDL(ctx context.Context, cypher string) (*ExecuteResult, error) {
	_ = ctx
	cmd, ok, err := ParseKnowledgePolicyDDL(cypher)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, localizedError(localization.CypherKnowledgePolicyUnsupportedCommand(cypher), nil)
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
	}

	switch c := cmd.(type) {
	case *CreateDecayProfileBundleCmd:
		return knowledgePolicySchemaResult("CREATE DECAY PROFILE", schema.CreateDecayProfileBundle(c.Bundle))
	case *CreateDecayProfileBindingCmd:
		return knowledgePolicySchemaResult("CREATE DECAY PROFILE FOR", schema.CreateDecayProfileBinding(c.Binding))
	case *AlterDecayProfileCmd:
		return knowledgePolicySchemaResult("ALTER DECAY PROFILE", schema.AlterDecayProfile(c.Name, c.Updates))
	case *DropDecayProfileCmd:
		return knowledgePolicySchemaResult("DROP DECAY PROFILE", schema.DropDecayProfile(c.Name, c.IfExists))
	case *ShowDecayProfilesCmd:
		return e.executeShowKnowledgeDecayProfiles(schema)
	case *CreatePromotionProfileCmd:
		return knowledgePolicySchemaResult("CREATE PROMOTION PROFILE", schema.CreatePromotionProfile(c.Profile))
	case *AlterPromotionProfileCmd:
		return knowledgePolicySchemaResult("ALTER PROMOTION PROFILE", schema.AlterPromotionProfile(c.Name, c.Updates))
	case *DropPromotionProfileCmd:
		return knowledgePolicySchemaResult("DROP PROMOTION PROFILE", schema.DropPromotionProfile(c.Name, c.IfExists))
	case *ShowPromotionProfilesCmd:
		return e.executeShowKnowledgePromotionProfiles(schema)
	case *CreatePromotionPolicyCmd:
		return knowledgePolicySchemaResult("CREATE PROMOTION POLICY", schema.CreatePromotionPolicy(c.Policy))
	case *AlterPromotionPolicyCmd:
		return knowledgePolicySchemaResult("ALTER PROMOTION POLICY", schema.AlterPromotionPolicy(c.Name, c.Updates))
	case *DropPromotionPolicyCmd:
		return knowledgePolicySchemaResult("DROP PROMOTION POLICY", schema.DropPromotionPolicy(c.Name, c.IfExists))
	case *ShowPromotionPoliciesCmd:
		return e.executeShowKnowledgePromotionPolicies(schema)
	default:
		return nil, localizedError(localization.CypherKnowledgePolicyUnsupportedCommandType(fmt.Sprintf("%T", cmd)), nil)
	}
}

func knowledgePolicySchemaResult(operation string, err error) (*ExecuteResult, error) {
	result := emptySchemaResult()
	if err != nil {
		return result, localizedError(localization.CypherKnowledgePolicyOperationFailed(operation, err), err)
	}
	return result, nil
}

func emptySchemaResult() *ExecuteResult {
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
}

func (e *StorageExecutor) executeShowKnowledgeDecayProfiles(schema *storage.SchemaManager) (*ExecuteResult, error) {
	bundles, bindings := schema.ShowDecayProfiles()
	rows := make([][]interface{}, 0, len(bundles)+len(bindings))
	for _, bundle := range bundles {
		rows = append(rows, []interface{}{
			"bundle",
			bundle.Name,
			string(bundle.Scope),
			"",
			"",
			bundle.Enabled,
		})
	}
	for _, binding := range bindings {
		rows = append(rows, []interface{}{
			"binding",
			binding.Name,
			bindingScope(binding),
			bindingTarget(binding),
			binding.ProfileRef,
			true,
		})
	}
	return &ExecuteResult{
		Columns: []string{"kind", "name", "scope", "target", "profileRef", "enabled"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) executeShowKnowledgePromotionProfiles(schema *storage.SchemaManager) (*ExecuteResult, error) {
	profiles := schema.ShowPromotionProfiles()
	rows := make([][]interface{}, 0, len(profiles))
	for _, profile := range profiles {
		rows = append(rows, []interface{}{
			profile.Name,
			string(profile.Scope),
			profile.Multiplier,
			profile.ScoreFloor,
			profile.ScoreCap,
			profile.Enabled,
		})
	}
	return &ExecuteResult{
		Columns: []string{"name", "scope", "multiplier", "scoreFloor", "scoreCap", "enabled"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) executeShowKnowledgePromotionPolicies(schema *storage.SchemaManager) (*ExecuteResult, error) {
	policies := schema.ShowPromotionPolicies()
	rows := make([][]interface{}, 0, len(policies))
	for _, policy := range policies {
		onAccessMutations := 0
		if policy.OnAccess != nil {
			onAccessMutations = len(policy.OnAccess.Mutations)
		}
		rows = append(rows, []interface{}{
			policy.Name,
			promotionPolicyScope(policy),
			promotionPolicyTarget(policy),
			policy.Enabled,
			len(policy.WhenClauses),
			onAccessMutations,
		})
	}
	return &ExecuteResult{
		Columns: []string{"name", "scope", "target", "enabled", "whenClauses", "onAccessMutations"},
		Rows:    rows,
	}, nil
}

func bindingScope(binding knowledgepolicy.DecayProfileBinding) string {
	if binding.IsEdge {
		return "EDGE"
	}
	return "NODE"
}

func bindingTarget(binding knowledgepolicy.DecayProfileBinding) string {
	if binding.IsWildcard {
		return "*"
	}
	if binding.IsEdge {
		return binding.TargetEdgeType
	}
	return strings.Join(binding.TargetLabels, ":")
}

func promotionPolicyScope(policy knowledgepolicy.PromotionPolicyDef) string {
	if policy.IsEdge {
		return "EDGE"
	}
	return "NODE"
}

func promotionPolicyTarget(policy knowledgepolicy.PromotionPolicyDef) string {
	if policy.IsWildcard {
		return "*"
	}
	if policy.IsEdge {
		return policy.TargetEdgeType
	}
	return strings.Join(policy.TargetLabels, ":")
}
