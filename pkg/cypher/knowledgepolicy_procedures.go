package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) callNornicDbKnowledgePolicyProfiles() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager unavailable")
	}

	bundles, bindings := schema.ShowDecayProfiles()
	rows := make([][]interface{}, 0, len(bundles)+len(bindings))
	for _, bundle := range bundles {
		rows = append(rows, []interface{}{
			"bundle",
			bundle.Name,
			bundle.HalfLifeSeconds,
			bundle.VisibilityThreshold,
			bundle.ScoreFloor,
			string(bundle.Function),
			string(bundle.Scope),
			bundle.DecayEnabled,
			string(bundle.ScoreFrom),
			bundle.ScoreFromProperty,
			bundle.Enabled,
			nil,
			"",
			false,
			false,
			"",
			false,
			0,
		})
	}
	for _, binding := range bindings {
		var visibilityThreshold interface{}
		if binding.VisibilityThreshold != nil {
			visibilityThreshold = *binding.VisibilityThreshold
		}
		rows = append(rows, []interface{}{
			"binding",
			binding.Name,
			binding.HalfLifeSeconds,
			visibilityThreshold,
			binding.ScoreFloor,
			"",
			bindingScope(binding),
			!binding.NoDecay,
			"",
			"",
			true,
			binding.TargetLabels,
			binding.TargetEdgeType,
			binding.IsWildcard,
			binding.IsEdge,
			binding.ProfileRef,
			binding.NoDecay,
			binding.Order,
		})
	}

	return &ExecuteResult{
		Columns: []string{"kind", "Name", "HalfLifeSeconds", "VisibilityThreshold", "ScoreFloor", "Function", "Scope", "DecayEnabled", "ScoreFrom", "ScoreFromProperty", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "ProfileRef", "NoDecay", "Order"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyPolicies() (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager unavailable")
	}

	profiles := schema.ShowPromotionProfiles()
	policies := schema.ShowPromotionPolicies()
	rows := make([][]interface{}, 0, len(profiles)+len(policies))
	for _, profile := range profiles {
		rows = append(rows, []interface{}{
			"profile",
			profile.Name,
			string(profile.Scope),
			profile.Multiplier,
			profile.ScoreFloor,
			profile.ScoreCap,
			profile.Enabled,
			nil,
			"",
			false,
			false,
		})
	}
	for _, policy := range policies {
		rows = append(rows, []interface{}{
			"policy",
			policy.Name,
			promotionPolicyScope(policy),
			nil,
			nil,
			nil,
			policy.Enabled,
			policy.TargetLabels,
			policy.TargetEdgeType,
			policy.IsWildcard,
			policy.IsEdge,
		})
	}

	return &ExecuteResult{
		Columns: []string{"kind", "Name", "Scope", "Multiplier", "ScoreFloor", "ScoreCap", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyResolve(args []interface{}) (*ExecuteResult, error) {
	entityID, err := optionalStringArg(args, 0)
	if err != nil {
		return nil, err
	}
	labelsCSV, err := optionalStringArg(args, 1)
	if err != nil {
		return nil, err
	}
	edgeType, err := optionalStringArg(args, 2)
	if err != nil {
		return nil, err
	}
	if entityID == "" && labelsCSV == "" && edgeType == "" {
		return nil, fmt.Errorf("nornicdb.knowledgepolicy.resolve requires entityId, labels, or edgeType")
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager unavailable")
	}
	bt := schema.GetBindingTable()
	if bt == nil {
		return nil, fmt.Errorf("knowledge policy binding table unavailable")
	}

	decayEnabled := false
	if be := unwrapBadgerEngine(e.storage); be != nil {
		decayEnabled = be.IsDecayEnabled()
	}
	resolver := knowledgepolicy.NewResolver(bt, nil)
	scorer := knowledgepolicy.NewScorer(resolver, decayEnabled)
	nowNanos := storage.DecayScoringTime()

	var resolution knowledgepolicy.ScoringResolution
	if entityID != "" {
		if node, nodeErr := e.storage.GetNode(storage.NodeID(entityID)); nodeErr == nil && node != nil {
			createdNanos := node.CreatedAt.UnixNano()
			versionNanos := createdNanos
			if !node.UpdatedAt.IsZero() {
				versionNanos = node.UpdatedAt.UnixNano()
			}
			resolution = scorer.ScoreNode(entityID, node.Labels, loadAccessMeta(e.storage, entityID), createdNanos, versionNanos, nowNanos)
		} else if edge, edgeErr := e.storage.GetEdge(storage.EdgeID(entityID)); edgeErr == nil && edge != nil {
			createdNanos := edge.CreatedAt.UnixNano()
			resolution = scorer.ScoreEdge(entityID, edge.Type, loadAccessMeta(e.storage, entityID), createdNanos, createdNanos, nowNanos)
		} else {
			return nil, fmt.Errorf("entity not found: %s", entityID)
		}
	} else if edgeType != "" {
		resolution = scorer.ScoreEdge("dry-run", edgeType, nil, nowNanos, nowNanos, nowNanos)
	} else {
		labels := splitCSVLabels(labelsCSV)
		resolution = scorer.ScoreNode("dry-run", labels, nil, nowNanos, nowNanos, nowNanos)
	}

	return &ExecuteResult{
		Columns: []string{"TargetID", "TargetScope", "ResolvedDecayProfileID", "ResolvedScoreFrom", "ResolutionSourceChain", "AppliedDecayProfileNames", "AppliedPromotionPolicyName", "AppliedPromotionProfileName", "EffectiveRate", "EffectiveThreshold", "EffectiveMultiplier", "BaseScore", "FinalScore", "NoDecay", "SuppressionEligible", "Explanation"},
		Rows: [][]interface{}{{
			resolution.TargetID,
			string(resolution.TargetScope),
			resolution.ResolvedDecayProfileID,
			string(resolution.ResolvedScoreFrom),
			resolution.ResolutionSourceChain,
			resolution.AppliedDecayProfileNames,
			resolution.AppliedPromotionPolicyName,
			resolution.AppliedPromotionProfileName,
			resolution.EffectiveRate,
			resolution.EffectiveThreshold,
			resolution.EffectiveMultiplier,
			resolution.BaseScore,
			resolution.FinalScore,
			resolution.NoDecay,
			resolution.SuppressionEligible,
			resolution.Explanation,
		}},
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyDeindexStatus() (*ExecuteResult, error) {
	columns := []string{"pending_count", "supported", "message", "workItemId", "targetId", "targetScope", "enqueuedAt", "status"}
	be := unwrapBadgerEngine(e.storage)
	if be == nil {
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{{0, false, "deindex status requires BadgerDB storage backend", "", "", "", nil, ""}},
		}, nil
	}

	items, err := be.ScanPendingDeindexWorkItems()
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{{0, true, "", "", "", "", nil, ""}},
		}, nil
	}

	rows := make([][]interface{}, 0, len(items))
	for _, item := range items {
		rows = append(rows, []interface{}{
			len(items),
			true,
			"",
			item.WorkItemID,
			item.TargetID,
			item.TargetScope,
			item.EnqueuedAt,
			item.Status,
		})
	}
	return &ExecuteResult{Columns: columns, Rows: rows}, nil
}

func optionalStringArg(args []interface{}, idx int) (string, error) {
	if idx >= len(args) || args[idx] == nil {
		return "", nil
	}
	s, ok := args[idx].(string)
	if !ok {
		return "", fmt.Errorf("argument %d must be a string", idx+1)
	}
	return strings.TrimSpace(s), nil
}

func splitCSVLabels(labelsCSV string) []string {
	parts := strings.Split(labelsCSV, ",")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			labels = append(labels, trimmed)
		}
	}
	return labels
}

func loadAccessMeta(eng storage.Engine, entityID string) *knowledgepolicy.AccessMetaEntry {
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return nil
	}
	meta, err := be.GetAccessMeta(entityID)
	if err != nil {
		return nil
	}
	return meta
}
