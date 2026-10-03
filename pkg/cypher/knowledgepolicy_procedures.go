package cypher

import (
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) knowledgePolicySchema() (*storage.SchemaManager, error) {
	var transaction *storage.BadgerTransaction
	if wrapper, ok := e.storage.(*transactionStorageWrapper); ok {
		transaction = wrapper.tx
	} else if e.txContext != nil && e.txContext.active {
		transaction, _ = e.txContext.tx.(*storage.BadgerTransaction)
	}
	if transaction == nil {
		return e.storage.GetSchema(), nil
	}
	if err := transaction.SetNamespace(e.currentDatabaseName()); err != nil {
		return nil, err
	}
	return transaction.KnowledgePolicySchema()
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyProfiles() (*ExecuteResult, error) {
	schema, err := e.knowledgePolicySchema()
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
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
			"",
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
			formatDecayBindingApply(binding),
		})
	}

	return &ExecuteResult{
		Columns: []string{"kind", "Name", "HalfLifeSeconds", "VisibilityThreshold", "ScoreFloor", "Function", "Scope", "DecayEnabled", "ScoreFrom", "ScoreFromProperty", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "ProfileRef", "NoDecay", "Order", "Apply"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyPolicies() (*ExecuteResult, error) {
	schema, err := e.knowledgePolicySchema()
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
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
			"",
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
			formatPromotionPolicyApply(policy),
		})
	}

	return &ExecuteResult{
		Columns: []string{"kind", "Name", "Scope", "Multiplier", "ScoreFloor", "ScoreCap", "Enabled", "TargetLabels", "TargetEdgeType", "IsWildcard", "IsEdge", "Apply"},
		Rows:    rows,
	}, nil
}

func formatDecayBindingApply(binding knowledgepolicy.DecayProfileBinding) string {
	lines := make([]string, 0, 5+len(binding.PropertyRules))
	if binding.ProfileRef != "" {
		lines = append(lines, "DECAY PROFILE "+quoteKnowledgePolicyName(binding.ProfileRef))
	}
	if binding.NoDecay {
		lines = append(lines, "NO DECAY")
	}
	if binding.HalfLifeSeconds != 0 {
		lines = append(lines, "DECAY HALF LIFE "+strconv.FormatInt(binding.HalfLifeSeconds, 10))
	}
	if binding.VisibilityThreshold != nil {
		lines = append(lines, "DECAY VISIBILITY THRESHOLD "+formatKnowledgePolicyFloat(*binding.VisibilityThreshold))
	}
	if binding.ScoreFloor != 0 {
		lines = append(lines, "DECAY FLOOR "+formatKnowledgePolicyFloat(binding.ScoreFloor))
	}

	variable := "n"
	if binding.IsEdge {
		variable = "r"
	}
	for _, rule := range binding.PropertyRules {
		prefix := variable + "." + rule.PropertyPath
		if rule.NoDecay {
			lines = append(lines, prefix+" NO DECAY")
		}
		if rule.ProfileRef != "" {
			lines = append(lines, prefix+" DECAY PROFILE "+quoteKnowledgePolicyName(rule.ProfileRef))
		}
		if rule.HalfLifeSeconds != 0 {
			lines = append(lines, prefix+" DECAY HALF LIFE "+strconv.FormatInt(rule.HalfLifeSeconds, 10))
		}
		if rule.ScoreFloor != 0 {
			lines = append(lines, prefix+" DECAY FLOOR "+formatKnowledgePolicyFloat(rule.ScoreFloor))
		}
	}
	return strings.Join(lines, "\n")
}

func formatPromotionPolicyApply(policy knowledgepolicy.PromotionPolicyDef) string {
	lines := make([]string, 0, len(policy.WhenClauses)+1)
	if policy.OnAccess != nil && len(policy.OnAccess.Mutations) > 0 {
		mutationLines := make([]string, 0, len(policy.OnAccess.Mutations))
		for _, mutation := range policy.OnAccess.Mutations {
			prefix := ""
			if mutation.Kalman != nil {
				config := []string{
					"q: " + formatKnowledgePolicyFloat(mutation.Kalman.Q),
					"varianceScale: " + formatKnowledgePolicyFloat(mutation.Kalman.VarianceScale),
					"windowSize: " + strconv.Itoa(mutation.Kalman.WindowSize),
				}
				if mutation.Kalman.Mode == knowledgepolicy.KalmanModeManual {
					config = append(config, "r: "+formatKnowledgePolicyFloat(mutation.Kalman.R))
				}
				prefix = "WITH KALMAN { " + strings.Join(config, ", ") + " } "
			}
			mutationLines = append(mutationLines, "  "+prefix+"SET "+mutation.Expression)
		}
		lines = append(lines, "ON ACCESS {\n"+strings.Join(mutationLines, "\n")+"\n}")
	}
	for _, clause := range policy.WhenClauses {
		lines = append(lines, "WHEN "+clause.Predicate+" APPLY PROFILE "+quoteKnowledgePolicyName(clause.ProfileRef))
	}
	return strings.Join(lines, "\n")
}

func quoteKnowledgePolicyName(name string) string {
	return "'" + strings.ReplaceAll(name, "'", "\\'") + "'"
}

func formatKnowledgePolicyFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

type knowledgePolicyEntityKind uint8

const (
	knowledgePolicyEntityAny knowledgePolicyEntityKind = iota
	knowledgePolicyEntityNode
	knowledgePolicyEntityEdge
)

func parseKnowledgePolicyEntityID(entityID string) (string, knowledgePolicyEntityKind) {
	parts := strings.SplitN(strings.TrimSpace(entityID), ":", 3)
	if len(parts) != 3 {
		return entityID, knowledgePolicyEntityAny
	}
	switch parts[0] {
	case "4":
		return parts[2], knowledgePolicyEntityNode
	case "5":
		return parts[2], knowledgePolicyEntityEdge
	default:
		return entityID, knowledgePolicyEntityAny
	}
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
		return nil, localizedError(localization.CypherKnowledgePolicyResolveTargetRequired(), nil)
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
	}
	bt := schema.GetBindingTable()
	if bt == nil {
		return nil, localizedError(localization.CypherKnowledgePolicyBindingTableUnavailable(), nil)
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
		lookupID, entityKind := parseKnowledgePolicyEntityID(entityID)
		if node, nodeErr := e.storage.GetNode(storage.NodeID(lookupID)); entityKind != knowledgePolicyEntityEdge && nodeErr == nil && node != nil {
			createdNanos := node.CreatedAt.UnixNano()
			versionNanos := createdNanos
			if !node.UpdatedAt.IsZero() {
				versionNanos = node.UpdatedAt.UnixNano()
			}
			resolution = scorer.ScoreNode(entityID, node.Labels, loadAccessMeta(e.storage, lookupID), createdNanos, versionNanos, nowNanos)
		} else if edge, edgeErr := e.storage.GetEdge(storage.EdgeID(lookupID)); entityKind != knowledgePolicyEntityNode && edgeErr == nil && edge != nil {
			createdNanos := edge.CreatedAt.UnixNano()
			resolution = scorer.ScoreEdge(entityID, edge.Type, loadAccessMeta(e.storage, lookupID), createdNanos, createdNanos, nowNanos)
		} else {
			return nil, localizedError(localization.CypherKnowledgePolicyEntityNotFound(entityID), nil)
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
			Rows:    [][]interface{}{{0, false, localization.CypherKnowledgePolicyDeindexBadgerRequired().Fallback, "", "", "", nil, ""}},
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
		return "", localizedError(localization.CypherKnowledgePolicyArgumentStringRequired(idx+1), nil)
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
