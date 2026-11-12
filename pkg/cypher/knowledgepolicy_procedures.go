package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: procedure headers are read from their registration.

import (
	"context"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// knowledgePolicyContext owns source and transactional schema selection for
// policy procedures and scalar readers alike.
func (e *StorageExecutor) knowledgePolicyContext(database string) (storage.Engine, *storage.SchemaManager, error) {
	engine := e.storage
	if source, ok := engine.(interface {
		ConstituentStorage(string) (storage.Engine, error)
	}); ok && database != "" {
		var err error
		engine, err = source.ConstituentStorage(database)
		if err != nil {
			return nil, nil, err
		}
	}
	var transaction *storage.BadgerTransaction
	if wrapper, ok := engine.(*transactionStorageWrapper); ok {
		transaction = wrapper.tx
	} else if database == "" && e.txContext != nil && e.txContext.active {
		transaction, _ = e.txContext.tx.(*storage.BadgerTransaction)
	}
	if transaction == nil {
		return engine, engine.GetSchema(), nil
	}
	namespace := database
	if namespace == "" {
		namespace = e.databaseName()
	}
	if err := transaction.SetNamespace(namespace); err != nil {
		return nil, nil, err
	}
	schema, err := transaction.Schema(storage.KnowledgePolicyChanges)
	return engine, schema, err
}

// The policy catalogs kp.info, kp.profiles and kp.policies acquire, the point
// reads kp.resolve performs and the queue scan kp.deindexStatus makes. Each
// record is one request the procedure actually made: Rows is the population that
// request returned - never the rows the statement builds from it, never a count
// taken from another read's output - and Hits is the one request.
const (
	// profileKindKnowledgePolicyDecayProfileCatalog is one acquisition of the
	// schema's decay profile catalog. The schema owner answers with its bundle
	// family and its binding family from one read, so the record is one request
	// whose rows are the entries both families returned.
	profileKindKnowledgePolicyDecayProfileCatalog = "KnowledgePolicyDecayProfileCatalog"
	// profileKindKnowledgePolicyPromotionProfileCatalog is one acquisition of
	// the schema's promotion profile catalog: Rows is the profile entries it
	// returned.
	profileKindKnowledgePolicyPromotionProfileCatalog = "KnowledgePolicyPromotionProfileCatalog"
	// profileKindKnowledgePolicyPromotionPolicyCatalog is one acquisition of the
	// schema's promotion policy catalog. The schema owner keeps its policies apart
	// from the profiles they select, so the request is its own record.
	profileKindKnowledgePolicyPromotionPolicyCatalog = "KnowledgePolicyPromotionPolicyCatalog"
	// profileKindKnowledgePolicyAccessMetaPoint is the point read of one entity's
	// stored access metadata: the attempt is one request, and Rows is the entry
	// when the store held one.
	profileKindKnowledgePolicyAccessMetaPoint = "KnowledgePolicyAccessMetaPoint"
	// profileKindKnowledgePolicyPendingDeindexScan is one scan of the stored
	// deindex work queue: Rows is the pending work items the scan returned, not
	// the rows this procedure lists them as and not the entries of the other
	// states the scan walked.
	profileKindKnowledgePolicyPendingDeindexScan = "KnowledgePolicyPendingDeindexScan"
)

// readKnowledgePolicyDecayProfiles records one acquisition of the schema's decay
// profile catalog before the caller projects or counts any of it. An empty
// catalog still represents the one request the procedure made.
func readKnowledgePolicyDecayProfiles(ctx context.Context, schema *storage.SchemaManager) ([]knowledgepolicy.DecayProfileBundle, []knowledgepolicy.DecayProfileBinding) {
	bundles, bindings := schema.ShowDecayProfiles()
	queryprofile.Access(ctx, profileKindKnowledgePolicyDecayProfileCatalog, "").Add(int64(len(bundles)+len(bindings)), 1)
	return bundles, bindings
}

// readKnowledgePolicyPromotionProfiles records one acquisition of the schema's
// promotion profile catalog before the caller projects or counts it.
func readKnowledgePolicyPromotionProfiles(ctx context.Context, schema *storage.SchemaManager) []knowledgepolicy.PromotionProfileDef {
	profiles := schema.ShowPromotionProfiles()
	queryprofile.Access(ctx, profileKindKnowledgePolicyPromotionProfileCatalog, "").Add(int64(len(profiles)), 1)
	return profiles
}

// readKnowledgePolicyPromotionPolicies records one acquisition of the schema's
// promotion policy catalog before the caller projects or counts it.
func readKnowledgePolicyPromotionPolicies(ctx context.Context, schema *storage.SchemaManager) []knowledgepolicy.PromotionPolicyDef {
	policies := schema.ShowPromotionPolicies()
	queryprofile.Access(ctx, profileKindKnowledgePolicyPromotionPolicyCatalog, "").Add(int64(len(policies)), 1)
	return policies
}

// readKnowledgePolicyNode is kp.resolve's point node read: it reports the one
// request where the read happens, so a node the store does not hold is an
// observed zero rather than no observation.
func (e *StorageExecutor) readKnowledgePolicyNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	node, err := e.storage.GetNode(ctx, id)
	queryprofile.EntityReadResult(queryprofile.Access(ctx, profileKindNodeByIdSeek, ""), node, err)
	return node, err
}

// readKnowledgePolicyEdge is kp.resolve's point relationship read, reported as
// its node read is.
func (e *StorageExecutor) readKnowledgePolicyEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	edge, err := e.storage.GetEdge(ctx, id)
	queryprofile.EntityReadResult(queryprofile.Access(ctx, queryprofile.KindRelationshipByIDSeek, ""), edge, err)
	return edge, err
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyProfiles(ctx context.Context) (*ExecuteResult, error) {
	_, schema, err := e.knowledgePolicyContext("")
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
	}

	bundles, bindings := readKnowledgePolicyDecayProfiles(ctx, schema)
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
		Columns: columnNames(kpProfilesColumns),
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyPolicies(ctx context.Context) (*ExecuteResult, error) {
	_, schema, err := e.knowledgePolicyContext("")
	if err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, localizedError(localization.CypherKnowledgePolicySchemaManagerUnavailable(), nil)
	}

	profiles := readKnowledgePolicyPromotionProfiles(ctx, schema)
	policies := readKnowledgePolicyPromotionPolicies(ctx, schema)
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
		Columns: columnNames(kpPoliciesColumns),
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
		return entityID, knowledgePolicyEntityNode
	case "5":
		return entityID, knowledgePolicyEntityEdge
	default:
		return entityID, knowledgePolicyEntityAny
	}
}

func (e *StorageExecutor) callNornicDbKnowledgePolicyResolve(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
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
		if entityKind != knowledgePolicyEntityAny {
			kind := "4"
			if entityKind == knowledgePolicyEntityEdge {
				kind = "5"
			}
			var found, applicable bool
			lookupID, found, applicable = e.entitySeekLookup(lookupID, kind, "elementId")
			if !found || !applicable {
				return nil, localizedError(localization.CypherKnowledgePolicyEntityNotFound(entityID), nil)
			}
		}
		if node, nodeErr := e.readKnowledgePolicyNode(ctx, storage.NodeID(lookupID)); entityKind != knowledgePolicyEntityEdge && nodeErr == nil && node != nil {
			createdNanos := node.CreatedAt.UnixNano()
			versionNanos := createdNanos
			if !node.UpdatedAt.IsZero() {
				versionNanos = node.UpdatedAt.UnixNano()
			}
			resolution = scorer.ScoreNode(entityID, node.Labels, loadAccessMeta(ctx, e.storage, lookupID), createdNanos, versionNanos, nowNanos)
		} else if edge, edgeErr := e.readKnowledgePolicyEdge(ctx, storage.EdgeID(lookupID)); entityKind != knowledgePolicyEntityNode && edgeErr == nil && edge != nil {
			createdNanos := edge.CreatedAt.UnixNano()
			versionNanos := createdNanos
			if !edge.UpdatedAt.IsZero() {
				versionNanos = edge.UpdatedAt.UnixNano()
			}
			resolution = scorer.ScoreEdge(entityID, edge.Type, loadAccessMeta(ctx, e.storage, lookupID), createdNanos, versionNanos, nowNanos)
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
		Columns: columnNames(kpResolveColumns),
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

func (e *StorageExecutor) callNornicDbKnowledgePolicyDeindexStatus(ctx context.Context) (*ExecuteResult, error) {
	columns := columnNames(kpDeindexStatusColumns)
	be := unwrapBadgerEngine(e.storage)
	if be == nil {
		// An engine that exposes no queue is no acquisition: this answer
		// reads no stored work item, so it reports none.
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{{0, false, localization.CypherKnowledgePolicyDeindexBadgerRequired().Fallback, "", "", "", nil, ""}},
		}, nil
	}

	items, err := be.ScanPendingDeindexWorkItems()
	if err != nil {
		return nil, err
	}
	queryprofile.Access(ctx, profileKindKnowledgePolicyPendingDeindexScan, "").Add(int64(len(items)), 1)
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

// loadAccessMeta reads one entity's stored access metadata for policy consumers.
// The point read is reported where it happens, so a store that holds no entry
// is an observed zero rather than no observation.
func loadAccessMeta(ctx context.Context, eng storage.Engine, entityID string) *knowledgepolicy.AccessMetaEntry {
	be := unwrapBadgerEngine(eng)
	if be == nil {
		return nil
	}
	counter := queryprofile.Access(ctx, profileKindKnowledgePolicyAccessMetaPoint, "")
	result, err := be.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: storage.QualifiedEntityID(eng, entityID)})
	queryprofile.EntityReadResult(counter, result.Entry, err)
	if err != nil {
		return nil
	}
	return result.Entry
}
