package cypher

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateKnowledgePolicyFunction owns recognition, source selection and
// result projection for policy(), decay() and decayScore().
func (e *StorageExecutor) evaluateKnowledgePolicyFunction(ctx context.Context, expr, lowerExpr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, bool) {
	handled := false
	for _, name := range []string{"decayScore", "decayscore", "decay", "policy"} {
		if matchFuncStartAndSuffix(expr, name) {
			handled = true
			break
		}
	}
	if !handled {
		return nil, false
	}

	originalName, _, _ := strings.Cut(expr, "(")
	name := strings.ToLower(strings.TrimSpace(originalName))
	args := e.splitFunctionArgs(extractFuncArgs(expr, strings.TrimSpace(originalName)))
	disabled := func(reason string) interface{} {
		if name == "policy" {
			return minimalPolicyMap("")
		}
		if name == "decayscore" {
			return 1.0
		}
		return decayDisabledMap(reason)
	}
	if len(args) == 0 {
		return disabled(localization.CypherKnowledgePolicyReasonNoEntityArgument().Fallback), true
	}
	ent, ok := e.resolveEntityForDecay(ctx, args[0], nodes, rels)
	if !ok {
		return disabled(localization.CypherKnowledgePolicyReasonEntityNotFound().Fallback), true
	}
	be, now, available := e.getDecayContext(ctx, ent.engine)
	var meta *knowledgepolicy.AccessMetaEntry
	if available {
		result, _ := be.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: ent.entityID})
		meta = result.Entry
	}
	if name == "policy" {
		if meta != nil {
			return accessMetaToMap(meta), true
		}
		scope := knowledgepolicy.ScopeNode
		if ent.isEdge {
			scope = knowledgepolicy.ScopeEdge
		}
		return map[string]interface{}{"targetId": ent.entityID, "targetScope": string(scope)}, true
	}
	if !available {
		return disabled(localization.CypherKnowledgePolicyReasonNoBadgerEngine().Fallback), true
	}
	if !be.IsDecayEnabled() {
		e.logDecayMismatchOnce()
		return disabled(localization.CypherKnowledgePolicyReasonDecayDisabled().Fallback), true
	}
	scorer := be.ScorerForNamespace(storage.ExtractNamespaceFromID(ent.entityID))
	if scorer == nil {
		return disabled(localization.CypherKnowledgePolicyReasonNoDecayProfile().Fallback), true
	}
	property := ""
	if len(args) >= 2 {
		var err error
		property, err = validateDecayOptions(ctx, args[1], nodes, rels, e)
		if err != nil {
			return nil, true
		}
	}
	resolution := ent.score(scorer, meta, property, now)
	if name == "decayscore" {
		return resolution.FinalScore, true
	}
	return resolutionToMap(resolution), true
}

type resolvedEntity struct {
	entityID       string
	engine         storage.Engine
	labels         []string
	createdAtNanos int64
	versionAtNanos int64
	isEdge         bool
	edgeType       string
}

func (e *StorageExecutor) resolveEntityForDecay(
	ctx context.Context,
	argExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) (resolvedEntity, bool) {
	arg := strings.TrimSpace(argExpr)
	if node, ok := nodes[arg]; ok {
		engine, _, err := e.knowledgePolicyContext(node.RuntimeMetadata().Database)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return resolvedEntity{}, false
		}
		return resolvedEntity{
			engine:         engine,
			entityID:       storage.QualifiedEntityID(engine, string(node.ID)),
			labels:         node.Labels,
			createdAtNanos: node.CreatedAt.UnixNano(),
			versionAtNanos: node.UpdatedAt.UnixNano(),
		}, true
	}
	if rel, ok := rels[arg]; ok {
		engine, _, err := e.knowledgePolicyContext(rel.Database)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return resolvedEntity{}, false
		}
		return resolvedEntity{
			engine:         engine,
			entityID:       storage.QualifiedEntityID(engine, string(rel.ID)),
			isEdge:         true,
			edgeType:       rel.Type,
			createdAtNanos: rel.CreatedAt.UnixNano(),
			versionAtNanos: rel.UpdatedAt.UnixNano(),
		}, true
	}
	return resolvedEntity{}, false
}

// score keeps entity-kind and property dispatch identical for decay() and
// decayScore(). Relationship property bindings resolve by type, not labels.
func (ent resolvedEntity) score(scorer *knowledgepolicy.Scorer, accessMeta *knowledgepolicy.AccessMetaEntry, property string, nowNanos int64) knowledgepolicy.ScoringResolution {
	if property != "" {
		if ent.isEdge {
			return scorer.ScoreEdgeProperty(ent.entityID, ent.edgeType, property, accessMeta, ent.createdAtNanos, ent.versionAtNanos, nowNanos)
		}
		return scorer.ScoreProperty(ent.entityID, ent.labels, property, accessMeta, ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	}
	if ent.isEdge {
		return scorer.ScoreEdge(ent.entityID, ent.edgeType, accessMeta, ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	}
	return scorer.ScoreNode(ent.entityID, ent.labels, accessMeta, ent.createdAtNanos, ent.versionAtNanos, nowNanos)
}

// getDecayContext returns the Badger engine that scores decay and the scoring
// time: the statement's clock (the instant datetime.statement() reads), so
// every decay function of one statement scores at one instant (#866). Outside
// a statement it is the current time.
func (e *StorageExecutor) getDecayContext(ctx context.Context, engine storage.Engine) (be *storage.BadgerEngine, nowNanos int64, ok bool) {
	be = unwrapBadgerEngine(engine)
	if be == nil {
		return nil, 0, false
	}
	return be, e.temporalClockTime(ctx, "statement").UnixNano(), true
}

func (e *StorageExecutor) logDecayMismatchOnce() {
	if e.decayMismatchLogged {
		return
	}
	e.decayMismatchLogged = true
	e.logEvent(slog.LevelWarn, localization.CypherDecaySubsystemDisabledEvent())
}

func decayDisabledMap(reason string) map[string]interface{} {
	return map[string]interface{}{
		"score":   1.0,
		"applies": false,
		"reason":  reason,
	}
}

func resolutionToMap(res knowledgepolicy.ScoringResolution) map[string]interface{} {
	applies := !res.NoDecay && res.ResolvedDecayProfileID != ""
	reason := ""
	if res.NoDecay {
		reason = localization.CypherKnowledgePolicyReasonNoDecay().Fallback
	} else if res.ResolvedDecayProfileID == "" {
		reason = localization.CypherKnowledgePolicyReasonNoDecayProfile().Fallback
		applies = false
	}
	return map[string]interface{}{
		"score":               res.FinalScore,
		"policy":              res.ResolvedDecayProfileID,
		"scope":               string(res.TargetScope),
		"function":            string(res.ResolvedDecayFunction),
		"visibilityThreshold": res.EffectiveThreshold,
		"floor":               res.EffectiveFloor,
		"applies":             applies,
		"reason":              reason,
		"scoreFrom":           string(res.ResolvedScoreFrom),
	}
}

func accessMetaToMap(meta *knowledgepolicy.AccessMetaEntry) map[string]interface{} {
	return map[string]interface{}{
		"targetId":       meta.TargetID,
		"targetScope":    string(meta.TargetScope),
		"accessCount":    meta.Fixed.AccessCount,
		"traversalCount": meta.Fixed.TraversalCount,
		"lastAccessedAt": meta.Fixed.LastAccessedAt,
		"lastMutatedAt":  meta.LastMutatedAt,
		"mutationCount":  meta.MutationCount,
	}
}

func minimalPolicyMap(entityID string) map[string]interface{} {
	return map[string]interface{}{
		"targetId":    entityID,
		"targetScope": string(knowledgepolicy.ScopeNode),
	}
}

// validateDecayOptions parses the second argument to decayScore/decay as a
// map literal and extracts the "property" key. Returns an error for unknown keys.
func validateDecayOptions(
	ctx context.Context,
	optExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	e *StorageExecutor,
) (string, error) {
	val := e.evaluateExpressionWithContext(ctx, optExpr, nodes, rels)
	m, ok := val.(map[string]interface{})
	if !ok {
		return "", localizedError(localization.CypherKnowledgePolicyDecayOptionsMapRequired(fmt.Sprintf("%T", val)), nil)
	}
	var property string
	for k, v := range m {
		switch k {
		case "property":
			s, _ := v.(string)
			property = s
		default:
			return "", localizedError(localization.CypherKnowledgePolicyUnknownDecayOption(k), nil)
		}
	}
	return property, nil
}
