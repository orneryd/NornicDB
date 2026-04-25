package cypher

import (
	"fmt"
	"log"
	"strings"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateKnowledgePolicyFunction dispatches decayScore(), decay(), and policy()
// calls. Returns (result, true) if the expression was handled, (nil, false) otherwise.
func (e *StorageExecutor) evaluateKnowledgePolicyFunction(
	expr string,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) (interface{}, bool) {
	if matchFuncStartAndSuffix(expr, "decayScore") || matchFuncStartAndSuffix(expr, "decayscore") {
		return e.evalDecayScore(expr, nodes, rels), true
	}
	if matchFuncStartAndSuffix(expr, "decay") {
		inner := extractFuncArgs(expr, "decay")
		if inner == "" || ContainsKeyword(inner, "score") {
			return nil, false
		}
		return e.evalDecay(expr, nodes, rels), true
	}
	if matchFuncStartAndSuffix(expr, "policy") {
		return e.evalPolicy(expr, nodes, rels), true
	}
	return nil, false
}

type resolvedEntity struct {
	entityID       string
	labels         []string
	createdAtNanos int64
	versionAtNanos int64
	isEdge         bool
	edgeType       string
}

func (e *StorageExecutor) resolveEntityForDecay(
	argExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) (resolvedEntity, bool) {
	arg := strings.TrimSpace(argExpr)
	if node, ok := nodes[arg]; ok {
		return resolvedEntity{
			entityID:       string(node.ID),
			labels:         node.Labels,
			createdAtNanos: node.CreatedAt.UnixNano(),
			versionAtNanos: node.UpdatedAt.UnixNano(),
		}, true
	}
	if rel, ok := rels[arg]; ok {
		return resolvedEntity{
			entityID:       string(rel.ID),
			isEdge:         true,
			edgeType:       rel.Type,
			createdAtNanos: rel.CreatedAt.UnixNano(),
			versionAtNanos: rel.CreatedAt.UnixNano(),
		}, true
	}
	return resolvedEntity{}, false
}

func (e *StorageExecutor) getDecayContext() (be *storage.BadgerEngine, nowNanos int64, ok bool) {
	be = unwrapBadgerEngine(e.storage)
	if be == nil {
		return nil, 0, false
	}
	return be, storage.DecayScoringTime(), true
}

func (e *StorageExecutor) logDecayMismatchOnce() {
	if e.decayMismatchLogged {
		return
	}
	e.decayMismatchLogged = true
	log.Printf("[knowledgepolicy] decay function called but decay subsystem is disabled; returning neutral scores")
}

// evalDecayScore implements decayScore(entity) and decayScore(entity, {property: "key"}).
func (e *StorageExecutor) evalDecayScore(
	expr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) interface{} {
	inner := extractFuncArgs(expr, "decayScore")
	if inner == "" {
		inner = extractFuncArgs(expr, "decayscore")
	}
	args := e.splitFunctionArgs(inner)
	if len(args) == 0 {
		return 1.0
	}

	ent, ok := e.resolveEntityForDecay(args[0], nodes, rels)
	if !ok {
		return 1.0
	}

	be, nowNanos, ok := e.getDecayContext()
	if !ok {
		return 1.0
	}
	if !be.IsDecayEnabled() {
		e.logDecayMismatchOnce()
		return 1.0
	}

	ns := storage.ExtractNamespaceFromID(ent.entityID)
	scorer := be.ScorerForNamespace(ns)
	if scorer == nil {
		return 1.0
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := be.GetAccessMeta(ent.entityID); err == nil {
		accessMeta = meta
	}

	var property string
	if len(args) >= 2 {
		var err error
		property, err = validateDecayOptions(args[1], nodes, rels, e)
		if err != nil {
			return nil
		}
	}

	if property != "" {
		res := scorer.ScoreProperty(ent.entityID, ent.labels, property, accessMeta,
			ent.createdAtNanos, ent.versionAtNanos, nowNanos)
		return res.FinalScore
	}

	if ent.isEdge {
		res := scorer.ScoreEdge(ent.entityID, ent.edgeType, accessMeta,
			ent.createdAtNanos, ent.versionAtNanos, nowNanos)
		return res.FinalScore
	}
	res := scorer.ScoreNode(ent.entityID, ent.labels, accessMeta,
		ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	return res.FinalScore
}

// evalDecay implements decay(entity) and decay(entity, {property: "key"}).
func (e *StorageExecutor) evalDecay(
	expr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) interface{} {
	inner := extractFuncArgs(expr, "decay")
	args := e.splitFunctionArgs(inner)
	if len(args) == 0 {
		return decayDisabledMap("no entity argument")
	}

	ent, ok := e.resolveEntityForDecay(args[0], nodes, rels)
	if !ok {
		return decayDisabledMap("entity not found")
	}

	be, nowNanos, ok := e.getDecayContext()
	if !ok {
		return decayDisabledMap("no BadgerEngine")
	}
	if !be.IsDecayEnabled() {
		e.logDecayMismatchOnce()
		return decayDisabledMap("decay subsystem disabled")
	}

	ns := storage.ExtractNamespaceFromID(ent.entityID)
	scorer := be.ScorerForNamespace(ns)
	if scorer == nil {
		return decayDisabledMap("no decay profile")
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := be.GetAccessMeta(ent.entityID); err == nil {
		accessMeta = meta
	}

	var property string
	if len(args) >= 2 {
		var err error
		property, err = validateDecayOptions(args[1], nodes, rels, e)
		if err != nil {
			return nil
		}
	}

	var res knowledgepolicy.ScoringResolution
	if property != "" {
		res = scorer.ScoreProperty(ent.entityID, ent.labels, property, accessMeta,
			ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	} else if ent.isEdge {
		res = scorer.ScoreEdge(ent.entityID, ent.edgeType, accessMeta,
			ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	} else {
		res = scorer.ScoreNode(ent.entityID, ent.labels, accessMeta,
			ent.createdAtNanos, ent.versionAtNanos, nowNanos)
	}

	return resolutionToMap(res)
}

// evalPolicy implements policy(entity) returning AccessMeta as a map.
func (e *StorageExecutor) evalPolicy(
	expr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
) interface{} {
	inner := extractFuncArgs(expr, "policy")
	args := e.splitFunctionArgs(inner)
	if len(args) == 0 {
		return minimalPolicyMap("")
	}

	ent, ok := e.resolveEntityForDecay(args[0], nodes, rels)
	if !ok {
		return minimalPolicyMap("")
	}

	be, _, ok := e.getDecayContext()
	if !ok {
		return minimalPolicyMap(ent.entityID)
	}

	meta, err := be.GetAccessMeta(ent.entityID)
	if err != nil || meta == nil {
		scope := string(knowledgepolicy.ScopeNode)
		if ent.isEdge {
			scope = string(knowledgepolicy.ScopeEdge)
		}
		return map[string]interface{}{
			"targetId":    ent.entityID,
			"targetScope": scope,
		}
	}

	return accessMetaToMap(meta)
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
		reason = "no decay"
	} else if res.ResolvedDecayProfileID == "" {
		reason = "no decay profile"
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
	optExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	e *StorageExecutor,
) (string, error) {
	val := e.evaluateExpressionWithContext(optExpr, nodes, rels)
	m, ok := val.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("decayScore/decay options must be a map, got %T", val)
	}
	var property string
	for k, v := range m {
		switch k {
		case "property":
			s, _ := v.(string)
			property = s
		default:
			return "", fmt.Errorf("unknown decay option key: %q", k)
		}
	}
	return property, nil
}
