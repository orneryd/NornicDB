package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type relationshipSeekProfileKey struct{}
type relationshipSeekProfile struct {
	mu           sync.Mutex
	seeks        map[string]*relationshipSeekStats
	order        []string
	fallbacks    map[string]int64
	firstPattern string
}
type relationshipSeekStats struct {
	seek                      relationshipSeek
	variable                  string
	lookups, candidates, rows int64
}

func withRelationshipSeekProfile(ctx context.Context) context.Context {
	return context.WithValue(ctx, relationshipSeekProfileKey{}, &relationshipSeekProfile{seeks: make(map[string]*relationshipSeekStats), fallbacks: make(map[string]int64)})
}

func recordRelationshipSeekFallback(ctx context.Context, pattern, where, variable string) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	if !profile.firstAccess(pattern, where) {
		return
	}
	profile.fallbacks[variable]++
}

// The surrounding plan is schematic and represents its first relationship
// expansion. Later MATCH sites must not donate a seek to an earlier scan.
func (profile *relationshipSeekProfile) firstAccess(pattern, where string) bool {
	key := pattern + "\x00" + where
	if profile.firstPattern == "" {
		profile.firstPattern = key
	}
	return profile.firstPattern == key
}

func recordRelationshipSeek(ctx context.Context, pattern, where, variable string, seek relationshipSeek, candidates, rows int64) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	if !profile.firstAccess(pattern, where) {
		return
	}
	key := variable + "\x00" + seek.name
	stats := profile.seeks[key]
	if stats == nil {
		seek.values = nil // Plan metadata retains the decision, not input row values.
		stats = &relationshipSeekStats{seek: seek, variable: variable}
		profile.seeks[key] = stats
		profile.order = append(profile.order, key)
	}
	stats.lookups++
	stats.candidates += candidates
	stats.rows += rows
}

// EXPLAIN asks the execution selector with the incoming scope supplied by the
// canonical semantic walk. WITH projections, UNWIND, CALL imports and Fabric
// bindings therefore have exactly the same scope owner as normal execution.
// PROFILE consumes execution's recorded decision without re-planning the query.
func (e *StorageExecutor) relationshipSeekPlanDecision(ctx context.Context, query string, mode ExecutionMode) (relationshipSeek, string, *relationshipSeekStats, int64, bool) {
	if mode == ModeProfile {
		profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
		if profile == nil {
			return relationshipSeek{}, "", nil, 0, false
		}
		profile.mu.Lock()
		defer profile.mu.Unlock()
		if len(profile.order) == 0 {
			return relationshipSeek{}, "", nil, 0, false
		}
		stats := *profile.seeks[profile.order[0]]
		return stats.seek, stats.variable, &stats, profile.fallbacks[stats.variable], true
	}
	if !containsRelationshipPattern(query) {
		return relationshipSeek{}, "", nil, 0, false
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(query)
	if !ok {
		return relationshipSeek{}, "", nil, 0, false
	}
	types := make(typeScope)
	for name := range e.fabricRecordBindings {
		types[name] = staticOperand{}
	}
	for name := range valueBindingsFromContext(ctx) {
		types[name] = staticOperand{}
	}
	var seek relationshipSeek
	var variable string
	var visited, eligible bool
	observe := func(clause pipelineClause, incoming typeScope) {
		if visited || (clause.kind != pipelineClauseMatch && clause.kind != pipelineClauseOptionalMatch) {
			return
		}
		pattern := strings.TrimSpace(clause.text)
		if clause.kind == pipelineClauseOptionalMatch {
			pattern = strings.TrimSpace(pattern[len("OPTIONAL"):])
		}
		where := ""
		if at := topLevelKeywordIndex(pattern, "WHERE"); at >= 0 {
			where = strings.TrimSpace(pattern[at+len("WHERE"):])
			pattern = strings.TrimSpace(pattern[:at])
		}
		if !containsRelationshipPattern(pattern) {
			return
		}
		visited = true // The existing schematic plan represents the first expansion.
		if len(splitTopLevelComma(pattern)) != 1 {
			return
		}
		match := e.parseTraversalPattern(ctx, pattern)
		if match == nil {
			return
		}
		unknown := make(map[string]bool, len(incoming))
		for name := range incoming {
			unknown[name] = true
		}
		seek, eligible = e.relationshipPropertySeek(ctx, match, where, unknown)
		variable = match.Relationship.Variable
	}
	_, _, err := e.semanticScopeWalk(clauses, types, cypherVersionFromContext(ctx) == "25", subqueryScope{}, observe)
	if err != nil {
		return relationshipSeek{}, "", nil, 0, false
	}
	return seek, variable, nil, 0, eligible
}

func (e *StorageExecutor) annotateRelationshipSeekPlan(ctx context.Context, query string, plan *ExecutionPlan) {
	seek, variable, stats, fallbacks, eligible := e.relationshipSeekPlanDecision(ctx, query, plan.Mode)
	if !eligible {
		return
	}
	var delta int64
	var replace func(*PlanOperator) bool
	replace = func(op *PlanOperator) bool {
		if op == nil {
			return false
		}
		if op.OperatorType == "Expand" {
			original := *op
			op.OperatorType = "RelationshipIndexSeek"
			op.Description = fmt.Sprintf("Equality seek on :%s using %s", seek.relType, seek.name)
			op.Arguments = map[string]interface{}{"index": seek.name, "relationshipType": seek.relType, "properties": seek.properties}
			op.Identifiers = []string{variable}
			op.EstimatedRows = seek.estimatedRows
			if seek.runtimeKey {
				op.Arguments["runtimeKeyCheck"] = "scan fallback for unsupported incoming values"
			}
			oldHits := op.DBHits
			op.Children = nil
			if fallbacks > 0 {
				op.Children = []*PlanOperator{&original}
				op.Arguments["scanFallbacks"] = fallbacks
			}
			if stats != nil {
				op.ActualRows = stats.rows
				// Logical index probes and candidate fetches, not physical I/O.
				op.DBHits = stats.lookups + stats.candidates
				op.Arguments["indexLookups"] = stats.lookups
				op.Arguments["relationshipCandidates"] = stats.candidates
				op.Arguments["dbHitsAccounting"] = "logical index probes and candidate fetches"
				if fallbacks > 0 {
					op.DBHits += oldHits
				}
				delta = op.DBHits - oldHits
				plan.TotalDBHits += delta
			}
			return true
		}
		for _, child := range op.Children {
			if replace(child) {
				op.DBHits += delta
				return true
			}
		}
		return false
	}
	replace(plan.Root)
}
