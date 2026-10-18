package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type relationshipSeekProfileKey struct{}
type relationshipSeekProfile struct {
	mu        sync.Mutex
	seeks     map[string]*relationshipSeekStats
	fallbacks map[string]int64
}
type relationshipSeekStats struct {
	lookups, candidates, rows int64
}

func withRelationshipSeekProfile(ctx context.Context) context.Context {
	return context.WithValue(ctx, relationshipSeekProfileKey{}, &relationshipSeekProfile{seeks: make(map[string]*relationshipSeekStats), fallbacks: make(map[string]int64)})
}

func recordRelationshipSeekFallback(ctx context.Context, variable string) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	profile.fallbacks[variable]++
	profile.mu.Unlock()
}

func recordRelationshipSeek(ctx context.Context, variable string, seek relationshipSeek, candidates, rows int64) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	key := variable + "\x00" + seek.name
	stats := profile.seeks[key]
	if stats == nil {
		stats = &relationshipSeekStats{}
		profile.seeks[key] = stats
	}
	stats.lookups++
	stats.candidates += candidates
	stats.rows += rows
}

// Annotate only the MATCH represented by the existing schematic plan. The
// execution's record supplies PROFILE's seek rows and candidate work, rather
// than copying final projection counts or claiming a traversal ran.
func (e *StorageExecutor) annotateRelationshipSeekPlan(ctx context.Context, query string, plan *ExecutionPlan) {
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(query)
	if !ok {
		return
	}
	unknown := make(map[string]bool)
	for _, clause := range clauses {
		if clause.kind == pipelineClauseUnwind {
			if as := topLevelKeywordIndex(clause.text, "AS"); as >= 0 {
				unknown[symbolicNameValue(strings.TrimSpace(clause.text[as+2:]))] = true
			}
		}
		if clause.kind != pipelineClauseMatch {
			continue
		}
		pattern := strings.TrimSpace(clause.text[len("MATCH"):])
		where := ""
		if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
			where = normalizePipelineWhitespace(pattern[index+len("WHERE"):])
			pattern = strings.TrimSpace(pattern[:index])
		}
		if len(splitTopLevelComma(pattern)) != 1 {
			return
		}
		match := e.parseTraversalPattern(ctx, pattern)
		seek, eligible := e.relationshipPropertySeek(ctx, match, where, unknown)
		if !eligible {
			return
		}
		var stats *relationshipSeekStats
		var fallbacks int64
		if plan.Mode == ModeProfile {
			profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			if profile == nil {
				return
			}
			profile.mu.Lock()
			fallbacks = profile.fallbacks[match.Relationship.Variable]
			if recorded := profile.seeks[match.Relationship.Variable+"\x00"+seek.name]; recorded != nil {
				copy := *recorded
				stats = &copy
			}
			profile.mu.Unlock()
			if stats == nil {
				return
			}
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
				op.Identifiers = []string{match.Relationship.Variable}
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
		return
	}
}
