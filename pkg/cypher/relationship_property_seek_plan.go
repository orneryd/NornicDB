package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type relationshipSeekProfileKey struct{}
type relationshipSeekProfile struct {
	mu    sync.Mutex
	seeks map[string]*relationshipSeekStats
}
type relationshipSeekStats struct {
	seek                      relationshipSeek
	lookups, candidates, rows int64
}

func withRelationshipSeekProfile(ctx context.Context) context.Context {
	return context.WithValue(ctx, relationshipSeekProfileKey{}, &relationshipSeekProfile{seeks: make(map[string]*relationshipSeekStats)})
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
		stats = &relationshipSeekStats{seek: seek}
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
				unknown[strings.TrimSpace(clause.text[as+2:])] = true
			}
		}
		if clause.kind != pipelineClauseMatch {
			continue
		}
		split := splitPipelineMatchWhere(clause.text)
		if len(splitTopLevelComma(split.pattern)) != 1 {
			return
		}
		match := e.parseTraversalPattern(ctx, split.pattern)
		seek, eligible := e.relationshipPropertySeek(ctx, match, split.where, unknown)
		if !eligible {
			return
		}
		var stats *relationshipSeekStats
		if plan.Mode == ModeProfile {
			profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
			if profile == nil {
				return
			}
			profile.mu.Lock()
			if recorded := profile.seeks[match.Relationship.Variable+"\x00"+seek.name]; recorded != nil {
				copy := *recorded
				stats = &copy
			}
			profile.mu.Unlock()
			if stats == nil {
				return
			}
		}
		var replace func(*PlanOperator) bool
		replace = func(op *PlanOperator) bool {
			if op == nil {
				return false
			}
			if op.OperatorType == "Expand" {
				op.OperatorType = "RelationshipIndexSeek"
				op.Description = fmt.Sprintf("Equality seek on :%s using %s", seek.relType, seek.name)
				op.Arguments = map[string]interface{}{"index": seek.name, "relationshipType": seek.relType, "properties": seek.properties}
				op.Identifiers = []string{match.Relationship.Variable}
				oldHits := op.DBHits
				op.Children = nil
				if stats != nil {
					op.ActualRows = stats.rows
					// Logical index probes and candidate fetches, not physical I/O.
					op.DBHits = stats.lookups + stats.candidates
					op.Arguments["indexLookups"] = stats.lookups
					op.Arguments["relationshipCandidates"] = stats.candidates
					op.Arguments["dbHitsAccounting"] = "logical index probes and candidate fetches"
					plan.TotalDBHits += op.DBHits - oldHits
				}
				return true
			}
			for _, child := range op.Children {
				if replace(child) {
					return true
				}
			}
			return false
		}
		replace(plan.Root)
		return
	}
}
