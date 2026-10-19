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
	seeks     map[string]*relationshipSeekStats // aggregate compatibility counters
	sites     map[string]*relationshipSeekStats
	fallbacks map[string]int64
	templates map[string]*relationshipAccessSite
	order     []string
	unregistered map[string]int64 // testable invariant: runtime cannot invent a site
}

func recordUnregisteredRelationshipAccess(ctx context.Context, pattern, where string) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	if profile.unregistered == nil {
		profile.unregistered = make(map[string]int64)
	}
	profile.unregistered[relationshipAccessID(pattern, where)]++
}
type relationshipSeekStats struct {
	seek                      relationshipSeek
	variable                  string
	lookups, candidates, rows int64
}

func withRelationshipSeekProfile(ctx context.Context) context.Context {
	return context.WithValue(ctx, relationshipSeekProfileKey{}, &relationshipSeekProfile{seeks: make(map[string]*relationshipSeekStats), sites: make(map[string]*relationshipSeekStats), fallbacks: make(map[string]int64), templates: make(map[string]*relationshipAccessSite)})
}

// Called under mu. PROFILE retains the actual planner descriptors and access
// order, including the scan branch. It never re-plans against a later schema or
// the enclosing executor's database instead of the view that actually ran.
func (profile *relationshipSeekProfile) accessSite(id, variable string) *relationshipAccessSite {
	site := profile.templates[id]
	if site == nil {
		site = &relationshipAccessSite{id: id, access: &relationshipAccess{variable: variable}}
		profile.templates[id] = site
		profile.order = append(profile.order, id)
	}
	site.occurrences++
	return site
}

// Generated endpoint names do not change a relationship access template.
// Equal templates at multiple syntactic sites aggregate their runtime counters;
// EXPLAIN reports their occurrence count rather than pretending they are one site.
func relationshipAccessID(pattern, where string) string {
	pattern = strings.ReplaceAll(pattern, traversalAnonVar, "")
	return strings.TrimSpace(pattern) + "\x00" + normalizePipelineWhitespace(where)
}

func recordRelationshipSeekFallback(ctx context.Context, pattern, where, variable string) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	id := relationshipAccessID(pattern, where)
	database := GetUseDatabaseFromContext(ctx)
	id = database + "\x00" + id
	profile.accessSite(id, variable).graph = database
	profile.fallbacks[id]++
}

func recordRelationshipSeek(ctx context.Context, pattern, where, variable string, seek relationshipSeek, candidates, rows int64) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	seek.values = nil
	id := relationshipAccessID(pattern, where)
	database := GetUseDatabaseFromContext(ctx)
	id = database + "\x00" + id
	site := profile.accessSite(id, variable)
	site.graph = database
	if profile.sites[id+"\x00"+seek.name] == nil {
		site.access.alternatives = append(site.access.alternatives, seek)
	}
	add := func(table map[string]*relationshipSeekStats, key string) {
		stats := table[key]
		if stats == nil {
			stats = &relationshipSeekStats{seek: seek, variable: variable}
			table[key] = stats
		}
		stats.lookups++
		stats.candidates += candidates
		stats.rows += rows
	}
	add(profile.seeks, variable+"\x00"+seek.name)
	add(profile.sites, id+"\x00"+seek.name)
}

type relationshipAccessSite struct {
	id          string
	access      *relationshipAccess
	occurrences int64
	graph       string
	deferred    string
}

func relationshipAccessMatchWhere(text string) (split struct{ pattern, where string }) {
	text = pipelineClauseBody(text, "MATCH")
	split.pattern = text
	if at := topLevelKeywordIndex(text, "WHERE"); at >= 0 {
		split.pattern = strings.TrimSpace(text[:at])
		split.where = normalizePipelineWhitespace(text[at+len("WHERE"):])
	}
	return split
}

// This is syntax discovery, not a scope walk: every binding form deliberately
// goes through unchanged. Only the access planner decides eligibility, and only
// execution's existing row owner resolves a key. Nested expression delimiters
// cover local binders without reproducing any binder's alias/import semantics.
func (e *StorageExecutor) relationshipAccessSites(ctx context.Context, query string) []*relationshipAccessSite {
	if e.shouldUseFabricPlanner(query) {
		return e.relationshipFabricAccessSites(ctx, query)
	}
	return e.relationshipLocalAccessSites(ctx, query)
}

func (e *StorageExecutor) relationshipLocalAccessSites(ctx context.Context, query string) []*relationshipAccessSite {
	program := relationshipProgramFromContext(ctx, query)
	ctx = context.WithValue(ctx, relationshipAccessProgramKey{}, program)
	sites := make([]*relationshipAccessSite, 0, len(program.sites))
	for _, template := range program.sites {
		sites = append(sites, &relationshipAccessSite{
			id: template.id, occurrences: template.occurrences,
			access: e.relationshipAccessFor(ctx, template.pattern, template.where),
		})
	}
	return sites
}

func (e *StorageExecutor) annotateRelationshipSeekPlan(ctx context.Context, query string, plan *ExecutionPlan) {
	var sites []*relationshipAccessSite
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if plan.Mode == ModeProfile && profile != nil {
		profile.mu.Lock()
		defer profile.mu.Unlock()
		for _, id := range profile.order {
			sites = append(sites, profile.templates[id])
		}
	} else if plan.Mode != ModeProfile {
		sites = e.relationshipAccessSites(ctx, query)
	}
	if len(sites) == 0 {
		return
	}
	var operators []*PlanOperator
	for _, site := range sites {
		if site.deferred != "" {
			operators = append(operators, &PlanOperator{OperatorType: site.deferred, Description: "Access planning belongs to the selected graph executor", EstimatedRows: 100, Arguments: map[string]interface{}{"graph": site.graph, "relationshipAccessPlanning": "deferred"}})
			continue
		}
		access := site.access
		fallbacks := int64(0)
		if plan.Mode == ModeProfile && profile != nil {
			fallbacks = profile.fallbacks[site.id]
		}
		var alternatives []*PlanOperator
		for _, seek := range access.alternatives {
			var stats *relationshipSeekStats
			if plan.Mode == ModeProfile && profile != nil {
				stats = profile.sites[site.id+"\x00"+seek.name]
				if stats == nil && fallbacks > 0 {
					continue
				}
			}
			op := &PlanOperator{OperatorType: "RelationshipIndexSeek", Description: fmt.Sprintf("Equality seek on :%s using %s", seek.relType, seek.name), Identifiers: []string{access.variable}, EstimatedRows: seek.estimatedRows, Arguments: map[string]interface{}{
				"index": seek.name, "relationshipType": seek.relType, "properties": seek.properties, "accessOccurrences": site.occurrences,
				"runtimeKeyCheck": "resolve against incoming row; scan fallback for unbound or unsupported keys",
			}}
			if stats != nil {
				op.ActualRows = stats.rows
				op.DBHits = stats.lookups + stats.candidates
				op.Arguments["indexLookups"] = stats.lookups
				op.Arguments["relationshipCandidates"] = stats.candidates
				op.Arguments["dbHitsAccounting"] = "logical index probes and candidate fetches"
			} else if plan.Mode == ModeProfile {
				op.Arguments["executed"] = false
			}
			if plan.Mode == ModeProfile {
				delete(op.Arguments, "accessOccurrences")
				op.Arguments["accessAttempts"] = site.occurrences
			}
			alternatives = append(alternatives, op)
		}
		if len(alternatives) == 0 || fallbacks > 0 {
			scan := &PlanOperator{OperatorType: "Expand", Description: "Relationship scan", Identifiers: []string{access.variable}, EstimatedRows: 100, Arguments: map[string]interface{}{"accessOccurrences": site.occurrences}}
			if fallbacks > 0 {
				delete(scan.Arguments, "accessOccurrences")
				scan.Arguments["accessAttempts"] = site.occurrences
				scan.Arguments["scanFallbacks"] = fallbacks
				scan.Arguments["dbHitsAccounting"] = "scan hits are not instrumented"
			}
			alternatives = append(alternatives, scan)
		}
		if len(alternatives) == 1 {
			operators = append(operators, alternatives[0])
		} else {
			choice := &PlanOperator{OperatorType: "RelationshipAccessChoice", Description: "Ordered key alternatives; scan fallback if no supported key", Children: alternatives}
			if plan.Mode == ModeProfile {
				choice.Description = "Executed relationship access alternatives"
			}
			for _, op := range alternatives {
				choice.DBHits += op.DBHits
				if op.EstimatedRows > choice.EstimatedRows {
					choice.EstimatedRows = op.EstimatedRows
				}
			}
			operators = append(operators, choice)
		}
		if site.graph != "" {
			for _, op := range alternatives {
				op.Arguments["database"] = site.graph
			}
		}
	}
	var replacement *PlanOperator
	if len(operators) == 1 {
		replacement = operators[0]
	} else {
		replacement = &PlanOperator{OperatorType: "RelationshipAccess", Description: "Relationship access templates (counters aggregate identical templates)", Children: operators}
		for _, op := range operators {
			replacement.DBHits += op.DBHits
			replacement.EstimatedRows += op.EstimatedRows
		}
	}
	var replace func(*PlanOperator) (int64, bool)
	replace = func(op *PlanOperator) (int64, bool) {
		if op.OperatorType == "Expand" {
			old := op.DBHits
			// Preserve existing heuristic work for a scan; seek probes have
			// actual counters and must not inherit a fictitious node scan.
			if len(operators) == 1 && replacement.OperatorType == "Expand" {
				replacement.DBHits = old
				replacement.Children = op.Children
				replacement.ActualRows = op.ActualRows
			}
			*op = *replacement
			return op.DBHits - old, true
		}
		for _, child := range op.Children {
			if delta, ok := replace(child); ok {
				op.DBHits += delta
				return delta, true
			}
		}
		return 0, false
	}
	if delta, ok := replace(plan.Root); ok {
		plan.TotalDBHits += delta
	} else {
		plan.Root.Children = append(plan.Root.Children, replacement)
		plan.Root.DBHits += replacement.DBHits
		plan.TotalDBHits += replacement.DBHits
	}
}
