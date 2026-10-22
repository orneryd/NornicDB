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
type relationshipAccessIdentityKey struct{ pattern, where string }

type relationshipAccessIdentity struct {
	key relationshipAccessIdentityKey
	id  string
}

// This bounded cache contains syntax identity only, never a program/template,
// row value, schema or read view. A cached ID cannot authorize a removed site.
var relationshipAccessIdentities = newBoundedCache[relationshipAccessIdentityKey, *relationshipAccessIdentity](8192, func(key relationshipAccessIdentityKey) int {
	return boundedCacheStringShardIndex(key.pattern)
})

func relationshipAccessID(pattern, where string) string {
	return relationshipAccessIdentityFor(pattern, where).id
}

func relationshipAccessIdentityFor(pattern, where string) *relationshipAccessIdentity {
	key := relationshipAccessIdentityKey{pattern: pattern, where: where}
	if identity, cached := relationshipAccessIdentities.get(key); cached {
		return identity
	}
	// Cartesian MATCH and named-path consumers add path assignments. They
	// do not create a different relationship access template.
	_, pattern = parseCreatePathAssignment(pattern)
	pattern = strings.ReplaceAll(pattern, traversalAnonVar, "")
	identity := &relationshipAccessIdentity{key: key, id: strings.TrimSpace(pattern) + "\x00" + normalizePipelineWhitespace(where)}
	relationshipAccessIdentities.put(key, identity)
	return identity
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

func recordRelationshipVectorAccess(ctx context.Context, pattern, where, variable string, access *relationshipVectorAccess, lookups, candidates, rows int64) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil || access == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	id := GetUseDatabaseFromContext(ctx) + "\x00" + relationshipAccessID(pattern, where)
	site := profile.accessSite(id, variable)
	site.graph = GetUseDatabaseFromContext(ctx)
	site.access.vector = access
	stats := profile.sites[id+"\x00"+access.index]
	if stats == nil {
		stats = &relationshipSeekStats{variable: variable}
		profile.sites[id+"\x00"+access.index] = stats
	}
	stats.lookups += lookups
	stats.candidates += candidates
	stats.rows += rows
}

func recordRelationshipIDAccess(ctx context.Context, pattern, where, variable string, access *relationshipIDAccess, lookups, candidates, rows int64) {
	profile, _ := ctx.Value(relationshipSeekProfileKey{}).(*relationshipSeekProfile)
	if profile == nil || access == nil {
		return
	}
	profile.mu.Lock()
	defer profile.mu.Unlock()
	id := GetUseDatabaseFromContext(ctx) + "\x00" + relationshipAccessID(pattern, where)
	site := profile.accessSite(id, variable)
	site.graph = GetUseDatabaseFromContext(ctx)
	site.access.id = access
	stats := profile.sites[id+"\x00"+access.kind]
	if stats == nil {
		stats = &relationshipSeekStats{variable: variable}
		profile.sites[id+"\x00"+access.kind] = stats
	}
	stats.lookups += lookups
	stats.candidates += candidates
	stats.rows += rows
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
	ctx, statement := compileRelationshipStatement(ctx, query, nil)
	program := statement.program
	var vector *relationshipVectorAccess
	if indexASCIIFold(query, "vector.similarity.cosine") >= 0 {
		if execution, ok := e.relationshipExecutionForQuery(statement, ctx, query); ok {
			vector = execution.vectorPlan(ctx, query)
		}
	}
	sites := make([]*relationshipAccessSite, 0, len(program.sites))
	for _, template := range program.sites {
		access := e.relationshipTemplateAccess(statement, ctx, template)
		if vector != nil {
			access = &relationshipAccess{variable: template.syntax.variable, vector: vector}
		}
		sites = append(sites, &relationshipAccessSite{
			id: template.id, occurrences: template.occurrences,
			access: access,
		})
	}
	for _, query := range program.graphQueries {
		sites = append(sites, e.relationshipFabricAccessSites(ctx, query)...)
	}
	return sites
}

func firstRelationshipExpand(operator *PlanOperator) *PlanOperator {
	if operator == nil {
		return nil
	}
	if operator.OperatorType == "Expand" {
		return operator
	}
	for _, child := range operator.Children {
		if found := firstRelationshipExpand(child); found != nil {
			return found
		}
	}
	return nil
}

func adjustRelationshipAccessParentHits(operator, target *PlanOperator, delta int64) bool {
	if operator == target {
		return true
	}
	for _, child := range operator.Children {
		if adjustRelationshipAccessParentHits(child, target, delta) {
			operator.DBHits += delta
			return true
		}
	}
	return false
}

// The common EXPLAIN shape has one immutable program template and one access
// alternative. Reuse the schematic Expand that the general planner already
// allocated instead of building parallel site/operator slices and copying a
// replacement over it.
func (e *StorageExecutor) annotateSingleRelationshipAccess(ctx context.Context, query string, plan *ExecutionPlan) bool {
	if plan.Mode == ModeProfile || e.shouldUseFabricPlanner(query) {
		return false
	}
	ctx, statement := compileRelationshipStatement(ctx, query, nil)
	program := statement.program
	if len(program.sites) != 1 || len(program.graphQueries) != 0 {
		return false
	}
	template := program.sites[0]
	access := e.relationshipTemplateAccess(statement, ctx, template)
	if indexASCIIFold(query, "vector.similarity.cosine") >= 0 {
		if execution, ok := e.relationshipExecutionForQuery(statement, ctx, query); ok {
			if vector := execution.vectorPlan(ctx, query); vector != nil {
				access = &relationshipAccess{variable: template.syntax.variable, vector: vector}
			}
		}
	}
	if access.vector != nil || len(access.alternatives) > 1 {
		return false
	}
	operator := firstRelationshipExpand(plan.Root)
	// Expression-only sites have no schematic Expand. Project their one
	// choice here too, rather than resolving it again through temporary
	// site/alternative/operator slices in the general multi-site projection.
	if operator == nil {
		operator = &PlanOperator{OperatorType: "Expand"}
		plan.Root.Children = append(plan.Root.Children, operator)
	}
	oldHits := operator.DBHits
	if operator.Arguments == nil {
		operator.Arguments = make(map[string]interface{}, 6)
	} else {
		clear(operator.Arguments)
	}
	if cap(operator.Identifiers) > 0 {
		operator.Identifiers = operator.Identifiers[:1]
		operator.Identifiers[0] = access.variable
	} else {
		operator.Identifiers = []string{access.variable}
	}
	operator.DBHits = 0
	operator.ActualRows = 0
	operator.EstimatedRows = 100
	operator.Description = "Relationship scan"
	operator.Arguments["accessOccurrences"] = template.occurrences
	if access.id != nil {
		operator.OperatorType = "RelationshipByIdSeek"
		if access.id.kind == "elementId" {
			operator.OperatorType = "RelationshipByElementIdSeek"
		}
		operator.Description = fmt.Sprintf("Relationship %s seek", access.id.kind)
		operator.EstimatedRows = access.id.estimatedRows
		operator.Arguments["expression"] = access.id.expression
		operator.Arguments["runtimeKeyCheck"] = "resolve against incoming row; scan fallback for an unbound or invalid ID"
	} else if len(access.alternatives) == 1 {
		seek := access.alternatives[0]
		operator.OperatorType = "RelationshipIndexSeek"
		operator.Description = fmt.Sprintf("Equality seek on :%s using %s", seek.relType, seek.name)
		operator.EstimatedRows = seek.estimatedRows
		operator.Arguments["index"] = seek.name
		operator.Arguments["relationshipType"] = seek.relType
		operator.Arguments["properties"] = seek.properties
		operator.Arguments["runtimeKeyCheck"] = "resolve against incoming row; scan fallback for unbound or unsupported keys"
	}
	delta := operator.DBHits - oldHits
	adjustRelationshipAccessParentHits(plan.Root, operator, delta)
	plan.TotalDBHits += delta
	return true
}

func (e *StorageExecutor) annotateRelationshipSeekPlan(ctx context.Context, query string, plan *ExecutionPlan) {
	if e.annotateSingleRelationshipAccess(ctx, query, plan) {
		return
	}
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
		if access.id != nil {
			operatorType := "RelationshipByIdSeek"
			if access.id.kind == "elementId" {
				operatorType = "RelationshipByElementIdSeek"
			}
			op := &PlanOperator{OperatorType: operatorType, Description: fmt.Sprintf("Relationship %s seek", access.id.kind), Identifiers: []string{access.variable}, EstimatedRows: access.id.estimatedRows, Arguments: map[string]interface{}{
				"expression": access.id.expression, "accessOccurrences": site.occurrences,
				"runtimeKeyCheck": "resolve against incoming row; scan fallback for an unbound or invalid ID",
			}}
			if plan.Mode == ModeProfile && profile != nil {
				stats := profile.sites[site.id+"\x00"+access.id.kind]
				if stats != nil {
					op.ActualRows = stats.rows
					op.DBHits = stats.lookups + stats.candidates
					op.Arguments["idLookups"] = stats.lookups
					op.Arguments["relationshipCandidates"] = stats.candidates
				}
				delete(op.Arguments, "accessOccurrences")
				op.Arguments["accessAttempts"] = site.occurrences
			}
			alternatives = append(alternatives, op)
		}
		if access.vector != nil {
			var stats *relationshipSeekStats
			if plan.Mode == ModeProfile && profile != nil {
				stats = profile.sites[site.id+"\x00"+access.vector.index]
			}
			alternatives = append(alternatives, relationshipVectorOperator(access.vector, access.variable, site.occurrences, stats, plan.Mode == ModeProfile))
		}
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
		if access.id == nil && access.vector == nil && (len(alternatives) == 0 || fallbacks > 0) {
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
			// Preserve heuristic work only for uninstrumented scans. Measured
			// vector scans, like seeks, retain the execution owner's counters.
			if len(operators) == 1 && replacement.OperatorType == "Expand" {
				replacement.Children = op.Children
				if _, measured := replacement.Arguments["scanRelationships"]; !measured {
					replacement.DBHits = old
					replacement.ActualRows = op.ActualRows
				}
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
