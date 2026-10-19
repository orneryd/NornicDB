package fabric

import (
	"fmt"
	"strings"
)

// FabricPlanner decomposes a Cypher query into a Fragment tree by splitting
// at USE-clause boundaries. Without USE clauses, it produces a single
// FragmentExec targeting the session database — identical to community behavior.
//
// This mirrors Neo4j's FabricPlanner.scala.
type FabricPlanner struct {
	catalog        *Catalog
	resolveColumns ColumnResolver
}

func splitTopLevelUnion(query string) ([]string, []bool, bool, error) {
	parts := make([]string, 0, 2)
	ops := make([]bool, 0, 1)
	structure := ReadQueryStructure(query)
	if structure.Err != nil {
		return nil, nil, false, structure.Err
	}
	start := 0
	for _, clause := range structure.Clauses {
		if clause.Kind != "UNION" {
			continue
		}
		part := strings.TrimSpace(query[start:clause.Start])
		if part == "" {
			return nil, nil, false, fmt.Errorf("invalid UNION: empty branch")
		}
		parts = append(parts, part)
		ops = append(ops, clause.Distinct)
		start = clause.End
	}
	if len(parts) == 0 {
		return nil, nil, false, nil
	}
	last := strings.TrimSpace(query[start:])
	if last == "" {
		return nil, nil, false, fmt.Errorf("invalid UNION: empty trailing branch")
	}
	parts = append(parts, last)
	return parts, ops, true, nil
}

func NewFabricPlanner(catalog *Catalog) *FabricPlanner {
	return &FabricPlanner{catalog: catalog}
}

// Plan decomposes a query into a Fragment tree.
//
// Parameters:
//   - query: the full Cypher query string
//   - sessionDB: the default database for the session (used when no USE clause is present)
//
// Returns a Fragment tree ready for execution by FabricExecutor.
func (p *FabricPlanner) Plan(query string, sessionDB string) (Fragment, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}
	if err := ReadQueryStructure(trimmed).ValidateUnions(); err != nil {
		return nil, err
	}
	// Preserve name roles while their binding context is still present. A
	// later fragment may legitimately receive that binding as an input row.
	trimmed = PreserveNameRoles(trimmed)
	session := planTarget{name: sessionDB}
	scope := compositeScopeRoot(sessionDB)
	fragment, err := p.planQuery(trimmed, session, scope, false)
	if err == nil && p.resolveColumns != nil {
		_, err = p.resolveLogicalColumns(fragment, nil, nil, trimmed)
	}
	return fragment, err
}

// planQuery is the entry for every statement and CALL body. Split UNION before
// selecting a branch's USE target so one branch cannot absorb another's text or
// graph scope. CALL import binding remains a per-branch responsibility.
func (p *FabricPlanner) planQuery(trimmed string, current planTarget, scope string, inSubquery bool) (Fragment, error) {
	parts, ops, hasUnion, err := splitTopLevelUnion(trimmed)
	if err != nil {
		return nil, err
	}
	if hasUnion {
		lhs, err := p.planQuery(parts[0], current, scope, inSubquery)
		if err != nil {
			return nil, err
		}
		root := lhs
		for i := 1; i < len(parts); i++ {
			rhs, err := p.planQuery(parts[i], current, scope, inSubquery)
			if err != nil {
				return nil, err
			}
			root = &FragmentUnion{
				Init:     &FragmentInit{Columns: nil},
				LHS:      root,
				RHS:      rhs,
				Distinct: ops[i-1],
				Columns:  nil,
			}
		}
		return root, nil
	}

	if inSubquery {
		return p.planCallBranch(trimmed, current, scope)
	}
	return p.planSingleQuery(trimmed, current, scope, false)
}

// planSingleQuery plans one query (no top-level UNION) that runs on
// current unless it starts with its own USE clause. scope is the session's
// composite database; inSubquery is true for a CALL { } body.
func (p *FabricPlanner) planSingleQuery(trimmed string, current planTarget, scope string, inSubquery bool) (Fragment, error) {
	// Extract leading USE clause if present.
	top := current
	remaining := trimmed
	use, rest, hasTopUse, err := parseLeadingUse(trimmed, inSubquery)
	if err != nil {
		return nil, err
	}
	if hasTopUse {
		top = useTarget(use)
		remaining = rest
		if err := p.validatePlanTarget(scope, top); err != nil {
			return nil, err
		}
		// Subqueries are in the scope of the graph a static USE selects.
		if top.dynamic == nil {
			scope = compositeScopeRoot(top.name)
		}
	}

	// Check whether the remaining query contains top-level CALL {} subqueries.
	callBlocks, err := extractTopLevelCallBlocks(remaining)
	if err != nil {
		return nil, err
	}
	fabricBlocks := make([]callSubqueryBlock, 0, len(callBlocks))
	for _, block := range callBlocks {
		isFabricBlock, err := callBlockContainsFabricUse(block.Body)
		if err != nil {
			return nil, err
		}
		if isFabricBlock {
			if block.InTransactions {
				return nil, fmt.Errorf("CALL IN TRANSACTIONS across composite constituents is not supported")
			}
			fabricBlocks = append(fabricBlocks, block)
		}
	}

	if len(fabricBlocks) == 0 {
		// Support top-level mid-query USE routing (e.g. "WITH ... USE db MATCH ...").
		// This keeps prefixes in the current graph and routes the remainder to the USE target.
		if prefix, rest, ok := splitAtTopLevelUse(remaining); ok {
			subUse, subRest, hasUse, err := parseLeadingUse(rest, true)
			if err != nil {
				return nil, err
			}
			if !hasUse {
				return nil, fmt.Errorf("invalid USE clause")
			}
			sub := useTarget(subUse)
			if err := p.validatePlanTarget(scope, sub); err != nil {
				return nil, err
			}
			inner, err := p.planSingleQuery(subRest, sub, scope, true)
			if err != nil {
				return nil, err
			}
			prefix = strings.TrimSpace(prefix)
			if prefix == "" {
				return inner, nil
			}
			prefixExec := newExec(ensureRowProducingPrefix(prefix), top, scope)
			return &FragmentApply{
				Projection: true,
				Input: &FragmentApply{
					Projection: true,
					Input:      &FragmentInit{Columns: nil},
					Inner:      prefixExec,
					Columns:    nil,
				},
				Inner:   inner,
				Columns: nil,
			}, nil
		}

		// Simple case: single-graph query, no CALL {} blocks at this scope.
		return newExec(remaining, top, scope), nil
	}

	// Multi-graph case: decompose into Apply chain.
	// The top-level USE sets the default graph; each CALL { USE ... } block
	// targets a different constituent.
	return p.planMultiGraph(top, scope, remaining, fabricBlocks)
}

// validatePlanTarget checks a static USE target at plan time; a dynamic
// reference is checked when it resolves (FabricExecutor.resolveExecGraph).
func (p *FabricPlanner) validatePlanTarget(scope string, target planTarget) error {
	if target.dynamic != nil {
		return nil
	}
	return p.validateUseTarget(scope, target.name)
}

func splitAtTopLevelUse(query string) (string, string, bool) {
	for i, clause := range ReadQueryStructure(query).Clauses {
		if clause.Kind == "USE" && i > 0 {
			return query[:clause.Start], query[clause.Start:], true
		}
	}
	return "", "", false
}

func ensureRowProducingPrefix(query string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return trimmed
	}
	if hasTopLevelReturnClause(trimmed) {
		return trimmed
	}
	if ReadQueryStructure(trimmed).Starts("WITH") {
		if aliases := trailingWithAliases(trimmed); len(aliases) > 0 {
			return trimmed + " RETURN " + strings.Join(aliases, ", ")
		}
	}
	return trimmed + " RETURN *"
}

func hasTopLevelReturnClause(query string) bool {
	return ReadQueryStructure(query).First("RETURN") != nil
}

func trailingWithAliases(query string) []string {
	with := ReadQueryStructure(query).Last("WITH")
	if with == nil || with.Projection == nil {
		return nil
	}
	var aliases []string
	for _, item := range with.Projection.Items {
		name := item.Column
		if item.HasAlias || item.Name != "" {
			aliases = append(aliases, quoteCypherIdentifier(name))
		}
	}
	return aliases
}

func isValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return true
}

// planMultiGraph builds a Fragment tree for queries with top-level CALL {} subqueries.
// Each CALL block is planned recursively so nested USE variants are decomposed correctly.
func (p *FabricPlanner) planMultiGraph(top planTarget, scope string, fullQuery string, blocks []callSubqueryBlock) (Fragment, error) {
	init := &FragmentInit{Columns: nil, logicalQuery: fullQuery}
	var currentInput Fragment = init
	lastPos := 0

	for _, block := range blocks {
		// Preserve outer query segments before each CALL block.
		prefix := strings.TrimSpace(fullQuery[lastPos:block.Start])
		if prefix != "" {
			currentInput = &FragmentApply{Input: currentInput, Inner: newExec(ensureRowProducingPrefix(prefix), top, scope), Projection: true, Columns: nil}
		}

		subqueryFragment, err := p.planQuery(block.Body, top, scope, true)
		if err != nil {
			return nil, err
		}

		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   subqueryFragment,
			Call:    &block,
			Columns: nil, // determined at execution time
		}
		lastPos = block.End
	}

	// Preserve trailing outer query clauses after the final CALL block.
	trailingQuery := strings.TrimSpace(fullQuery[lastPos:])
	if trailingQuery != "" {
		currentInput = &FragmentApply{
			Input:      currentInput,
			Inner:      newExec(trailingQuery, top, scope),
			Projection: true,
			Columns:    nil,
		}
	}

	return currentInput, nil
}

// planCallBranch preserves the existing USE and import handling for one CALL
// branch. planQuery has already separated any sibling UNION branches.
func (p *FabricPlanner) planCallBranch(body string, top planTarget, scope string) (Fragment, error) {
	subUse, subBody, hasUse, err := parseLeadingUse(body, true)
	if err == nil && !hasUse {
		subUse, subBody, hasUse, err = parseLeadingWithUse(body)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasUse {
		sub := useTarget(subUse)
		if err := p.validatePlanTarget(scope, sub); err != nil {
			return nil, err
		}
		fragment, err := p.planSingleQuery(subBody, sub, scope, true)
		if err != nil {
			return nil, err
		}
		return bindLeadingImportColumns(fragment, extractWithImports(subBody)), nil
	}
	fragment, err := p.planSingleQuery(body, top, scope, true)
	if err != nil {
		return nil, err
	}
	return bindLeadingImportColumns(fragment, extractWithImports(body)), nil
}

// parseLeadingWithUse extracts a leading WITH ... USE <graph> pattern from a CALL body.
// It returns the USE target graph and a rewritten body where USE is removed but the WITH
// import clause is preserved (e.g. "WITH x USE db MATCH ..." -> "WITH x MATCH ...").
func parseLeadingWithUse(body string) (use UseClause, rewritten string, ok bool, err error) {
	trimmed := strings.TrimSpace(body)
	structure := ReadQueryStructure(trimmed)
	if structure.Err != nil {
		return UseClause{}, "", false, structure.Err
	}
	if !structure.Starts("WITH") {
		return UseClause{}, body, false, nil
	}

	withEnd, found := LeadingWithClauseEnd(trimmed)
	if !found || withEnd <= 0 || withEnd >= len(trimmed) {
		return UseClause{}, body, false, nil
	}

	withClause := strings.TrimSpace(trimmed[:withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	if !ReadQueryStructure(rest).Starts("USE") {
		return UseClause{}, body, false, nil
	}

	use, remaining, hasUse, parseErr := parseLeadingUse(rest, true)
	if parseErr != nil {
		return UseClause{}, "", false, parseErr
	}
	if !hasUse {
		return UseClause{}, body, false, nil
	}
	return use, strings.TrimSpace(withClause + " " + remaining), true, nil
}

func (p *FabricPlanner) validateUseTarget(sessionDB string, targetDB string) error {
	target := strings.TrimSpace(targetDB)
	if target == "" {
		return fmt.Errorf("USE clause requires a database name")
	}
	if p.catalog != nil {
		if _, err := p.catalog.Resolve(target); err != nil {
			return fmt.Errorf("invalid USE target '%s': %w", target, err)
		}
	}

	scopeRoot := compositeScopeRoot(sessionDB)
	targetRoot := compositeScopeRoot(target)
	if strings.Contains(target, ".") && p.inCompositeScope(sessionDB) && scopeRoot != "" && !strings.EqualFold(scopeRoot, targetRoot) {
		return fmt.Errorf("invalid USE target '%s': target is out of scope for composite '%s'", target, scopeRoot)
	}
	return nil
}

func (p *FabricPlanner) inCompositeScope(sessionDB string) bool {
	db := strings.TrimSpace(sessionDB)
	if db == "" {
		return false
	}
	if strings.Contains(db, ".") {
		return true
	}
	if p.catalog == nil {
		return false
	}
	prefix := strings.ToLower(db) + "."
	return p.catalog.HasGraphWithPrefix(prefix)
}

func compositeScopeRoot(graph string) string {
	graph = strings.TrimSpace(graph)
	if graph == "" {
		return ""
	}
	if idx := strings.IndexByte(graph, '.'); idx >= 0 {
		return graph[:idx]
	}
	return graph
}

func bindLeadingImportColumns(fragment Fragment, importCols []string) Fragment {
	if len(importCols) == 0 || fragment == nil {
		return fragment
	}

	switch f := fragment.(type) {
	case *FragmentExec:
		copied := *f
		copied.Input = &FragmentInit{Columns: importCols, ImportColumns: importCols}
		return &copied
	case *FragmentApply:
		copied := *f
		copied.Input = bindLeadingImportColumns(copied.Input, importCols)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = bindLeadingImportColumns(copied.LHS, importCols)
		copied.RHS = bindLeadingImportColumns(copied.RHS, importCols)
		return &copied
	default:
		return fragment
	}
}

// parseLeadingUse reads a leading USE clause with the one USE grammar
// (ParseUseClause). inSubquery is true for a CALL { } body.
func parseLeadingUse(query string, inSubquery bool) (UseClause, string, bool, error) {
	return ParseUseClause(query, inSubquery)
}

// planTarget is the graph a planned fragment runs on: a static graph name,
// or a dynamic reference resolved each time the fragment runs.
type planTarget struct {
	name    string
	dynamic *UseClause
}

// useTarget is the plan target a USE clause selects.
func useTarget(clause UseClause) planTarget {
	if clause.IsDynamic() {
		c := clause
		return planTarget{dynamic: &c}
	}
	return planTarget{name: clause.Name}
}

// newExec is an executable fragment running query on target; a dynamic
// target may name the constituents of the composite scope.
func newExec(query string, target planTarget, scope string) *FragmentExec {
	exec := &FragmentExec{
		Input:     &FragmentInit{Columns: nil},
		Query:     query,
		GraphName: target.name,
		Columns:   ReadQueryStructure(query).ResultColumns(), // stars are resolved at execution
		IsWrite:   queryIsWrite(query),
	}
	if target.dynamic != nil {
		exec.Graph = target.dynamic
		exec.Scope = scope
	}
	return exec
}

// extractTopLevelCallBlocks finds CALL { ... } blocks in the current query scope.
func extractTopLevelCallBlocks(query string) ([]callSubqueryBlock, error) {
	structure := ReadQueryStructure(query)
	if structure.Err != nil {
		return nil, structure.Err
	}
	var blocks []callSubqueryBlock
	for _, clause := range structure.Clauses {
		if clause.Kind != "CALL" || clause.Nested == nil {
			continue
		}
		blocks = append(blocks, clause)
	}
	return blocks, nil
}

func callBlockContainsFabricUse(body string) (bool, error) {
	_, _, hasUse, err := parseLeadingUse(body, true)
	if err != nil {
		return false, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasUse {
		return true, nil
	}
	_, _, hasWithUse, err := parseLeadingWithUse(body)
	if err != nil {
		return false, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasWithUse {
		return true, nil
	}
	if _, _, ok := splitAtTopLevelUse(body); ok {
		return true, nil
	}

	nested, err := extractTopLevelCallBlocks(body)
	if err != nil {
		return false, err
	}
	for _, block := range nested {
		found, err := callBlockContainsFabricUse(block.Body)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// findMatchingBrace finds the position of the closing } matching the { at pos.
// Handles nested braces, string literals, and comments.
func extractWithImports(body string) []string {
	return ReadQueryStructure(body).ImportNames()
}

func queryIsWrite(query string) bool {
	return ReadQueryStructure(query).Writes()
}

// startsWithFold checks if s starts with prefix (case-insensitive).
func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
