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
	catalog *Catalog
}

func splitTopLevelUnion(query string) ([]string, []bool, bool, error) {
	parts := make([]string, 0, 2)
	ops := make([]bool, 0, 1)
	start := 0
	inSingleQuote := false
	inDoubleQuote := false
	braceDepth := 0
	parenDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}

		switch ch {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if braceDepth != 0 || parenDepth != 0 {
			continue
		}

		if !keywordAt(query, i, "UNION") {
			continue
		}

		part := strings.TrimSpace(query[start:i])
		if part == "" {
			return nil, nil, false, fmt.Errorf("invalid UNION: empty branch")
		}
		parts = append(parts, part)

		i += len("UNION")
		for i < len(query) && isWhitespace(query[i]) {
			i++
		}
		distinct := true
		if keywordAt(query, i, "ALL") {
			distinct = false
			i += len("ALL")
		}
		ops = append(ops, distinct)
		start = i
		i--
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

func keywordAt(s string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(s) {
		return false
	}
	if !strings.EqualFold(s[idx:idx+len(keyword)], keyword) {
		return false
	}
	if idx > 0 && isIdentChar(s[idx-1]) {
		return false
	}
	after := idx + len(keyword)
	if after < len(s) && isIdentChar(s[after]) {
		return false
	}
	return true
}

// NewFabricPlanner creates a planner backed by the given catalog.
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
	session := planTarget{name: sessionDB}
	scope := compositeScopeRoot(sessionDB)

	// Handle top-level UNION / UNION ALL by planning each branch independently.
	parts, ops, hasUnion, err := splitTopLevelUnion(trimmed)
	if err != nil {
		return nil, err
	}
	if hasUnion {
		lhs, err := p.planSingleQuery(parts[0], session, scope, false)
		if err != nil {
			return nil, err
		}
		root := lhs
		for i := 1; i < len(parts); i++ {
			rhs, err := p.planSingleQuery(parts[i], session, scope, false)
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

	return p.planSingleQuery(trimmed, session, scope, false)
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
		isFabricBlock, err := callBlockContainsFabricUse(block.body)
		if err != nil {
			return nil, err
		}
		if isFabricBlock {
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
				Input: &FragmentApply{
					Input:   &FragmentInit{Columns: nil},
					Inner:   prefixExec,
					Columns: nil,
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
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	parenDepth := 0
	braceDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]

		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
			continue
		case '"':
			inDoubleQuote = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			continue
		}

		if parenDepth != 0 || braceDepth != 0 || bracketDepth != 0 {
			continue
		}
		if !keywordAt(query, i, "USE") {
			continue
		}
		// Leading USE is already handled by parseLeadingUse.
		if strings.TrimSpace(query[:i]) == "" {
			continue
		}
		return query[:i], query[i:], true
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
	if startsWithFold(trimmed, "WITH ") {
		if aliases := trailingWithAliases(trimmed); len(aliases) > 0 {
			return trimmed + " RETURN " + strings.Join(aliases, ", ")
		}
	}
	return trimmed + " RETURN *"
}

func hasTopLevelReturnClause(query string) bool {
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	parenDepth := 0
	braceDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
			continue
		case '"':
			inDoubleQuote = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			continue
		}
		if parenDepth != 0 || braceDepth != 0 || bracketDepth != 0 {
			continue
		}
		if keywordAt(query, i, "RETURN") {
			return true
		}
	}
	return false
}

func trailingWithAliases(query string) []string {
	lastWith := -1
	inSingleQuote := false
	inDoubleQuote := false
	inBacktick := false
	parenDepth := 0
	braceDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch {
		case inSingleQuote:
			if ch == '\'' {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		case inDoubleQuote:
			if ch == '"' {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingleQuote = true
			continue
		case '"':
			inDoubleQuote = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			parenDepth++
			continue
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			continue
		}
		if parenDepth != 0 || braceDepth != 0 || bracketDepth != 0 {
			continue
		}
		if keywordAt(query, i, "WITH") {
			lastWith = i
		}
	}
	if lastWith < 0 {
		return nil
	}

	clause := strings.TrimSpace(query[lastWith+len("WITH"):])
	if clause == "" {
		return nil
	}
	parts := splitTopLevelCSV(clause)
	aliases := make([]string, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			continue
		}
		if idx := lastAsIndexFold(item); idx >= 0 {
			alias := strings.TrimSpace(item[idx+4:])
			if isValidIdentifier(alias) {
				aliases = append(aliases, alias)
			}
			continue
		}
		if isValidIdentifier(item) {
			aliases = append(aliases, item)
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
	init := &FragmentInit{Columns: nil}
	var currentInput Fragment = init
	lastPos := 0

	for _, block := range blocks {
		// Preserve outer query segments before each CALL block.
		prefix := strings.TrimSpace(fullQuery[lastPos:block.startPos])
		if prefix != "" {
			currentInput = &FragmentApply{Input: currentInput, Inner: newExec(ensureRowProducingPrefix(prefix), top, scope), Columns: nil}
		}

		subUse, subBody, hasUse, err := parseLeadingUse(block.body, true)
		if err == nil && !hasUse {
			subUse, subBody, hasUse, err = parseLeadingWithUse(block.body)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid USE in CALL subquery: %w", err)
		}

		var (
			subqueryFragment Fragment
			importCols       []string
		)
		if hasUse {
			sub := useTarget(subUse)
			if err := p.validatePlanTarget(scope, sub); err != nil {
				return nil, err
			}
			subqueryFragment, err = p.planSingleQuery(subBody, sub, scope, true)
			if err != nil {
				return nil, err
			}
			importCols = extractWithImports(subBody)
		} else {
			subqueryFragment, err = p.planSingleQuery(block.body, top, scope, true)
			if err != nil {
				return nil, err
			}
			importCols = extractWithImports(block.body)
		}
		subqueryFragment = bindLeadingImportColumns(subqueryFragment, importCols)

		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   subqueryFragment,
			Columns: nil, // determined at execution time
		}
		lastPos = block.endPos
	}

	// Preserve trailing outer query clauses after the final CALL block.
	trailingQuery := strings.TrimSpace(fullQuery[lastPos:])
	if trailingQuery != "" {
		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   newExec(trailingQuery, top, scope),
			Columns: nil,
		}
	}

	return currentInput, nil
}

// parseLeadingWithUse extracts a leading WITH ... USE <graph> pattern from a CALL body.
// It returns the USE target graph and a rewritten body where USE is removed but the WITH
// import clause is preserved (e.g. "WITH x USE db MATCH ..." -> "WITH x MATCH ...").
func parseLeadingWithUse(body string) (use UseClause, rewritten string, ok bool, err error) {
	trimmed := strings.TrimSpace(body)
	if !startsWithFold(trimmed, "WITH") {
		return UseClause{}, body, false, nil
	}

	withEnd, found := findLeadingWithClauseEnd(trimmed)
	if !found || withEnd <= 0 || withEnd >= len(trimmed) {
		return UseClause{}, body, false, nil
	}

	withClause := strings.TrimSpace(trimmed[:withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	if !startsWithFold(rest, "USE") {
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

// callSubqueryBlock represents a top-level CALL { ... } block for a single scope.
type callSubqueryBlock struct {
	// body is the Cypher body inside the CALL block (without the CALL { } wrapper).
	body string

	// startPos is the byte offset in the original query where CALL { starts.
	startPos int

	// endPos is the byte offset after the closing }.
	endPos int
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
		Columns:   nil, // determined at execution time
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
	var blocks []callSubqueryBlock
	inSingleQuote := false
	inDoubleQuote := false
	braceDepth := 0
	parenDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}

		if ch == '/' && i+1 < len(query) && query[i+1] == '/' {
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			i += 2
			for i+1 < len(query) {
				if query[i] == '*' && query[i+1] == '/' {
					i++
					break
				}
				i++
			}
			continue
		}

		switch ch {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if braceDepth != 0 || parenDepth != 0 {
			continue
		}
		if !keywordAt(query, i, "CALL") {
			continue
		}

		j := i + len("CALL")
		for j < len(query) && isWhitespace(query[j]) {
			j++
		}
		if j >= len(query) || query[j] != '{' {
			continue
		}

		closePos, err := findMatchingBrace(query, j)
		if err != nil {
			return nil, fmt.Errorf("unmatched brace in CALL subquery at position %d: %w", i, err)
		}
		body := strings.TrimSpace(query[j+1 : closePos])
		blocks = append(blocks, callSubqueryBlock{
			body:     body,
			startPos: i,
			endPos:   closePos + 1,
		})
		i = closePos
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
		found, err := callBlockContainsFabricUse(block.body)
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
func findMatchingBrace(s string, pos int) (int, error) {
	if pos >= len(s) || s[pos] != '{' {
		return -1, fmt.Errorf("expected '{' at position %d", pos)
	}

	depth := 1
	inSingleQuote := false
	inDoubleQuote := false

	for i := pos + 1; i < len(s); i++ {
		ch := s[i]

		// Handle string literals (skip brace counting inside strings).
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				// Check for escaped quote.
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}

		if inSingleQuote || inDoubleQuote {
			continue
		}

		// Handle line comments (// ...).
		if ch == '/' && i+1 < len(s) && s[i+1] == '/' {
			// Skip to end of line.
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}

		// Handle block comments (/* ... */).
		if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
			i += 2
			for i+1 < len(s) {
				if s[i] == '*' && s[i+1] == '/' {
					i++
					break
				}
				i++
			}
			continue
		}

		if ch == '{' {
			depth++
		} else if ch == '}' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}

	return -1, fmt.Errorf("unmatched brace (depth=%d remaining)", depth)
}

// extractWithImports parses a leading WITH clause to extract imported variable names.
// e.g. "WITH translationId MATCH ..." returns ["translationId"]
func extractWithImports(body string) []string {
	trimmed := strings.TrimSpace(body)
	if !startsWithFold(trimmed, "WITH") {
		return nil
	}

	// Must be followed by whitespace.
	if len(trimmed) > 4 && !isWhitespace(trimmed[4]) {
		return nil
	}

	rest := strings.TrimSpace(trimmed[4:])

	// Extract identifiers until we hit a keyword (MATCH, RETURN, CREATE, etc.).
	var imports []string
	parts := strings.FieldsFunc(rest, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})

	for _, part := range parts {
		cleaned := strings.TrimSpace(part)
		if cleaned == "" {
			continue
		}
		if isWithImportStopKeyword(cleaned) {
			break
		}
		// Strip AS alias if present.
		if strings.EqualFold(cleaned, "AS") {
			continue
		}
		imports = append(imports, cleaned)
	}

	return imports
}

func isWithImportStopKeyword(token string) bool {
	switch {
	case strings.EqualFold(token, "MATCH"),
		strings.EqualFold(token, "OPTIONAL"),
		strings.EqualFold(token, "CREATE"),
		strings.EqualFold(token, "MERGE"),
		strings.EqualFold(token, "DELETE"),
		strings.EqualFold(token, "DETACH"),
		strings.EqualFold(token, "SET"),
		strings.EqualFold(token, "REMOVE"),
		strings.EqualFold(token, "RETURN"),
		strings.EqualFold(token, "WITH"),
		strings.EqualFold(token, "WHERE"),
		strings.EqualFold(token, "ORDER"),
		strings.EqualFold(token, "SKIP"),
		strings.EqualFold(token, "LIMIT"),
		strings.EqualFold(token, "UNWIND"),
		strings.EqualFold(token, "CALL"),
		strings.EqualFold(token, "FOREACH"),
		strings.EqualFold(token, "LOAD"),
		strings.EqualFold(token, "USE"):
		return true
	default:
		return false
	}
}

// queryIsWrite performs a simple heuristic check for write operations.
func queryIsWrite(query string) bool {
	for i := 0; i < len(query); i++ {
		if hasKeywordAt(query, i, "CREATE") ||
			hasKeywordAt(query, i, "MERGE") ||
			hasKeywordAt(query, i, "DETACH DELETE") ||
			hasKeywordAt(query, i, "DELETE") ||
			hasKeywordAt(query, i, "SET") ||
			hasKeywordAt(query, i, "REMOVE") {
			return true
		}
	}
	return false
}

// startsWithFold checks if s starts with prefix (case-insensitive).
func startsWithFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func containsFold(s, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(needle); i++ {
		if strings.EqualFold(s[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}
