package fabric

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)

// FabricPlanner decomposes a Cypher query into a Fragment tree by splitting
// at USE-clause boundaries. Without USE clauses, it produces a single
// FragmentExec targeting the session database — identical to community behavior.
//
// This mirrors Neo4j's FabricPlanner.scala.
type FabricPlanner struct {
	catalog *Catalog
	// keywordIsName is the Cypher executor's reader of a clause keyword that is
	// a name where it stands; nil reads every keyword as a clause.
	keywordIsName ClauseKeywordNameReader
	// callImports is the Cypher executor's reader of the variables a CALL
	// subquery imports; nil imports none.
	callImports CallImportReader
}

// CallImportReader returns the variables a CALL subquery imports from its
// enclosing query ("*" for every variable): the variables its scope clause
// lists when scoped (scope is the text between CALL's parentheses), else the
// ones its body's importing WITH lists (body is one UNION branch, without its
// USE clause).
//
// An error, such as a scope clause item that is not a variable, fails the
// plan.
type CallImportReader func(scope string, scoped bool, body string) ([]string, error)

// SetCallImportReader installs the Cypher executor's CALL subquery import
// reader, so Fabric imports exactly the variables a single-database CALL
// would.
func (p *FabricPlanner) SetCallImportReader(read CallImportReader) {
	p.callImports = read
}

// readCallImports applies the installed CallImportReader.
func (p *FabricPlanner) readCallImports(scope string, scoped bool, body string) ([]string, error) {
	if p.callImports == nil {
		return nil, nil
	}
	return p.callImports(scope, scoped, body)
}

// ClauseKeywordNameReader reports whether the clause keyword at text[pos:end]
// is a name there: a variable, parameter, property key or label spelled like
// the keyword (WITH 1 AS union, $union, n.optional).
type ClauseKeywordNameReader func(text string, pos, end int, keyword string) bool

// SetClauseKeywordNameReader installs the reader the Cypher executor uses for
// its own statements, so Fabric reads UNION and OPTIONAL CALL where they do.
func (p *FabricPlanner) SetClauseKeywordNameReader(read ClauseKeywordNameReader) {
	p.keywordIsName = read
}

// clauseKeywordAt reports whether keyword starts a clause at text[idx].
func clauseKeywordAt(text string, idx int, keyword string, isName ClauseKeywordNameReader) bool {
	return keywordAt(text, idx, keyword) && (isName == nil || !isName(text, idx, idx+len(keyword), keyword))
}

func splitTopLevelUnion(query string, isName ClauseKeywordNameReader) ([]string, []bool, bool, error) {
	var parts []string
	var ops []bool
	start := 0
	braceDepth := 0
	parenDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		ch := query[i]

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
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		}
		if braceDepth != 0 || parenDepth != 0 || bracketDepth != 0 {
			continue
		}

		if !clauseKeywordAt(query, i, "UNION", isName) {
			continue
		}

		part := strings.TrimSpace(query[start:i])
		if part == "" {
			return nil, nil, false, fmt.Errorf("invalid UNION: empty branch")
		}
		parts = append(parts, part)

		i += len("UNION")
		i = skipSpacesAndComments(query, i)
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
	for _, distinct := range ops[1:] {
		if distinct != ops[0] {
			return nil, nil, false, ErrMixedUnion
		}
	}
	parts = append(parts, last)
	return parts, ops, true, nil
}

// ErrMixedUnion is returned for a query that combines UNION and UNION ALL at
// one level. Cypher rejects it before any branch is planned or runs.
var ErrMixedUnion = errors.New("UNION and UNION ALL cannot be combined in the same query")

func keywordAt(s string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(s) {
		return false
	}
	if !strings.EqualFold(s[idx:idx+len(keyword)], keyword) {
		return false
	}
	// A word after '$' is a parameter name ($union, $use), never a clause.
	if idx > 0 && (cyphertext.NamePartAt(s, idx-1) || s[idx-1] == '$') {
		return false
	}
	after := idx + len(keyword)
	if after < len(s) && cyphertext.NamePartAt(s, after) {
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
//   - stage: RawSource for client text; AcceptedSource for retained compiler text
//   - sessionDB: the default database for the session (used when no USE clause is present)
//
// Returns a Fragment tree of accepted source ready for FabricExecutor.
// RawSource input is projected once before clause and UNION decomposition.
func (p *FabricPlanner) Plan(ctx context.Context, query string, sessionDB string, stage SourceStage) (Fragment, error) {
	if err := bulk.ObserveContext(ctx); err != nil {
		return nil, err
	}
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return nil, source.Err
	}
	query = source.Text
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}
	session := planTarget{name: sessionDB}
	scope := compositeScopeRoot(sessionDB)
	return p.planQuery(ctx, trimmed, session, scope, false)
}

// planQuery is the entry for every statement and CALL body. Split UNION before
// selecting a branch's USE target so one branch cannot absorb another's text or
// graph scope. CALL import binding remains a per-branch responsibility.
func (p *FabricPlanner) planQuery(ctx context.Context, trimmed string, current planTarget, scope string, inSubquery bool) (Fragment, error) {
	if err := bulk.ObserveContext(ctx); err != nil {
		return nil, err
	}
	parts, ops, hasUnion, err := splitTopLevelUnion(trimmed, p.keywordIsName)
	if err != nil {
		return nil, err
	}
	if hasUnion {
		lhs, err := p.planQuery(ctx, parts[0], current, scope, inSubquery)
		if err != nil {
			return nil, err
		}
		root := lhs
		for i := 1; i < len(parts); i++ {
			rhs, err := p.planQuery(ctx, parts[i], current, scope, inSubquery)
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
		return p.planCallBranch(ctx, trimmed, current, scope)
	}
	return p.planSingleQuery(ctx, trimmed, current, scope, false)
}

// planSingleQuery plans one query (no top-level UNION) that runs on
// current unless it starts with its own USE clause. scope is the session's
// composite database; inSubquery is true for a CALL { } body.
func (p *FabricPlanner) planSingleQuery(ctx context.Context, trimmed string, current planTarget, scope string, inSubquery bool) (Fragment, error) {
	// Extract leading USE clause if present.
	top := current
	remaining := trimmed
	use, rest, hasTopUse, err := parseLeadingUse(ctx, trimmed, inSubquery)
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
	callBlocks, err := extractTopLevelCallBlocks(remaining, p.keywordIsName)
	if err != nil {
		return nil, err
	}
	fabricBlocks := make([]callSubqueryBlock, 0, len(callBlocks))
	for _, block := range callBlocks {
		isFabricBlock, err := callBlockContainsFabricUse(ctx, block.body, p.keywordIsName)
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
		if prefix, rest, ok := splitAtTopLevelUse(remaining, p.keywordIsName); ok {
			subUse, subRest, hasUse, err := parseLeadingUse(ctx, rest, true)
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
			inner, err := p.planQuery(ctx, subRest, sub, scope, true)
			if err != nil {
				return nil, err
			}
			prefix = strings.TrimSpace(prefix)
			if prefix == "" {
				return inner, nil
			}
			prefixExec := newExec(ensureRowProducingPrefix(prefix, p.keywordIsName), top, scope)
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
	return p.planMultiGraph(ctx, top, scope, remaining, fabricBlocks)
}

// SingleGraphStatement is query with every USE clause the planner reads
// removed: a statement's or a UNION branch's leading USE, one later in the
// statement, and a CALL body's leading USE, after the body's importing WITH
// too. It is the statement as one graph runs it, which is how Neo4j checks a
// composite statement's variable scopes, whichever graphs its parts run on.
func (p *FabricPlanner) SingleGraphStatement(ctx context.Context, query string, stage SourceStage) (string, error) {
	if err := bulk.ObserveContext(ctx); err != nil {
		return "", err
	}
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return "", source.Err
	}
	query = source.Text
	return p.singleGraphStatement(ctx, strings.TrimSpace(query), false)
}

func (p *FabricPlanner) singleGraphStatement(ctx context.Context, query string, inSubquery bool) (string, error) {
	parts, ops, hasUnion, err := splitTopLevelUnion(query, p.keywordIsName)
	if err != nil {
		return "", err
	}
	if hasUnion {
		var b strings.Builder
		for i, part := range parts {
			branch, err := p.singleGraphStatement(ctx, strings.TrimSpace(part), inSubquery)
			if err != nil {
				return "", err
			}
			if i > 0 {
				if ops[i-1] {
					b.WriteString(" UNION ")
				} else {
					b.WriteString(" UNION ALL ")
				}
			}
			b.WriteString(branch)
		}
		return b.String(), nil
	}
	_, rest, hasUse, err := parseLeadingUse(ctx, query, inSubquery)
	if err == nil && !hasUse && inSubquery {
		_, rest, hasUse, err = parseLeadingWithUse(ctx, query, p.keywordIsName)
	}
	if err != nil {
		return "", err
	}
	if hasUse {
		query = strings.TrimSpace(rest)
	}
	if prefix, after, ok := splitAtTopLevelUse(query, p.keywordIsName); ok {
		_, rest, hasUse, err := parseLeadingUse(ctx, after, true)
		if err != nil {
			return "", err
		}
		if hasUse {
			tail, err := p.singleGraphStatement(ctx, strings.TrimSpace(rest), true)
			if err != nil {
				return "", err
			}
			query = strings.TrimSpace(prefix) + " " + tail
		}
	}
	blocks, err := extractTopLevelCallBlocks(query, p.keywordIsName)
	if err != nil || len(blocks) == 0 {
		return query, err
	}
	var b strings.Builder
	last := 0
	for _, block := range blocks {
		body, err := p.singleGraphStatement(ctx, block.body, true)
		if err != nil {
			return "", err
		}
		b.WriteString(query[last : block.open+1])
		b.WriteString(" ")
		b.WriteString(body)
		b.WriteString(" }")
		last = block.endPos
	}
	b.WriteString(query[last:])
	return b.String(), nil
}

// validatePlanTarget checks a static USE target at plan time; a dynamic
// reference is checked when it resolves (FabricExecutor.resolveExecGraph).
func (p *FabricPlanner) validatePlanTarget(scope string, target planTarget) error {
	if target.dynamic != nil {
		return nil
	}
	return p.validateUseTarget(scope, target.name)
}

func ensureRowProducingPrefix(query string, isName ClauseKeywordNameReader) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return trimmed
	}
	if hasTopLevelReturnClause(trimmed, isName) {
		return trimmed
	}
	if startsWithFold(trimmed, "WITH ") {
		if aliases := trailingWithAliases(trimmed, isName); len(aliases) > 0 {
			return trimmed + " RETURN " + strings.Join(aliases, ", ")
		}
	}
	return trimmed + " RETURN *"
}

func hasTopLevelReturnClause(query string, isName ClauseKeywordNameReader) bool {
	parenDepth := 0
	braceDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		ch := query[i]
		switch ch {
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
		if clauseKeywordAt(query, i, "RETURN", isName) {
			return true
		}
	}
	return false
}

func trailingWithAliases(query string, isName ClauseKeywordNameReader) []string {
	lastWith := -1
	parenDepth := 0
	braceDepth := 0
	bracketDepth := 0

	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		ch := query[i]
		switch ch {
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
		if clauseKeywordAt(query, i, "WITH", isName) {
			lastWith = i
		}
	}
	if lastWith < 0 {
		return nil
	}
	// The WITH's aliases are the prefix's columns only when it is the
	// prefix's last clause, its WHERE, ORDER BY, SKIP and LIMIT aside; a
	// later clause (UNWIND, MATCH, ...) keeps them and adds its own.
	withEnd := nextTopLevelClause(query, lastWith+len("WITH"), isName)
	for end := withEnd; end < len(query); end = nextTopLevelClause(query, end+1, isName) {
		if !projectionModifierAt(query, end) {
			return nil
		}
	}

	clause := strings.TrimSpace(query[lastWith+len("WITH") : withEnd])
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
			if token, ok := cyphertext.ScanIdentifier(alias, 0); ok && token.End == len(alias) {
				aliases = append(aliases, alias)
			}
			continue
		}
		if token, ok := cyphertext.ScanIdentifier(item, 0); ok && token.End == len(item) {
			aliases = append(aliases, item)
		}
	}
	return aliases
}

// planMultiGraph builds a Fragment tree for queries with top-level CALL {} subqueries.
// Each CALL block is planned recursively so nested USE variants are decomposed correctly.
func (p *FabricPlanner) planMultiGraph(ctx context.Context, top planTarget, scope string, fullQuery string, blocks []callSubqueryBlock) (Fragment, error) {
	init := &FragmentInit{Columns: nil}
	var currentInput Fragment = init
	lastPos := 0

	for _, block := range blocks {
		// Preserve outer query segments before each CALL block.
		prefix := strings.TrimSpace(fullQuery[lastPos:block.startPos])
		if prefix != "" {
			currentInput = &FragmentApply{Input: currentInput, Inner: newExec(ensureRowProducingPrefix(prefix, p.keywordIsName), top, scope), Projection: true, Columns: nil}
		}

		subqueryFragment, err := p.planQuery(ctx, block.body, top, scope, true)
		if err != nil {
			return nil, err
		}

		var scope []string
		if block.scoped {
			// The scope clause, not a leading WITH, imports the body's variables,
			// and they stay visible throughout the body.
			imports, err := p.readCallImports(block.scope, true, block.body)
			if err != nil {
				return nil, err
			}
			scope = imports
			subqueryFragment = bindScopeImports(bindLeadingImportColumns(subqueryFragment, scope), scope)
		}
		currentInput = &FragmentApply{
			Input:    currentInput,
			Inner:    subqueryFragment,
			Optional: block.optional,
			Scoped:   block.scoped,
			Scope:    scope,
			Unit:     !callBodyReturns(block.body, p.keywordIsName),
			Columns:  nil, // determined at execution time
		}
		lastPos = block.endPos
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
func (p *FabricPlanner) planCallBranch(ctx context.Context, body string, top planTarget, scope string) (Fragment, error) {
	subUse, subBody, hasUse, err := parseLeadingUse(ctx, body, true)
	if err == nil && !hasUse {
		subUse, subBody, hasUse, err = parseLeadingWithUse(ctx, body, p.keywordIsName)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasUse {
		sub := useTarget(subUse)
		if err := p.validatePlanTarget(scope, sub); err != nil {
			return nil, err
		}
		fragment, err := p.planSingleQuery(ctx, subBody, sub, scope, true)
		if err != nil {
			return nil, err
		}
		imports, err := p.readCallImports("", false, subBody)
		if err != nil {
			return nil, err
		}
		return bindLeadingImportColumns(fragment, imports), nil
	}
	fragment, err := p.planSingleQuery(ctx, body, top, scope, true)
	if err != nil {
		return nil, err
	}
	imports, err := p.readCallImports("", false, body)
	if err != nil {
		return nil, err
	}
	return bindLeadingImportColumns(fragment, imports), nil
}

// parseLeadingWithUse extracts a leading WITH ... USE <graph> pattern from a CALL body.
// It returns the USE target graph and a rewritten body where USE is removed but the WITH
// import clause is preserved (e.g. "WITH x USE db MATCH ..." -> "WITH x MATCH ...").
func parseLeadingWithUse(ctx context.Context, body string, isName ClauseKeywordNameReader) (use UseClause, rewritten string, ok bool, err error) {
	trimmed := strings.TrimSpace(body)
	if !startsWithFold(trimmed, "WITH") {
		return UseClause{}, body, false, nil
	}

	withEnd, found := findLeadingWithClauseEnd(trimmed, isName)
	if !found || withEnd <= 0 || withEnd >= len(trimmed) {
		return UseClause{}, body, false, nil
	}

	withClause := strings.TrimSpace(trimmed[:withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	use, remaining, hasUse, parseErr := parseLeadingUse(ctx, rest, true)
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

	// open is the byte offset of the body's opening brace.
	open int

	// endPos is the byte offset after the closing }.
	endPos int

	// optional is true for OPTIONAL CALL { }; startPos is then OPTIONAL's.
	optional bool

	// scoped is true for CALL (...) { }; scope is its scope clause's text,
	// between the parentheses.
	scoped bool
	scope  string
}

func (p *FabricPlanner) validateUseTarget(sessionDB string, targetDB string) error {
	target := targetDB
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
	if strings.Contains(target, ".") && p.inCompositeScope(sessionDB) && scopeRoot != "" && !cyphertext.NameEqualFold(scopeRoot, targetRoot) {
		return fmt.Errorf("invalid USE target '%s': target is out of scope for composite '%s'", target, scopeRoot)
	}
	return nil
}

func (p *FabricPlanner) inCompositeScope(sessionDB string) bool {
	db := sessionDB
	if db == "" {
		return false
	}
	if strings.Contains(db, ".") {
		return true
	}
	if p.catalog == nil {
		return false
	}
	prefix := cyphertext.NameLower(db) + "."
	return p.catalog.HasGraphWithPrefix(prefix)
}

func compositeScopeRoot(graph string) string {
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

// bindScopeImports marks every executable fragment of a scoped CALL body
// with the variables its scope clause imports: the body's leading fragments
// and its continuations, but not a nested CALL's body, which imports its own.
func bindScopeImports(fragment Fragment, scope []string) Fragment {
	if len(scope) == 0 || fragment == nil {
		return fragment
	}
	switch f := fragment.(type) {
	case *FragmentExec:
		copied := *f
		copied.ScopeImports = scope
		return &copied
	case *FragmentApply:
		copied := *f
		copied.Input = bindScopeImports(copied.Input, scope)
		if copied.Projection {
			copied.Inner = bindScopeImports(copied.Inner, scope)
		}
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = bindScopeImports(copied.LHS, scope)
		copied.RHS = bindScopeImports(copied.RHS, scope)
		return &copied
	default:
		return fragment
	}
}

// parseLeadingUse reads a leading USE clause with the one USE grammar
// (ParseUseClause). inSubquery is true for a CALL { } body.
func parseLeadingUse(ctx context.Context, query string, inSubquery bool) (UseClause, string, bool, error) {
	return ParseUseClause(ctx, query, inSubquery, cyphertext.AcceptedSource)
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

// extractTopLevelCallBlocks finds CALL { ... } and OPTIONAL CALL { ... }
// blocks in the current query scope.
func extractTopLevelCallBlocks(query string, isName ClauseKeywordNameReader) ([]callSubqueryBlock, error) {
	var blocks []callSubqueryBlock
	braceDepth := 0
	parenDepth := 0

	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		ch := query[i]

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
		start, optional := i, false
		if clauseKeywordAt(query, i, "OPTIONAL", isName) {
			i = skipSpacesAndComments(query, i+len("OPTIONAL"))
			optional = true
		}
		if !keywordAt(query, i, "CALL") {
			i = start
			continue
		}

		j := skipSpacesAndComments(query, i+len("CALL"))
		scoped, scope := false, ""
		if j < len(query) && query[j] == '(' {
			scopeEnd, ok := matchingParen(query, j)
			if !ok {
				i = start
				continue
			}
			scoped, scope = true, query[j+1:scopeEnd]
			j = skipSpacesAndComments(query, scopeEnd+1)
		}
		if j >= len(query) || query[j] != '{' {
			i = start
			continue
		}

		closePos, err := findMatchingBrace(query, j)
		if err != nil {
			return nil, fmt.Errorf("unmatched brace in CALL subquery at position %d: %w", i, err)
		}
		body := strings.TrimSpace(query[j+1 : closePos])
		blocks = append(blocks, callSubqueryBlock{
			body:     body,
			open:     j,
			startPos: start,
			endPos:   closePos + 1,
			optional: optional,
			scoped:   scoped,
			scope:    scope,
		})
		i = closePos
	}

	return blocks, nil
}

// callBodyReturns reports whether a CALL body has a top-level RETURN; one
// without is a unit subquery (it ends in an update or FINISH).
func callBodyReturns(body string, isName ClauseKeywordNameReader) bool {
	depth := 0
	for i := 0; i < len(body); i++ {
		if next, quoted := cyphertext.SkipToken(body, i); quoted {
			i = next - 1
			continue
		}
		switch body[i] {
		case '{', '(', '[':
			depth++
		case '}', ')', ']':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && clauseKeywordAt(body, i, "RETURN", isName) {
				return true
			}
		}
	}
	return false
}

func callBlockContainsFabricUse(ctx context.Context, body string, isName ClauseKeywordNameReader) (bool, error) {
	_, _, hasUse, err := parseLeadingUse(ctx, body, true)
	if err != nil {
		return false, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasUse {
		return true, nil
	}
	_, _, hasWithUse, err := parseLeadingWithUse(ctx, body, isName)
	if err != nil {
		return false, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasWithUse {
		return true, nil
	}
	if _, _, ok := splitAtTopLevelUse(body, isName); ok {
		return true, nil
	}

	nested, err := extractTopLevelCallBlocks(body, isName)
	if err != nil {
		return false, err
	}
	for _, block := range nested {
		found, err := callBlockContainsFabricUse(ctx, block.body, isName)
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

	for i := pos + 1; i < len(s); i++ {
		if next, quoted := cyphertext.SkipToken(s, i); quoted {
			i = next - 1
			continue
		}
		ch := s[i]

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

// queryIsWrite performs a simple heuristic check for write operations.
// Strings, quoted names and comments are skipped; keywords there are text.
func queryIsWrite(query string) bool {
	for i := 0; i < len(query); i++ {
		if next, quoted := cyphertext.SkipToken(query, i); quoted {
			i = next - 1
			continue
		}
		if keywordAt(query, i, "CREATE") ||
			keywordAt(query, i, "MERGE") ||
			keywordAt(query, i, "DETACH DELETE") ||
			keywordAt(query, i, "DELETE") ||
			keywordAt(query, i, "SET") ||
			keywordAt(query, i, "REMOVE") {
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
