package cypher

import (
	"maps"
	"sort"
	"strings"
)

// bodyScope is what one query body (a CALL body, or the body of an
// EXISTS / COUNT / COLLECT subquery) reads from the scope around it and
// declares itself. It is static: names only, never values. It is the one
// owner of that question (#1317 S08): the runtime CALL imports, the static
// CALL scope, the importing-WITH check, the correlated-body shortcuts and
// Fabric read it here, from the clause walk that validates the body
// (semanticScopeWalk, through a scopeRecorder).
type bodyScope struct {
	// Reads are the enclosing names the body reads, at any depth, in the
	// order it first reads them.
	Reads []string
	// Branches has one entry per top-level UNION branch (one without UNION).
	Branches []branchScope
}

// patternPartEnd excludes a pattern clause's WHERE and MERGE actions from
// the pattern declarations recorded by the shared body scope walk.
func patternPartEnd(clause pipelineClause) int {
	end := len(clause.text)
	for _, keyword := range [...]string{"WHERE", "ON"} {
		if index := topLevelKeywordIndex(clause.text, keyword); index > 0 && index < end {
			end = index
		}
	}
	return end
}

// branchScope is one UNION branch of a body.
type branchScope struct {
	// Reads are the enclosing names this branch reads.
	Reads []string
	// Declares are the names this branch binds before any read of them: a
	// new variable there, even when the enclosing scope has one.
	Declares []string
	// Importing is an unscoped CALL branch's importing WITH list (CALL {
	// WITH a, b … }); nil when the branch has none.
	Importing []string
}

// bodyScopeEntry is a cached bodyScopeOf result.
type bodyScopeEntry struct {
	scope bodyScope
	err   error
}

// bodyScopes caches bodyScopeOf by its key (bodyScopeKey): one walk per
// distinct body, not per row.
var bodyScopes = newBoundedCache[string, bodyScopeEntry](1024, boundedCacheStringShardIndex)

// bodyScopeImplicit marks an unscoped CALL body's key: its visible names
// are the ones it reads.
const bodyScopeImplicit = "\x00implicit"

// bodyScopeKey is bodyScopeOf's cache key: the language version, the
// visible names (sorted) or bodyScopeImplicit, and the body.
func bodyScopeKey(body string, visible []string, cypher25 bool) string {
	var key strings.Builder
	if cypher25 {
		key.WriteString("25\x00")
	} else {
		key.WriteString("5\x00")
	}
	if visible == nil {
		key.WriteString(bodyScopeImplicit)
	} else {
		names := append([]string(nil), visible...)
		sort.Strings(names)
		key.WriteString(strings.Join(names, ","))
	}
	key.WriteString("\x00")
	key.WriteString(body)
	return key.String()
}

// bodyScopeOf is what body reads from the scope around it and declares
// (bodyScope). visible holds the enclosing names the body can see; nil is an
// unscoped CALL { } body, whose visible names are exactly the ones it reads
// (NornicDB's implicit import, gh648): a name it uses only in pattern
// positions is a new variable, as Neo4j gives it for CALL () { … }. A body
// that starts with a pattern (an EXISTS / COUNT body) is read as MATCH.
// The error is the walk's: a body the static checks reject.
func (e *StorageExecutor) bodyScopeOf(body string, visible []string, cypher25 bool) (bodyScope, error) {
	key := bodyScopeKey(body, visible, cypher25)
	if cached, ok := bodyScopes.get(key); ok {
		return cached.scope, cached.err
	}
	scope, err := e.walkBodyScope(body, visible, cypher25)
	bodyScopes.put(key, bodyScopeEntry{scope: scope, err: err})
	return scope, err
}

func (e *StorageExecutor) walkBodyScope(body string, visible []string, cypher25 bool) (bodyScope, error) {
	text := strings.TrimSpace(body)
	if strings.HasPrefix(text, "(") {
		text = "MATCH " + text
	}
	branches := []string{text}
	if union, _, _, ok := parseTopLevelUnionBranches(text); ok && len(union) > 0 {
		branches = union
	}
	scope := bodyScope{Branches: make([]branchScope, len(branches))}
	for index, branch := range branches {
		recorder := &scopeRecorder{implicit: visible == nil, seen: make(typeScope, len(visible)), local: map[string]struct{}{}, branch: &scope.Branches[index], body: &scope}
		for _, name := range visible {
			recorder.seen[name] = staticOperand{}
		}
		if visible == nil {
			names, importing, err := branchLeadingWithImport(branch, func(string) bool { return true }, true)
			if err != nil {
				return scope, err
			}
			if importing {
				scope.Branches[index].Importing = names
			}
		}
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
		if !ok {
			continue
		}
		if _, _, err := e.semanticScopeWalk(clauses, maps.Clone(recorder.seen), cypher25, subqueryScope{visible: recorder.seen, reads: recorder}); err != nil {
			return scope, err
		}
	}
	return scope, nil
}

// scopeRecorder records, as semanticScopeWalk walks a body's clauses, which
// names each clause reads from the enclosing scope and which it declares
// (bodyScopeOf). The body's own recorder has no parent; a nested subquery
// body's (validateSubqueryPatternBindings, callSubqueryReturnTypes) passes
// what it reads beyond its own names up to its parent. All methods accept a
// nil recorder: a walk that records nothing.
type scopeRecorder struct {
	parent *scopeRecorder
	// implicit: a name is visible only once something reads it (an
	// unscoped CALL body).
	implicit bool
	// seen: the body's visible names (given, or read so far when implicit).
	// A nested EXISTS / COUNT / COLLECT body's recorder has none: it sees
	// what its parent sees.
	seen typeScope
	// local: the names this body (branch) has declared so far.
	local map[string]struct{}
	// branch and body: the result, on the body's own recorder only.
	branch *branchScope
	body   *bodyScope
}

// child is a nested EXISTS / COUNT / COLLECT body's (or CALL (*) body's)
// recorder: it sees what r sees at this point, plus locals, the names an
// enclosing expression binds around it (a comprehension's iterator).
func (r *scopeRecorder) child(locals []string) *scopeRecorder {
	if r == nil {
		return nil
	}
	child := &scopeRecorder{parent: r, local: make(map[string]struct{}, len(locals))}
	for _, name := range locals {
		child.local[name] = struct{}{}
	}
	return child
}

// implicitChild is a nested unscoped CALL body's recorder: it sees only the
// names it reads, which it reads from r.
func (r *scopeRecorder) implicitChild() *scopeRecorder {
	if r == nil {
		return nil
	}
	return &scopeRecorder{parent: r, implicit: true, seen: typeScope{}, local: map[string]struct{}{}}
}

// inScope reports whether name is visible at this point of the body.
func (r *scopeRecorder) inScope(name string) bool {
	if r == nil {
		return false
	}
	if _, local := r.local[name]; local {
		return true
	}
	if _, seen := r.seen[name]; seen {
		return true
	}
	if !r.implicit && r.parent != nil {
		return r.parent.inScope(name)
	}
	return false
}

// read records a read of name that the body hasn't declared, and reports
// whether the walk needs an incoming binding for it. Local declarations are
// bound by the clause walk itself, after its checks of the incoming scope.
func (r *scopeRecorder) read(name string) bool {
	if r == nil {
		return false
	}
	if _, local := r.local[name]; local {
		return false
	}
	if r.implicit {
		if _, seen := r.seen[name]; !seen {
			r.seen[name] = staticOperand{}
		}
		if r.parent != nil {
			r.parent.read(name)
			return true
		}
		r.record(name)
		return true
	}
	if r.parent != nil {
		return r.parent.read(name)
	}
	if _, seen := r.seen[name]; !seen {
		// Not visible: an undefined variable, which the walk reports.
		return false
	}
	r.record(name)
	return true
}

// record adds name to the body's reads and its branch's.
func (r *scopeRecorder) record(name string) {
	if r.branch != nil && !containsString(r.branch.Reads, name) {
		r.branch.Reads = append(r.branch.Reads, name)
	}
	if r.body != nil && !containsString(r.body.Reads, name) {
		r.body.Reads = append(r.body.Reads, name)
	}
}

// declare binds name in the body from here on.
func (r *scopeRecorder) declare(name string) {
	if r == nil || name == "" {
		return
	}
	r.local[name] = struct{}{}
	if r.branch != nil && !containsString(r.branch.Reads, name) && !containsString(r.branch.Declares, name) {
		r.branch.Declares = append(r.branch.Declares, name)
	}
}

// pattern records a pattern-position variable: a read when the name is
// visible here, a declaration otherwise.
func (r *scopeRecorder) pattern(name string, visible *[]string) {
	if r.inScope(name) {
		if r.read(name) {
			*visible = append(*visible, name)
		}
		return
	}
	r.declare(name)
}

// clause records one clause of the body: what it reads, then what it
// declares. Its EXISTS / COUNT / COLLECT and CALL bodies are recorded by
// their own walks. It returns the names the clause reads that are visible
// now, for the walk to bind (an unscoped body's implicit imports).
func (r *scopeRecorder) clause(clause pipelineClause) []string {
	if r == nil {
		return nil
	}
	var visible []string
	expression := func(text string) {
		visible = append(visible, r.expression(text)...)
	}
	text := maskSubqueryBodies(clause.text)
	switch clause.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
		masked := pipelineClause{kind: clause.kind, text: text}
		start := clauseKeywordEnd(text)
		end := patternPartEnd(masked)
		if end < start {
			end = start
		}
		pattern := text[start:end]
		bindings := patternVariables(pattern)
		for _, name := range bindings {
			r.pattern(name, &visible)
		}
		visible = append(visible, r.expressionExcept(pattern, bindings)...)
		rest := blankTopLevelKeywords(text[end:], "ON CREATE SET", "ON MATCH SET")
		expression(rest)
	case pipelineClauseWith, pipelineClauseReturn:
		projection, rest := splitWithProjection(text)
		body, _ := cutDistinct(projection[clauseKeywordEnd(projection):])
		var aliases []string
		for _, item := range splitTopLevelComma(body) {
			item = strings.TrimSpace(item)
			if item == "" || item == "*" {
				continue
			}
			expr, alias := parseProjectionExprAlias(item)
			expression(expr)
			aliases = append(aliases, alias)
		}
		for _, alias := range aliases {
			r.declare(alias)
		}
		expression(blankTopLevelKeywords(rest, "ORDER BY", "SKIP", "LIMIT", "OFFSET", "ASCENDING", "DESCENDING", "ASC", "DESC"))
	case pipelineClauseUnwind:
		if source, alias, ok := parsePipelineIteration(text); ok {
			expression(source)
			r.declare(alias)
		}
	case pipelineClauseCall:
		open := strings.IndexByte(text, '(')
		close := -1
		if open >= 0 {
			close = findMatchingParen(text, open)
		}
		if close > open {
			expression(text[open+1 : close])
		} else {
			close = 0
		}
		if yield := parseYieldClause(text); yield != nil && !yield.yieldAll {
			for _, item := range yield.items {
				name := item.name
				if item.alias != "" {
					name = item.alias
				}
				r.declare(name)
			}
		}
		if where := topLevelKeywordIndex(text, "WHERE"); where > close {
			expression(text[where:])
		}
	case pipelineClauseCallSubquery:
		// CALL (a, b) reads a and b; its body sees nothing else. CALL (*)
		// and an unscoped body read what their bodies read (their walks).
		if scope := readCallSubqueryScope(clause.text); scope.scoped {
			for _, name := range scope.names {
				if r.read(name) {
					visible = append(visible, name)
				}
			}
		}
	case pipelineClauseLet:
		projections, _ := parsePipelineLet(text)
		for _, projection := range projections {
			expression(projection.expression)
		}
		for _, projection := range projections {
			r.declare(projection.alias)
		}
	case pipelineClauseFilter, pipelineClauseSet, pipelineClauseRemove:
		expression(text[clauseKeywordEnd(text):])
	case pipelineClauseDelete:
		targets, _, _ := deleteClauseTargets(text)
		for _, target := range targets {
			expression(target)
		}
	case pipelineClauseForeach:
		visible = append(visible, r.foreach(text)...)
	}
	return visible
}

// rest records the clauses a walk stopped before (after YIELD * or
// RETURN *, which name nothing statically): their reads still count.
func (r *scopeRecorder) rest(clauses []pipelineClause) {
	for _, clause := range clauses {
		r.clause(clause)
	}
}

// foreach records FOREACH (x IN list | updates): list is read, x is local
// to the updates.
func (r *scopeRecorder) foreach(text string) []string {
	open := strings.IndexByte(text, '(')
	if open < 0 {
		return nil
	}
	close := findMatchingParen(text, open)
	if close < 0 {
		return nil
	}
	inner := text[open+1 : close]
	bar := topLevelByteIndex(inner, 0, len(inner), '|')
	if bar < 0 {
		return nil
	}
	head := inner[:bar]
	in := topLevelKeywordIndex(head, "IN")
	if in < 0 {
		return nil
	}
	visible := r.expression(head[in+len("IN"):])
	child := r.child([]string{strings.TrimSpace(head[:in])})
	if clauses, ok := splitPipelineClauses(strings.TrimSpace(inner[bar+1:])); ok {
		for _, clause := range clauses {
			visible = append(visible, child.clause(clause)...)
		}
	}
	return visible
}

// expression records the names an expression reads
// (expressionFreeVariables: not its own locals, map or property keys, or
// strings), and in its pattern comprehensions the pattern variables that
// are visible here (an unbound one is the comprehension's own).
func (r *scopeRecorder) expression(text string) []string {
	return r.expressionExcept(text, nil)
}

// expressionExcept is expression without the names in except (a pattern's
// own variables, recorded already).
func (r *scopeRecorder) expressionExcept(text string, except []string) []string {
	var visible []string
	for _, name := range expressionFreeVariables(text) {
		if containsString(except, name) {
			continue
		}
		if r.read(name) {
			visible = append(visible, name)
		}
	}
	forEachPatternComprehension(text, func(pattern string) {
		path := extractPathAssignmentVariable(pattern)
		for _, name := range patternVariables(pattern) {
			if name != path && r.inScope(name) && r.read(name) {
				visible = append(visible, name)
			}
		}
	})
	return visible
}

// patternVariables are the variables a pattern names (nodes,
// relationships, a path), in order of appearance. A pattern's WHERE is not
// part of it.
func patternVariables(pattern string) []string {
	if where := topLevelKeywordIndex(pattern, "WHERE"); where >= 0 {
		pattern = pattern[:where]
	}
	bindings := make(matchSemanticScope)
	addMatchPatternBindingKinds(bindings, pattern)
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		left, right := firstVariableIndex(pattern, names[i]), firstVariableIndex(pattern, names[j])
		if left != right {
			return left < right
		}
		return names[i] < names[j]
	})
	return names
}

// firstVariableIndex is where name first appears in text as a whole word.
func firstVariableIndex(text, name string) int {
	for from := 0; from < len(text); {
		index := strings.Index(text[from:], name)
		if index < 0 {
			return len(text)
		}
		index += from
		end := index + len(name)
		if (index == 0 || !isIdentByte(text[index-1])) && (end == len(text) || !isIdentByte(text[end])) {
			return index
		}
		from = index + 1
	}
	return len(text)
}

// forEachPatternComprehension calls fn with the pattern (WHERE included) of
// every pattern comprehension in text, nested ones too.
func forEachPatternComprehension(text string, fn func(pattern string)) {
	for index := 0; index < len(text); index++ {
		switch c := text[index]; c {
		case '\'', '"', '`':
			index = skipCypherQuotedText(text, index, c) - 1
		case '[':
			closing := findMatchingBracket(text, index)
			if closing < 0 {
				return
			}
			if pattern, _, ok := splitPatternComprehension(text[index : closing+1]); ok {
				fn(pattern)
			}
		}
	}
}

// clauseKeywordEnd is where a clause's leading keyword(s) end: MATCH,
// OPTIONAL MATCH, CREATE, MERGE, WITH, RETURN, SET, REMOVE, FILTER.
func clauseKeywordEnd(text string) int {
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH", "CREATE", "MERGE", "WITH", "RETURN", "SET", "REMOVE", "FILTER"} {
		if startsWithKeywordFold(text, keyword) {
			return len(keyword)
		}
	}
	return 0
}

// blankTopLevelKeywords is text with every top-level occurrence of the
// keywords replaced by spaces (offsets unchanged), so an expression reader
// doesn't take them for variables.
func blankTopLevelKeywords(text string, keywords ...string) string {
	var blanked []byte
	for _, keyword := range keywords {
		for from := 0; from < len(text); {
			index := topLevelKeywordIndex(text[from:], keyword)
			if index < 0 {
				break
			}
			if blanked == nil {
				blanked = []byte(text)
			}
			index += from
			for i := index; i < index+len(keyword) && i < len(blanked); i++ {
				blanked[i] = ' '
			}
			from = index + len(keyword)
		}
		if blanked != nil {
			text = string(blanked)
		}
	}
	return text
}
