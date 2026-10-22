package cypher

import (
	"context"
	"strings"
)

// relationshipAccessProgram is the statement's immutable source of relationship
// access templates. EXPLAIN enumerates these records; execution can resolve only
// these records. Neither consumer may manufacture an additional access site.
// Row values, bindings and graph selection stay with their existing owners.
type relationshipAccessProgram struct {
	query string
	sites []*relationshipAccessTemplate
	byID  map[string]*relationshipAccessTemplate
	// Cold index of path-kind templates by their unqualified syntax identity.
	// byID remains the authority, including explicit source-removal controls.
	paths map[string]*relationshipAccessTemplate

	graphQueries []string

	// The common whole-statement binding is immutable and owned by this source.
	// Fragments retain their distinct query/source binding in the bounded cache.
	self relationshipStatementBinding
}

// Only occurrences with the same access kind may share eligibility and counters.
// A path search cannot turn an ordinary pattern's equality access into a scan.
type relationshipAccessKind uint8

const (
	relationshipPatternAccess relationshipAccessKind = iota
	relationshipPathAccess
)

func relationshipAccessKindID(id string, kind relationshipAccessKind) string {
	if kind == relationshipPathAccess {
		return "\x00path\x00" + id
	}
	return id
}

type relationshipAccessTemplate struct {
	id, pattern, where string
	occurrences        int64
	kind               relationshipAccessKind
	identity           *relationshipAccessIdentity

	syntax relationshipAccessSyntax

	// Syntax-only descriptors are immutable; live schema choices are separate.
	scan relationshipAccess
	base relationshipAccess
}

func newRelationshipAccessTemplate(program *relationshipAccessProgram, id, pattern, where string, kind relationshipAccessKind) *relationshipAccessTemplate {
	syntax := compileRelationshipAccessSyntax(pattern, where)
	identity := relationshipAccessIdentityFor(pattern, where)
	if kind == relationshipPathAccess {
		identity = &relationshipAccessIdentity{key: identity.key, id: id}
	}
	template := &relationshipAccessTemplate{id: id, pattern: pattern, where: where, syntax: syntax, kind: kind, identity: identity}
	template.scan = relationshipAccess{program: program, variable: syntax.variable}
	template.base = relationshipAccess{program: program, variable: syntax.variable, inline: syntax.inline, id: syntax.id, expressions: syntax.expressions}
	if kind == relationshipPathAccess {
		if program.paths == nil {
			program.paths = make(map[string]*relationshipAccessTemplate)
		}
		program.paths[relationshipAccessID(pattern, where)] = template
	}
	return template
}

type relationshipAccessProgramKey struct{}

var relationshipAccessPrograms = newBoundedCache[string, *relationshipAccessProgram](4096, boundedCacheStringShardIndex)

func relationshipProgramFor(query string) *relationshipAccessProgram {
	_, query = parseExecutionMode(query)
	if program, ok := relationshipAccessPrograms.get(query); ok {
		return program
	}
	program := compileRelationshipAccessProgram(query)
	relationshipAccessPrograms.put(query, program)
	return program
}

// relationshipStatementContext binds an executable statement to its immutable
// source program. Lowered fragments retain that source; a procedure-supplied
// statement compiles its own source, even inside another admitted statement.
// Statements with parameters share their existing queryParamsContext frame.
type relationshipStatementContext struct {
	context.Context
	binding *relationshipStatementBinding
}

type relationshipStatementKey struct{}

// Bindings contain only immutable syntax and source identity. Their bounded
// cache never retains parameter values, schema choices or storage views.
type relationshipStatementBinding struct {
	query   string
	program *relationshipAccessProgram
}

type relationshipStatementBindingKey struct {
	query   string
	program *relationshipAccessProgram
}

var relationshipStatementBindings = newBoundedCache[relationshipStatementBindingKey, *relationshipStatementBinding](4096, func(key relationshipStatementBindingKey) int {
	return boundedCacheStringShardIndex(key.query)
})

func (statement *relationshipStatementContext) Value(key interface{}) interface{} {
	switch key.(type) {
	case relationshipStatementKey:
		return statement.binding
	case relationshipAccessProgramKey:
		return statement.binding.program
	default:
		return statement.Context.Value(key)
	}
}

// compileRelationshipStatement is the sole admission owner for execution and
// planning. A non-nil source denotes a fragment of an already compiled statement;
// nil denotes a new executable statement whose source is the actual lowered text.
// A supplied parameter map constructs the shared parameter/admission frame in
// one step, after structural validation and before runtime expression evaluation.
func compileRelationshipStatement(ctx context.Context, query string, source *relationshipAccessProgram, params ...map[string]interface{}) (context.Context, admittedRelationshipStatement) {
	mode, query := parseExecutionMode(query)
	current, _ := ctx.Value(relationshipStatementKey{}).(*relationshipStatementBinding)
	if current != nil && current.query == query && (source == nil || current.program == source) {
		if len(params) == 0 {
			return ctx, admittedRelationshipStatement{program: current.program}
		}
		source = current.program
	}
	if source == nil {
		// Preserve an explicitly supplied program for this exact source. This
		// also lets the shared-source test remove a template for both consumers.
		if current, _ := ctx.Value(relationshipAccessProgramKey{}).(*relationshipAccessProgram); current != nil && current.query == query {
			source = current
		} else {
			source = relationshipProgramFor(query)
		}
	}
	// Only admission can install mutable statement-local schema choices. A
	// source without equality operands needs no plan map, even for MATCH.
	ctx = withRelationshipAccessPlans(ctx, source, mode)
	binding := &source.self
	if binding.program != source || binding.query != query {
		key := relationshipStatementBindingKey{query: query, program: source}
		var ok bool
		binding, ok = relationshipStatementBindings.get(key)
		if !ok {
			binding = &relationshipStatementBinding{query: query, program: source}
			relationshipStatementBindings.put(key, binding)
		}
	}
	if len(params) != 0 {
		return &queryParamsContext{Context: ctx, params: params[0], statement: binding}, admittedRelationshipStatement{program: source}
	}
	return &relationshipStatementContext{Context: ctx, binding: binding}, admittedRelationshipStatement{program: source}
}

func relationshipProgramShortestPattern(pattern string) (string, bool) {
	candidate := strings.TrimSpace(pattern)
	if variable := extractPathAssignmentVariable(candidate); variable != "" {
		candidate = strings.TrimSpace(candidate[strings.Index(candidate, "=")+1:])
	}
	_, inner, index, ok := extractShortestPathCall(candidate)
	return inner, ok && index == 0
}

// The expression walk is already at an unquoted opening parenthesis. Reuse the
// shortest-path reader only for that function's exact identifier and argument.
func relationshipProgramPathCallAt(text string, open int) (string, int, bool) {
	head := strings.TrimSpace(text[:open])
	for _, name := range []string{"shortestPath", "allShortestPaths"} {
		start := len(head) - len(name)
		if start < 0 || !equalFoldASCII(head[start:], name) || !functionNameStartsAt(head, start) {
			continue
		}
		end := findMatchingParen(text, open)
		if end < 0 {
			return "", 0, false
		}
		_, inner, _, ok := extractShortestPathCall(text[start : end+1])
		return inner, end, ok
	}
	return "", 0, false
}

// This compiler uses the existing clause, subquery and pattern readers. It is
// independent of storage and never evaluates expressions, including write tokens.
func compileRelationshipAccessProgram(query string) *relationshipAccessProgram {
	program := &relationshipAccessProgram{query: query, byID: make(map[string]*relationshipAccessTemplate)}
	program.self = relationshipStatementBinding{query: query, program: program}
	seen := make(map[string]*relationshipAccessTemplate)
	graphDepth := 0
	addSite := func(pattern, where string, kind relationshipAccessKind) {
		if !patternHasRelationship(pattern) {
			return
		}
		id := relationshipAccessKindID(relationshipAccessID(pattern, where), kind)
		if graphDepth > 0 {
			if program.byID[id] == nil {
				program.byID[id] = newRelationshipAccessTemplate(program, id, pattern, where, kind)
			}
			return
		}
		if site := seen[id]; site != nil {
			site.occurrences++
			return
		}
		site := newRelationshipAccessTemplate(program, id, pattern, where, kind)
		site.occurrences = 1
		seen[id] = site
		program.byID[id] = site
		program.sites = append(program.sites, site)
	}
	add := func(pattern, where string) { addSite(pattern, where, relationshipPatternAccess) }
	var statement func(string)
	var expressions func(string)
	patternExpressions := func(pattern string) {
		for i := 0; i < len(pattern); i++ {
			if isCypherQuoteByte(pattern[i]) {
				i = skipCypherQuotedText(pattern, i, pattern[i]) - 1
				continue
			}
			if open, _, ok := dynamicLabelOpenAt(pattern, i); ok {
				if end := findMatchingDelimiter(pattern, open, '(', ')'); end >= 0 {
					expressions(pattern[open+1 : end])
					i = end
					continue
				}
			}
			if pattern[i] == '{' {
				if end := findMatchingDelimiter(pattern, i, '{', '}'); end >= 0 {
					expressions(pattern[i : end+1])
					i = end
				}
			}
		}
	}
	expressions = func(text string) {
		for i := 0; i < len(text); i++ {
			if isCypherQuoteByte(text[i]) {
				i = skipCypherQuotedText(text, i, text[i]) - 1
				continue
			}
			if _, open, ok := subqueryExpressionKeywordAt(text, i); ok {
				if end := findMatchingDelimiter(text, open, '{', '}'); end >= 0 {
					body := strings.TrimSpace(text[open+1 : end])
					if strings.HasPrefix(body, "(") {
						body = "MATCH " + body
					}
					statement(body)
					i = end
					continue
				}
			}
			if text[i] == '[' {
				if end := findMatchingDelimiter(text, i, '[', ']'); end >= 0 {
					if pattern, projection, ok := splitPatternComprehension(text[i : end+1]); ok {
						split := relationshipAccessMatchWhere("MATCH " + pattern)
						add(split.pattern, split.where)
						patternExpressions(split.pattern)
						expressions(split.where)
						expressions(projection)
						i = end
						continue
					}
				}
			}
			if text[i] == '(' {
				// Expression-position shortest paths use the same search kernel as
				// MATCH shortestPath. Register that kind, not an ordinary traversal.
				if inner, end, ok := relationshipProgramPathCallAt(text, i); ok {
					addSite(inner, "", relationshipPathAccess)
					patternExpressions(inner)
					i = end
					continue
				}
				if end, ok := relationshipChainEnd(text, i, len(text)); ok {
					add(text[i:end], "")
					patternExpressions(text[i:end])
					i = end - 1
				}
			}
		}
	}
	statement = func(text string) {
		beginText := strings.TrimSpace(text)
		if len(beginText) >= len("BEGIN") && strings.EqualFold(beginText[:len("BEGIN")], "BEGIN") {
			if body, script := stripBeginTransactionPrefix(beginText); script {
				if source, _, complete := splitTransactionScriptTailAction(body); complete {
					statement(source)
				} else {
					expressions(body)
				}
				return
			}
		}
		// USE selects a view, not a different access-site grammar.
		if _, body, hasUse, err := parseUseClause(text, true); hasUse && err == nil {
			if graphDepth == 0 {
				program.graphQueries = append(program.graphQueries, text)
			}
			graphDepth++
			statement(body)
			graphDepth--
			return
		}
		if branches, _, _, ok := parseTopLevelUnionBranches(text); ok && len(branches) > 1 {
			for _, branch := range branches {
				statement(branch)
			}
			return
		}
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(text)
		if !ok {
			expressions(text)
			return
		}
		for _, clause := range clauses {
			switch clause.kind {
			case pipelineClauseCallSubquery:
				body, _, _, _ := parseCallSubquery(clause.text)
				statement(body)
			case pipelineClauseForeach:
				_, source, updates, err := parsePipelineForeach(clause.text)
				if err == nil {
					expressions(source)
					for _, update := range updates {
						statement(update.text)
					}
				}
			case pipelineClauseCreate, pipelineClauseMerge:
				patternExpressions(clause.text)
			case pipelineClauseMatch, pipelineClauseOptionalMatch:
				text := strings.TrimSpace(clause.text)
				if clause.kind == pipelineClauseOptionalMatch {
					text = strings.TrimSpace(text[len("OPTIONAL"):])
				}
				split := relationshipAccessMatchWhere(text)
				for _, pattern := range splitTopLevelComma(split.pattern) {
					if selected, ok := relationshipProgramShortestPattern(pattern); ok {
						addSite(selected, "", relationshipPathAccess)
					} else {
						add(pattern, split.where)
					}
				}
				// Maps can contain graph expressions, but the pattern itself is
				// already registered with its full predicate.
				patternExpressions(split.pattern)
				expressions(split.where)
			default:
				expressions(clause.text)
			}
		}
	}
	statement(query)
	return program
}
