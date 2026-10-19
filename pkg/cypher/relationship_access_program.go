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
	byID map[string]*relationshipAccessTemplate
}

type relationshipAccessTemplate struct {
	id, pattern, where string
	occurrences int64
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

func relationshipProgramFromContext(ctx context.Context, query string) *relationshipAccessProgram {
	_, query = parseExecutionMode(query)
	if program, _ := ctx.Value(relationshipAccessProgramKey{}).(*relationshipAccessProgram); program != nil && program.query == query {
		return program
	}
	return relationshipProgramFor(query)
}

func withRelationshipAccessProgram(ctx context.Context, query string) context.Context {
	if ctx.Value(relationshipAccessProgramKey{}) != nil || !containsRelationshipPattern(query) {
		return ctx
	}
	return context.WithValue(ctx, relationshipAccessProgramKey{}, relationshipProgramFor(query))
}

// This compiler uses the existing clause, subquery and pattern readers. It is
// independent of storage and never evaluates expressions, including write tokens.
func compileRelationshipAccessProgram(query string) *relationshipAccessProgram {
	program := &relationshipAccessProgram{query: query, byID: make(map[string]*relationshipAccessTemplate)}
	seen := program.byID
	add := func(pattern, where string) {
		if !patternHasRelationship(pattern) {
			return
		}
		id := relationshipAccessID(pattern, where)
		if site := seen[id]; site != nil {
			site.occurrences++
			return
		}
		site := &relationshipAccessTemplate{id: id, pattern: pattern, where: where, occurrences: 1}
		seen[id] = site
		program.sites = append(program.sites, site)
	}
	var statement func(string)
	var expressions func(string)
	patternExpressions := func(pattern string) {
		for i := 0; i < len(pattern); i++ {
			if pattern[i] == '\'' || pattern[i] == '"' || pattern[i] == '`' {
				i = skipCypherQuotedText(pattern, i, pattern[i]) - 1
				continue
			}
			if pattern[i] == '$' && i+1 < len(pattern) && pattern[i+1] == '(' {
				if end := findMatchingDelimiter(pattern, i+1, '(', ')'); end >= 0 {
					expressions(pattern[i+2 : end])
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
			switch text[i] {
			case '\'', '"', '`':
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
				if end, ok := relationshipChainEnd(text, i, len(text)); ok {
					add(text[i:end], "")
					patternExpressions(text[i:end])
					i = end - 1
				}
			}
		}
	}
	statement = func(text string) {
		// USE selects a view, not a different access-site grammar.
		if _, body, hasUse, err := parseUseClause(text, true); hasUse && err == nil {
			statement(body)
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
					add(pattern, split.where)
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

