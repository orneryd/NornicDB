package cypher

import (
	"fmt"
	"regexp"
	"strings"
)

// Fail-loud guard for the legacy multi-clause WITH interpreters.
//
// A subset of multi-clause Cypher queries is handled by the string-slicing
// interpreters in match_with.go (executeMatchWithClause /
// executeMatchWithOptionalMatch and their delegates). For a handful of query
// shapes those interpreters cannot faithfully evaluate the projection, and
// historically they returned CORRUPT results silently instead of erroring:
//
//   - raw expression text as a column value (e.g. the literal string
//     "DISTINCT a.name" or "b.name"),
//   - a boolean/nil where a real value was expected,
//   - skipped DISTINCT deduplication, or
//   - dropped rows (an empty result for a query that should return data).
//
// Silent data corruption is the worst failure mode for a graph database: a
// caller cannot tell a wrong answer from a right one. This guard converts those
// specific unsupported shapes into a clear, actionable error that mirrors the
// existing "unsupported clause after CALL {}" contract, so the failure is loud
// and debuggable.
//
// Scope discipline: every rule below keys off a signal that ONLY the broken
// multi-clause path exhibits. The guard is invoked from the WITH-based read
// handlers, which single-clause queries never reach, so single-clause
// RETURN DISTINCT, single-clause min()/max(), etc. are unaffected. Working
// multi-clause shapes (bare WITH + OPTIONAL MATCH, count()/sum()/collect()
// projections, simple property projections) match no rule and pass through
// untouched. EXPLAIN/PROFILE queries are dispatched to the plan path before
// routing and never reach these handlers, so parse-only tests do not flip.

var (
	// namedPathRe matches a named-path assignment such as "p=(" or "path = (".
	namedPathRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*\s*=\s*\(`)
	// zeroLengthVarRe matches a zero-minimum variable-length relationship hop,
	// e.g. "*0..", "*0..3", "*0]" or "* 0 ..". Such hop-0 rows leak raw text
	// through the string interpreter.
	zeroLengthVarRe = regexp.MustCompile(`\*\s*0\s*(\.\.|\])`)
)

// unsupportedMultiClauseShape reports whether a multi-clause (WITH-containing)
// read query is one of the shapes the legacy string interpreters cannot
// evaluate correctly. When it returns true, the second value is a human-facing
// error detail describing the unsupported shape and the supported alternative.
//
// The caller (a WITH-based read handler) should return an error built from the
// detail instead of dispatching into the interpreter, so the query fails loudly
// rather than emitting corrupt rows. The query text passed here is the routed,
// pre-parameter-substitution form: the guard inspects query STRUCTURE
// (DISTINCT, aggregate functions, pattern shape), none of which lives in
// parameters.
func (e *StorageExecutor) unsupportedMultiClauseShape(cypher string) (string, bool) {
	withIdx := findKeywordIndex(cypher, "WITH")
	returnIdx := findKeywordIndex(cypher, "RETURN")
	optMatchIdx := findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	if returnIdx < 0 {
		return "", false
	}

	hasWith := withIdx >= 0 && withIdx < returnIdx
	hasOptional := optMatchIdx >= 0 && optMatchIdx < returnIdx
	// Guard only multi-clause read queries: a clause (WITH or OPTIONAL MATCH)
	// must sit between MATCH and RETURN. Single-clause "MATCH ... RETURN ..."
	// is handled correctly elsewhere and is left untouched — never over-trigger.
	if !hasWith && !hasOptional {
		return "", false
	}

	// matchSection spans from query start to the earliest preceding clause.
	preIdx := returnIdx
	if hasWith && withIdx < preIdx {
		preIdx = withIdx
	}
	if hasOptional && optMatchIdx < preIdx {
		preIdx = optMatchIdx
	}
	matchSection := cypher[:preIdx]
	hasRelPattern := strings.Contains(matchSection, "-[") || strings.Contains(matchSection, "]-")

	// An OPTIONAL MATCH that follows the WITH stage (or exists with no WITH at
	// all) introduces a join the string interpreter mishandles for some shapes.
	hasOptionalAfterWith := hasOptional && (!hasWith || optMatchIdx > withIdx)

	// WITH section spans from after WITH to the next pipeline stage
	// (OPTIONAL MATCH if present, otherwise RETURN). Empty when there is no WITH.
	withSection := ""
	if hasWith {
		withEnd := returnIdx
		if hasOptional && optMatchIdx > withIdx {
			withEnd = optMatchIdx
		}
		withSection = cypher[withIdx+4 : withEnd]
	}

	// Raw RETURN projection text (mirrors match_with.go's own slicing).
	returnProj := strings.TrimSpace(cypher[returnIdx+6:])
	returnItems := e.parseReturnItems(returnProj)

	// Rule 1 — RETURN DISTINCT in a multi-clause query (symptoms 1 and 1b).
	// The interpreter slices the RETURN body without stripping the DISTINCT
	// keyword, so "DISTINCT <expr>" is treated as an expression: the value
	// leaks as literal text or resolves to nil, and deduplication is skipped.
	if leadingKeyword(returnProj, "DISTINCT") {
		return "RETURN DISTINCT is not supported after WITH/OPTIONAL MATCH in this query shape; " +
			"deduplication is skipped and the projected value is corrupted. " +
			"Use a single-clause 'MATCH ... RETURN DISTINCT ...', or dedupe earlier with 'WITH DISTINCT ...'", true
	}

	// Rule 2 — length(<path>) projected across an OPTIONAL MATCH (symptom 2).
	// The path variable is not carried across the OPTIONAL MATCH join, so the
	// query drops all rows.
	if hasOptionalAfterWith && namedPathRe.MatchString(matchSection) && projectionUsesFunc(returnItems, "length") {
		return "length(<path>) projected across an OPTIONAL MATCH is not supported in this query shape; " +
			"the path binding is lost and all rows are dropped. " +
			"Compute length() in a single MATCH ... RETURN, before introducing OPTIONAL MATCH", true
	}

	// Rule 3 — min()/max()/avg() over a node pattern in a multi-clause RETURN
	// (symptoms 3, 3b, 3c). The node-path interpreter has no min/max/avg
	// aggregation case, so min/max leak per-row values (no aggregation) and avg
	// returns nil. Scoped to node-only patterns; the relationship handler
	// implements these correctly.
	if !hasRelPattern {
		for _, it := range returnItems {
			if isAggregateFuncName(it.expr, "min") ||
				isAggregateFuncName(it.expr, "max") ||
				isAggregateFuncName(it.expr, "avg") {
				return fmt.Sprintf("min()/max()/avg() aggregation in a multi-clause RETURN over a node pattern "+
					"is not supported and returns un-aggregated or null values: %s. "+
					"Use a single-clause 'MATCH (n) RETURN min(n.x)'", it.expr), true
			}
		}
	}

	// Rule 4 — aggregation in WITH followed by OPTIONAL MATCH (symptom 4).
	// The aggregated value is not carried across the OPTIONAL MATCH join and is
	// projected as nil.
	if hasOptionalAfterWith && containsAggregateFunc(withSection) {
		return "aggregation in WITH followed by OPTIONAL MATCH is not supported in this query shape; " +
			"the aggregated value is dropped (projected as null). " +
			"Aggregate after the OPTIONAL MATCH, or split the query", true
	}

	// Rule 5 — comma-separated multi-pattern MATCH combined with WITH
	// (symptom 5). Only the first pattern is parsed, so the cross product is
	// lost and the query returns zero rows. Scoped to node-only patterns.
	if !hasRelPattern && hasTopLevelComma(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(matchSection), "MATCH"))) {
		return "comma-separated multi-pattern MATCH combined with WITH is not supported in this query shape; " +
			"only the first pattern is evaluated and the result is empty. " +
			"Rewrite the patterns as an explicit relationship pattern or separate MATCH clauses", true
	}

	// Rule 6 — pattern comprehension in a projection (symptom 6).
	// The interpreter cannot evaluate "[(a)-[:X]->(b) | b.prop]" and yields a
	// boolean instead of the projected list.
	for _, it := range returnItems {
		if looksLikePatternComprehension(it.expr) {
			return "pattern comprehension ([(a)-[:REL]->(b) | expr]) is not supported in a multi-clause " +
				"projection and yields an incorrect value. Rewrite using OPTIONAL MATCH + collect()", true
		}
	}
	if looksLikePatternComprehension(withSection) {
		return "pattern comprehension ([(a)-[:REL]->(b) | expr]) is not supported in a multi-clause WITH " +
			"and yields an incorrect value. Rewrite using OPTIONAL MATCH + collect()", true
	}

	// Rule 7 — zero-length variable-length relationship in the PRIMARY MATCH
	// pattern consumed by WITH (symptom 7). The rel-with interpreter mishandles
	// the hop-0 row: a property projection of the far node leaks raw expression
	// text, and a node projection binds the far node to nil instead of the
	// start node. Scoped to matchSection (the pattern before the first WITH/
	// OPTIONAL MATCH); a *0.. inside an OPTIONAL MATCH is evaluated by a
	// different, correct path (e.g. subgraph traversal) and is left untouched.
	if zeroLengthVarRe.MatchString(matchSection) {
		return "zero-length variable-length pattern (*0..) in a MATCH consumed by WITH is not supported " +
			"in this query shape; the hop-0 row corrupts the projected value. " +
			"Use *1.. (and add the start node explicitly if needed), or move the traversal into an OPTIONAL MATCH", true
	}

	return "", false
}

// leadingKeyword reports whether s, after trimming leading whitespace, begins
// with keyword as a whole word (case-insensitive). It is used to detect a
// clause modifier such as DISTINCT at the head of a RETURN body.
func leadingKeyword(s, keyword string) bool {
	s = strings.TrimSpace(s)
	if len(s) < len(keyword) {
		return false
	}
	if !strings.EqualFold(s[:len(keyword)], keyword) {
		return false
	}
	// Whole-word boundary: next char must be whitespace or end-of-string.
	if len(s) == len(keyword) {
		return true
	}
	next := s[len(keyword)]
	return next == ' ' || next == '\t' || next == '\n' || next == '\r'
}

// projectionUsesFunc reports whether any RETURN item invokes the named function
// (whitespace-tolerant), e.g. projectionUsesFunc(items, "length").
func projectionUsesFunc(items []returnItem, funcName string) bool {
	for _, it := range items {
		if isFunctionCallWS(it.expr, funcName) {
			return true
		}
	}
	return false
}

// hasTopLevelComma reports whether s contains a comma at bracket depth zero,
// i.e. a comma that separates top-level items rather than one nested inside a
// (), [], or {} group or a quoted string. It is used to detect a
// comma-separated multi-pattern MATCH.
func hasTopLevelComma(s string) bool {
	depth := 0
	var inQuote bool
	var quoteChar byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == quoteChar {
				inQuote = false
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQuote = true
			quoteChar = c
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				return true
			}
		}
	}
	return false
}

// looksLikePatternComprehension reports whether expr contains a Cypher pattern
// comprehension: a "[(" opener (a list bracket immediately followed by a node
// pattern), a relationship arrow, and a "|" projection separator. It excludes
// ordinary list comprehensions ("[x IN list | expr]") which do not open with a
// node pattern.
func looksLikePatternComprehension(expr string) bool {
	idx := indexBracketParen(expr)
	if idx < 0 {
		return false
	}
	rest := expr[idx:]
	if !strings.Contains(rest, "|") {
		return false
	}
	return strings.Contains(rest, "-[") || strings.Contains(rest, "->") || strings.Contains(rest, "<-")
}

// indexBracketParen returns the index of a '[' that is followed (ignoring
// whitespace) by a '(', or -1 if none exists. This is the opener of a pattern
// comprehension.
func indexBracketParen(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] != '[' {
			continue
		}
		j := i + 1
		for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
			j++
		}
		if j < len(s) && s[j] == '(' {
			return i
		}
	}
	return -1
}
