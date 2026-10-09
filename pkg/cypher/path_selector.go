package cypher

import (
	"sort"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Path selectors and path modes (#907).
//
// A MATCH path pattern may start with a selector (Neo4j 5.21+) and a path
// mode (Cypher 25):
//
//	[p =] [ANY SHORTEST | ALL SHORTEST | ANY [k] | ALL | SHORTEST k
//	       | SHORTEST [k] GROUP(S)] [PATH(S)] [WALK | TRAIL | ACYCLIC [PATH(S)]] pattern
//
// and a MATCH may name its match mode: DIFFERENT RELATIONSHIP(S), the
// default. The statement rewrite (desugarLabelExpressions) writes these in
// forms every route reads:
//   - ALL, TRAIL, WALK and DIFFERENT RELATIONSHIPS are dropped. ALL selects
//     every path, as a plain pattern does, and under the default match mode
//     no relationship repeats in a MATCH, so a WALK is a TRAIL;
//   - ACYCLIC without a selective selector keeps the paths whose nodes are
//     all distinct: a __nornic_acyclic(p) predicate;
//   - a selective selector (ANY, SHORTEST) makes the pattern
//     p = shortestPath(pattern), with
//     __nornic_path_selector(kind, count, groups, mode, predicate) as the
//     first conjunct of its WHERE, which the shortestPath MATCH step reads
//     (shortestPathMatch.selector). predicate is the pattern's own predicates
//     (label expressions, element WHERE, a parenthesised path's WHERE): they
//     constrain the paths to select from, while the clause's WHERE filters
//     the selected paths, as in Neo4j.
//
// As in Neo4j, a selective selector in a MATCH with another pattern, a count
// of 0, a selector, path mode or match mode with shortestPath(), an explicit
// path mode with a variable-length relationship (-[*]->), and a selector in
// CREATE or MERGE are SyntaxErrors. Cypher 5 has no path modes or match
// modes; NornicDB reads them in both versions.

func init() {
	cypherfn.Register(acyclicPathFunction, fnAcyclicPath)
	cypherfn.Register(pathSelectorFunction, fnPatternMarker)
	cypherfn.Register(repeatableElementsFunction, fnPatternMarker)
}

// acyclicPathFunction reports whether a path's nodes are all distinct.
const acyclicPathFunction = "__nornic_acyclic"

// pathSelectorFunction carries a selective selector to the shortestPath
// MATCH step; it is never evaluated as an expression.
const pathSelectorFunction = "__nornic_path_selector"

// repeatableElementsFunction marks a MATCH REPEATABLE ELEMENTS for the
// pipeline's MATCH step (repeatable_elements.go); it is never evaluated as
// an expression.
const repeatableElementsFunction = "__nornic_repeatable_elements"

// pathPatternPrefix is the selector and path mode at query[start:end]
// before a path pattern.
type pathPatternPrefix struct {
	start, end int
	// kind is "" (no selector), "ALL", "ANY" or "SHORTEST" (ANY SHORTEST is
	// SHORTEST 1; ALL SHORTEST is SHORTEST 1 GROUPS).
	kind string
	// count is the count as written (digits or a parameter), "" for 1.
	count  string
	groups bool
	// mode is "", "WALK", "TRAIL" or "ACYCLIC".
	mode string
}

// selective reports whether the selector selects some of the paths.
func (p pathPatternPrefix) selective() bool {
	return p.kind == "ANY" || p.kind == "SHORTEST"
}

// scanPathPatternPrefix reads a selector and path mode at query[i:end],
// spaces skipped. ok is false when the text there isn't one followed by a
// node pattern.
func scanPathPatternPrefix(q string, i, end int) (pathPatternPrefix, bool, error) {
	prefix := pathPatternPrefix{start: skipASCIISpaces(q, i, end)}
	at := prefix.start
	next := func() (string, int) {
		k := skipASCIISpaces(q, at, end)
		j := k
		if j < end && q[j] == '$' {
			j++
		}
		for j < end && isIdentByte(q[j]) {
			j++
		}
		return q[k:j], j
	}
	accept := func(words ...string) string {
		token, tokenEnd := next()
		for _, word := range words {
			if strings.EqualFold(token, word) {
				at = tokenEnd
				return word
			}
		}
		return ""
	}
	acceptCount := func() bool {
		token, tokenEnd := next()
		if len(token) > 1 && token[0] == '$' || token != "" && strings.Trim(token, "0123456789") == "" {
			prefix.count, at = token, tokenEnd
			return true
		}
		return false
	}
	// PATH or PATHS ends the prefix, but for SHORTEST k PATHS GROUPS.
	switch accept("ANY", "ALL", "SHORTEST") {
	case "ANY":
		if accept("SHORTEST") != "" {
			prefix.kind, prefix.count = "SHORTEST", "1"
		} else {
			prefix.kind = "ANY"
			acceptCount()
		}
	case "ALL":
		if accept("SHORTEST") != "" {
			prefix.kind, prefix.count, prefix.groups = "SHORTEST", "1", true
		} else {
			prefix.kind = "ALL"
		}
	case "SHORTEST":
		prefix.kind = "SHORTEST"
		counted := acceptCount()
		path := accept("PATH", "PATHS")
		prefix.groups = accept("GROUP", "GROUPS") != ""
		if !counted && !prefix.groups {
			return pathPatternPrefix{}, false, nil
		}
		if path != "" && !prefix.groups {
			return prefix.finish(q, at, end)
		}
	}
	prefix.mode = accept("WALK", "TRAIL", "ACYCLIC")
	accept("PATH", "PATHS")
	return prefix.finish(q, at, end)
}

// finish checks the prefix read up to query[at:end], where its pattern
// starts.
func (prefix pathPatternPrefix) finish(q string, at, end int) (pathPatternPrefix, bool, error) {
	if prefix.kind == "" && prefix.mode == "" {
		return pathPatternPrefix{}, false, nil
	}
	if word, _, _ := scanSymbolicName(q[:end], skipASCIISpaces(q, at, end)); strings.EqualFold(word, "shortestPath") || strings.EqualFold(word, "allShortestPaths") {
		return pathPatternPrefix{}, false, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorWithShortestPathFunction())
	}
	prefix.end = skipASCIISpaces(q, at, end)
	if prefix.end >= end || q[prefix.end] != '(' {
		return pathPatternPrefix{}, false, nil
	}
	if prefix.count != "" && strings.Trim(prefix.count, "0") == "" {
		if prefix.groups {
			return pathPatternPrefix{}, false, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorGroupCountNotPositive())
		}
		return pathPatternPrefix{}, false, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorPathCountNotPositive())
	}
	return prefix, true, nil
}

// mayUsePathPatternPrefix reports whether query may hold a selector, a path
// mode or a match mode: one of their words, or ANY or ALL after =, a comma or
// MATCH. It never answers false for one.
func mayUsePathPatternPrefix(query string) bool {
	for _, word := range []string{"shortest", "acyclic", "walk", "trail", "different", "repeatable"} {
		if indexASCIIFold(query, word) >= 0 {
			return true
		}
	}
	for _, word := range []string{"any", "all"} {
		for from := 0; from < len(query); {
			index := indexASCIIFold(query[from:], word)
			if index < 0 {
				break
			}
			index += from
			from = index + len(word)
			if index > 0 && isIdentByte(query[index-1]) || from < len(query) && isIdentByte(query[from]) {
				continue
			}
			before := trimRightIndex(query, 0, index)
			if before > 0 && (query[before-1] == '=' || query[before-1] == ',') ||
				before >= len("MATCH") && equalFoldASCII(query[before-len("MATCH"):before], "MATCH") {
				return true
			}
		}
	}
	return false
}

// pathPrefixRewrite is what patternWithWhere adds for the selectors and
// path modes of a MATCH pattern.
type pathPrefixRewrite struct {
	// selector is the selective selector's call without its predicate
	// argument; "" when there is none.
	selector string
	// patternStart and patternEnd bound what pattern() reads: a
	// parenthesised path's inside, before its WHERE.
	patternStart, patternEnd int
	// where is the parenthesised path's WHERE, rewritten.
	where string
	// acyclic are the __nornic_acyclic predicates of ACYCLIC patterns.
	acyclic []string
	// repeatable marks MATCH REPEATABLE ELEMENTS.
	repeatable bool
}

// pathPrefixes rewrites the match mode and the selectors and path modes of
// the MATCH pattern query[start:end].
func (r *labelExpressionRewriter) pathPrefixes(start, end int) (pathPrefixRewrite, error) {
	rewrite := pathPrefixRewrite{patternStart: start, patternEnd: end}
	q := r.query
	explicitMode := false
	if modeEnd, repeatable, ok := matchModeAt(q, start, end); ok {
		explicitMode = true
		r.edit(skipASCIISpaces(q, start, end), modeEnd, "")
		start = modeEnd
		rewrite.repeatable = repeatable
	}
	var parts [][2]int
	for partStart := start; partStart <= end; {
		comma := topLevelByteIndex(q, partStart, end, ',')
		if comma < 0 {
			parts = append(parts, [2]int{partStart, end})
			break
		}
		parts = append(parts, [2]int{partStart, comma})
		partStart = comma + 1
	}
	for _, part := range parts {
		at := skipASCIISpaces(q, part[0], part[1])
		variable := ""
		if name, nameEnd, ok := scanSymbolicName(q[:part[1]], at); ok {
			if eq := skipASCIISpaces(q, nameEnd, part[1]); eq < part[1] && q[eq] == '=' && (eq+1 >= part[1] || q[eq+1] != '=') {
				variable, at = name, skipASCIISpaces(q, eq+1, part[1])
			}
		}
		if explicitMode {
			if word, _, ok := scanSymbolicName(q[:part[1]], at); ok && (strings.EqualFold(word, "shortestPath") || strings.EqualFold(word, "allShortestPaths")) {
				return rewrite, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorWithShortestPathFunction())
			}
		}
		prefix, ok, err := scanPathPatternPrefix(q, at, part[1])
		if err != nil {
			return rewrite, err
		}
		if !ok {
			continue
		}
		if rewrite.repeatable && (prefix.mode == "TRAIL" || prefix.mode == "ACYCLIC") {
			return rewrite, labelExpressionSyntaxError(localization.CypherMatchingRepeatableElementsPathMode(prefix.mode))
		}
		if prefix.selective() && len(parts) > 1 {
			return rewrite, labelExpressionSyntaxError(localization.CypherMatchingPathSelectorMultiplePatterns())
		}
		patternEnd := trimRightIndex(q, prefix.end, part[1])
		if prefix.mode != "" && hasVariableLengthRelationship(q, prefix.end, patternEnd) {
			return rewrite, labelExpressionSyntaxError(localization.CypherMatchingPathModeVariableLength(prefix.mode))
		}
		switch {
		case prefix.selective():
			call := "shortestPath"
			if variable == "" {
				call = r.variable() + " = " + call
			}
			parenthesised, err := r.parenthesisedPathWhere(prefix.end, patternEnd, &rewrite)
			if err != nil {
				return rewrite, err
			}
			if parenthesised {
				// The path's parentheses are the call's.
				r.edit(prefix.start, prefix.end, call)
			} else {
				r.edit(prefix.start, prefix.end, call+"(")
				r.edit(patternEnd, patternEnd, ")")
			}
			count := prefix.count
			if count == "" {
				count = "1"
			}
			groups := "false"
			if prefix.groups {
				groups = "true"
			}
			rewrite.selector = pathSelectorFunction + "('" + prefix.kind + "', " + count + ", " + groups + ", '" + prefix.mode + "', "
		case prefix.mode == "ACYCLIC":
			if variable == "" {
				variable = r.variable()
				r.edit(prefix.start, prefix.end, variable+" = ")
			} else {
				r.edit(prefix.start, prefix.end, "")
			}
			rewrite.acyclic = append(rewrite.acyclic, acyclicPathFunction+"("+variable+")")
		default:
			r.edit(prefix.start, prefix.end, "")
		}
	}
	if rewrite.repeatable && hasUnboundedRepetition(q, start, end) {
		return rewrite, labelExpressionSyntaxError(localization.CypherMatchingRepeatableElementsUnbounded())
	}
	return rewrite, nil
}

// parenthesisedPathWhere reports whether a selected pattern query[start:end]
// is one parenthesised path, ((a)-->(b) [WHERE predicate]): pattern() reads
// its inside, and its WHERE becomes part of the selector's predicate.
func (r *labelExpressionRewriter) parenthesisedPathWhere(start, end int, rewrite *pathPrefixRewrite) (bool, error) {
	q := r.query
	if inner := skipASCIISpaces(q, start+1, end); inner >= end || q[inner] != '(' || findMatchingDelimiter(q[:end], start, '(', ')') != end-1 {
		return false, nil
	}
	rewrite.patternStart, rewrite.patternEnd = start+1, end-1
	where := findTopLevelKeyword(q[start+1:end-1], "WHERE")
	if where < 0 {
		return true, nil
	}
	where += start + 1
	rewrite.patternEnd = where
	r.edit(trimRightIndex(q, start+1, where), end-1, "")
	sub := &labelExpressionRewriter{query: q, generated: r.generated, named: r.named}
	bodyStart, bodyEnd := skipASCIISpaces(q, where+len("WHERE"), end-1), trimRightIndex(q, where+len("WHERE"), end-1)
	if err := sub.expression(bodyStart, bodyEnd); err != nil {
		return true, err
	}
	r.generated = sub.generated
	sort.SliceStable(sub.edits, func(i, j int) bool { return sub.edits[i].start < sub.edits[j].start })
	var text strings.Builder
	last := bodyStart
	for _, edit := range sub.edits {
		text.WriteString(q[last:edit.start])
		text.WriteString(edit.text)
		last = edit.end
	}
	text.WriteString(q[last:bodyEnd])
	rewrite.where = "(" + text.String() + ")"
	return true, nil
}

// writePatternSelectorError is Neo4j's SyntaxError for a path selector in
// the pattern of a CREATE or MERGE clause, or nil.
func writePatternSelectorError(q string, clause labelClause) error {
	at := skipASCIISpaces(q, clause.bodyStart, clause.end)
	if _, nameEnd, ok := scanSymbolicName(q[:clause.end], at); ok {
		if eq := skipASCIISpaces(q, nameEnd, clause.end); eq < clause.end && q[eq] == '=' {
			at = eq + 1
		}
	}
	if prefix, ok, _ := scanPathPatternPrefix(q, at, clause.end); ok && prefix.kind != "" {
		return labelExpressionSyntaxError(localization.CypherMatchingPathSelectorInWritePattern(clause.keyword))
	}
	return nil
}

// matchModeAt returns the end of the match mode at the start of the MATCH
// body query[start:end]: DIFFERENT RELATIONSHIP(S), or REPEATABLE ELEMENT(S)
// [BINDINGS] (repeatable).
func matchModeAt(q string, start, end int) (int, bool, bool) {
	word, wordEnd, ok := scanSymbolicName(q[:end], skipASCIISpaces(q, start, end))
	if !ok {
		return 0, false, false
	}
	next, nextEnd, ok := scanSymbolicName(q[:end], skipASCIISpaces(q, wordEnd, end))
	switch {
	case !ok:
		return 0, false, false
	case strings.EqualFold(word, "DIFFERENT") && (strings.EqualFold(next, "RELATIONSHIP") || strings.EqualFold(next, "RELATIONSHIPS")):
		return nextEnd, false, true
	case strings.EqualFold(word, "REPEATABLE") && (strings.EqualFold(next, "ELEMENT") || strings.EqualFold(next, "ELEMENTS")):
		if bindings, bindingsEnd, ok := scanSymbolicName(q[:end], skipASCIISpaces(q, nextEnd, end)); ok && strings.EqualFold(bindings, "BINDINGS") {
			nextEnd = bindingsEnd
		}
		return nextEnd, true, true
	}
	return 0, false, false
}

// hasUnboundedRepetition reports whether the pattern q[start:end] repeats a
// relationship without an upper bound: a quantifier (+, *, {m,}) or a
// variable-length relationship (-[*]->, -[*2..]->).
func hasUnboundedRepetition(q string, start, end int) bool {
	for i := start; i < end; i++ {
		switch c := q[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(q, i, c) - 1
		case '{':
			if close := findMatchingDelimiter(q[:end], i, '{', '}'); close > i {
				i = close
			}
		case '[':
			close := findMatchingDelimiter(q[:end], i, '[', ']')
			if close < 0 {
				return false
			}
			if quantifier, _, ok := quantifierAfterArrow(q, close, end); ok && quantifier.max < 0 {
				return true
			}
			inner := close
			if where := elementWhereIndex(q, i, close); where >= 0 {
				inner = where
			}
			if brace := indexOutsideQuotes(q[i+1:inner], '{'); brace >= 0 {
				inner = i + 1 + brace
			}
			if star := indexOutsideQuotes(q[i+1:inner], '*'); star >= 0 {
				length := strings.TrimSpace(q[i+1+star+1 : inner])
				if dots := strings.Index(length, ".."); length == "" || dots >= 0 && strings.TrimSpace(length[dots+2:]) == "" {
					return true
				}
			}
			i = close
		case '-', '<':
			arrowEnd := arrowRunEnd(q, i, end)
			if next := skipASCIISpaces(q, arrowEnd, end); next < end && q[next] != '[' {
				if quantifier, ok := relationshipQuantifierAt(q, next, end); ok && quantifier.max < 0 {
					return true
				}
			}
			i = arrowEnd - 1
		}
	}
	return false
}

// hasVariableLengthRelationship reports whether the pattern q[start:end]
// writes a variable-length relationship (-[*]->, -[:R*1..3]->); a quantifier
// after a relationship is not one.
func hasVariableLengthRelationship(q string, start, end int) bool {
	for i := start; i < end; i++ {
		switch c := q[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(q, i, c) - 1
		case '{':
			if close := findMatchingDelimiter(q[:end], i, '{', '}'); close > i {
				i = close
			}
		case '[':
			close := findMatchingDelimiter(q[:end], i, '[', ']')
			if close < 0 {
				return false
			}
			inner := close
			if where := elementWhereIndex(q, i, close); where >= 0 {
				inner = where
			}
			if brace := indexOutsideQuotes(q[i+1:inner], '{'); brace >= 0 {
				inner = i + 1 + brace
			}
			if indexOutsideQuotes(q[i+1:inner], '*') >= 0 {
				return true
			}
			i = close
		}
	}
	return false
}

// fnAcyclicPath is __nornic_acyclic(path): whether no node of path repeats
// (null for null).
func fnAcyclicPath(ctx cypherfn.Context, args []string) (interface{}, error) {
	values, err := evalArgs(ctx, args)
	if err != nil || len(values) != 1 || values[0] == nil {
		return nil, err
	}
	nodes, ok := pathNodeIDs(values[0])
	if !ok {
		return nil, nil
	}
	return distinctNodeIDs(nodes), nil
}

// fnPatternMarker is reached only when a step other than the MATCH step a
// selector or match mode is written for reads it.
func fnPatternMarker(cypherfn.Context, []string) (interface{}, error) {
	return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", localization.CypherMatchingPatternMarkerOutsideMatch())
}

// pathNodeIDs returns the IDs of a path value's nodes in order.
func pathNodeIDs(value interface{}) ([]storage.NodeID, bool) {
	var nodes []interface{}
	switch path := value.(type) {
	case map[string]interface{}:
		parts, _, hasNodes, _ := pathValueParts(path)
		if !hasNodes {
			return nil, false
		}
		nodes = parts
	case PathResult:
		return pathResultNodeIDs(&path), true
	case *PathResult:
		return pathResultNodeIDs(path), true
	default:
		return nil, false
	}
	ids := make([]storage.NodeID, 0, len(nodes))
	for _, item := range nodes {
		node, ok := item.(*storage.Node)
		if !ok || node == nil {
			return nil, false
		}
		ids = append(ids, node.ID)
	}
	return ids, true
}

func pathResultNodeIDs(path *PathResult) []storage.NodeID {
	ids := make([]storage.NodeID, len(path.Nodes))
	for i, node := range path.Nodes {
		ids[i] = node.ID
	}
	return ids
}

// distinctNodeIDs reports whether no ID in ids repeats.
func distinctNodeIDs(ids []storage.NodeID) bool {
	for i := 1; i < len(ids); i++ {
		for j := 0; j < i; j++ {
			if ids[i] == ids[j] {
				return false
			}
		}
	}
	return true
}
