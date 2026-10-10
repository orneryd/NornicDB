package cypher

import (
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Label expressions in patterns (#860).
//
// Neo4j defines a pattern's label expression as a predicate on the element
// it labels: MATCH (n:A|B) is MATCH (n) WHERE n:A|B, and [r:!R] is [r] WHERE
// r:!R. desugarLabelExpressions rewrites a statement once, before it is
// routed, so that every MATCH route sees only the label forms it evaluates:
//
//   - a conjunction of labels in a node pattern ((n:A:B) for n:A&B, n IS A);
//   - a disjunction of types in a relationship pattern ([r:R|S], for R|:S,
//     (R|S) and r IS R|S);
//   - any other expression as a WHERE predicate on the element (a generated
//     variable names an anonymous one), which the predicate evaluators test
//     with entityHasAllLabelsOrTypes. The pattern keeps the labels the
//     expression requires (A&(B|C) keeps :A), so label scans still narrow.
//
// Patterns outside MATCH get the same treatment: a pattern-only EXISTS,
// COUNT or COLLECT body becomes a MATCH (EXISTS { (n)-->(:A|B) } is
// EXISTS { MATCH (n)-->(x) WHERE x:A|B }), a pattern predicate becomes an
// EXISTS subquery, and a pattern comprehension gets the predicate in its
// WHERE. In expressions, n IS A becomes the colon test n:A.
//
// The statements Neo4j rejects are rejected with its messages: label
// expression symbols mixed with colons (n:A|B:C), IS mixed with colons,
// R|:S on a relationship with a variable, properties or a length, R:S,
// a type expression on a variable-length relationship, and label or type
// expressions in CREATE and MERGE (which accept only A&B and IS A).
//
// The rewrite's edits are kept (queryRewrite), so column names and error
// messages show the client's text.

// labelRewriteEdit replaces query[start:end] with text (an insertion when
// start == end).
type labelRewriteEdit struct {
	start, end int
	text       string
}

type labelExpressionRewriter struct {
	query     string
	edits     []labelRewriteEdit
	generated int
	// params are the statement's parameters: a dynamic label or type whose
	// value is a literal or a parameter is resolved to names here
	// (resolveConstant).
	params map[string]interface{}
	// writeItems is set while a SET or REMOVE clause is read: its item
	// heads may name dynamic labels (n:$(e)), nothing else may.
	writeItems bool
	// named holds the variables given to anonymous elements, by the index of
	// their opening bracket, so an element is named once.
	named map[int]string
	// cypher25 is set for a Cypher 25 statement, where a dynamic label or
	// type may also be tested in an expression (WHERE n:$(e), RETURN
	// r:$any(l)), as Neo4j 2026.09 allows: the test becomes the label
	// predicate a MATCH pattern's row-dependent term becomes.
	cypher25 bool
}

// labelPatternMode is how a pattern's label expressions are read.
type labelPatternMode uint8

const (
	labelPatternMatch labelPatternMode = iota // MATCH and expressions
	labelPatternCreate
	labelPatternMerge
)

func (m labelPatternMode) clause() string {
	if m == labelPatternMerge {
		return "MERGE"
	}
	return "CREATE"
}

// desugarLabelExpressions returns query with its label expressions
// rewritten (see above) and the rewrite that maps the result back, or query
// and nil when nothing changes.
func desugarLabelExpressions(query string, params map[string]interface{}, cypher25 bool) (string, *queryRewrite, error) {
	if !mayUseLabelExpressions(query) && !mayUseRelationshipQuantifier(query) && !mayUsePatternPredicate(query) &&
		!mayAssignAnonymousNodePath(query) && !mayUsePathPatternPrefix(query) &&
		!mayRepeatRelationshipVariable(query) && !mayUseQuantifiedGroup(query) &&
		!mayUseParenthesisedPath(query) && !mayUseVectorCall(query) {
		return query, nil, nil
	}
	r := &labelExpressionRewriter{query: query, params: params, cypher25: cypher25}
	if err := r.statement(0, len(query)); err != nil {
		return query, nil, err
	}
	if len(r.edits) == 0 {
		return query, nil, nil
	}
	rewritten, rewrite := applyLabelRewriteEdits(query, r.edits, true)
	return rewritten, rewrite, nil
}

// applyLabelRewriteEdits applies non-overlapping edits to query and returns
// the result with the rewrite that maps it back (verbatimColumns: a column
// whose text the client wrote is kept as is).
func applyLabelRewriteEdits(query string, edits []labelRewriteEdit, verbatimColumns bool) (string, *queryRewrite) {
	sort.SliceStable(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	rewrite := &queryRewrite{original: query, edits: make([]queryTextEdit, 0, len(edits)), verbatimColumns: verbatimColumns}
	var out strings.Builder
	out.Grow(len(query) + 32*len(edits))
	last := 0
	for _, edit := range edits {
		out.WriteString(query[last:edit.start])
		canonStart := out.Len()
		out.WriteString(edit.text)
		rewrite.edits = append(rewrite.edits, queryTextEdit{origStart: edit.start, origEnd: edit.end, canonStart: canonStart, canonEnd: out.Len()})
		last = edit.end
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// mayUsePatternPredicate reports whether query may hold a pattern element's
// own WHERE (#878): a WHERE right inside ( or [ that isn't a function's
// argument list (all(x IN l WHERE …)) and has no IN before it (a list
// comprehension or list predicate). It never answers false for one.
func mayUsePatternPredicate(query string) bool {
	var openers []int
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '(', '[', '{':
			openers = append(openers, i)
		case ')', ']', '}':
			if len(openers) > 0 {
				openers = openers[:len(openers)-1]
			}
		case 'W', 'w':
			if len(openers) == 0 || !matchKeywordAt(query, i, "WHERE") {
				continue
			}
			open := openers[len(openers)-1]
			if query[open] == '{' || query[open] == '(' && open > 0 && isIdentByte(query[open-1]) {
				continue
			}
			if topLevelKeywordIndex(query[open+1:i], "IN") >= 0 {
				continue
			}
			return true
		}
	}
	return false
}

// mayUseLabelExpressions reports whether query may hold a label expression:
// one of | & ! % outside quotes, a group after a colon (:(A)), a
// colon-joined type list in brackets
// ([:R:S]), or the word IS not followed by NULL, TYPED, NORMALIZED, a
// normal form or ::. It never answers false for one.
func mayUseLabelExpressions(query string) bool {
	brackets := 0
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(query, i, c) - 1
		case '|', '&', '!', '%':
			return true
		case '$':
			if dynamicLabelStartsAt(query, i) {
				return true // $(e), $all(e), $any(e)
			}
		case '(':
			if i > 0 && query[i-1] == ':' {
				return true // :(R) and :(A|B) groups
			}
		case '[':
			brackets++
		case ']':
			brackets--
		case ':':
			// [:R:S], rejected (a relationship's types can't be joined).
			if brackets > 0 && i+1 < len(query) && query[i+1] != ':' && i > 0 && query[i-1] != ':' {
				j := i + 1
				for j < len(query) && isIdentByte(query[j]) {
					j++
				}
				if j > i+1 && j+1 < len(query) && query[j] == ':' && query[j+1] != ':' {
					return true
				}
			}
		case 'I', 'i':
			if i > 0 && isIdentByte(query[i-1]) || i+2 >= len(query) || query[i+1]|0x20 != 's' || isIdentByte(query[i+2]) {
				continue
			}
			if isLabelIsKeyword(query, i+2) {
				return true
			}
		}
	}
	return false
}

// isLabelIsKeyword reports whether the IS that ends at query[end] starts a
// label test: what follows is not NULL, NOT, TYPED, NORMALIZED, a normal
// form (NFC …) or ::.
func isLabelIsKeyword(query string, end int) bool {
	j := end
	for j < len(query) && isASCIISpace(query[j]) {
		j++
	}
	if j == end || j >= len(query) || query[j] == ':' {
		return false
	}
	k := j
	for k < len(query) && isIdentByte(query[k]) {
		k++
	}
	switch strings.ToUpper(query[j:k]) {
	case "NULL", "TYPED", "NORMALIZED", "NFC", "NFD", "NFKC", "NFKD":
		return false
	case "NOT":
		// IS NOT NULL …, or IS NOT <label>, which is an error.
		_, ok := isNotLabelOperand(query, k)
		return ok
	}
	return true
}

// isNotLabelOperand returns the word after IS NOT (which ends at
// query[end]) when it is not one of the words IS NOT takes (NULL, TYPED,
// NORMALIZED, a normal form, ::): Neo4j rejects n IS NOT <label>.
func isNotLabelOperand(query string, end int) (string, bool) {
	j := end
	for j < len(query) && isASCIISpace(query[j]) {
		j++
	}
	if j == end || j >= len(query) || query[j] == ':' {
		return "", false
	}
	k := j
	for k < len(query) && isIdentByte(query[k]) {
		k++
	}
	switch strings.ToUpper(query[j:k]) {
	case "", "NULL", "TYPED", "NORMALIZED", "NFC", "NFD", "NFKC", "NFKD":
		return "", false
	}
	return query[j:k], true
}

func (r *labelExpressionRewriter) edit(start, end int, text string) {
	r.edits = append(r.edits, labelRewriteEdit{start: start, end: end, text: text})
}

// generatedVariablePrefix starts the variables the rewrite names anonymous
// pattern elements with. A * projection doesn't list them: Neo4j's * lists
// only the variables a statement names.
const generatedVariablePrefix = "__nornic_lx"

// isGeneratedVariable reports whether name is a variable the rewrite named.
func isGeneratedVariable(name string) bool {
	return strings.HasPrefix(name, generatedVariablePrefix)
}

// withoutGeneratedColumns returns result without the columns of generated
// variables, which a * projection over every binding lists; a copy when it
// has any, so a cached result is never changed.
func withoutGeneratedColumns(result *ExecuteResult) *ExecuteResult {
	if result == nil {
		return nil
	}
	var keep []int
	for i, column := range result.Columns {
		if !isGeneratedVariable(column) {
			keep = append(keep, i)
		}
	}
	if len(keep) == len(result.Columns) {
		return result
	}
	trimmed := *result
	trimmed.Columns = make([]string, len(keep))
	for i, index := range keep {
		trimmed.Columns[i] = result.Columns[index]
	}
	trimmed.Rows = make([][]interface{}, len(result.Rows))
	for r, row := range result.Rows {
		out := make([]interface{}, 0, len(keep))
		for _, index := range keep {
			if index < len(row) {
				out = append(out, row[index])
			}
		}
		trimmed.Rows[r] = out
	}
	return &trimmed
}

// variable returns a fresh variable for an anonymous pattern element.
func (r *labelExpressionRewriter) variable() string {
	for {
		name := generatedVariablePrefix + strconv.Itoa(r.generated)
		r.generated++
		if !strings.Contains(r.query, name) {
			return name
		}
	}
}

func labelExpressionSyntaxError(message localization.Message) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidSyntax", message)
}

// labelClause is one clause of a statement: its keyword (MATCH for OPTIONAL
// MATCH) and where its body starts and the clause ends.
type labelClause struct {
	keyword        string
	bodyStart, end int
}

// labelClauseKeywords are the words that start a clause.
var labelClauseKeywords = map[string]bool{
	"MATCH": true, "OPTIONAL": true, "CREATE": true, "MERGE": true, "WHERE": true, "WITH": true,
	"RETURN": true, "UNWIND": true, "SET": true, "REMOVE": true, "DELETE": true, "DETACH": true,
	"CALL": true, "FOREACH": true, "ORDER": true, "SKIP": true, "LIMIT": true, "UNION": true,
	"YIELD": true, "LOAD": true, "FINISH": true, "USE": true, "ON": true, "OFFSET": true,
}

// clauses splits query[start:end] into its top-level clauses.
func (r *labelExpressionRewriter) clauses(start, end int) []labelClause {
	q := r.query
	var out []labelClause
	depth := 0
	for i := start; i < end; i++ {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(q, i, c) - 1
			continue
		case c == '(' || c == '[' || c == '{':
			depth++
			continue
		case c == ')' || c == ']' || c == '}':
			depth--
			continue
		}
		if depth != 0 || !isIdentStartByte(c) || (i > start && isIdentByte(q[i-1])) {
			continue
		}
		j := i
		for j < end && isIdentByte(q[j]) {
			j++
		}
		word := strings.ToUpper(q[i:j])
		if !labelClauseKeywords[word] || clauseKeywordUsedAsName(q[start:end], i-start, j-start, word) {
			i = j - 1
			continue
		}
		bodyStart := j
		switch word {
		case "OPTIONAL", "ON":
			// OPTIONAL MATCH; ON CREATE / ON MATCH inside MERGE.
			k := skipASCIISpaces(q, j, end)
			l := k
			for l < end && isIdentByte(q[l]) {
				l++
			}
			next := strings.ToUpper(q[k:l])
			if word == "OPTIONAL" && next == "MATCH" {
				word, bodyStart = "MATCH", l
			} else if word == "ON" && (next == "CREATE" || next == "MATCH") {
				bodyStart = l
			}
		}
		if len(out) > 0 {
			out[len(out)-1].end = i
		}
		out = append(out, labelClause{keyword: word, bodyStart: bodyStart, end: end})
		i = bodyStart - 1
	}
	return out
}

// statement rewrites the clauses of query[start:end].
func (r *labelExpressionRewriter) statement(start, end int) error {
	clauses := r.clauses(start, end)
	for i := 0; i < len(clauses); i++ {
		clause := clauses[i]
		switch clause.keyword {
		case "MATCH":
			whereStart, whereEnd := -1, -1
			if i+1 < len(clauses) && clauses[i+1].keyword == "WHERE" {
				whereStart, whereEnd = clauses[i+1].bodyStart, clauses[i+1].end
				i++
			}
			if err := r.patternWithWhere(clause.bodyStart, clause.end, whereStart, whereEnd); err != nil {
				return err
			}
		case "CREATE", "MERGE":
			if hasQuantifiedGroup(r.query[clause.bodyStart:clause.end]) {
				return labelExpressionSyntaxError(localization.CypherMatchingQuantifiedPathInWritePattern(clause.keyword))
			}
			if err := writePatternSelectorError(r.query, clause); err != nil {
				return err
			}
			if first := skipASCIISpaces(r.query, clause.bodyStart, clause.end); first >= clause.end ||
				r.query[first] != '(' && !startsPathAssignment(r.query, first, clause.end) {
				// CREATE INDEX … FOR (n:A|B), CREATE CONSTRAINT …: not a
				// pattern to write (a fulltext index lists its labels so).
				continue
			}
			mode := labelPatternCreate
			if clause.keyword == "MERGE" {
				mode = labelPatternMerge
			}
			if _, err := r.pattern(clause.bodyStart, clause.end, mode); err != nil {
				return err
			}
		case "FOREACH":
			if err := r.foreach(clause.bodyStart, clause.end); err != nil {
				return err
			}
		case "ON":
			// ON CREATE SET / ON MATCH SET: the SET clause follows.
		default:
			r.writeItems = clause.keyword == "SET" || clause.keyword == "REMOVE"
			err := r.expression(clause.bodyStart, clause.end)
			r.writeItems = false
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// patternWithWhere rewrites a MATCH pattern query[start:end] and its WHERE
// body query[whereStart:whereEnd] (whereStart < 0: none). The predicates the
// pattern's label expressions become are ANDed in front of the WHERE body,
// which is parenthesised when it has a top-level OR or XOR.
func (r *labelExpressionRewriter) patternWithWhere(start, end, whereStart, whereEnd int) error {
	prefixes, err := r.pathPrefixes(start, end)
	if err != nil {
		return err
	}
	r.nameSingleNodePaths(start, end)
	predicates, err := r.pattern(prefixes.patternStart, prefixes.patternEnd, labelPatternMatch)
	if err != nil {
		return err
	}
	repeated, err := r.repeatedRelationshipVariables(prefixes.patternStart, prefixes.patternEnd, prefixes.modeWritten)
	if err != nil {
		return err
	}
	predicates = append(predicates, repeated...)
	predicates = append(predicates, prefixes.acyclic...)
	predicates = append(predicates, prefixes.pathWheres...)
	if prefixes.selector != "" {
		if prefixes.where != "" {
			predicates = append(predicates, prefixes.where)
		}
		predicate := "true"
		if len(predicates) > 0 {
			predicate = strings.Join(predicates, " AND ")
		}
		predicates = []string{prefixes.selector + predicate + ")"}
	}
	if prefixes.repeatable {
		// The mode marker comes first: the MATCH step reads it before a
		// selector (repeatable_elements.go).
		predicates = append([]string{repeatableElementsFunction + "()"}, predicates...)
	}
	switch {
	case whereStart >= 0 && len(predicates) > 0:
		return r.whereWithPredicates(whereStart, whereEnd, predicates)
	case whereStart >= 0:
		return r.expression(whereStart, whereEnd)
	case len(predicates) > 0:
		at := trimRightIndex(r.query, start, end)
		r.edit(at, at, " WHERE "+strings.Join(predicates, " AND "))
	}
	return nil
}

// pattern rewrites the elements of the pattern query[start:end] and returns
// the predicates its label expressions become (MATCH only).
func (r *labelExpressionRewriter) pattern(start, end int, mode labelPatternMode) ([]string, error) {
	var predicates []string
	if err := r.patternElements(start, end, mode, &predicates); err != nil {
		return nil, err
	}
	return predicates, nil
}

func (r *labelExpressionRewriter) patternElements(start, end int, mode labelPatternMode, predicates *[]string) error {
	q := r.query
	for i := start; i < end; i++ {
		c := q[i]
		switch c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(q, i, c) - 1
		case '{':
			// A quantifier ({1,3}) or a stray map: no labels in it.
			if close := findMatchingDelimiter(q[:end], i, '{', '}'); close > i {
				i = close
			}
		case '(':
			close := findMatchingDelimiter(q[:end], i, '(', ')')
			if close < 0 {
				return nil
			}
			inner := skipASCIISpaces(q, i+1, close)
			if inner < close && q[inner] == '(' && mode == labelPatternMatch && quantifiedGroupAt(q, close, end) {
				// A quantified group's predicates are its own WHERE's.
				if err := r.quantifiedGroup(i, close); err != nil {
					return err
				}
			} else if inner < close && q[inner] == '(' || i > start && isIdentByte(q[i-1]) {
				// A parenthesised path, a quantified group in a write
				// pattern, or shortestPath(…): its elements are pattern
				// elements, up to a parenthesised path's own WHERE (which
				// pathPrefixes moves to the clause's).
				stop := close
				if inner < close && q[inner] == '(' {
					if where := topLevelKeywordIndex(q[i+1:close], "WHERE"); where >= 0 {
						stop = i + 1 + where
					}
				}
				if err := r.patternElements(i+1, stop, mode, predicates); err != nil {
					return err
				}
			} else if err := r.element(i, close, false, false, mode, predicates); err != nil {
				return err
			}
			i = close
		case '[':
			open := i
			close := findMatchingDelimiter(q[:end], i, '[', ']')
			if close < 0 {
				return nil
			}
			quantifier, at, quantified := quantifierAfterArrow(q, close, end)
			if err := r.element(open, close, true, quantified, mode, predicates); err != nil {
				return err
			}
			i = close
			if quantified {
				if err := r.quantifiedRelationship(open, close, at, quantifier, mode); err != nil {
					return err
				}
				i = quantifier.end - 1
			}
		case '-', '<':
			// An abbreviated relationship (--, -->, <--) may be quantified.
			arrowEnd := arrowRunEnd(q, i, end)
			if next := skipASCIISpaces(q, arrowEnd, end); next < end && q[next] != '[' {
				if quantifier, ok := relationshipQuantifierAt(q, next, end); ok {
					if err := r.quantifiedArrow(i, arrowEnd, next, quantifier, mode); err != nil {
						return err
					}
					i = quantifier.end - 1
					continue
				}
			}
			i = arrowEnd - 1
		}
	}
	return nil
}

// element rewrites the node (or relationship) pattern query[open:close+1]:
// its variable, then : or IS and a label chain, and its own WHERE (#878).
// quantified marks a relationship followed by a quantifier (#864).
func (r *labelExpressionRewriter) element(open, close int, relationship, quantified bool, mode labelPatternMode, predicates *[]string) error {
	q := r.query
	i := skipASCIISpaces(q, open+1, close)
	variable, variableEnd := "", i
	if written, end, ok := scanSymbolicName(q[:close], i); ok && !(strings.EqualFold(written, "IS") && end < close && isASCIISpace(q[end])) {
		variable, variableEnd = written, end
		i = skipASCIISpaces(q, end, close)
	}
	if where := elementWhereIndex(q, open, close); where >= 0 {
		if err := r.elementWhere(open, where, close, variable, relationship, quantified, mode, predicates); err != nil {
			return err
		}
		close = trimRightIndex(q, open+1, where)
	}
	if i >= close {
		return nil
	}
	chainStart, textStart, viaIS := i, -1, false
	switch {
	case q[i] == ':':
		textStart = i + 1
	case i+2 < close && strings.EqualFold(q[i:i+2], "IS") && isASCIISpace(q[i+2]):
		// n IS A is rewritten from the end of the variable: n:A.
		chainStart, textStart, viaIS = variableEnd, i+2, true
	default:
		return nil
	}
	chain, ok := scanLabelChain(q[textStart:close], relationship)
	if !ok {
		return nil
	}
	chainEnd := textStart + chain.end
	rest := skipASCIISpaces(q, chainEnd, close)
	if relationship {
		return r.relationshipElement(open, chainStart, chainEnd, rest, close, variable, viaIS, chain, quantified, mode, predicates)
	}
	if viaIS && chain.colons {
		return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionMixedIs(chain.expr.String()))
	}
	if chain.colons && chain.symbols {
		return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionMixedColon(chain.expr.String()))
	}
	if chain.dynamic {
		resolved, constant, err := r.resolveDynamicChain(chain, mode)
		if err != nil {
			return err
		}
		if !constant {
			// Its value depends on the row: in MATCH a predicate reads it
			// (labelExpression.predicate); CREATE and MERGE resolve it per
			// row (resolveRowDynamicTokens).
			if mode != labelPatternMatch {
				return nil
			}
			variable = r.elementVariable(open, variable)
			r.edit(chainStart, chainEnd, labelChainText(chain.expr.requiredLabels()))
			*predicates = append(*predicates, chain.expr.predicate(variable))
			return nil
		}
		chain.expr, chain.symbols = resolved, true
	}
	if !chain.symbols && !viaIS {
		return nil // :A:B, read as it always was
	}
	if names, plain := chain.expr.names(); plain {
		r.edit(chainStart, chainEnd, labelChainText(names))
		return nil
	}
	if mode != labelPatternMatch {
		return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionInWritePattern(mode.clause()))
	}
	variable = r.elementVariable(open, variable)
	r.edit(chainStart, chainEnd, labelChainText(chain.expr.requiredLabels()))
	*predicates = append(*predicates, variable+":"+chain.expr.String())
	return nil
}

// elementWhereIndex is the index of the WHERE that starts the pattern
// element query[open:close+1]'s own predicate (#878), or -1. The element's
// variable comes first, so a variable named where isn't the keyword.
func elementWhereIndex(q string, open, close int) int {
	i := skipASCIISpaces(q, open+1, close)
	if _, end, ok := scanSymbolicName(q[:close], i); ok {
		i = end
	}
	if where := topLevelKeywordIndex(q[i:close], "WHERE"); where >= 0 {
		return i + where
	}
	return -1
}

// elementWhere moves the predicate of a pattern element's own WHERE
// (query[where:close], close being the element's ] or )) to the clause's
// WHERE, as Neo4j defines it (#878): on a quantified relationship it applies
// to each of the relationships, written all(r IN r WHERE …) so the predicate
// reads the iteration variable under the relationship's own name. Neo4j
// rejects the predicate on a * variable-length relationship and in CREATE or
// MERGE.
func (r *labelExpressionRewriter) elementWhere(open, where, close int, variable string, relationship, quantified bool, mode labelPatternMode, predicates *[]string) error {
	q := r.query
	element := "Node"
	if relationship {
		element = "Relationship"
	}
	if mode != labelPatternMatch {
		return labelExpressionSyntaxError(localization.CypherMatchingPatternPredicateInWritePattern(element, mode.clause()))
	}
	if relationship && indexOutsideQuotes(q[open+1:where], '*') >= 0 {
		return labelExpressionSyntaxError(localization.CypherMatchingPatternPredicateVariableLength())
	}
	predicate := strings.TrimSpace(q[where+len("WHERE") : close])
	r.edit(where, close, "")
	if quantified {
		variable = r.elementVariable(open, variable)
		*predicates = append(*predicates, "all("+variable+" IN "+variable+" WHERE "+predicate+")")
		return nil
	}
	*predicates = append(*predicates, "("+predicate+")")
	return nil
}

// relationshipElement rewrites a relationship pattern's type chain
// (query[chainStart:chainEnd]; rest is where the length or properties
// start). A quantified relationship's type expression applies to each of its
// relationships (#864).
func (r *labelExpressionRewriter) relationshipElement(open, chainStart, chainEnd, rest, close int, variable string, viaIS bool, chain labelChain, quantified bool, mode labelPatternMode, predicates *[]string) error {
	q := r.query
	if chain.colons {
		return labelExpressionSyntaxError(localization.CypherMatchingRelationshipTypeColonConjunction())
	}
	variableLength := rest < close && q[rest] == '*'
	if chain.dynamic {
		resolved, constant, err := r.resolveDynamicChain(chain, mode)
		if err != nil {
			return err
		}
		if !constant {
			// Its value depends on the row: in MATCH a predicate reads it,
			// on each relationship of a variable-length or quantified one;
			// CREATE and MERGE resolve it per row (resolveRowDynamicTokens).
			if mode != labelPatternMatch {
				return nil
			}
			variable = r.elementVariable(open, variable)
			r.edit(chainStart, chainEnd, "")
			if variableLength || quantified {
				each := r.variable()
				*predicates = append(*predicates, "all("+each+" IN "+variable+" WHERE "+chain.expr.predicate(each)+")")
				return nil
			}
			*predicates = append(*predicates, chain.expr.predicate(variable))
			return nil
		}
		if resolved.kind == labelExpressionAnd && len(resolved.operands) == 0 {
			// $([]) or $all([]): no type to match, so every relationship
			// (in CREATE or MERGE, its validation asks for one).
			r.edit(chainStart, chainEnd, "")
			return nil
		}
		chain.expr, chain.symbols = resolved, true
	}
	alternatives, plain := chain.expr.alternatives()
	if chain.barColons {
		hasProperties := strings.IndexByte(q[rest:close], '{') >= 0
		if variable != "" || variableLength || quantified || hasProperties {
			return labelExpressionSyntaxError(localization.CypherMatchingRelationshipTypeColonDisjunction(chain.expr.String()))
		}
	}
	if mode != labelPatternMatch && !plain {
		return labelExpressionSyntaxError(localization.CypherMatchingRelationshipTypeExpressionInWritePattern(mode.clause()))
	}
	if plain {
		// In CREATE and MERGE, the clause's own validation rejects more
		// than one type (NoSingleRelationshipType).
		if viaIS || chain.symbols || chain.barColons {
			// (R|S), R|:S, IS R|S: the plain alternatives.
			text := ":" + labelExpressionNameText(alternatives[0])
			for _, name := range alternatives[1:] {
				text += "|" + labelExpressionNameText(name)
			}
			if text != q[chainStart:chainEnd] {
				r.edit(chainStart, chainEnd, text)
			}
		}
		return nil
	}
	if variableLength {
		return labelExpressionSyntaxError(localization.CypherMatchingVariableLengthTypeExpression())
	}
	kept := ""
	if required := chain.expr.requiredLabels(); len(required) == 1 {
		kept = labelChainText(required)
	}
	variable = r.elementVariable(open, variable)
	r.edit(chainStart, chainEnd, kept)
	if quantified {
		each := r.variable()
		*predicates = append(*predicates, "all("+each+" IN "+variable+" WHERE "+each+":"+chain.expr.String()+")")
		return nil
	}
	*predicates = append(*predicates, variable+":"+chain.expr.String())
	return nil
}

// resolveDynamicChain resolves the dynamic terms of a pattern's label or
// type chain whose values are known before the rows exist (resolveConstant):
// a literal or a parameter. constant is false when a term's value depends on
// the rows; the chain is then read when the rows are. $any() is only for
// MATCH.
func (r *labelExpressionRewriter) resolveDynamicChain(chain labelChain, mode labelPatternMode) (*labelExpression, bool, error) {
	if chain.dynamicAny && mode != labelPatternMatch {
		return nil, false, labelExpressionSyntaxError(localization.CypherCoreDynamicAnyInWritePattern())
	}
	return chain.expr.resolveDynamic(r.resolveConstant)
}

// resolveConstant reads a dynamic term's expression when its value is known
// before the rows exist: a literal (whose wrong type or name is a
// SyntaxError, staticDynamicTokenError) or a supplied parameter.
func (r *labelExpressionRewriter) resolveConstant(expression string) (interface{}, bool, error) {
	expression = strings.TrimSpace(expression)
	if err := staticDynamicTokenError(expression, staticTypeScope{}, dynamicTokenLabel); err != nil {
		return nil, false, err
	}
	if strings.HasPrefix(expression, "$") {
		if name := simpleSemanticIdentifier(expression[1:]); name != "" {
			value, bound := r.params[name]
			return value, bound, nil
		}
		return nil, false, nil
	}
	if value, literal := parseLiteralValueForPipeline(expression); literal {
		return value, true, nil
	}
	return nil, false, nil
}

// writeItemHead reports whether the variable that starts at query[wordStart]
// begins an item of the SET or REMOVE clause being read: the clause keyword
// or a comma comes before it. Only there may a label test be dynamic.
func (r *labelExpressionRewriter) writeItemHead(wordStart int) bool {
	if !r.writeItems {
		return false
	}
	q := r.query
	i := wordStart - 1
	for i >= 0 && isASCIISpace(q[i]) {
		i--
	}
	if i < 0 {
		return false
	}
	if q[i] == ',' {
		return true
	}
	end := i + 1
	for i >= 0 && isIdentByte(q[i]) {
		i--
	}
	word := q[i+1 : end]
	return strings.EqualFold(word, "SET") || strings.EqualFold(word, "REMOVE")
}

// elementVariable is the variable of the element that opens at query[open]:
// its own, or a fresh one inserted after the bracket (before the element's
// other edits, which start at or after it).
func (r *labelExpressionRewriter) elementVariable(open int, variable string) string {
	if variable != "" {
		return variable
	}
	if named, ok := r.named[open]; ok {
		return named
	}
	variable = r.variable()
	r.edit(open+1, open+1, variable)
	if r.named == nil {
		r.named = make(map[int]string)
	}
	r.named[open] = variable
	return variable
}

// nameSingleNodePaths names the node of each path assignment in the MATCH
// pattern query[start:end] that is a single anonymous node (p = (:L {k: 1})),
// so every route binds the node, and the path, as for p = (n:L) (#907).
func (r *labelExpressionRewriter) nameSingleNodePaths(start, end int) {
	q := r.query
	partStart := start
	for i := start; i <= end; i++ {
		if i < end {
			switch c := q[i]; c {
			case '\'', '"', '`':
				i = skipCypherQuotedText(q[:end], i, c) - 1
				continue
			case '(', '[', '{':
				closer := map[byte]rune{'(': ')', '[': ']', '{': '}'}[c]
				if close := findMatchingDelimiter(q[:end], i, rune(c), closer); close > i {
					i = close
				}
				continue
			case ',':
			default:
				continue
			}
		}
		first := skipASCIISpaces(q, partStart, i)
		if startsPathAssignment(q, first, i) {
			open := skipASCIISpaces(q, strings.IndexByte(q[first:i], '=')+first+1, i)
			close := findMatchingDelimiter(q[:i], open, '(', ')')
			inner := skipASCIISpaces(q, open+1, i)
			// Nothing follows the node (a single-node path), and it has no
			// variable: its text starts with ":", ")" or "{".
			if close > open && trimRightIndex(q, close+1, i) == close+1 && strings.IndexByte(":){", q[inner]) >= 0 {
				r.elementVariable(open, "")
			}
		}
		partStart = i + 1
	}
}

// mayAssignAnonymousNodePath is the quick check for nameSingleNodePaths: an
// "= (" whose node starts without a variable (":", ")" or "{").
func mayAssignAnonymousNodePath(query string) bool {
	for i := strings.IndexByte(query, '='); i >= 0; {
		open := skipASCIISpaces(query, i+1, len(query))
		if open < len(query) && query[open] == '(' {
			if inner := skipASCIISpaces(query, open+1, len(query)); inner < len(query) && strings.IndexByte(":){", query[inner]) >= 0 {
				return true
			}
		}
		next := strings.IndexByte(query[i+1:], '=')
		if next < 0 {
			return false
		}
		i += next + 1
	}
	return false
}

// foreach rewrites FOREACH (x IN list | clauses).
func (r *labelExpressionRewriter) foreach(start, end int) error {
	q := r.query
	open := skipASCIISpaces(q, start, end)
	if open >= end || q[open] != '(' {
		return r.expression(start, end)
	}
	close := findMatchingDelimiter(q[:end], open, '(', ')')
	if close < 0 {
		return nil
	}
	bar := topLevelByteIndex(q, open+1, close, '|')
	if bar < 0 {
		return r.expression(open+1, close)
	}
	if err := r.expression(open+1, bar); err != nil {
		return err
	}
	return r.statement(bar+1, close)
}

// expression rewrites the expression query[start:end]: the subqueries, pattern
// predicates and pattern comprehensions in it, and its IS label tests; it
// rejects a colon test that mixes colons with label expression symbols.
func (r *labelExpressionRewriter) expression(start, end int) error {
	q := r.query
	for i := start; i < end; i++ {
		c := q[i]
		// A pattern predicate or comprehension can't quantify a relationship;
		// Neo4j allows it only in MATCH and subqueries.
		if _, quantified := relationshipQuantifierAt(q, i, end); quantified && arrowEndsAt(q, start, i) {
			return labelExpressionSyntaxError(localization.CypherMatchingQuantifierInExpressionPattern(string(c)))
		}
		switch {
		case c == '\'' || c == '"' || c == '`':
			i = skipCypherQuotedText(q, i, c) - 1
		case c == '{':
			close := findMatchingDelimiter(q[:end], i, '{', '}')
			if close < 0 {
				return nil
			}
			if r.opensSubquery(i) {
				if err := r.subquery(i+1, close); err != nil {
					return err
				}
			} else if err := r.expression(i+1, close); err != nil {
				return err
			}
			i = close
		case c == '[':
			close := findMatchingDelimiter(q[:end], i, '[', ']')
			if close < 0 {
				return nil
			}
			if handled, err := r.comprehension(i, close); err != nil {
				return err
			} else if !handled {
				if err := r.expression(i+1, close); err != nil {
					return err
				}
			}
			i = close
		case c == '(':
			next, err := r.parenthesised(start, i, end)
			if err != nil {
				return err
			}
			i = next - 1
		case isIdentStartByte(c) || c == '_':
			if i > start && (isIdentByte(q[i-1]) || q[i-1] == '.' || q[i-1] == '$') {
				continue
			}
			j := i
			for j < end && isIdentByte(q[j]) {
				j++
			}
			if err := shortestPathExpressionError(q, i, j, end); err != nil {
				return err
			}
			if err := r.vectorCall(i, j, end); err != nil {
				return err
			}
			if err := r.labelTest(i, j, end); err != nil {
				return err
			}
			i = j - 1
		}
	}
	return nil
}

// labelTest checks the word at query[wordStart:wordEnd]: a variable followed
// by a colon test (mixing colons with symbols is rejected), by IS and a label
// expression (rewritten to a colon test), or by Cypher 25's IS [NOT] LABELED
// and one (rewritten to the colon test, or NOT it).
func (r *labelExpressionRewriter) labelTest(wordStart, wordEnd, end int) error {
	q := r.query
	if wordEnd < end && q[wordEnd] == ':' && (wordEnd+1 >= end || q[wordEnd+1] != ':') {
		// n:A|B:C (written without spaces: a list comprehension's
		// x:A | x:B is a test and a projection).
		chainEnd, depth := wordEnd+1, 0
	scan:
		for ; chainEnd < end; chainEnd++ {
			c := q[chainEnd]
			if depth == 0 && (isASCIISpace(c) || strings.IndexByte(",]}=<>+-*/^", c) >= 0) {
				break
			}
			switch c {
			case '\'', '"', '`':
				chainEnd = skipCypherQuotedText(q, chainEnd, c) - 1
			case '(':
				depth++
			case ')':
				if depth == 0 {
					break scan
				}
				depth--
			}
		}
		chain, ok := scanLabelChain(q[wordEnd+1:chainEnd], false)
		wordStart := labelTestWordStart(q, wordEnd)
		inChain := wordStart > 0 && q[wordStart-1] == ':' // a label of a chain (n:A:$(e)), read with its variable
		if ok && chain.dynamic && !inChain && !r.writeItemHead(wordStart) {
			if !r.cypher25 || chain.end != chainEnd-wordEnd-1 {
				return labelExpressionSyntaxError(localization.CypherCoreDynamicTokenPositionInvalid())
			}
			// A Cypher 25 label test with a dynamic term: its predicate.
			r.edit(wordStart, chainEnd, "("+chain.expr.predicate(q[wordStart:wordEnd])+")")
			return nil
		}
		if ok && chain.end == chainEnd-wordEnd-1 && chain.colons && chain.symbols {
			return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionMixedColon(chain.expr.String()))
		}
		return nil
	}
	is := skipASCIISpaces(q, wordEnd, end)
	if is == wordEnd || is+2 >= end || !strings.EqualFold(q[is:is+2], "IS") || isIdentByte(q[is+2]) || !isLabelIsKeyword(q[:end], is+2) {
		return nil
	}
	if handled := r.isLabeledTest(wordStart, wordEnd, is+2, end); handled {
		return nil
	}
	if not := skipASCIISpaces(q, is+2, end); not+3 <= end && strings.EqualFold(q[not:not+3], "NOT") && (not+3 == end || !isIdentByte(q[not+3])) {
		// isLabelIsKeyword let IS NOT through: a label follows it.
		operand, _ := isNotLabelOperand(q[:end], not+3)
		return labelExpressionSyntaxError(localization.CypherMatchingIsNotOperandInvalid(operand))
	}
	textStart := skipASCIISpaces(q, is+2, end)
	if dynamicLabelStartsAt(q[:end], textStart) {
		// A dynamic label: x IS $(e) is x:$(e), at a SET or REMOVE item's
		// head only; IS takes one label, so a colon after it is an error as
		// for a static chain.
		closing := findMatchingDelimiter(q[:end], strings.IndexByte(q[textStart:end], '(')+textStart, '(', ')')
		if !r.writeItemHead(labelTestWordStart(q, wordEnd)) {
			if !r.cypher25 || closing < 0 {
				return labelExpressionSyntaxError(localization.CypherCoreDynamicTokenPositionInvalid())
			}
			// A Cypher 25 x IS $(e) test in an expression: x:$(e)'s
			// predicate.
			if chain, ok := scanLabelChain(q[textStart:closing+1], false); ok && chain.dynamic && chain.end == closing+1-textStart {
				variableStart := labelTestWordStart(q, wordEnd)
				r.edit(variableStart, closing+1, "("+chain.expr.predicate(q[variableStart:wordEnd])+")")
				return nil
			}
			return labelExpressionSyntaxError(localization.CypherCoreDynamicTokenPositionInvalid())
		}
		if closing < 0 {
			return nil
		}
		if after := skipASCIISpaces(q, closing+1, end); after < end && q[after] == ':' {
			return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionMixedIs(q[textStart : closing+1]))
		}
		r.edit(wordEnd, textStart, ":")
		return nil
	}
	chain, ok := scanLabelChain(q[textStart:end], false)
	if !ok {
		return nil
	}
	if chain.colons {
		return labelExpressionSyntaxError(localization.CypherMatchingLabelExpressionMixedIs(chain.expr.String()))
	}
	r.edit(wordEnd, textStart, ":")
	return nil
}

// isLabeledTest rewrites x IS [NOT] LABELED <label expression>, whose IS
// ends at query[afterIs], to (x:<expression>) or (NOT x:<expression>);
// handled is false when LABELED doesn't follow.
func (r *labelExpressionRewriter) isLabeledTest(wordStart, wordEnd, afterIs, end int) bool {
	q := r.query
	at, negated := skipASCIISpaces(q, afterIs, end), false
	if matchKeywordAt(q[:end], at, "NOT") {
		at, negated = skipASCIISpaces(q, at+3, end), true
	}
	if !matchKeywordAt(q[:end], at, "LABELED") {
		return false
	}
	textStart := skipASCIISpaces(q, at+len("LABELED"), end)
	chain, ok := scanLabelChain(q[textStart:end], false)
	if !ok || chain.colons {
		return true
	}
	test := q[wordStart:wordEnd] + ":" + chain.expr.String()
	if negated {
		test = "NOT " + test
	}
	r.edit(wordStart, textStart+chain.end, "("+test+")")
	return true
}

// opensSubquery reports whether the { at query[brace] opens a subquery body:
// EXISTS {, COUNT {, COLLECT {, CALL { or CALL (…) {. The word may be the
// clause keyword before the scanned body (CALL).
func (r *labelExpressionRewriter) opensSubquery(brace int) bool {
	q, start := r.query, 0
	i := brace - 1
	for i >= start && isASCIISpace(q[i]) {
		i--
	}
	if i >= start && q[i] == ')' {
		depth := 0
		for ; i >= start; i-- {
			if q[i] == ')' {
				depth++
			} else if q[i] == '(' {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		i--
		for i >= start && isASCIISpace(q[i]) {
			i--
		}
		end := i + 1
		for i >= start && isIdentByte(q[i]) {
			i--
		}
		return strings.EqualFold(q[i+1:end], "CALL")
	}
	end := i + 1
	for i >= start && isIdentByte(q[i]) {
		i--
	}
	switch strings.ToUpper(q[i+1 : end]) {
	case "EXISTS", "COUNT", "COLLECT", "CALL":
		return true
	}
	return false
}

// subquery rewrites a subquery body query[start:end]: a statement, or a
// pattern with an optional WHERE (EXISTS { (n)-->(m) WHERE … }), which
// becomes a MATCH when its label expressions add predicates.
func (r *labelExpressionRewriter) subquery(start, end int) error {
	q := r.query
	first := skipASCIISpaces(q, start, end)
	if first >= end {
		return nil
	}
	if q[first] != '(' && !startsPathAssignment(q, first, end) {
		return r.statement(start, end)
	}
	patternEnd, whereStart := end, -1
	for _, clause := range r.clauses(first, end) {
		if clause.keyword == "WHERE" {
			patternEnd, whereStart = clauseKeywordStart(q, clause.bodyStart), clause.bodyStart
			break
		}
	}
	mark := len(r.edits)
	predicates, err := r.pattern(first, patternEnd, labelPatternMatch)
	if err != nil {
		return err
	}
	if len(predicates) == 0 {
		if whereStart >= 0 {
			return r.expression(whereStart, end)
		}
		return nil
	}
	// The MATCH keyword goes before the pattern's own edits.
	r.edits = append(r.edits[:mark], append([]labelRewriteEdit{{start: first, end: first, text: "MATCH "}}, r.edits[mark:]...)...)
	if whereStart < 0 {
		at := trimRightIndex(q, first, end)
		r.edit(at, at, " WHERE "+strings.Join(predicates, " AND "))
		return nil
	}
	return r.whereWithPredicates(whereStart, end, predicates)
}

// whereWithPredicates ANDs predicates in front of the WHERE body
// query[start:end] and rewrites the body.
func (r *labelExpressionRewriter) whereWithPredicates(start, end int, predicates []string) error {
	q := r.query
	bodyStart := skipASCIISpaces(q, start, end)
	bodyEnd := trimRightIndex(q, bodyStart, end)
	body := q[bodyStart:bodyEnd]
	wrap := findTopLevelKeyword(body, " OR ") >= 0 || findTopLevelKeyword(body, " XOR ") >= 0
	prefix := strings.Join(predicates, " AND ") + " AND "
	if wrap {
		prefix += "("
	}
	r.edit(bodyStart, bodyStart, prefix)
	if err := r.expression(bodyStart, bodyEnd); err != nil {
		return err
	}
	if wrap {
		r.edit(bodyEnd, bodyEnd, ")")
	}
	return nil
}

// parenthesised handles the ( at query[open] in an expression: a pattern
// predicate ((n)-->(:A|B), also as exists(…)'s argument), or a group or
// call whose inside is an expression. It returns where scanning resumes.
func (r *labelExpressionRewriter) parenthesised(start, open, end int) (int, error) {
	q := r.query
	close := findMatchingDelimiter(q[:end], open, '(', ')')
	if close < 0 {
		return end, nil
	}
	call := open > start && isIdentByte(q[open-1])
	if !call {
		if chainEnd, ok := relationshipChainEnd(r.query, open, end); ok {
			return chainEnd, r.patternPredicate(open, chainEnd, -1, -1)
		}
	} else {
		// exists((n)-->(m)): the argument as the predicate.
		nameStart := open - 1
		for nameStart > start && isIdentByte(q[nameStart-1]) {
			nameStart--
		}
		inner := skipASCIISpaces(q, open+1, close)
		if strings.EqualFold(q[nameStart:open], "exists") {
			if chainEnd, ok := relationshipChainEnd(r.query, inner, close); ok && skipASCIISpaces(q, chainEnd, close) == close {
				return close + 1, r.patternPredicate(inner, chainEnd, nameStart, close)
			}
		}
	}
	return close + 1, r.expression(open+1, close)
}

// patternPredicate rewrites the pattern predicate query[start:end]. When its
// label expressions add predicates it becomes EXISTS { MATCH … WHERE … }; a
// call exists(…) around it (query[callStart:callClose+1]) is replaced.
func (r *labelExpressionRewriter) patternPredicate(start, end, callStart, callClose int) error {
	mark := len(r.edits)
	predicates, err := r.pattern(start, end, labelPatternMatch)
	if err != nil || len(predicates) == 0 {
		return err
	}
	open := labelRewriteEdit{start: start, end: start, text: "EXISTS { MATCH "}
	closing := labelRewriteEdit{start: end, end: end, text: " WHERE " + strings.Join(predicates, " AND ") + " }"}
	if callStart >= 0 {
		open = labelRewriteEdit{start: callStart, end: start, text: "EXISTS { MATCH "}
		closing = labelRewriteEdit{start: end, end: callClose + 1, text: closing.text}
	}
	r.edits = append(r.edits[:mark], append([]labelRewriteEdit{open}, r.edits[mark:]...)...)
	r.edits = append(r.edits, closing)
	return nil
}

// comprehension rewrites the pattern comprehension query[open:close+1]
// ([(n)-->(m:A|B) WHERE … | m.x]); handled is false for any other list.
func (r *labelExpressionRewriter) comprehension(open, close int) (bool, error) {
	q := r.query
	i := skipASCIISpaces(q, open+1, close)
	if startsPathAssignment(q, i, close) {
		i = skipASCIISpaces(q, strings.IndexByte(q[i:close], '=')+i+1, close)
	}
	if i >= close || q[i] != '(' {
		return false, nil
	}
	chainEnd, ok := relationshipChainEnd(r.query, i, close)
	if !ok {
		return false, nil
	}
	after := skipASCIISpaces(q, chainEnd, close)
	whereStart := -1
	if after+5 <= close && strings.EqualFold(q[after:after+5], "WHERE") && (after+5 == close || !isIdentByte(q[after+5])) {
		whereStart = after + 5
	}
	bar := r.projectionBar(chainEnd, close)
	if bar < 0 {
		return false, nil
	}
	predicates, err := r.pattern(i, chainEnd, labelPatternMatch)
	if err != nil {
		return true, err
	}
	switch {
	case whereStart >= 0 && len(predicates) > 0:
		err = r.whereWithPredicates(whereStart, bar, predicates)
	case whereStart >= 0:
		err = r.expression(whereStart, bar)
	case len(predicates) > 0:
		r.edit(chainEnd, chainEnd, " WHERE "+strings.Join(predicates, " AND "))
	}
	if err != nil {
		return true, err
	}
	return true, r.expression(bar+1, close)
}

// projectionBar finds the | before a pattern comprehension's projection in
// query[start:end]: the first top-level | that is not a label expression's
// (written without spaces between two names, as in m:A|B).
func (r *labelExpressionRewriter) projectionBar(start, end int) int {
	q := r.query
	for i := start; ; {
		bar := topLevelByteIndex(q, i, end, '|')
		if bar < 0 || !labelExpressionBarAt(q[:end], start, bar) {
			return bar
		}
		i = bar + 1
	}
}

// relationshipChainEnd returns where the pattern starting with the node at
// q[open] ends, within q[:end], when it has at least one relationship:
// (a)-[r]->(b)<--(c). It is the one reader of a pattern's extent in an
// expression: the label-expression rewriter, the free-variable scan and the
// pattern placement check use it.
func relationshipChainEnd(q string, open, end int) (int, bool) {
	close := findMatchingDelimiter(q[:end], open, '(', ')')
	if close < 0 {
		return 0, false
	}
	inner := skipASCIISpaces(q, open+1, close)
	if inner < close && q[inner] == '(' {
		return 0, false
	}
	chainEnd, relationships := close+1, 0
	for {
		i := skipASCIISpaces(q, chainEnd, end)
		if i < end && q[i] == '<' {
			i++
		}
		if i >= end || q[i] != '-' {
			break
		}
		i++
		if i < end && q[i] == '[' {
			bracket := findMatchingDelimiter(q[:end], i, '[', ']')
			if bracket < 0 {
				break
			}
			i = bracket + 1
			if i >= end || q[i] != '-' {
				break
			}
			i++
		} else if i >= end || q[i] != '-' {
			break // a minus sign
		} else {
			i++
		}
		if i < end && q[i] == '>' {
			i++
		}
		i = skipASCIISpaces(q, i, end)
		if i >= end || q[i] != '(' {
			break
		}
		node := findMatchingDelimiter(q[:end], i, '(', ')')
		if node < 0 {
			break
		}
		chainEnd = node + 1
		relationships++
	}
	return chainEnd, relationships > 0
}

// startsPathAssignment reports whether query[i:end] starts with a path
// assignment (p = (…)).
func startsPathAssignment(q string, i, end int) bool {
	_, nameEnd, ok := scanSymbolicName(q[:end], i)
	if !ok {
		return false
	}
	eq := skipASCIISpaces(q, nameEnd, end)
	if eq >= end || q[eq] != '=' || eq+1 < end && q[eq+1] == '=' {
		return false
	}
	paren := skipASCIISpaces(q, eq+1, end)
	return paren < end && q[paren] == '('
}

// topLevelByteIndex returns the index of the first b in q[start:end] outside
// quotes and brackets, -1 when there is none.
func topLevelByteIndex(q string, start, end int, b byte) int {
	depth := 0
	for i := start; i < end; i++ {
		c := q[i]
		switch c {
		case '\'', '"', '`':
			i = skipCypherQuotedText(q, i, c) - 1
			continue
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		}
		if c == b && depth == 0 {
			return i
		}
	}
	return -1
}

// clauseKeywordStart returns where the clause keyword that ends at
// query[bodyStart] starts.
func clauseKeywordStart(q string, bodyStart int) int {
	i := bodyStart
	for i > 0 && isIdentByte(q[i-1]) {
		i--
	}
	return i
}

func skipASCIISpaces(q string, i, end int) int {
	for i < end && isASCIISpace(q[i]) {
		i++
	}
	return i
}

// trimRightIndex returns end moved back over trailing spaces, not before
// start.
func trimRightIndex(q string, start, end int) int {
	for end > start && isASCIISpace(q[end-1]) {
		end--
	}
	return end
}

func isASCIILetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// labelTestWordStart is where the variable of a label test that ends at
// query[wordEnd] starts.
func labelTestWordStart(q string, wordEnd int) int {
	start := wordEnd
	for start > 0 && isIdentByte(q[start-1]) {
		start--
	}
	return start
}
