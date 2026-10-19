// CASE expression implementation for NornicDB Cypher.
// Supports both searched CASE and simple CASE expressions.
//
// Searched CASE:
//   CASE WHEN condition THEN result [WHEN ...] [ELSE default] END
//
// Simple CASE:
//   CASE expression WHEN value THEN result [WHEN ...] [ELSE default] END

package cypher

import (
	"context"
	"reflect"
	"strings"
	"unicode"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// caseWhenClause represents a single WHEN ... THEN clause in a CASE expression.
type caseWhenClause struct {
	condition string // WHEN condition (for searched CASE)
	value     string // WHEN value (for simple CASE)
	result    string // THEN result
}

// caseExpression represents a parsed CASE expression.
type caseExpression struct {
	isSimple       bool             // true for simple CASE, false for searched CASE
	testExpression string           // expression to test (simple CASE only)
	whenClauses    []caseWhenClause // list of WHEN clauses
	elseResult     string           // ELSE result (optional)
}

// isCaseExpression checks if an expression is a CASE expression.
func isCaseExpression(expr string) bool {
	trimmed := strings.TrimSpace(expr)
	return hasPrefixFoldASCII(trimmed, "CASE") && hasSuffixFoldASCII(trimmed, "END") && leadingCaseExpressionEnd(trimmed) == len(trimmed)
}

// leadingCaseExpressionEnd returns the exclusive end of a leading CASE ... END
// expression, including nested CASE expressions. A trailing operator or
// postfix expression is deliberately excluded so callers can treat CASE as an
// atomic operand without allowing operators inside WHEN predicates to escape.
func leadingCaseExpressionEnd(expr string) int {
	expr = strings.TrimSpace(expr)
	// The first identifier token must be CASE itself; a CASE keyword nested
	// inside a compound expression (acc + CASE … END) is not a leading CASE.
	firstWord, firstNext, firstOK := scanIdentifierToken(expr, 0)
	if !firstOK || !strings.EqualFold(firstWord, "case") {
		return -1
	}
	depth := 1
	for index := firstNext; index < len(expr); {
		if expr[index] == '\'' || expr[index] == '"' || expr[index] == '`' {
			index = numericValidationSkipQuoted(expr, index)
			continue
		}
		word, next, ok := scanIdentifierToken(expr, index)
		if !ok {
			index++
			continue
		}
		switch {
		case strings.EqualFold(word, "case") && caseKeywordAt(expr, index, "CASE"):
			depth++
		case strings.EqualFold(word, "end") && depth > 0 && caseKeywordAt(expr, index, "END"):
			depth--
			if depth == 0 {
				return next
			}
		}
		index = next
	}
	return -1
}

// parseCaseExpression parses a CASE expression into its components.
// Supports both searched and simple CASE expressions.
func parseCaseExpression(expr string) (*caseExpression, error) {
	expr = strings.TrimSpace(expr)
	upper := upperASCII(expr)

	// Remove CASE and END keywords
	if !strings.HasPrefix(upper, "CASE") || !strings.HasSuffix(upper, "END") {
		return nil, localizedError(localization.CypherCoreCaseEnvelopeInvalid(), nil)
	}

	// Extract the content between CASE and END
	inner := expr[4 : len(expr)-3]
	content := strings.TrimSpace(inner)
	// Keywords are found in the whole expression, so the keyword reader sees
	// the CASE and END around the body (CASE case WHEN 1 THEN case END, where
	// case is the test; #1162); indices are content's.
	contentStart := 4 + len(inner) - len(strings.TrimLeftFunc(inner, unicode.IsSpace))
	findAtLevel := func(from int, keyword string) int {
		index := findCaseKeywordAtLevel(expr, contentStart+from, keyword)
		if index < 0 || index >= contentStart+len(content) {
			return -1
		}
		return index - contentStart
	}

	ce := &caseExpression{
		whenClauses: []caseWhenClause{},
	}

	// Only keywords at the current CASE level delimit the outer expression.
	// A plain string search incorrectly treats WHEN/ELSE from a nested CASE as
	// belonging to its parent.
	firstWhenIdx := findAtLevel(0, "WHEN")
	if firstWhenIdx == -1 {
		return nil, localizedError(localization.CypherCoreCaseWhenRequired(), nil)
	}

	beforeFirstWhen := strings.TrimSpace(content[:firstWhenIdx])
	if beforeFirstWhen != "" {
		// Simple CASE: CASE expression WHEN value THEN result ...
		ce.isSimple = true
		ce.testExpression = beforeFirstWhen
	}

	for cursor := firstWhenIdx; cursor < len(content); {
		conditionStart := cursor + len("WHEN")
		thenIndex := findAtLevel(conditionStart, "THEN")
		if thenIndex < 0 {
			return nil, localizedError(localization.CypherCoreCaseThenRequired(content[cursor:]), nil)
		}
		condition := strings.TrimSpace(content[conditionStart:thenIndex])
		resultStart := thenIndex + len("THEN")
		nextWhen := findAtLevel(resultStart, "WHEN")
		nextElse := findAtLevel(resultStart, "ELSE")
		resultEnd := len(content)
		next := len(content)
		if nextWhen >= 0 && nextWhen < resultEnd {
			resultEnd, next = nextWhen, nextWhen
		}
		if nextElse >= 0 && nextElse < resultEnd {
			resultEnd, next = nextElse, nextElse
		}
		result := strings.TrimSpace(content[resultStart:resultEnd])
		if condition == "" || result == "" {
			return nil, localizedError(localization.CypherCoreCaseThenRequired(content[cursor:]), nil)
		}
		clause := caseWhenClause{result: result}
		if ce.isSimple {
			clause.value = condition
		} else {
			clause.condition = condition
		}
		ce.whenClauses = append(ce.whenClauses, clause)

		if next == len(content) {
			break
		}
		if next == nextElse {
			ce.elseResult = strings.TrimSpace(content[nextElse+len("ELSE"):])
			break
		}
		cursor = nextWhen
	}

	if len(ce.whenClauses) == 0 {
		return nil, localizedError(localization.CypherCoreCaseWhenRequired(), nil)
	}

	return ce, nil
}

// findCaseKeywordAtLevel finds a CASE grammar keyword while ignoring quoted
// text, nested delimiters, and complete nested CASE ... END expressions.
func findCaseKeywordAtLevel(expression string, start int, keyword string) int {
	parenDepth, bracketDepth, braceDepth, caseDepth := 0, 0, 0, 0
	for index := start; index < len(expression); {
		if expression[index] == '\'' || expression[index] == '"' || expression[index] == '`' {
			index = numericValidationSkipQuoted(expression, index)
			continue
		}
		switch expression[index] {
		case '(':
			parenDepth++
			index++
			continue
		case ')':
			parenDepth--
			index++
			continue
		case '[':
			bracketDepth++
			index++
			continue
		case ']':
			bracketDepth--
			index++
			continue
		case '{':
			braceDepth++
			index++
			continue
		case '}':
			braceDepth--
			index++
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || !isCaseWordStart(expression, index) {
			index++
			continue
		}
		end := index + 1
		for end < len(expression) && isIdentByte(expression[end]) {
			end++
		}
		word := expression[index:end]
		if caseKeywordAt(expression, index, "CASE") {
			caseDepth++
		} else if caseDepth > 0 && caseKeywordAt(expression, index, "END") {
			caseDepth--
		} else if caseDepth == 0 && strings.EqualFold(word, keyword) && caseKeywordAt(expression, index, keyword) {
			return index
		}
		index = end
	}
	return -1
}

// caseKeywordAt reports whether s[i:] is the CASE grammar keyword keyword
// (CASE, WHEN, THEN, ELSE, END) as a keyword, not a name that is the same
// word: a property (n.end, m.when), a label or type (n:End), a parameter
// ($end) or a map key ({end: 1}, followed by one colon; END :: INTEGER is a
// type predicate on a CASE), or a variable of the keyword's name where an
// operand stands (ELSE end.id END, THEN then, WHEN case.id = 1; see
// caseOperandExpectedBefore). Every scanner of the CASE grammar reads its
// keywords through it.
func caseKeywordAt(s string, i int, keyword string) bool {
	return caseKeywordAtMode(s, i, keyword, true)
}

// caseKeywordAtMode can use the non-balancing prefix reader when the closure
// budget itself scans keywords, avoiding recursive balance scans.
func caseKeywordAtMode(s string, i int, keyword string, balancePrefix bool) bool {
	if !matchKeywordAt(s, i, keyword) {
		return false
	}
	if i > 0 {
		switch s[i-1] {
		case '.', ':', '$':
			return false
		}
	}
	switch keyword {
	case "END":
		// END follows a complete operand; where one is expected the word is
		// a variable (ELSE end.id END).
		if caseOperandExpectedBeforeMode(s, i, balancePrefix) {
			return false
		}
	case "THEN", "ELSE":
		// As END. Scanners also read a CASE's body (WHEN … THEN … ELSE …)
		// as text of its own: at its start the word is a keyword unless it
		// is a simple CASE's test (then WHEN 1 …).
		if !caseHasPreviousToken(s, i) {
			if caseSimpleTestContinuesAt(s, i+len(keyword)) {
				return false
			}
		} else if caseOperandExpectedBeforeMode(s, i, balancePrefix) {
			return false
		}
	case "WHEN":
		// WHEN follows a complete operand, or opens a searched CASE right
		// after CASE or at the start of a CASE body: there the word is a
		// variable only when what follows continues an operand (CASE when
		// WHEN 1, CASE when.id WHEN 1).
		if !caseHasPreviousToken(s, i) || casePreviousWordIsCase(s, i) {
			if caseSimpleTestContinuesAt(s, i+len(keyword)) {
				return false
			}
		} else if caseOperandExpectedBeforeMode(s, i, balancePrefix) {
			return false
		}
	case "CASE":
		// A CASE that opens an expression is followed by an operand or WHEN;
		// followed by what only follows an operand, it is a variable
		// (THEN case END, WHEN case.id = 1). After AS it is the name AS
		// gives (WITH 1 AS case RETURN case), and right after CASE it is a
		// simple CASE's test when a nested CASE would leave the outer one
		// without its END (CASE case WHEN 1 THEN case END; #1162).
		if caseVariableNamedCase(s, i+len(keyword)) || casePreviousWordIn(s, i, "as") ||
			(casePreviousWordIsCase(s, i) && caseNestedLacksEnd(s, i, balancePrefix)) {
			return false
		}
	}
	next := i + len(keyword)
	for next < len(s) && isASCIISpace(s[next]) {
		next++
	}
	return next >= len(s) || s[next] != ':' || (next+1 < len(s) && s[next+1] == ':')
}

// caseOperandExpectedBefore reports whether an operand, not an operator or
// END, is expected at s[i]: at the start, after an opening bracket, a comma or
// an operator, or after a keyword that takes an operand (WHEN, THEN, ELSE, AND,
// IN, AS, …). END always follows a complete operand, so a word end there is a
// variable, as Neo4j reads ELSE end.id END and ELSE end END.id.
func caseOperandExpectedBefore(s string, i int) bool {
	return caseOperandExpectedBeforeMode(s, i, true)
}

func caseOperandExpectedBeforeMode(s string, i int, balancePrefix bool) bool {
	j := queryGapStart(s, i) - 1
	if j < 0 {
		return true
	}
	if !isIdentByte(s[j]) {
		if s[j] == '>' && caseTypeParameterCloses(s, j) {
			return false // LIST<INTEGER> completes a type, as in x IS :: LIST<INTEGER>
		}
		if s[j] == '%' {
			// After a colon or a label expression symbol, % is the wildcard
			// label (n:%), which completes an operand, not the modulo operator.
			k := j - 1
			for k >= 0 && isASCIISpace(s[k]) {
				k--
			}
			if k >= 0 && strings.IndexByte(":|&!", s[k]) >= 0 {
				return false
			}
		}
		if s[j] == ':' && (j == 0 || s[j-1] != ':') {
			return true // a map value follows its key's colon ({end: end})
		}
		return strings.IndexByte("([{,=<>+-*/%^|", s[j]) >= 0
	}
	start := j
	for start > 0 && isIdentByte(s[start-1]) {
		start--
	}
	if !functionNameStartsAt(s, start) || start > 0 && (s[start-1] == '$' || s[start-1] == ':') {
		return false // a property, parameter or label completes an operand
	}
	switch word := lowerASCII(s[start : j+1]); word {
	case "case", "when", "then", "else":
		// After the keyword an operand is expected; after a variable of that
		// name, it is complete.
		return caseKeywordAtMode(s, start, upperASCII(word), balancePrefix)
	case "with", "where", "return":
		if word == "with" && casePreviousWordIn(s, start, "starts", "ends") {
			return true // STARTS WITH, ENDS WITH
		}
		// Clause keywords start the text or follow a complete clause; where
		// an operand is expected, or as a CASE body's test (where WHEN 1 …),
		// the word is a variable that completes one.
		if !caseHasPreviousToken(s, start) {
			return !caseSimpleTestContinuesAt(s, j+1)
		}
		return !caseOperandExpectedBeforeMode(s, start, balancePrefix)
	case "and", "or", "xor", "in", "contains", "as":
		// Infix keywords follow a complete operand. Where one is expected,
		// or at the start of a CASE body read on its own (and WHEN 1 …), the
		// word is a variable that completes one.
		return caseHasPreviousToken(s, start) && !caseOperandExpectedBeforeMode(s, start, balancePrefix)
	case "not", "distinct":
		// Prefix keywords stand where an operand is expected, as a variable
		// does: the word is the keyword unless what follows ends an operand.
		return !casePrefixWordIsVariable(s, j+1, balancePrefix)
	}
	return false
}

// caseTypeParameterCloses reports whether the > at s[j] closes a type's
// parameter (LIST<INTEGER>, ARRAY<STRING>, ANY<INTEGER | STRING>, nested
// ones too), not the greater-than operator: its matching < follows a type
// constructor word.
func caseTypeParameterCloses(s string, j int) bool {
	depth := 0
	for k := j; k >= 0; k-- {
		switch s[k] {
		case '>':
			depth++
		case '<':
			depth--
			if depth == 0 {
				end := k
				for end > 0 && isASCIISpace(s[end-1]) {
					end--
				}
				start := end
				for start > 0 && isIdentByte(s[start-1]) {
					start--
				}
				switch lowerASCII(s[start:end]) {
				case "list", "array", "any":
					return true
				}
				return false
			}
		case '(', ')', '[', ']', '{', '}', '"', '\'':
			if depth == 1 {
				return false // not inside one type's brackets
			}
		}
	}
	return false
}

// casePreviousWordIn reports whether the word before s[i] is one of words.
func casePreviousWordIn(s string, i int, words ...string) bool {
	j := queryGapStart(s, i) - 1
	end := j + 1
	for j >= 0 && isIdentByte(s[j]) {
		j--
	}
	previous := lowerASCII(s[j+1 : end])
	for _, word := range words {
		if previous == word {
			return true
		}
	}
	return false
}

// casePrefixWordIsVariable reports, for a NOT or DISTINCT word ending at
// s[i], whether what follows makes it a variable: the end of the text, or a
// word that is itself followed by what ends an operand (NOT end.x: end is
// the variable and NOT the keyword; = not THEN 1: THEN is the keyword and
// not the variable).
func casePrefixWordIsVariable(s string, i int, balancePrefix bool) bool {
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) || !isIdentStartByte(s[i]) {
		return i >= len(s)
	}
	// A terminal END closes the CASE after a variable named NOT or
	// DISTINCT; treating it as their operand would leave no closing END.
	if caseWordAt(s, i) == "end" && strings.TrimSpace(s[i+len("end"):]) == "" {
		return true
	}
	if balancePrefix && caseWordAt(s, i) == "end" && casePrefixEndRequired(s, i) {
		return true
	}
	// WHEN terminates a simple CASE's test, so NOT end WHEN ... reads
	// END as the negated variable rather than NOT as the test variable.
	if matchKeywordAt(s, queryGapEnd(s, i+len(caseWordAt(s, i))), "WHEN") {
		return false
	}
	return !caseOperandEndsAt(s, i+len(caseWordAt(s, i)))
}

// caseHasPreviousToken reports whether anything but space precedes s[i].
func caseHasPreviousToken(s string, i int) bool {
	for j := i - 1; j >= 0; j-- {
		if !isASCIISpace(s[j]) {
			return true
		}
	}
	return false
}

// casePreviousWordIsCase reports whether the word before s[i] is CASE.
// caseNestedLacksEnd reports, for a CASE right after CASE at s[i], whether
// reading it as a nested CASE leaves the outer CASE without an END before
// the enclosing bracket or the text ends: CASE case WHEN 1 THEN case END has
// one END, a nested CASE CASE WHEN … END WHEN … END two.
func caseNestedLacksEnd(s string, i int, balancePrefix bool) bool {
	open, depth := 2, 0
	for j := i + len("CASE"); j < len(s); j++ {
		switch c := s[j]; c {
		case '\'', '"', '`':
			j = skipCypherQuotedText(s, j, c) - 1
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth == 0 {
				return open > 0
			}
			depth--
			continue
		}
		if depth != 0 || !isIdentStartByte(s[j]) || isIdentByte(s[j-1]) {
			continue
		}
		if caseKeywordAtMode(s, j, "CASE", balancePrefix) {
			open++
		} else if caseKeywordAtMode(s, j, "END", balancePrefix) {
			if open--; open == 0 {
				return false
			}
		}
	}
	return open > 0
}

func casePreviousWordIsCase(s string, i int) bool {
	j := queryGapStart(s, i) - 1
	return j >= 3 && matchKeywordAt(s, j-3, "CASE")
}

// caseOperandEndsAt reports whether what starts at s[i] can only follow a
// complete operand: the end of the text, a property access, an infix or
// closing symbol, or THEN, ELSE, END and the infix keywords.
func caseOperandEndsAt(s string, i int) bool {
	start := i
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) {
		return true
	}
	if !isIdentStartByte(s[i]) {
		return strings.IndexByte(".=<>*/%^,)]}", s[i]) >= 0 || caseBinaryOperatorAt(s, i) || (s[i] == '[' && i == start)
	}
	switch caseWordAt(s, i) {
	case "then", "else", "end", "as", "and", "or", "xor", "is", "in", "contains", "starts", "ends":
		return true
	}
	return false
}

// caseVariableNamedCase reports, for the word case ending at s[i], whether it
// is a variable: what follows can only follow an operand. A word that itself
// continues a simple CASE's test (CASE end WHEN 1, CASE and WHEN 1) is that
// test, so the CASE before it opens an expression. A clause keyword after
// case is read alike: WITH case WHERE … is the variable case, CASE where
// WHEN 1 a simple CASE over where (#1162).
func caseVariableNamedCase(s string, i int) bool {
	if !caseOperandEndsAt(s, i) && !isNameableClauseKeyword(caseWordAt(s, skipSpaces(s, i))) {
		return false
	}
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i < len(s) && isIdentStartByte(s[i]) {
		word := caseWordAt(s, i)
		return !caseSimpleTestContinuesAt(s, i+len(word))
	}
	return true
}

// caseSimpleTestContinuesAt reports, for a word right after CASE, whether
// what follows it at s[i] continues a simple CASE's test operand: a
// property access, an infix symbol or keyword, or the WHEN that ends the
// test (a WHEN itself followed by an operand).
func caseSimpleTestContinuesAt(s string, i int) bool {
	start := i
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) {
		return false
	}
	if !isIdentStartByte(s[i]) {
		return strings.IndexByte(".=<>*/%^", s[i]) >= 0 || caseBinaryOperatorAt(s, i) || (s[i] == '[' && i == start)
	}
	switch word := caseWordAt(s, i); word {
	case "and", "or", "xor", "is", "in", "contains", "starts", "ends":
		// The infix keyword is followed by an operand; a variable of that
		// name by what ends one (WHEN and.id, WHEN in > 1).
		return !caseOperandEndsAt(s, i+len(word))
	case "when":
		return !caseOperandEndsAt(s, i+len(word))
	}
	return false
}

// caseBinaryOperatorAt reports a + or - at s[i] followed by space: the
// binary operator, which continues an operand (case + 1), not a sign that
// starts one (CASE -1 WHEN …). A [ right against the word before it is a
// subscript (when[0]); after a space it may open a list (CASE WHEN [1] …).
func caseBinaryOperatorAt(s string, i int) bool {
	return (s[i] == '+' || s[i] == '-') && i+1 < len(s) && isASCIISpace(s[i+1])
}

// caseWordAt is the lower-case identifier word starting at s[i].
func caseWordAt(s string, i int) string {
	end := i
	for end < len(s) && isIdentByte(s[end]) {
		end++
	}
	return lowerASCII(s[i:end])
}

func isCaseWordStart(expression string, index int) bool {
	return (index == 0 || !isIdentByte(expression[index-1])) &&
		isIdentStartByte(expression[index])
}

// caseBlockSpan is the [start, end) text range of one CASE … END expression.
type caseBlockSpan struct {
	start int
	end   int
}

// caseBlockSpans returns the text spans of the outermost CASE … END blocks in
// expr, outside quoted literals, for the evaluators that evaluate each block
// for the row before the rest of the expression. Nested CASE blocks are
// contained in their outer span. A CASE inside a scope that binds names (a
// list or pattern comprehension, reduce / all / any / none / single / filter,
// a subquery's { … }) is not returned: it reads the scope's variables, so it is
// evaluated with the scope, not for the row. It returns nil (no allocation)
// when there are no such blocks. A keyword preceded by a dot is a property
// access, not a CASE keyword.
func caseBlockSpans(expr string) []caseBlockSpan {
	var spans []caseBlockSpan
	stack := make([]int, 0, 2)
	var delimiters []bool // open ( [ {: true when it opens a scope
	scopes := 0
	quote := byte(0)
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if quote != 0 {
			if ch == quote && !isBackslashEscaped(expr, i) {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
			continue
		case '(', '[', '{':
			scope := caseVariableScopeOpensAt(expr, i)
			delimiters = append(delimiters, scope)
			if scope {
				scopes++
			}
			continue
		case ')', ']', '}':
			if n := len(delimiters); n > 0 {
				if delimiters[n-1] {
					scopes--
				}
				delimiters = delimiters[:n-1]
			}
			continue
		}
		if scopes > 0 {
			continue
		}
		{
			if (ch == 'C' || ch == 'c') && caseKeywordAt(expr, i, "CASE") {
				stack = append(stack, i)
				continue
			}
			if (ch == 'E' || ch == 'e') && caseKeywordAt(expr, i, "END") && len(stack) > 0 {
				start := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					spans = append(spans, caseBlockSpan{start: start, end: i + 3})
				}
			}
		}
	}
	return spans
}

// caseVariableScopeOpensAt reports whether the delimiter at expr[open] (one
// of ( [ {) opens a scope that binds names: a local binding (localBindingAt:
// [x IN …], [p = (a)-->(b) | …], reduce / allReduce / all / any / none /
// single / filter), an unnamed pattern comprehension [(a)-->(b) | …], or
// the { … } of an EXISTS, COUNT, COLLECT or CALL subquery.
func caseVariableScopeOpensAt(expr string, open int) bool {
	if expr[open] != '{' {
		if _, ok := localBindingAt(expr, open); ok {
			return true
		}
	}
	switch expr[open] {
	case '[':
		next := skipSpaces(expr, open+1)
		return next < len(expr) && expr[next] == '('
	case '(':
		return false
	default: // {
		end := open
		for end > 0 && isASCIISpace(expr[end-1]) {
			end--
		}
		start := end
		for start > 0 && isIdentByte(expr[start-1]) {
			start--
		}
		if start == end || start > 0 && expr[start-1] == '.' {
			return false
		}
		name := lowerASCII(expr[start:end])
		return name == "exists" || name == "count" || name == "collect" || name == "call"
	}
}

// evaluateCaseExpression evaluates a CASE expression and returns the result.
func (e *StorageExecutor) evaluateCaseExpression(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	parsed, err := parseCaseExpression(expr)
	if err != nil {
		return nil
	}
	value, _, err := runCaseExpression(parsed, cypherVersionFromContext(ctx) == "25", func(part caseValuePart, _ int, text string) (interface{}, bool, error) {
		return localSubqueryScope(ctx, text, func(ctx context.Context) (interface{}, bool, error) {
			if part == caseWhen && !parsed.isSimple {
				return e.conditionValue(ctx, text, nodes, rels), true, nil
			}
			return e.evaluateExpressionWithContextFull(ctx, text, nodes, rels, paths, allPathEdges, allPathNodes, pathLength), true, nil
		})
	})
	if err != nil {
		recordExpressionFailure(ctx, err)
	}
	return value
}

// evaluateCondition reports whether a CASE WHEN (or WITH … WHERE) condition
// holds: its value read as a predicate (cypherPredicateTruth), so a
// non-predicate value is a TypeError, recorded on ctx (#907).
func (e *StorageExecutor) evaluateCondition(ctx context.Context, condition string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) bool {
	truth, err := cypherPredicateTruth(e.conditionValue(ctx, condition, nodes, rels))
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	return truth == truthTrue
}

// conditionValue is the value of a condition (evaluateCondition): OR, XOR,
// AND and NOT as everywhere (evaluateLogicalExpression), then the
// predicates conditions read directly, then any other expression.
func (e *StorageExecutor) conditionValue(ctx context.Context, condition string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	condition = strings.TrimSpace(condition)
	upper := upperASCII(condition)

	if value, logical, _, err := evaluateLogicalExpressionInContext(ctx, condition, func(operand string, position foldPosition) (interface{}, bool, error) {
		return contextFoldOperand(ctx, operand, position, func(ctx context.Context) interface{} {
			return e.conditionValue(ctx, operand, nodes, rels)
		})
	}); logical {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return value
	}

	resolveComparisonOperand := func(operand string) interface{} {
		return e.evaluateExpressionWithContext(ctx, operand, nodes, rels)
	}
	// IN and NOT IN are unknown for a null in the list (x IN [1, null]).
	if indexASCIIFold(condition, " in ") >= 0 {
		if truth, ok := inPredicateTruth(condition, resolveComparisonOperand); ok {
			return truth.value()
		}
	}
	if result, matched, _, err := evaluateComparisonChainInContext(ctx, condition, func(operand string, position foldPosition) (interface{}, bool, error) {
		return contextFoldOperand(ctx, operand, position, func(ctx context.Context) interface{} {
			return e.evaluateExpressionWithContext(ctx, operand, nodes, rels)
		})
	}, compareCypherPredicateValue, cypherVersionFromContext(ctx) == "25", e.graphIdentityDomain()); matched {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return result
	}

	// Handle IS NULL / IS NOT NULL
	if strings.HasSuffix(upper, " IS NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-8])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return nullEvaluationHandler(false).evaluate(val)
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-12])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return nullEvaluationHandler(true).evaluate(val)
	}

	for _, p := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{" STARTS WITH ", strings.HasPrefix}, {" ENDS WITH ", strings.HasSuffix}, {" CONTAINS ", strings.Contains},
	} {
		if left, right, ok := splitByOperatorOutsideCase(condition, p.operator, true, true); ok {
			return rowStringPredicateValue(e.evaluateExpressionWithContext(ctx, left, nodes, rels), e.evaluateExpressionWithContext(ctx, right, nodes, rels), p.match)
		}
	}

	// Handle label check: n:Label, n:A:B, n:A|B (the shared label test, #860).
	if variable, expr, ok := parseWithWhereLabelTest(condition); ok {
		if node, bound := nodes[variable]; bound {
			return entityHasAllLabelsOrTypesPredicate(node, expr)
		}
		if rel, bound := rels[variable]; bound {
			return entityHasAllLabelsOrTypesPredicate(rel, expr)
		}
	}

	return e.evaluateExpressionWithContext(ctx, condition, nodes, rels)
}

// findTopLevelKeyword finds a keyword at the top level (not inside parentheses or strings)
// Returns the byte index in the original string, or -1 if not found.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func findTopLevelKeyword(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	depth := 0
	inString := false
	var stringChar byte
	for i := 0; i+len(keyword) <= len(s); i++ {
		ch := s[i]
		if ch == '\'' || ch == '"' {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		case 'C', 'c':
			// CASE … END nests like parentheses: the AND / OR / comparison
			// of a WHEN condition is not a top-level operator (#699).
			if caseKeywordAt(s, i, "CASE") {
				depth++
				i += len("CASE") - 1
				continue
			}
		case 'E', 'e':
			if depth > 0 && caseKeywordAt(s, i, "END") {
				depth--
				i += len("END") - 1
				continue
			}
		}
		if depth == 0 && strings.EqualFold(s[i:i+len(keyword)], keyword) {
			return i
		}
	}
	return -1
}

// compareValues applies Cypher equality without coercing values across type
// families. Integer and floating-point values share Cypher's numeric family,
// while strings, booleans, and composite values retain their types.
func compareValues(a, b interface{}) bool {
	if a == nil || b == nil {
		return false
	}

	if equal, numeric := cypherNumericEquality(a, b); numeric {
		return equal
	}

	return reflect.DeepEqual(a, b)
}

func strictNumericValue(value interface{}) (float64, bool) {
	switch number := value.(type) {
	case int:
		return float64(number), true
	case int8:
		return float64(number), true
	case int16:
		return float64(number), true
	case int32:
		return float64(number), true
	case int64:
		return float64(number), true
	case uint:
		return float64(number), true
	case uint8:
		return float64(number), true
	case uint16:
		return float64(number), true
	case uint32:
		return float64(number), true
	case uint64:
		return float64(number), true
	case float32:
		return float64(number), true
	case float64:
		return number, true
	default:
		return 0, false
	}
}

// indexCaseInsensitive finds the index of a keyword in a case-insensitive manner.
func indexCaseInsensitive(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	for i := 0; i+len(keyword) <= len(s); i++ {
		if strings.EqualFold(s[i:i+len(keyword)], keyword) {
			return i
		}
	}
	return -1
}
