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
	"fmt"
	"reflect"
	"strings"

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
	return hasPrefixFoldASCII(trimmed, "CASE") && hasSuffixFoldASCII(trimmed, "END")
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
	content := strings.TrimSpace(expr[4 : len(expr)-3])

	ce := &caseExpression{
		whenClauses: []caseWhenClause{},
	}

	// Only keywords at the current CASE level delimit the outer expression.
	// A plain string search incorrectly treats WHEN/ELSE from a nested CASE as
	// belonging to its parent.
	firstWhenIdx := findCaseKeywordAtLevel(content, 0, "WHEN")
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
		thenIndex := findCaseKeywordAtLevel(content, conditionStart, "THEN")
		if thenIndex < 0 {
			return nil, localizedError(localization.CypherCoreCaseThenRequired(content[cursor:]), nil)
		}
		condition := strings.TrimSpace(content[conditionStart:thenIndex])
		resultStart := thenIndex + len("THEN")
		nextWhen := findCaseKeywordAtLevel(content, resultStart, "WHEN")
		nextElse := findCaseKeywordAtLevel(content, resultStart, "ELSE")
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
		if caseOperandExpectedBefore(s, i) {
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
		} else if caseOperandExpectedBefore(s, i) {
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
		} else if caseOperandExpectedBefore(s, i) {
			return false
		}
	case "CASE":
		// A CASE that opens an expression is followed by an operand or WHEN;
		// followed by what only follows an operand, it is a variable
		// (THEN case END, WHEN case.id = 1).
		if caseVariableNamedCase(s, i+len(keyword)) {
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
	j := i - 1
	for j >= 0 && isASCIISpace(s[j]) {
		j--
	}
	if j < 0 {
		return true
	}
	if !isIdentByte(s[j]) {
		return strings.IndexByte("([{,=<>+-*/%^|", s[j]) >= 0
	}
	start := j
	for start > 0 && isIdentByte(s[start-1]) {
		start--
	}
	if start > 0 && (s[start-1] == '.' || s[start-1] == '$' || s[start-1] == ':') {
		return false // a property, parameter or label completes an operand
	}
	switch word := lowerASCII(s[start : j+1]); word {
	case "case", "when", "then", "else":
		// After the keyword an operand is expected; after a variable of that
		// name, it is complete.
		return caseKeywordAt(s, start, upperASCII(word))
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
		return !caseOperandExpectedBefore(s, start)
	case "and", "or", "xor", "in", "contains", "as":
		// Infix keywords follow a complete operand. Where one is expected,
		// or at the start of a CASE body read on its own (and WHEN 1 …), the
		// word is a variable that completes one.
		return caseHasPreviousToken(s, start) && !caseOperandExpectedBefore(s, start)
	case "not", "distinct":
		// Prefix keywords stand where an operand is expected, as a variable
		// does: the word is the keyword unless what follows ends an operand.
		return !casePrefixWordIsVariable(s, j+1)
	}
	return false
}

// casePreviousWordIn reports whether the word before s[i] is one of words.
func casePreviousWordIn(s string, i int, words ...string) bool {
	j := i - 1
	for j >= 0 && isASCIISpace(s[j]) {
		j--
	}
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
// s[i], whether what follows makes it a variable: the end of the text, a
// property access or an infix or closing symbol, a CASE keyword (a THEN,
// ELSE, END or WHEN that is itself the keyword), or an infix keyword.
func casePrefixWordIsVariable(s string, i int) bool {
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) {
		return true
	}
	if !isIdentStartByte(s[i]) {
		return strings.IndexByte(".=<>*/%^,)]}", s[i]) >= 0
	}
	word := caseWordAt(s, i)
	after := i + len(word)
	switch word {
	case "then", "else", "end", "when":
		// NOT end.x: the word is a variable, so NOT is the keyword.
		return !caseOperandEndsAt(s, after)
	case "and", "or", "xor", "as", "is", "in", "contains":
		// NOT in.x, NOT in > 1: likewise; otherwise the infix keyword
		// follows a variable.
		return !caseOperandEndsAt(s, after)
	}
	return false
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
func casePreviousWordIsCase(s string, i int) bool {
	j := i - 1
	for j >= 0 && isASCIISpace(s[j]) {
		j--
	}
	return j >= 3 && matchKeywordAt(s, j-3, "CASE")
}

// caseOperandEndsAt reports whether what starts at s[i] can only follow a
// complete operand: the end of the text, a property access, an infix or
// closing symbol, or THEN, ELSE, END and the infix keywords.
func caseOperandEndsAt(s string, i int) bool {
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) {
		return true
	}
	if !isIdentStartByte(s[i]) {
		return strings.IndexByte(".=<>*/%^,)]}", s[i]) >= 0
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
// test, so the CASE before it opens an expression.
func caseVariableNamedCase(s string, i int) bool {
	if !caseOperandEndsAt(s, i) {
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
	for i < len(s) && isASCIISpace(s[i]) {
		i++
	}
	if i >= len(s) {
		return false
	}
	if !isIdentStartByte(s[i]) {
		return strings.IndexByte(".=<>*/%^", s[i]) >= 0
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
// of ( [ {) opens a scope that binds names: [x IN …] or [(a)-->(b) | …], the
// arguments of reduce / all / any / none / single / filter, or the { … } of an
// EXISTS, COUNT, COLLECT or CALL subquery.
func caseVariableScopeOpensAt(expr string, open int) bool {
	switch expr[open] {
	case '[':
		next := skipSpaces(expr, open+1)
		if next < len(expr) && expr[next] == '(' {
			return true
		}
		_, end, ok := scanIdentifierToken(expr, next)
		return ok && matchKeywordAt(expr, skipSpaces(expr, end), "IN")
	default: // ( or {
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
		if expr[open] == '{' {
			return name == "exists" || name == "count" || name == "collect" || name == "call"
		}
		return name == "reduce" || name == "filter" || isQuantifierFunction(name)
	}
}

// evaluateCaseExpression evaluates a CASE expression and returns the result.
func (e *StorageExecutor) evaluateCaseExpression(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	ce, err := parseCaseExpression(expr)
	if err != nil {
		// Return nil if parsing fails
		return nil
	}

	if ce.isSimple {
		// Simple CASE: evaluate test expression once
		testValue := e.evaluateExpressionWithContextFull(ctx, ce.testExpression, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)

		// Check each WHEN clause
		for _, clause := range ce.whenClauses {
			whenValue := e.evaluateExpressionWithContextFull(ctx, clause.value, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			// CASE x WHEN y compares as x = y does (promoteConstantNumbers).
			if compareValues(promoteConstantNumbers("=", ce.testExpression, clause.value, testValue, whenValue, cypherVersionFromContext(ctx) == "25")) {
				return e.evaluateExpressionWithContextFull(ctx, clause.result, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			}
		}
	} else {
		// Searched CASE: evaluate each WHEN condition
		for _, clause := range ce.whenClauses {
			if e.evaluateCondition(ctx, clause.condition, nodes, rels) {
				return e.evaluateExpressionWithContextFull(ctx, clause.result, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			}
		}
	}

	// No WHEN matched, return ELSE result or NULL
	if ce.elseResult != "" {
		return e.evaluateExpressionWithContextFull(ctx, ce.elseResult, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}
	return nil
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

	if value, logical, _, err := evaluateLogicalExpression(condition, func(operand string) (interface{}, bool, error) {
		return e.conditionValue(ctx, operand, nodes, rels), true, nil
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
	if result, ok := evaluateComparisonChain(condition, resolveComparisonOperand, compareCypherPredicateValue, cypherVersionFromContext(ctx) == "25"); ok {
		return result
	}

	// Handle IS NULL / IS NOT NULL
	if strings.HasSuffix(upper, " IS NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-8])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return val == nil
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := strings.TrimSpace(condition[:len(condition)-12])
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return val != nil
	}

	// Handle CONTAINS, STARTS WITH, ENDS WITH string predicates
	containsIdx := findTopLevelKeyword(condition, " CONTAINS ")
	if containsIdx > 0 {
		left := strings.TrimSpace(condition[:containsIdx])
		right := strings.TrimSpace(condition[containsIdx+10:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.Contains(leftStr, rightStr)
		}
		return false
	}
	if idx := findTopLevelKeyword(condition, " STARTS WITH "); idx > 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+13:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.HasPrefix(leftStr, rightStr)
		}
		return false
	}
	if idx := findTopLevelKeyword(condition, " ENDS WITH "); idx > 0 {
		left := strings.TrimSpace(condition[:idx])
		right := strings.TrimSpace(condition[idx+11:])
		leftVal := e.evaluateExpressionWithContext(ctx, left, nodes, rels)
		rightVal := e.evaluateExpressionWithContext(ctx, right, nodes, rels)
		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.HasSuffix(leftStr, rightStr)
		}
		return false
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

// compareWithOperator compares two values using the given operator.
func compareWithOperator(left, right interface{}, op string) bool {
	// Handle NULL comparisons
	if left == nil || right == nil {
		switch op {
		case "=":
			return left == nil && right == nil
		case "<>":
			return !(left == nil && right == nil)
		default:
			return false // NULL comparisons with <, >, etc. are false
		}
	}
	if equal, temporal := compareTemporalValues(left, right); temporal {
		switch op {
		case "=":
			return equal
		case "<>":
			return !equal
		}
	}
	if equal, point := comparePointValues(left, right); point {
		switch op {
		case "=":
			return equal
		case "<>":
			return !equal
		default:
			return false // points are not ordered by <, >, <=, >=
		}
	}
	if comparison, temporal := compareTemporalOrdering(left, right); temporal {
		switch op {
		case "<":
			return comparison < 0
		case ">":
			return comparison > 0
		case "<=":
			return comparison <= 0
		case ">=":
			return comparison >= 0
		}
	}

	// Try numeric comparison: an integer exactly, two floats as float64.
	if comparison, exact := compareCypherNumbersExactly(left, right); exact {
		switch op {
		case "<":
			return comparison < 0
		case ">":
			return comparison > 0
		case "<=":
			return comparison <= 0
		case ">=":
			return comparison >= 0
		case "=":
			return comparison == 0
		case "<>":
			return comparison != 0
		}
	}
	numLeft, okLeft := toFloat64(left)
	numRight, okRight := toFloat64(right)
	if okLeft && okRight {
		switch op {
		case "<":
			return numLeft < numRight
		case ">":
			return numLeft > numRight
		case "<=":
			return numLeft <= numRight
		case ">=":
			return numLeft >= numRight
		case "=":
			return numLeft == numRight
		case "<>":
			return numLeft != numRight
		}
	}

	// String comparison
	strLeft := fmt.Sprintf("%v", left)
	strRight := fmt.Sprintf("%v", right)
	switch op {
	case "<":
		return strLeft < strRight
	case ">":
		return strLeft > strRight
	case "<=":
		return strLeft <= strRight
	case ">=":
		return strLeft >= strRight
	case "=":
		return strLeft == strRight
	case "<>":
		return strLeft != strRight
	}

	return false
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
