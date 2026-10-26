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
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// caseWhenClause represents a single WHEN ... THEN clause in a CASE expression.
type caseWhenClause struct {
	condition                 string // WHEN condition (for searched CASE)
	value                     string // WHEN value (for simple CASE)
	result                    string // THEN result
	conditionSpan, resultSpan cyphertext.Span
}

// caseExpression represents a parsed CASE expression.
type caseExpression struct {
	isSimple           bool             // true for simple CASE, false for searched CASE
	testExpression     string           // expression to test (simple CASE only)
	whenClauses        []caseWhenClause // list of WHEN clauses
	elseResult         string           // ELSE result (optional)
	testSpan, elseSpan cyphertext.Span
}

// isCaseExpression checks if an expression is a CASE expression.
func isCaseExpression(view braceView) bool {
	expr := view.text()

	trimmed := strings.TrimSpace(expr)
	return hasPrefixFoldASCII(trimmed, "CASE") && hasSuffixFoldASCII(trimmed, "END") && leadingCaseExpressionEnd(view.trim()) == len(trimmed)
}

// leadingCaseExpressionEnd returns the exclusive end of a leading CASE ... END
// expression, including nested CASE expressions. A trailing operator or
// postfix expression is deliberately excluded so callers can treat CASE as an
// atomic operand without allowing operators inside WHEN predicates to escape.
func leadingCaseExpressionEnd(view braceView) int {
	expr := view.text()

	view = view.trim()
	expr = view.text()
	// The first identifier token must be CASE itself; a CASE keyword nested
	// inside a compound expression (acc + CASE … END) is not a leading CASE.
	firstWord, firstOK := cyphertext.ScanIdentifier(expr, 0)
	if !firstOK || cyphertext.KeywordEnd(expr, 0, "CASE", cyphertext.CypherWord) != firstWord.End {
		return -1
	}
	depth := 1
	for index := firstWord.End; index < len(expr); {
		if isCypherQuoteByte(expr[index]) {
			index = skipCypherQuotedText(expr, index, expr[index])
			continue
		}
		word, ok := cyphertext.ScanIdentifier(expr, index)
		if !ok {
			index++
			continue
		}
		switch {
		case caseKeywordAt(view, index, "CASE"):
			depth++
		case depth > 0 && caseKeywordAt(view, index, "END"):
			depth--
			if depth == 0 {
				return word.End
			}
		}
		index = word.End
	}
	return -1
}

// parseCaseExpression parses a CASE expression into its components.
// Supports both searched and simple CASE expressions.
func parseCaseExpression(view braceView) (*caseExpression, error) {
	expr := view.text()

	view = view.trim()
	expr = view.text()
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
	contentStart := 4 + len(inner) - len(strings.TrimLeftFunc(inner, cyphertext.IsWhitespace))
	findAtLevel := func(from int, keyword string) int {
		index := findCaseKeywordAtLevel(view, contentStart+from, keyword)
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
		ce.testSpan = cyphertext.Span{Start: contentStart, End: contentStart + firstWhenIdx}
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
		clause := caseWhenClause{result: result,
			conditionSpan: cyphertext.Span{Start: contentStart + conditionStart, End: contentStart + thenIndex},
			resultSpan:    cyphertext.Span{Start: contentStart + resultStart, End: contentStart + resultEnd}}
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
			ce.elseSpan = cyphertext.Span{Start: contentStart + nextElse + len("ELSE"), End: contentStart + len(content)}
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
func findCaseKeywordAtLevel(view braceView, start int, keyword string) int {
	expression := view.text()

	parenDepth, bracketDepth, braceDepth, caseDepth := 0, 0, cyphertext.BraceDepth(0), 0
	for index := start; index < len(expression); {
		if isCypherQuoteByte(expression[index]) {
			index = skipCypherQuotedText(expression, index, expression[index])
			continue
		}
		switch {
		case expression[index] == '(':
			parenDepth++
			index++
			continue
		case expression[index] == ')':
			parenDepth--
			index++
			continue
		case expression[index] == '[':
			bracketDepth++
			index++
			continue
		case expression[index] == ']':
			bracketDepth--
			index++
			continue
		case cyphertext.IsOpenBrace(expression[index]):
			braceDepth.Open()
			index++
			continue
		case cyphertext.IsCloseBrace(expression[index]):
			braceDepth.Close(false)
			index++
			continue
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || !isCaseWordStart(expression, index) {
			index++
			continue
		}
		end := index + 1
		end = cyphertext.NamePartEnd(expression, end)

		if caseKeywordAt(view, index, "CASE") {
			caseDepth++
		} else if caseDepth > 0 && caseKeywordAt(view, index, "END") {
			caseDepth--
		} else if caseDepth == 0 && caseKeywordAt(view, index, keyword) {
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
func caseKeywordAt(view braceView, i int, keyword string) bool {
	return caseKeywordAtMode(view, i, keyword, true)
}

// caseKeywordAtMode can use the non-balancing prefix reader when the closure
// budget itself scans keywords, avoiding recursive balance scans.
func caseKeywordAtMode(view braceView, i int, keyword string, balancePrefix bool) bool {
	s := view.text()

	if !(cyphertext.KeywordEnd(s, i, keyword, cyphertext.CypherWord) == i+len(keyword)) {
		return false
	}
	if i > 0 {
		switch s[i-1] {
		case '.', '$':
			return false
		case ':':
			// A label or type (n:End); a map value written without a space
			// ({a:CASE … END}) is an expression like any other.
			if !colonIsMapKeySeparator(view, i-1) {
				return false
			}
		}
	}
	switch keyword {
	case "END":
		// END follows a complete operand; where one is expected the word is
		// a variable (ELSE end.id END).
		if caseOperandExpectedBeforeMode(view, i, balancePrefix) {
			return false
		}
	case "THEN", "ELSE":
		// As END. Scanners also read a CASE's body (WHEN … THEN … ELSE …)
		// as text of its own: at its start the word is a keyword unless it
		// is a simple CASE's test (then WHEN 1 …).
		if !caseHasPreviousToken(s, i) {
			if caseWordRoleAt(view, i+len(keyword), caseTestContinuation, false) {
				return false
			}
		} else if caseOperandExpectedBeforeMode(view, i, balancePrefix) {
			return false
		}
	case "WHEN":
		// WHEN follows a complete operand, or opens a searched CASE right
		// after CASE or at the start of a CASE body: there the word is a
		// variable only when what follows continues an operand (CASE when
		// WHEN 1, CASE when.id WHEN 1).
		if !caseHasPreviousToken(s, i) || casePreviousWordIn(s, i, "case") {
			if caseWordRoleAt(view, i+len(keyword), caseTestContinuation, false) {
				return false
			}
		} else if caseOperandExpectedBeforeMode(view, i, balancePrefix) {
			return false
		}
	case "CASE":
		// A CASE that opens an expression is followed by an operand or WHEN;
		// followed by what only follows an operand, it is a variable
		// (THEN case END, WHEN case.id = 1). After AS it is the name AS
		// gives (WITH 1 AS case RETURN case), and right after CASE it is a
		// simple CASE's test when a nested CASE would leave the outer one
		// without its END (CASE case WHEN 1 THEN case END; #1162).
		if caseWordRoleAt(view, i+len(keyword), caseNamedVariable, false) || casePreviousWordIn(s, i, "as") ||
			(casePreviousWordIn(s, i, "case") && caseNestedLacksEnd(view, i, balancePrefix)) {
			return false
		}
	}
	next := i + len(keyword)
	for next < len(s) && cyphertext.IsWhitespaceAt(s, next) {
		next++
	}
	return next >= len(s) || s[next] != ':' || (next+1 < len(s) && s[next+1] == ':')
}

// caseOperandExpectedBefore reports whether an operand, not an operator or
// END, is expected at s[i]: at the start, after an opening bracket, a comma or
// an operator, or after a keyword that takes an operand (WHEN, THEN, ELSE, AND,
// IN, AS, …). END always follows a complete operand, so a word end there is a
// variable, as Neo4j reads ELSE end.id END and ELSE end END.id.
func caseOperandExpectedBefore(view braceView, i int) bool {
	return caseOperandExpectedBeforeMode(view, i, true)
}

func caseOperandExpectedBeforeMode(view braceView, i int, balancePrefix bool) bool {
	s := view.text()

	j := i - 1
	for j >= 0 && cyphertext.IsWhitespaceAt(s, j) {
		j--
	}
	if j < 0 {
		return true
	}
	if !cyphertext.NamePartAt(s, j) {
		if s[j] == '>' && caseTypeParameterCloses(s, j) {
			return false // LIST<INTEGER> completes a type, as in x IS :: LIST<INTEGER>
		}
		if s[j] == '%' {
			// After a colon or a label expression symbol, % is the wildcard
			// label (n:%), which completes an operand, not the modulo operator.
			k := j - 1
			for k >= 0 && cyphertext.IsWhitespaceAt(s, k) {
				k--
			}
			if k >= 0 && strings.IndexByte(":|&!", s[k]) >= 0 {
				return false
			}
		}
		if s[j] == ':' && (j == 0 || s[j-1] != ':') {
			return true // a map value follows its key's colon ({end: end})
		}
		return syntaxByteInSet("([{,=<>+-*/%^|", s[j])
	}
	start := j
	for start > 0 && cyphertext.NamePartAt(s, start-1) {
		start--
	}
	if start > 0 && (s[start-1] == '.' || s[start-1] == '$' || s[start-1] == ':') {
		return false // a property, parameter or label completes an operand
	}
	word := s[start : j+1]
	switch {
	// A constant keyword preserves the contextual CASE decision without copying source.
	case equalFoldASCII(word, "case"):
		return caseKeywordAtMode(view, start, "CASE", balancePrefix)
	case equalFoldASCII(word, "when"):
		return caseKeywordAtMode(view, start, "WHEN", balancePrefix)
	case equalFoldASCII(word, "then"):
		return caseKeywordAtMode(view, start, "THEN", balancePrefix)
	case equalFoldASCII(word, "else"):
		return caseKeywordAtMode(view, start, "ELSE", balancePrefix)
	case equalFoldASCII(word, "with"), equalFoldASCII(word, "where"), equalFoldASCII(word, "return"):
		if equalFoldASCII(word, "with") && casePreviousWordIn(s, start, "starts", "ends") {
			return true // STARTS WITH, ENDS WITH
		}
		// Clause keywords start the text or follow a complete clause; where
		// an operand is expected, or as a CASE body's test (where WHEN 1 …),
		// the word is a variable that completes one.
		if !caseHasPreviousToken(s, start) {
			return !caseWordRoleAt(view, j+1, caseTestContinuation, false)
		}
		return !caseOperandExpectedBeforeMode(view, start, balancePrefix)
	case equalFoldASCII(word, "and"), equalFoldASCII(word, "or"), equalFoldASCII(word, "xor"), equalFoldASCII(word, "in"), equalFoldASCII(word, "contains"), equalFoldASCII(word, "as"):
		// Infix keywords follow a complete operand. Where one is expected,
		// or at the start of a CASE body read on its own (and WHEN 1 …), the
		// word is a variable that completes one.
		return caseHasPreviousToken(s, start) && !caseOperandExpectedBeforeMode(view, start, balancePrefix)
	case equalFoldASCII(word, "not"), equalFoldASCII(word, "distinct"):
		// Prefix keywords stand where an operand is expected, as a variable
		// does: the word is the keyword unless what follows ends an operand.
		return !caseWordRoleAt(view, j+1, casePrefixVariable, balancePrefix)
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
		switch {
		case s[k] == '>':
			depth++
		case s[k] == '<':
			depth--
			if depth == 0 {
				end := k
				for end > 0 && cyphertext.IsWhitespaceAt(s, end-1) {
					end--
				}
				start := end
				for start > 0 && cyphertext.NamePartAt(s, start-1) {
					start--
				}
				switch word := s[start:end]; {
				case equalFoldASCII(word, "list"), equalFoldASCII(word, "array"), equalFoldASCII(word, "any"):
					return true
				}
				return false
			}
		case s[k] == '(', s[k] == ')', s[k] == '[', s[k] == ']', cyphertext.IsOpenBrace(s[k]), cyphertext.IsCloseBrace(s[k]):
			if depth == 1 {
				return false // not inside one type's brackets
			}
		default:
			if isCypherStringQuoteByte(s[k]) && depth == 1 {
				return false
			}
		}
	}
	return false
}

// casePreviousWordIn reports whether the word before s[i] is one of words.
func casePreviousWordIn(s string, i int, words ...string) bool {
	j, _ := cyphertext.NeighborIndex(s, i, cyphertext.Before)
	end := j + 1
	for j >= 0 && cyphertext.NamePartAt(s, j) {
		j--
	}
	previous := s[j+1 : end]
	for _, word := range words {
		if equalFoldASCII(previous, word) {
			return true
		}
	}
	return false
}

// caseHasPreviousToken reports whether anything but space precedes s[i].
func caseHasPreviousToken(s string, i int) bool {
	_, found := cyphertext.NeighborIndex(s, i, cyphertext.Before)
	return found
}

// caseNestedLacksEnd reports, for a CASE right after CASE at s[i], whether
// reading it as a nested CASE leaves the outer CASE without an END before
// the enclosing bracket or the text ends: CASE case WHEN 1 THEN case END has
// one END, a nested CASE CASE WHEN … END WHEN … END two.
func caseNestedLacksEnd(view braceView, i int, balancePrefix bool) bool {
	s := view.text()

	open, depth := 2, 0
	for j := i + len("CASE"); j < len(s); j++ {
		switch c := s[j]; {
		case (cypherLexicalByte(c)) == cypherQuoteClass:
			j = skipCypherQuotedText(s, j, c) - 1
			continue
		case (cypherLexicalByte(c)) == '(', (cypherLexicalByte(c)) == '[', cyphertext.IsOpenBrace(cypherLexicalByte(c)):
			depth = cyphertext.AdvanceDelimiterDepth(depth, cypherLexicalByte(c), false)
			continue
		case (cypherLexicalByte(c)) == ')', (cypherLexicalByte(c)) == ']', cyphertext.IsCloseBrace(cypherLexicalByte(c)):
			if depth == 0 {
				return open > 0
			}
			depth = cyphertext.AdvanceDelimiterDepth(depth, cypherLexicalByte(c), false)
			continue
		}
		if depth != 0 || !cyphertext.NameStartAt(s, j) || cyphertext.NamePartAt(s, j-1) {
			continue
		}
		if caseKeywordAtMode(view, j, "CASE", balancePrefix) {
			open++
		} else if caseKeywordAtMode(view, j, "END", balancePrefix) {
			if open--; open == 0 {
				return false
			}
		}
	}
	return open > 0
}

// caseOperandEndsAt reports whether what starts at s[i] can only follow a
// complete operand: the end of the text, a property access, an infix or
// closing symbol, or THEN, ELSE, END and the infix keywords.
func caseOperandEndsAt(s string, i int) bool {
	start := i
	for i < len(s) && cyphertext.IsWhitespaceAt(s, i) {
		i++
	}
	if i >= len(s) {
		return true
	}
	if !cyphertext.NameStartAt(s, i) {
		return strings.IndexByte(".=<>*/%^,)]}", s[i]) >= 0 || caseBinaryOperatorAt(s, i) || (s[i] == '[' && i == start)
	}
	switch word := caseWordAt(s, i); {
	case equalFoldASCII(word, "then"), equalFoldASCII(word, "else"), equalFoldASCII(word, "end"), equalFoldASCII(word, "as"), equalFoldASCII(word, "and"), equalFoldASCII(word, "or"), equalFoldASCII(word, "xor"), equalFoldASCII(word, "is"), equalFoldASCII(word, "in"), equalFoldASCII(word, "contains"), equalFoldASCII(word, "starts"), equalFoldASCII(word, "ends"):
		return true
	}
	return false
}

// caseBinaryOperatorAt reports a + or - at s[i] followed by space: the
// binary operator, which continues an operand (case + 1), not a sign that
// starts one (CASE -1 WHEN …). A [ right against the word before it is a
// subscript (when[0]); after a space it may open a list (CASE WHEN [1] …).
func caseBinaryOperatorAt(s string, i int) bool {
	return (s[i] == '+' || s[i] == '-') && i+1 < len(s) && cyphertext.IsWhitespaceAt(s, i+1)
}

// caseWordRole selects the source context in which a CASE word is read.
type caseWordRole uint8

const (
	caseNamedVariable    caseWordRole = iota // CASE used as a variable.
	casePrefixVariable                       // NOT or DISTINCT used as a variable.
	caseTestContinuation                     // Syntax continuing a simple CASE test operand.
)

// caseWordRoleAt owns the shared following-word boundary and contextual role
// decision. balancePrefix applies only to a NOT/DISTINCT variable's closing END.
func caseWordRoleAt(view braceView, i int, role caseWordRole, balancePrefix bool) bool {
	s := view.text()
	start := i
	for i < len(s) && cyphertext.IsWhitespaceAt(s, i) {
		i++
	}
	word := caseWordAt(s, i)
	switch role {
	case caseNamedVariable:
		if !caseOperandEndsAt(s, start) && !isNameableClauseKeyword(word) {
			return false
		}
		if i < len(s) && cyphertext.NameStartAt(s, i) {
			return !caseWordRoleAt(view, i+len(word), caseTestContinuation, false)
		}
		return true
	case casePrefixVariable:
		if i >= len(s) || !cyphertext.NameStartAt(s, i) {
			return i >= len(s)
		}
		if equalFoldASCII(word, "end") && (strings.TrimSpace(s[i+len(word):]) == "" || balancePrefix && casePrefixEndRequired(view, i)) {
			return true
		}
		if matchKeywordAt(s, queryGapEnd(s, i+len(word)), "WHEN") {
			return false
		}
		return !caseOperandEndsAt(s, i+len(word))
	case caseTestContinuation:
		if i >= len(s) {
			return false
		}
		if !cyphertext.NameStartAt(s, i) {
			return strings.IndexByte(".=<>*/%^", s[i]) >= 0 || caseBinaryOperatorAt(s, i) || s[i] == '[' && i == start
		}
		switch {
		case equalFoldASCII(word, "and"), equalFoldASCII(word, "or"), equalFoldASCII(word, "xor"), equalFoldASCII(word, "is"), equalFoldASCII(word, "in"), equalFoldASCII(word, "contains"), equalFoldASCII(word, "starts"), equalFoldASCII(word, "ends"), equalFoldASCII(word, "when"):
			return !caseOperandEndsAt(s, i+len(word))
		}
	}
	return false
}

// caseWordAt returns the written source view; consumers compare ASCII keywords
// without normalizing or allocating an identity copy.
func caseWordAt(s string, i int) string {
	end := i
	end = cyphertext.NamePartEnd(s, end)
	return s[i:end]
}

func isCaseWordStart(expression string, index int) bool {
	return (index == 0 || !cyphertext.NamePartAt(expression, index-1)) &&
		cyphertext.NameStartAt(expression, index)
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
func caseBlockSpans(view braceView) []caseBlockSpan {
	expr := view.text()

	var spans []caseBlockSpan
	stack := make([]int, 0, 2)
	var delimiters []bool // open ( [ {: true when it opens a scope
	scopes := 0
	for i := 0; i < len(expr); i++ {
		if end, quoted := skipCypherLexicalToken(expr, i); quoted {
			i = end - 1
			continue
		}
		ch := expr[i]
		switch {
		case ch == '(', ch == '[', cyphertext.IsOpenBrace(ch):
			scope := caseVariableScopeOpensAt(view, i)
			delimiters = append(delimiters, scope)
			if scope {
				scopes++
			}
			continue
		case ch == ')', ch == ']', cyphertext.IsCloseBrace(ch):
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
			if (ch == 'C' || ch == 'c') && caseKeywordAt(view, i, "CASE") {
				stack = append(stack, i)
				continue
			}
			if (ch == 'E' || ch == 'e') && caseKeywordAt(view, i, "END") && len(stack) > 0 {
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
func caseVariableScopeOpensAt(view braceView, open int) bool {
	expr := view.text()

	if !cyphertext.IsOpenBrace(expr[open]) {
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
		return view.isBody(open)
	}
}

// evaluateCaseExpression evaluates a CASE expression and returns the result.
func (e *StorageExecutor) evaluateCaseExpression(ctx context.Context, view braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	parsed, err := parseCaseExpression(view)
	if err != nil {
		return nil
	}
	value, _, err := runCaseExpression(parsed, cypherVersionFromContext(ctx) == "25", func(part caseValuePart, index int, text string) (interface{}, bool, error) {
		partView := view.spanWindow(parsed.partSpan(part, index))
		if part == caseWhen && !parsed.isSimple {
			return e.conditionValue(ctx, partView, nodes, rels), true, nil
		}
		return e.evaluateExpressionWithContextFull(ctx, partView, nodes, rels, paths, allPathEdges, allPathNodes, pathLength), true, nil
	})
	if err != nil {
		recordExpressionFailure(ctx, err)
	}
	return value
}

// evaluateCondition reports whether a CASE WHEN (or WITH … WHERE) condition
// holds: its value read as a predicate (cypherPredicateTruth), so a
// non-predicate value is a TypeError, recorded on ctx (#907).
func (e *StorageExecutor) evaluateCondition(ctx context.Context, view braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge) bool {
	truth, err := cypherPredicateTruth(e.conditionValue(ctx, view, nodes, rels))
	if err != nil {
		recordExpressionFailure(ctx, err)
		return false
	}
	return truth == truthTrue
}

// conditionValue is the value of a condition (evaluateCondition): OR, XOR,
// AND and NOT as everywhere (evaluateLogicalExpression), then the
// predicates conditions read directly, then any other expression.
func (e *StorageExecutor) conditionValue(ctx context.Context, view braceView, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	view = view.trim()
	condition := view.text()
	ctx = withBraceView(ctx, view)
	upper := upperASCII(condition)

	if value, logical, _, err := evaluateLogicalExpression(view, func(operand braceView) (interface{}, bool, error) {
		return deferredExpressionValue(ctx, func(ctx context.Context) interface{} {
			return e.conditionValue(ctx, operand, nodes, rels)
		})
	}); logical {
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		return value
	}

	resolveComparisonOperand := func(operand braceView) interface{} {
		return e.evaluateExpressionWithContext(ctx, operand, nodes, rels)
	}
	// IN and NOT IN are unknown for a null in the list (x IN [1, null]).
	if indexASCIIFold(condition, " in ") >= 0 {
		if truth, ok := inPredicateTruth(view, resolveComparisonOperand); ok {
			return truth.value()
		}
	}
	if result, matched, _, err := evaluateComparisonChain(view, func(operand braceView) (interface{}, bool, error) {
		return deferredExpressionValue(ctx, func(ctx context.Context) interface{} {
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
		expr := view.window(0, len(condition)-8)
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return nullEvaluationHandler(false).evaluate(val)
	}
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := view.window(0, len(condition)-12)
		val := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
		return nullEvaluationHandler(true).evaluate(val)
	}

	for _, p := range []struct {
		operator string
		match    func(string, string) bool
	}{
		{" STARTS WITH ", strings.HasPrefix}, {" ENDS WITH ", strings.HasSuffix}, {" CONTAINS ", strings.Contains},
	} {
		if left, right, ok := splitByOperatorOutsideCase(view, p.operator, true, true); ok {
			return rowStringPredicateValue(e.evaluateExpressionWithContext(ctx, view.window(0, len(left)), nodes, rels), e.evaluateExpressionWithContext(ctx, view.window(len(condition)-len(right), len(condition)), nodes, rels), p.match)
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

	return e.evaluateExpressionWithContext(ctx, view, nodes, rels)
}

// findTopLevelKeyword finds a keyword at the top level (not inside parentheses or strings)
// Returns the byte index in the original string, or -1 if not found.
// Properly handles UTF-8 encoded strings with multi-byte characters.
func findTopLevelKeyword(view braceView, keyword string) int {
	s := view.text()

	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	depth := 0
	for i := 0; i+len(keyword) <= len(s); i++ {
		if end, quoted := skipCypherLexicalToken(s, i); quoted {
			i = end - 1
			continue
		}
		ch := s[i]
		switch {
		case ch == '(', ch == '[', cyphertext.IsOpenBrace(ch):
			depth = cyphertext.AdvanceDelimiterDepth(depth, ch, false)
			continue
		case ch == ')', ch == ']', cyphertext.IsCloseBrace(ch):
			if depth > 0 {
				depth = cyphertext.AdvanceDelimiterDepth(depth, ch, false)
			}
			continue
		case ch == 'C', ch == 'c':
			// CASE … END nests like parentheses: the AND / OR / comparison
			// of a WHEN condition is not a top-level operator (#699).
			if caseKeywordAt(view, i, "CASE") {
				depth++
				i += len("CASE") - 1
				continue
			}
		case ch == 'E', ch == 'e':
			if depth > 0 && caseKeywordAt(view, i, "END") {
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
	return valueconvert.NumericFloat64(value)
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
