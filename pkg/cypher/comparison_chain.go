package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

type comparisonOperatorSpan struct {
	offset int
	length int
}

type comparisonChainScan struct {
	inline   [4]comparisonOperatorSpan
	overflow []comparisonOperatorSpan
	count    int
}

func (scan *comparisonChainScan) append(span comparisonOperatorSpan) {
	if scan.count < len(scan.inline) {
		scan.inline[scan.count] = span
	} else {
		scan.overflow = append(scan.overflow, span)
	}
	scan.count++
}

func (scan *comparisonChainScan) operand(expression string, index int) string {
	start := 0
	if index > 0 {
		previous := inlineOverflowAt(scan.inline[:], scan.overflow, index-1)
		start = previous.offset + previous.length
	}
	end := len(expression)
	if index < scan.count {
		end = inlineOverflowAt(scan.inline[:], scan.overflow, index).offset
	}
	return strings.TrimSpace(expression[start:end])
}

// evaluateComparisonChain evaluates every adjacent pair in a Cypher
// comparison. Each operand is resolved once and then reused as the left side
// of the next pair, so the implementation supports arbitrary chain length
// without duplicating expression work. cypher25 is the statement's version,
// which decides how constant numbers compare (promoteConstantNumbers).
func evaluateComparisonChain(
	expression string,
	resolve func(string) (interface{}, bool, error),
	compare func(interface{}, interface{}, string) interface{},
	cypher25 bool,
	domains ...graphIdentityDomain,
) (interface{}, bool, bool, error) {
	scan, ok := scanComparisonChain(expression)
	if !ok {
		return nil, false, false, nil
	}
	value, resolved, err := evaluateScannedComparisonChain(expression, &scan, comparisonChainHasIdentity(expression, &scan), func(_ int, operand string) (interface{}, bool, error) {
		return resolve(operand)
	}, compare, cypher25, domains...)
	return value, true, resolved, err
}

// comparisonChainHasIdentity reports a chain with an id() / elementId()
// operand: all its operands compare by identity payload.
func comparisonChainHasIdentity(expression string, scan *comparisonChainScan) bool {
	for index := 0; index <= scan.count; index++ {
		if isRowIdentityExpression(scan.operand(expression, index)) {
			return true
		}
	}
	return false
}

// evaluateScannedComparisonChain is evaluateComparisonChain for an
// expression already scanned: an evaluator that keeps the scan
// (rowValueProgram) evaluates the chain without reading the text again.
// resolve gets each operand's index and text; normalizeIdentity is
// comparisonChainHasIdentity.
func evaluateScannedComparisonChain(
	expression string,
	scan *comparisonChainScan,
	normalizeIdentity bool,
	resolve func(int, string) (interface{}, bool, error),
	compare func(interface{}, interface{}, string) interface{},
	cypher25 bool,
	domains ...graphIdentityDomain,
) (interface{}, bool, error) {
	left, leftOK, leftErr := resolve(0, scan.operand(expression, 0))
	hasNull, resolved := false, leftOK
	firstErr := leftErr
	for index := 0; index < scan.count; index++ {
		span := inlineOverflowAt(scan.inline[:], scan.overflow, index)
		operator := expression[span.offset : span.offset+span.length]
		right, rightOK, rightErr := resolve(index+1, scan.operand(expression, index+1))
		if firstErr == nil {
			firstErr = rightErr
		}
		resolved = resolved && rightOK
		// A later false discards an earlier error; a decided false never
		// evaluates the remaining operands. Each operand is evaluated once.
		if leftOK && rightOK && leftErr == nil && rightErr == nil {
			comparisonLeft, comparisonRight := left, right
			if normalizeIdentity {
				comparisonLeft, comparisonRight = normalizeRowIdentityValues(left, right, domains...)
			}
			if left == nil || right == nil {
				hasNull = true
			} else {
				if operator == "!=" {
					operator = "<>"
				}
				leftValue, rightValue := promoteConstantNumbers(operator, scan.operand(expression, index), scan.operand(expression, index+1), comparisonLeft, comparisonRight, cypher25)
				comparison := compare(leftValue, rightValue, operator)
				if comparison == nil {
					hasNull = true
				} else if matched, ok := comparison.(bool); !ok || !matched {
					return false, true, nil
				}
			}
		}
		left, leftOK, leftErr = right, rightOK, rightErr
	}
	if firstErr != nil || !resolved {
		return nil, false, firstErr
	}
	if hasNull {
		return nil, true, nil
	}
	return true, true, nil
}

// incomingArrowAt reports whether expression[index] ('<') starts an
// incoming relationship arrow: <-- or <-[. Any other < before a '-' is a
// comparison with a negative operand (n.v <-1 is n.v < -1), as in Neo4j.
func incomingArrowAt(expression string, index int) bool {
	return index+2 < len(expression) && expression[index] == '<' && expression[index+1] == '-' &&
		(expression[index+2] == '-' || expression[index+2] == '[')
}

func splitComparisonChain(expression string) ([]string, []string, bool) {
	scan, ok := scanComparisonChain(expression)
	if !ok {
		return nil, nil, false
	}
	operands := make([]string, scan.count+1)
	operators := make([]string, scan.count)
	for index := 0; index < scan.count; index++ {
		span := inlineOverflowAt(scan.inline[:], scan.overflow, index)
		operands[index] = scan.operand(expression, index)
		operators[index] = expression[span.offset : span.offset+span.length]
	}
	operands[scan.count] = scan.operand(expression, scan.count)
	return operands, operators, true
}

func scanComparisonChain(expression string) (comparisonChainScan, bool) {
	if scan, ok, needsComplexScan := scanPlainComparisonChain(expression); !needsComplexScan {
		return scan, ok
	}
	// The < and > of a type (x IS :: LIST<INTEGER>, #838) are not
	// comparisons: the plain scan hands text with ':' or LIST< / ARRAY< /
	// ANY< here, and this scan reads a copy of the same length with types
	// blanked; operands are still cut from the caller's text.
	expression = maskTypePredicateTypes(expression)

	var scan comparisonChainScan
	operandStart := 0
	parenDepth, bracketDepth, braceDepth, caseDepth := 0, 0, 0, 0

	for index := 0; index < len(expression); index++ {
		if end, quoted := skipCypherLexicalToken(expression, index); quoted {
			index = end - 1
			continue
		}
		current := expression[index]
		switch current {
		case '(':
			parenDepth++
			continue
		case ')':
			parenDepth--
			continue
		case '[':
			bracketDepth++
			continue
		case ']':
			bracketDepth--
			continue
		case '{':
			braceDepth++
			continue
		case '}':
			braceDepth--
			continue
		case 'C', 'c':
			// CASE … END nests like parentheses: the comparisons of its WHEN
			// conditions are not the chain's (#699).
			if caseKeywordAt(expression, index, "CASE") {
				caseDepth++
				index += len("CASE") - 1
				continue
			}
		case 'E', 'e':
			if caseDepth > 0 && caseKeywordAt(expression, index, "END") {
				caseDepth--
				index += len("END") - 1
				continue
			}
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 || caseDepth != 0 {
			continue
		}

		operator := ""
		switch current {
		case '<':
			if incomingArrowAt(expression, index) {
				continue
			}
			operator = "<"
			if index+1 < len(expression) && (expression[index+1] == '>' || expression[index+1] == '=') {
				operator = expression[index : index+2]
			}
		case '>':
			if index > 0 && expression[index-1] == '-' {
				continue
			}
			operator = ">"
			if index+1 < len(expression) && expression[index+1] == '=' {
				operator = ">="
			}
		case '!':
			if index+1 < len(expression) && expression[index+1] == '=' {
				operator = "!="
			}
		case '=':
			operator = "="
			if index+1 < len(expression) && expression[index+1] == '~' {
				operator = "=~"
			}
		}
		if operator == "" {
			continue
		}

		operand := strings.TrimSpace(expression[operandStart:index])
		if operand == "" {
			return comparisonChainScan{}, false
		}
		scan.append(comparisonOperatorSpan{offset: index, length: len(operator)})
		index += len(operator) - 1
		operandStart = index + 1
	}

	if scan.count == 0 {
		return comparisonChainScan{}, false
	}
	operand := strings.TrimSpace(expression[operandStart:])
	if operand == "" {
		return comparisonChainScan{}, false
	}
	return scan, true
}

func scanPlainComparisonChain(expression string) (comparisonChainScan, bool, bool) {
	var scan comparisonChainScan
	operandStart := 0
	for index := 0; index < len(expression); index++ {
		if _, quoted := skipCypherLexicalToken(expression, index); quoted {
			return comparisonChainScan{}, false, true
		}
		switch expression[index] {
		case '(', ')', '[', ']', '{', '}', ':':
			return comparisonChainScan{}, false, true
		case '/':
			if index+1 < len(expression) && (expression[index+1] == '/' || expression[index+1] == '*') {
				return comparisonChainScan{}, false, true
			}
		}
		operatorLength := 0
		switch expression[index] {
		case '<':
			if incomingArrowAt(expression, index) {
				continue
			}
			if index > 0 && cyphertext.NamePartAt(expression, index-1) && typeConstructorBefore(expression, index) {
				return comparisonChainScan{}, false, true
			}
			operatorLength = 1
			if index+1 < len(expression) && (expression[index+1] == '>' || expression[index+1] == '=') {
				operatorLength = 2
			}
		case '>':
			if index > 0 && expression[index-1] == '-' {
				continue
			}
			operatorLength = 1
			if index+1 < len(expression) && expression[index+1] == '=' {
				operatorLength = 2
			}
		case '!':
			if index+1 < len(expression) && expression[index+1] == '=' {
				operatorLength = 2
			}
		case '=':
			operatorLength = 1
			if index+1 < len(expression) && expression[index+1] == '~' {
				operatorLength = 2
			}
		}
		if operatorLength == 0 {
			continue
		}
		if strings.TrimSpace(expression[operandStart:index]) == "" {
			return comparisonChainScan{}, false, false
		}
		// A CASE block's WHEN comparisons are not the chain's (#699): a CASE
		// enclosing this operator starts in the operand before it, which
		// then goes to the full scan. Checking only there keeps the byte
		// loop as it was.
		if index-operandStart >= len("CASE") && operandMayContainCase(expression[operandStart:index]) {
			return comparisonChainScan{}, false, true
		}
		scan.append(comparisonOperatorSpan{offset: index, length: operatorLength})
		index += operatorLength - 1
		operandStart = index + 1
	}
	if scan.count == 0 || strings.TrimSpace(expression[operandStart:]) == "" {
		return comparisonChainScan{}, false, false
	}
	return scan, true, false
}

// operandMayContainCase reports whether operand contains the letters "case"
// in any letter case (a superset of the CASE keywords in it).
func operandMayContainCase(operand string) bool {
	for i := 0; i+4 <= len(operand); i++ {
		if operand[i]|0x20 == 'c' && operand[i+1]|0x20 == 'a' && operand[i+2]|0x20 == 's' && operand[i+3]|0x20 == 'e' {
			return true
		}
	}
	return false
}

// typeConstructorBefore reports whether LIST, ARRAY or ANY ends right before
// position index (LIST<INTEGER> in a type predicate, #838).
func typeConstructorBefore(expression string, index int) bool {
	start := index
	for start > 0 && cyphertext.NamePartAt(expression, start-1) {
		start--
	}
	switch upperASCII(expression[start:index]) {
	case "LIST", "ARRAY", "ANY":
		return true
	}
	return false
}
