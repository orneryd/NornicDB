package cypher

import (
	"context"
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

func (scan *comparisonChainScan) operator(index int) comparisonOperatorSpan {
	if index < len(scan.inline) {
		return scan.inline[index]
	}
	return scan.overflow[index-len(scan.inline)]
}

func (scan *comparisonChainScan) operand(expression string, index int) string {
	start := 0
	if index > 0 {
		previous := scan.operator(index - 1)
		start = previous.offset + previous.length
	}
	end := len(expression)
	if index < scan.count {
		end = scan.operator(index).offset
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
	resolve func(string, foldPosition) (interface{}, bool, error),
	compare func(interface{}, interface{}, string) interface{},
	cypher25 bool,
) (interface{}, bool, bool, error) {
	return evaluateComparisonChainInContext(context.Background(), expression, resolve, compare, cypher25)
}
func evaluateComparisonChainInContext(ctx context.Context, expression string, resolve func(string, foldPosition) (interface{}, bool, error), compare func(interface{}, interface{}, string) interface{}, cypher25 bool) (interface{}, bool, bool, error) {
	scan, ok := scanComparisonChain(expression)
	if !ok {
		return nil, false, false, nil
	}
	position := comparisonFoldPosition(&scan)
	value, resolved, err := evaluateScannedComparisonChain(ctx, expression, &scan, comparisonChainHasIdentity(expression, &scan), func(_ int, operand string) (interface{}, bool, error) {
		return resolve(operand, position)
	}, compare, cypher25)
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
	ctx context.Context,
	expression string,
	scan *comparisonChainScan,
	normalizeIdentity bool,
	resolve func(int, string) (interface{}, bool, error),
	compare func(interface{}, interface{}, string) interface{},
	cypher25 bool,
) (interface{}, bool, error) {
	if scan.count > 1 && mayContainSubqueryExpression(expression) && constantFalseLink(expression, scan, cypher25) {
		// Neo4j folds a link of two literals before it plans the chain's
		// subqueries: a false one is the chain's value (1 < 0 < COUNT {…}).
		return false, true, nil
	}
	if scan.count > 1 && mayContainSubqueryExpression(expression) && !inLocalSubqueryScope(ctx) {
		indexes := applyOperandIndexes(scan.count+1, func(i int) string { return scan.operand(expression, i) })
		cached, err := evaluateApplyOperands(ctx, indexes, func(i int) (interface{}, bool, error) { return resolve(i, scan.operand(expression, i)) })
		if err != nil {
			return nil, false, err
		}
		if len(cached) > 0 {
			// Strip Apply metadata through a separate fold entry, retaining the
			// same comparison implementation and exactly one evaluation per operand.
			return evaluateComparisonLinks(expression, scan, normalizeIdentity, func(i int, text string) (interface{}, bool, error) {
				return resolveApplyOperand(cached, i, func(i int) (interface{}, bool, error) { return resolve(i, text) })
			}, compare, cypher25, true)
		}
	}
	return evaluateComparisonLinks(expression, scan, normalizeIdentity, resolve, compare, cypher25, true)
}

// constantFalseLink reports a link of the chain between two literals that is
// false, as Neo4j folds it (constantPredicateTruth).
func constantFalseLink(expression string, scan *comparisonChainScan, cypher25 bool) bool {
	for i := 0; i < scan.count; i++ {
		leftText, rightText := scan.operand(expression, i), scan.operand(expression, i+1)
		left, ok := parseRowOperand(leftText)
		if !ok || left.kind != rowOperandLiteral {
			continue
		}
		right, ok := parseRowOperand(rightText)
		if !ok || right.kind != rowOperandLiteral {
			continue
		}
		span := scan.operator(i)
		operator := expression[span.offset : span.offset+span.length]
		leftValue, rightValue := promoteConstantNumbers(operator, leftText, rightText, left.literal, right.literal, cypher25)
		if held, known := compareCypherPredicateValue(leftValue, rightValue, operator).(bool); known && !held {
			return true
		}
	}
	return false
}

// resolveText is false for compiled operands, which already retain their text.
// Numeric constant folding still reads the text when promotion can apply.
func evaluateComparisonLinks(expression string, scan *comparisonChainScan, normalizeIdentity bool, resolve func(int, string) (interface{}, bool, error), compare func(interface{}, interface{}, string) interface{}, cypher25, resolveText bool) (interface{}, bool, error) {
	leftText := ""
	if resolveText {
		leftText = scan.operand(expression, 0)
	}
	left, leftOK, leftErr := resolve(0, leftText)
	if normalizeIdentity {
		left = rowIdentityPayload(left)
	}
	hasNull, resolved := false, leftOK
	firstErr := leftErr
	for index := 0; index < scan.count; index++ {
		span := scan.operator(index)
		operator := expression[span.offset : span.offset+span.length]
		rightText := ""
		if resolveText {
			rightText = scan.operand(expression, index+1)
		}
		right, rightOK, rightErr := resolve(index+1, rightText)
		if firstErr == nil {
			firstErr = rightErr
		}
		resolved = resolved && rightOK
		if normalizeIdentity {
			right = rowIdentityPayload(right)
		}
		// A later false discards an earlier error; a decided false never
		// evaluates the remaining operands. Each operand is evaluated once.
		if leftOK && rightOK && leftErr == nil && rightErr == nil {
			if left == nil || right == nil {
				hasNull = true
			} else {
				if operator == "!=" {
					operator = "<>"
				}
				leftValue, rightValue := left, right
				_, leftFloat := cypherFloatValue(left)
				_, rightFloat := cypherFloatValue(right)
				if leftFloat != rightFloat {
					leftValue, rightValue = promoteConstantNumbers(operator, scan.operand(expression, index), scan.operand(expression, index+1), left, right, cypher25)
				}
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
		span := scan.operator(index)
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
			if index > 0 && isIdentByte(expression[index-1]) && typeConstructorBefore(expression, index) {
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
	for start > 0 && isIdentByte(expression[start-1]) {
		start--
	}
	switch upperASCII(expression[start:index]) {
	case "LIST", "ARRAY", "ANY":
		return true
	}
	return false
}
