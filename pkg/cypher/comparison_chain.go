package cypher

import "strings"

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
// without duplicating expression work.
func evaluateComparisonChain(
	expression string,
	resolve func(string) interface{},
	compare func(interface{}, interface{}, string) interface{},
) (interface{}, bool) {
	scan, ok := scanComparisonChain(expression)
	if !ok {
		return nil, false
	}
	normalizeIdentity := false
	for index := 0; index <= scan.count; index++ {
		if isRowIdentityExpression(scan.operand(expression, index)) {
			normalizeIdentity = true
			break
		}
	}

	left := resolve(scan.operand(expression, 0))
	if normalizeIdentity {
		left = rowIdentityPayload(left)
	}
	hasNull := false
	for index := 0; index < scan.count; index++ {
		span := scan.operator(index)
		operator := expression[span.offset : span.offset+span.length]
		right := resolve(scan.operand(expression, index+1))
		if normalizeIdentity {
			right = rowIdentityPayload(right)
		}
		if left == nil || right == nil {
			hasNull = true
		} else {
			if operator == "!=" {
				operator = "<>"
			}
			comparison := compare(left, right, operator)
			if comparison == nil {
				hasNull = true
			} else if matched, ok := comparison.(bool); !ok || !matched {
				return false, true
			}
		}
		left = right
	}
	if hasNull {
		return nil, true
	}
	return true, true
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

	var scan comparisonChainScan
	operandStart := 0
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	var quote byte
	inLineComment, inBlockComment := false, false

	for index := 0; index < len(expression); index++ {
		current := expression[index]
		if inLineComment {
			if current == '\n' || current == '\r' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			if current == '*' && index+1 < len(expression) && expression[index+1] == '/' {
				inBlockComment = false
				index++
			}
			continue
		}
		if quote != 0 {
			if current == '\\' && quote != '`' && index+1 < len(expression) {
				index++
				continue
			}
			if current == quote {
				if index+1 < len(expression) && expression[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}

		if current == '/' && index+1 < len(expression) {
			switch expression[index+1] {
			case '/':
				inLineComment = true
				index++
				continue
			case '*':
				inBlockComment = true
				index++
				continue
			}
		}
		switch current {
		case '\'', '"', '`':
			quote = current
			continue
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
		}
		if parenDepth != 0 || bracketDepth != 0 || braceDepth != 0 {
			continue
		}

		operator := ""
		switch current {
		case '<':
			if index+1 < len(expression) && expression[index+1] == '-' {
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
		switch expression[index] {
		case '\'', '"', '`', '(', ')', '[', ']', '{', '}':
			return comparisonChainScan{}, false, true
		case '/':
			if index+1 < len(expression) && (expression[index+1] == '/' || expression[index+1] == '*') {
				return comparisonChainScan{}, false, true
			}
		}
		operatorLength := 0
		switch expression[index] {
		case '<':
			if index+1 < len(expression) && expression[index+1] == '-' {
				continue
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
		scan.append(comparisonOperatorSpan{offset: index, length: operatorLength})
		index += operatorLength - 1
		operandStart = index + 1
	}
	if scan.count == 0 || strings.TrimSpace(expression[operandStart:]) == "" {
		return comparisonChainScan{}, false, false
	}
	return scan, true, false
}
