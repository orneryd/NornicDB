package cypher

import "strings"

func (e *StorageExecutor) evaluateRowReduce(argument string, values map[string]interface{}) (interface{}, bool) {
	parts := splitTopLevelComma(argument)
	if len(parts) != 2 {
		return nil, false
	}
	assignment := strings.TrimSpace(parts[0])
	equals := findTopLevelMapKeyValueSeparator(assignment)
	if equals <= 0 {
		equals = strings.IndexByte(assignment, '=')
	}
	if equals <= 0 {
		return nil, false
	}
	accumulatorName := strings.TrimSpace(assignment[:equals])
	if !isValidIdentifier(accumulatorName) {
		return nil, false
	}
	accumulator, resolved := e.evaluateRowExpression(strings.TrimSpace(assignment[equals+1:]), values)
	if !resolved {
		return nil, false
	}

	iteration := strings.TrimSpace(parts[1])
	inIndex := topLevelKeywordIndex(iteration, "IN")
	if inIndex <= 0 {
		return nil, false
	}
	variableName := strings.TrimSpace(iteration[:inIndex])
	if !isValidIdentifier(variableName) {
		return nil, false
	}
	remainder := strings.TrimSpace(iteration[inIndex+len("IN"):])
	pipeIndex := rowTopLevelPipeIndex(remainder)
	if pipeIndex <= 0 {
		return nil, false
	}
	listValue, resolved := e.evaluateRowExpression(strings.TrimSpace(remainder[:pipeIndex]), values)
	if !resolved {
		return nil, false
	}
	if listValue == nil {
		return nil, true
	}
	items := toAnySlice(listValue)
	if items == nil {
		return nil, false
	}
	reduction := strings.TrimSpace(remainder[pipeIndex+1:])
	scope := make(map[string]interface{}, len(values)+2)
	for name, value := range values {
		scope[name] = value
	}
	for _, item := range items {
		scope[accumulatorName] = accumulator
		scope[variableName] = item
		accumulator, resolved = e.evaluateRowExpression(reduction, scope)
		if !resolved {
			return nil, false
		}
	}
	return accumulator, true
}

func rowTopLevelPipeIndex(expression string) int {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	var quote byte
	for index := 0; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			if current == quote && (index == 0 || expression[index-1] != '\\') {
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"', '`':
			quote = current
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		case '|':
			if parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 {
				return index
			}
		}
	}
	return -1
}
