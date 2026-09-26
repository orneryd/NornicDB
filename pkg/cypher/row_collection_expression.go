package cypher

import "strings"

func (e *StorageExecutor) evaluateRowReduce(argument string, values map[string]interface{}) (interface{}, bool, error) {
	parts := splitTopLevelComma(argument)
	if len(parts) != 2 {
		return nil, false, nil
	}
	assignment := strings.TrimSpace(parts[0])
	equals := findTopLevelMapKeyValueSeparator(assignment)
	if equals <= 0 {
		equals = strings.IndexByte(assignment, '=')
	}
	if equals <= 0 {
		return nil, false, nil
	}
	accumulatorName := strings.TrimSpace(assignment[:equals])
	if !isValidIdentifier(accumulatorName) {
		return nil, false, nil
	}
	accumulator, resolved, err := e.evaluateRowValue(strings.TrimSpace(assignment[equals+1:]), values)
	if err != nil {
		return nil, false, err
	}
	if !resolved {
		return nil, false, nil
	}

	iteration := strings.TrimSpace(parts[1])
	inIndex := topLevelKeywordIndex(iteration, "IN")
	if inIndex <= 0 {
		return nil, false, nil
	}
	variableName := strings.TrimSpace(iteration[:inIndex])
	if !isValidIdentifier(variableName) {
		return nil, false, nil
	}
	remainder := strings.TrimSpace(iteration[inIndex+len("IN"):])
	pipeIndex := rowTopLevelPipeIndex(remainder)
	if pipeIndex <= 0 {
		return nil, false, nil
	}
	listValue, resolved, err := e.evaluateRowValue(strings.TrimSpace(remainder[:pipeIndex]), values)
	if err != nil {
		return nil, false, err
	}
	if !resolved {
		return nil, false, nil
	}
	if listValue == nil {
		return nil, true, nil
	}
	items := toAnySlice(listValue)
	if items == nil {
		return nil, false, nil
	}
	reduction := strings.TrimSpace(remainder[pipeIndex+1:])
	scope := make(map[string]interface{}, len(values)+2)
	for name, value := range values {
		scope[name] = value
	}
	for _, item := range items {
		scope[accumulatorName] = accumulator
		scope[variableName] = item
		var err error
		accumulator, resolved, err = e.evaluateRowValue(reduction, scope)
		if err != nil {
			return nil, false, err
		}
		if !resolved {
			return nil, false, nil
		}
	}
	return accumulator, true, nil
}

func rowTopLevelPipeIndex(expression string) int {
	parenDepth, bracketDepth, braceDepth := 0, 0, 0
	var quote byte
	for index := 0; index < len(expression); index++ {
		current := expression[index]
		if quote != 0 {
			if current == quote && !isBackslashEscaped(expression, index) {
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
