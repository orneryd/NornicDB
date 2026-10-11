package cypher

// evaluateRowReduce evaluates reduce and allReduce (parseReduceForm) for a
// row, every part evaluated by evaluate (the row evaluator, or the context
// evaluator for a reduce whose expressions hold subqueries); resolved is false
// when a part can't be evaluated.
func (e *StorageExecutor) evaluateRowReduce(function, argument string, values map[string]interface{}, evaluate rowValueEvaluator) (interface{}, bool, error) {
	form, ok := parseReduceForm(function, argument)
	if !ok {
		return nil, false, nil
	}
	accumulator, resolved, err := evaluate(form.initial, values)
	if err != nil || !resolved {
		return nil, false, err
	}
	listValue, resolved, err := evaluate(form.list, values)
	if err != nil || !resolved {
		return nil, false, err
	}
	if listValue == nil {
		return nil, true, nil
	}
	scope := make(map[string]interface{}, len(values)+2)
	for name, value := range values {
		scope[name] = value
	}
	step := func(expression string) func(accumulator, item interface{}) (interface{}, error) {
		return func(accumulator, item interface{}) (interface{}, error) {
			scope[form.accumulator], scope[form.variable] = accumulator, item
			value, resolved, err := evaluate(expression, scope)
			if err == nil && !resolved {
				err = errRowArgumentUnresolved
			}
			return value, err
		}
	}
	result, err := runReduceForm(form, accumulator, coerceToUnwindItems(listValue), step(form.step), step(form.predicate))
	if err == errRowArgumentUnresolved {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
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
