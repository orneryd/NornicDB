package cypher

// rowListLiteral constructs a fresh list, stopping at an unresolved or failed item.
// Text and compiled evaluators supply item evaluation, never their own loops.
func rowListLiteral(count int, evaluate func(int) (interface{}, bool, error)) (interface{}, bool, error) {
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		value, ok, err := evaluate(i)
		if err != nil || !ok {
			return nil, false, err
		}
		result = append(result, value)
	}
	return result, true, nil
}

// rowStringPredicateValue owns STARTS WITH, ENDS WITH and CONTAINS on values.
// Null and non-string operands give unknown; WHERE reads that as not held.
func rowStringPredicateValue(left, right interface{}, predicate func(string, string) bool) interface{} {
	leftText, leftOK := left.(string)
	rightText, rightOK := right.(string)
	if !leftOK || !rightOK {
		return nil
	}
	return predicate(leftText, rightText)
}
