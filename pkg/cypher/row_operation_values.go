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

// cypherMembershipValues owns the IN fold over comparisons supplied by an
// ordered scan or an index adapter. Empty lists decide before a null needle;
// a match decides before any unknown comparison.
func cypherMembershipValues(value interface{}, count int, compare func(int) interface{}) interface{} {
	if count == 0 {
		return false
	}
	if value == nil {
		return nil
	}
	unknown := false
	for i := 0; i < count; i++ {
		equal := compare(i)
		if equal == true {
			return true
		}
		if equal == nil {
			unknown = true
		}
	}
	if unknown {
		return nil
	}
	return false
}

// indexedMembershipValue supplies the same fold with a single indexed probe,
// followed by comparisons for values that cannot be indexed. A null list item
// is an unknown probe; non-comparable items preserve Cypher equality's nulls.
func indexedMembershipValue(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, hasNull bool, equals func(interface{}, interface{}) interface{}) interface{} {
	indexed := len(comparableSet) > 0 || hasNull
	count := len(nonComparable)
	if indexed {
		count++
	}
	return cypherMembershipValues(actual, count, func(i int) interface{} {
		if indexed {
			if i == 0 {
				if isComparableValue(actual) {
					if _, hit := comparableSet[membershipKey(actual)]; hit {
						return true
					}
				}
				if hasNull {
					return nil
				}
				return false
			}
			i--
		}
		return equals(actual, nonComparable[i])
	})
}
