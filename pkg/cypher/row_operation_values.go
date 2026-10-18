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

// membershipFold is the one IN fold: x IN list over the comparisons of x
// with the list's items, supplied by an ordered scan (cypherMembershipValues)
// or an index adapter (indexedMembershipTruth). An empty list decides before
// a null needle (membershipDecided); a match decides before any unknown
// comparison.
type membershipFold struct{ unknown bool }

// membershipDecided is the fold's answer when the list's size or the needle
// decides it before any comparison: false for an empty list, even for a
// null needle, then null for a null needle.
func membershipDecided(value interface{}, count int) (cypherTruth, bool) {
	if count == 0 {
		return truthFalse, true
	}
	if value == nil {
		return truthUnknown, true
	}
	return truthUnknown, false
}

// add folds one comparison and reports a match, which decides the fold.
func (fold *membershipFold) add(equal interface{}) bool {
	if equal == true {
		return true
	}
	if equal == nil {
		fold.unknown = true
	}
	return false
}

// result is the fold's answer after every comparison failed to match.
func (fold *membershipFold) result() cypherTruth {
	if fold.unknown {
		return truthUnknown
	}
	return truthFalse
}

// cypherMembershipValues is the IN fold over an ordered scan's comparisons.
func cypherMembershipValues(value interface{}, count int, compare func(int) interface{}) interface{} {
	if truth, decided := membershipDecided(value, count); decided {
		return truth.value()
	}
	var fold membershipFold
	for i := 0; i < count; i++ {
		if fold.add(compare(i)) {
			return true
		}
	}
	return fold.result().value()
}

// indexedMembershipTruth supplies the same fold with a single indexed probe,
// followed by comparisons for values that cannot be indexed. A null list item
// is an unknown probe; non-comparable items preserve Cypher equality's nulls.
// It runs once per row and comparison, so it takes no callback per probe.
func indexedMembershipTruth(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, hasNull bool, equals func(interface{}, interface{}) interface{}, probe *pipelineCancellation) cypherTruth {
	indexed := len(comparableSet) > 0 || hasNull
	count := len(nonComparable)
	if indexed {
		count++
	}
	if truth, decided := membershipDecided(actual, count); decided {
		return truth
	}
	var fold membershipFold
	if indexed {
		if isComparableValue(actual) {
			if _, hit := comparableSet[membershipKey(actual)]; hit {
				return truthTrue
			}
		}
		if hasNull {
			fold.add(nil)
		}
	}
	for _, item := range nonComparable {
		if fold.add(equals(actual, item)) {
			return truthTrue
		}
	}
	return fold.result()
}
