package cypher

import "reflect"

// cypherEquality applies Cypher's three-valued structural equality. A nil
// result represents unknown, which occurs when no definite inequality exists
// but at least one nested comparison involves null.
func cypherEquality(left, right interface{}) interface{} {
	if left == nil || right == nil {
		return nil
	}
	leftList, leftIsList := cypherListValue(left)
	rightList, rightIsList := cypherListValue(right)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList || len(leftList) != len(rightList) {
			return false
		}
		unknown := false
		for index := range leftList {
			equal := cypherEquality(leftList[index], rightList[index])
			if equal == nil {
				unknown = true
				continue
			}
			if !equal.(bool) {
				return false
			}
		}
		if unknown {
			return nil
		}
		return true
	}

	leftMap, leftIsMap := toStringAnyMap(left)
	rightMap, rightIsMap := toStringAnyMap(right)
	if leftIsMap || rightIsMap {
		if !leftIsMap || !rightIsMap || len(leftMap) != len(rightMap) {
			return false
		}
		unknown := false
		for key, leftValue := range leftMap {
			rightValue, exists := rightMap[key]
			if !exists {
				return false
			}
			equal := cypherEquality(leftValue, rightValue)
			if equal == nil {
				unknown = true
				continue
			}
			if !equal.(bool) {
				return false
			}
		}
		if unknown {
			return nil
		}
		return true
	}

	if equal, temporal := compareTemporalValues(left, right); temporal {
		return equal
	}
	return compareValues(left, right)
}

func cypherListValue(value interface{}) ([]interface{}, bool) {
	valueType := reflect.TypeOf(value)
	if valueType == nil || (valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array) {
		return nil, false
	}
	return toAnySlice(value), true
}
