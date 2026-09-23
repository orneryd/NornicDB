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

func cypherNumericEquality(left, right interface{}) (bool, bool) {
	leftSigned, leftIsSigned := cypherSignedInteger(left)
	rightSigned, rightIsSigned := cypherSignedInteger(right)
	leftUnsigned, leftIsUnsigned := cypherUnsignedInteger(left)
	rightUnsigned, rightIsUnsigned := cypherUnsignedInteger(right)
	if (leftIsSigned || leftIsUnsigned) && (rightIsSigned || rightIsUnsigned) {
		switch {
		case leftIsSigned && rightIsSigned:
			return leftSigned == rightSigned, true
		case leftIsUnsigned && rightIsUnsigned:
			return leftUnsigned == rightUnsigned, true
		case leftIsSigned:
			return leftSigned >= 0 && uint64(leftSigned) == rightUnsigned, true
		default:
			return rightSigned >= 0 && leftUnsigned == uint64(rightSigned), true
		}
	}
	leftNumber, leftIsNumber := strictNumericValue(left)
	rightNumber, rightIsNumber := strictNumericValue(right)
	if !leftIsNumber && !rightIsNumber {
		return false, false
	}
	if !leftIsNumber || !rightIsNumber {
		return false, true
	}
	return leftNumber == rightNumber, true
}

func cypherSignedInteger(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int8:
		return int64(number), true
	case int16:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	default:
		return 0, false
	}
}

func cypherUnsignedInteger(value interface{}) (uint64, bool) {
	switch number := value.(type) {
	case uint:
		return uint64(number), true
	case uint8:
		return uint64(number), true
	case uint16:
		return uint64(number), true
	case uint32:
		return uint64(number), true
	case uint64:
		return number, true
	default:
		return 0, false
	}
}

func cypherListValue(value interface{}) ([]interface{}, bool) {
	valueType := reflect.TypeOf(value)
	if valueType == nil || (valueType.Kind() != reflect.Slice && valueType.Kind() != reflect.Array) {
		return nil, false
	}
	return toAnySlice(value), true
}
