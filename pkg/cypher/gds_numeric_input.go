package cypher

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// gdsNumericInput applies projection policy after the shared readers have
// recognized the scalar/list. An unrepresentable Long is invalid, never zero.
func gdsNumericInput(value interface{}, relationship, mixedImport bool, renderStrings func([]string) string) (interface{}, string, bool, error) {
	if number, numeric := valueconvert.ReadNumber(value); numeric {
		if number.IsInteger() {
			integer, fits := number.Int64(valueconvert.IntegerOnly)
			return integer, "Integer", fits, nil
		}
		return number.Float64(), "Float", true, nil
	}
	list, ok := valueconvert.ReadList(value)
	if !ok {
		return nil, "", false, nil
	}
	if list.Len() == 0 && relationship {
		return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("TEXT_ARRAY", renderStrings([]string{})), nil)
	}
	integers := make([]int64, list.Len())
	numbers := make([]valueconvert.Number, list.Len())
	allIntegers := list.ElementNumberKind() != valueconvert.FloatNumber
	for i := range numbers {
		item := list.At(i)
		if _, text := item.(string); text {
			if relationship {
				texts := make([]string, list.Len())
				for j := range texts {
					texts[j], _ = list.At(j).(string)
				}
				return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("TEXT_ARRAY", renderStrings(texts)), nil)
			}
			return nil, "", false, localizedError(localization.CypherGraphProceduresUnsupportedArrayType("StringArray"), nil)
		}
		number, numeric := valueconvert.ReadNumber(item)
		if !numeric {
			return nil, "", false, nil
		}
		numbers[i] = number
		if number.IsInteger() {
			integer, fits := number.Int64(valueconvert.IntegerOnly)
			if !fits {
				return nil, "", false, nil
			}
			integers[i] = integer
		} else {
			allIntegers = false
		}
	}
	// Keep the existing empty-list extension: a boxed [] infers Integer.
	if allIntegers {
		return integers, "List of Integer", true, nil
	}
	floats := make([]float64, len(numbers))
	for i, number := range numbers {
		if number.IsInteger() {
			floating, err := gdsLongToDouble(integers[i], mixedImport)
			if err != nil {
				return nil, "", false, err
			}
			floats[i] = floating
		} else {
			floats[i] = number.Float64()
		}
	}
	return floats, "List of Float", true, nil
}
