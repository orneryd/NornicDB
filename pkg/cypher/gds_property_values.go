package cypher

import (
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"math"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// projectedNodePropertyValueMap keeps every value in its projected type.
type projectedNodePropertyValueMap = map[string]map[string]interface{}

// gdsConvertPropertyValue is the single conversion of projected values.
// An empty target infers the storage type. A target converts to that type;
// the Relationship target requires a numeric scalar and stores Float. Only
// native scalar import may widen Integer to Float. All imports validate
// Long-to-Double conversion with GDS's bound. Array builders keep
// their first type and permit only lossless Double-to-Long conversion.
func gdsConvertPropertyValue(value interface{}, target string, widen bool) (exact interface{}, kind string, ok bool, err error) {
	defaultValue := strings.HasPrefix(target, "Default ")
	target = strings.TrimPrefix(target, "Default ")
	relationship := target == "Relationship"
	if relationship {
		target = "Float"
	}
	if value == nil {
		return nil, "", false, nil
	}
	if relationship {
		switch typed := value.(type) {
		case string:
			return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("TEXT", "String("+strconv.Quote(typed)+")"), nil)
		case bool:
			return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("BOOLEAN", "Boolean('"+strconv.FormatBool(typed)+"')"), nil)
		}
	}
	exact, kind, ok, err = gdsNumericInput(value, relationship, true, func(texts []string) string { return "StringArray[" + strings.Join(texts, ", ") + "]" })
	if err != nil || !ok {
		return nil, "", false, err
	}
	if defaultValue && strings.HasPrefix(target, "List") && kind != target {
		expected := "long[]"
		if target == "List of Float" {
			expected = "double[]"
		}
		return nil, "", false, localizedError(localization.CypherGraphProceduresDefaultPropertyType(expected, gdsJavaType(exact)), nil)
	}
	if relationship && strings.HasPrefix(kind, "List") {
		return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("NUMBER_ARRAY", gdsJavaValueText(exact)), nil)
	}
	if target == "" || target == kind {
		return exact, kind, true, nil
	}
	switch {
	case target == "Float" && kind == "Integer":
		number, err := gdsLongToDouble(exact.(int64), true)
		return number, target, err == nil, err
	case target == "Integer" && kind == "Float":
		if widen {
			return exact, "Float", true, nil
		}
		number := exact.(float64)
		if integer, fits := valueconvert.Int64(number, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat); fits {
			return integer, target, true, nil
		}
		return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeLongConversion(strconv.FormatFloat(number, 'f', 2, 64)), nil)
	case target == "List of Float" && kind == "List of Integer":
		integers := exact.([]int64)
		floats := make([]float64, len(integers))
		for i, number := range integers {
			floats[i], err = gdsLongToDouble(number, true)
			if err != nil {
				return nil, "", false, err
			}
		}
		return floats, target, true, nil
	case target == "List of Integer" && kind == "List of Float":
		floats := exact.([]float64)
		integers := make([]int64, len(floats))
		for i, number := range floats {
			integer, fits := valueconvert.Int64(number, valueconvert.IntegerFloat|valueconvert.IntegerWholeFloat)
			if !fits {
				return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeLongArrayConversion(gdsJavaValueText(exact), strconv.FormatFloat(number, 'f', 2, 64)), nil)
			}
			integers[i] = integer
		}
		return integers, target, true, nil
	}
	return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeConversion(gdsJavaValueText(exact), gdsJavaTypeTarget(target)), nil)
}

// gdsLongToDouble owns Long-to-Double conversion. GDS 2.13.12 import
// validation bounds only positive Longs, even when a larger Long is exactly
// representable. Reads of already stored Long properties permit rounding.
func gdsLongToDouble(value int64, importing bool) (float64, error) {
	if importing && value > 1<<53 {
		return 0, localizedError(localization.CypherGraphProceduresUnsafeDoubleConversion(strconv.FormatInt(value, 10)), nil)
	}
	return float64(value), nil
}

// nodePropertyValue is the typed reader for every projected node property.
// Integer reads preserve Long values exactly and refuse Double properties;
// Double reads use GDS's NaN missing-value sentinel (including Long.MIN_VALUE).
// An empty requested type reads the property's exact projected storage type.
func (p *GraphProjection) nodePropertyValue(id, key, requested string) (interface{}, error) {
	kind := p.NodePropertyTypes[key]
	if requested == "" {
		requested = kind
	}
	if requested == "Integer" && kind == "Float" {
		return nil, localizedError(localization.CypherGraphProceduresPropertyReadType("LONG", "DOUBLE"), nil)
	}
	value, present := p.NodeProperties[id][key]
	if !present {
		switch requested {
		case "Integer":
			value = int64(math.MinInt64)
		case "List of Integer":
			value = []int64{}
		case "List of Float":
			value = []float64{}
		default:
			value = math.NaN()
		}
	} else if requested == "Float" && kind == "Integer" && value == int64(math.MinInt64) {
		value = math.NaN()
	}
	if requested == "Float" && kind == "Integer" {
		if number, ok := value.(int64); ok {
			return gdsLongToDouble(number, false)
		}
	}
	exact, _, _, err := gdsConvertPropertyValue(value, requested, false)
	return exact, err
}
