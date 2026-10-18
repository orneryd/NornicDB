package cypher

import (
	"math"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// projectedNodePropertyValueMap keeps every value in its projected type.
type projectedNodePropertyValueMap = map[string]map[string]interface{}

// gdsValueUse distinguishes graph import validation from coercion of an
// algorithm configuration or an already imported property. All three use the
// same conversion; only imports apply GDS's positive Long-to-Double bound.
type gdsValueUse uint8

const (
	gdsImportValue gdsValueUse = iota
	gdsConfigurationValue
	gdsStoredValue
)

// gdsConvertPropertyValue is the single conversion of projected values.
// An empty target infers the storage type. A target converts to that type;
// the Relationship target requires a numeric scalar and stores Float. Only
// native scalar import may widen Integer to Float. Imports validate
// Long-to-Double conversion with GDS's bound, except where GDS converts
// before reading: the Longs in one mixed numeric list, and a native
// relationship's stored Long. Array builders keep their first type and permit
// only lossless Double-to-Long conversion.
func gdsConvertPropertyValue(value interface{}, target string, widen bool, uses ...gdsValueUse) (exact interface{}, kind string, ok bool, err error) {
	importing := len(uses) == 0 || uses[0] == gdsImportValue
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
		// A relationship default must be a Double; a value must be numeric.
		valueType := ""
		switch value.(type) {
		case string:
			valueType = "TEXT"
		case bool:
			valueType = "BOOLEAN"
		}
		if defaultValue {
			// GDS checks a default's type before reading it as a value: a
			// map, or an empty list (Object[] to GDS), is not a Double.
			if _, isMap := value.(map[string]interface{}); isMap {
				valueType = "MAP"
			} else if items, list := cypherListValue(value); list && len(items) == 0 {
				return nil, "", false, localizedError(localization.CypherGraphProceduresDefaultPropertyType("Double", "Object[]"), nil)
			}
		}
		if valueType != "" && defaultValue {
			return nil, "", false, localizedError(localization.CypherGraphProceduresDefaultPropertyType("Double", gdsJavaType(value)), nil)
		}
		if valueType != "" {
			return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType(valueType, gdsJavaValueText(value, true)), nil)
		}
	}
	switch typed := value.(type) {
	case []int64:
		exact, kind = typed, "List of Integer"
	case []float64:
		exact, kind = typed, "List of Float"
	default:
		if isIntegerProcedureValue(value) {
			exact, kind = gdsInteger(value), "Integer"
		} else if number, numeric := toFloat64(value); numeric {
			// Stored strings are not numeric GDS properties, even if parseable.
			if _, text := value.(string); text {
				return nil, "", false, nil
			}
			exact, kind = number, "Float"
		} else if items, list := cypherListValue(value); list {
			if len(items) == 0 {
				if relationship {
					return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("TEXT_ARRAY", gdsJavaValueText([]string{}, true)), nil)
				}
				// Extension: native Neo4j stores untyped [] as StringArray
				// and refuses it. NornicDB's storage erases array element
				// types, so retain a zero-length numeric array instead of
				// losing a well-defined property. An existing builder sets
				// its type; a new empty array defaults to List of Integer.
				exact, kind = []int64{}, "List of Integer"
				break
			}
			integers := make([]int64, len(items))
			allIntegers := true
			for i, item := range items {
				if _, text := item.(string); text {
					if relationship {
						texts := make([]string, len(items))
						for j, element := range items {
							texts[j], _ = element.(string)
						}
						return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("TEXT_ARRAY", gdsJavaValueText(texts, true)), nil)
					}
					return nil, "", false, localizedError(localization.CypherGraphProceduresUnsupportedArrayType("StringArray"), nil)
				}
				if isIntegerProcedureValue(item) {
					integers[i] = gdsInteger(item)
				} else {
					allIntegers = false
				}
			}
			if allIntegers {
				exact, kind = integers, "List of Integer"
			} else {
				floats := make([]float64, len(items))
				for i, item := range items {
					if isIntegerProcedureValue(item) {
						// One list mixing Longs and Doubles is a Double array
						// before GDS reads it: its Longs round, unbounded.
						floats[i], _ = gdsLongToDouble(gdsInteger(item), false)
					} else if number, numeric := toFloat64(item); numeric {
						floats[i] = number
					} else {
						return nil, "", false, nil
					}
				}
				exact, kind = floats, "List of Float"
			}
		} else {
			return nil, "", false, nil
		}
	}
	if defaultValue && strings.HasPrefix(target, "List") && kind != target {
		expected := "long[]"
		if target == "List of Float" {
			expected = "double[]"
		}
		return nil, "", false, localizedError(localization.CypherGraphProceduresDefaultPropertyType(expected, gdsJavaType(exact)), nil)
	}
	if defaultValue && relationship && strings.HasPrefix(kind, "List") {
		actual := "long[]"
		if kind == "List of Float" {
			actual = "double[]"
		}
		return nil, "", false, localizedError(localization.CypherGraphProceduresDefaultPropertyType("Double", actual), nil)
	}
	if relationship && strings.HasPrefix(kind, "List") {
		return nil, "", false, localizedError(localization.CypherGraphProceduresRelationshipValueType("NUMBER_ARRAY", gdsJavaValueText(exact, true)), nil)
	}
	if target == "" || target == kind {
		return exact, kind, true, nil
	}
	switch {
	case target == "Float" && kind == "Integer":
		// A native relationship's stored Long widens to its Double property
		// as GDS reads it, unbounded; its default and aggregation values keep
		// the bound.
		bounded := importing && !(relationship && widen && !defaultValue)
		number, err := gdsLongToDouble(exact.(int64), bounded)
		return number, target, err == nil, err
	case target == "Integer" && kind == "Float":
		if widen {
			return exact, "Float", true, nil
		}
		number := exact.(float64)
		if gdsLosslessLong(number) {
			return int64(number), target, true, nil
		}
		return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeLongConversion(gdsNumberText(number, 'f', 2)), nil)
	case target == "List of Float" && kind == "List of Integer":
		integers := exact.([]int64)
		floats := make([]float64, len(integers))
		for i, number := range integers {
			floats[i], err = gdsLongToDouble(number, importing)
			if err != nil {
				return nil, "", false, err
			}
		}
		return floats, target, true, nil
	case target == "List of Integer" && kind == "List of Float":
		floats := exact.([]float64)
		integers := make([]int64, len(floats))
		for i, number := range floats {
			if !gdsLosslessLong(number) {
				return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeLongArrayConversion(gdsJavaValueText(exact, true), gdsNumberText(number, 'f', 2)), nil)
			}
			integers[i] = int64(number)
		}
		return integers, target, true, nil
	}
	return nil, "", false, localizedError(localization.CypherGraphProceduresUnsafeConversion(gdsJavaValueText(exact, true), gdsJavaTypeTarget(target)), nil)
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

// The upper bound is exclusive: float64(MaxInt64) rounds to 2^63.
func gdsLosslessLong(value float64) bool {
	return !math.IsNaN(value) && value >= -0x1p63 && value < 0x1p63 && value == math.Trunc(value)
}

// nodePropertyValue is the typed reader for every projected node property.
// Integer reads preserve Long values exactly and refuse other property types;
// Double reads use GDS's NaN missing-value sentinel (including Long.MIN_VALUE).
// An empty requested type reads the property's exact projected storage type.
func (p *GraphProjection) nodePropertyValue(id, key, requested string, probe *pipelineCancellation) (interface{}, error) {
	kind := p.NodePropertyTypes[key]
	if requested == "" {
		requested = kind
	}
	if requested == "Integer" && kind != "Integer" {
		return nil, localizedError(localization.CypherGraphProceduresPropertyReadType(gdsPropertyReadType(requested), gdsPropertyReadType(kind)), nil)
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
	exact, _, _, err := gdsConvertPropertyValue(value, requested, false, gdsStoredValue)
	return exact, err
}

// gdsPropertyReadType is a projected property type as GDS read errors name it.
func gdsPropertyReadType(kind string) string {
	switch kind {
	case "Integer":
		return "LONG"
	case "Float":
		return "DOUBLE"
	case "List of Integer":
		return "LONG_ARRAY"
	case "List of Float":
		return "DOUBLE_ARRAY"
	}
	return "UNKNOWN"
}
