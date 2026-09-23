package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateRowMapProjection(expression string, values map[string]interface{}) (interface{}, bool, bool) {
	open := strings.Index(expression, " {")
	if open <= 0 || !strings.HasSuffix(strings.TrimSpace(expression), "}") {
		return nil, false, false
	}
	baseExpression := strings.TrimSpace(expression[:open])
	// Cypher map projection starts from a bound variable (`node {.*}` or
	// `node {.name}`). Requiring that grammar here prevents nested map
	// literals and map comparisons from being mistaken for projections merely
	// because they contain a space followed by an opening brace.
	if !isValidIdentifier(baseExpression) {
		return nil, false, false
	}
	trimmed := strings.TrimSpace(expression)
	open = strings.Index(trimmed, " {")
	inner := strings.TrimSpace(trimmed[open+2 : len(trimmed)-1])
	base, resolved := e.evaluateRowExpression(baseExpression, values)
	if !resolved {
		return nil, true, false
	}
	if base == nil {
		return nil, true, true
	}

	properties, valid := rowProjectionProperties(base)
	if !valid {
		return nil, true, false
	}
	result := make(map[string]interface{})
	if inner == "" {
		return result, true, true
	}
	for _, rawItem := range splitTopLevelComma(inner) {
		item := strings.TrimSpace(rawItem)
		switch {
		case item == ".*":
			for name, value := range properties {
				result[name] = value
			}
		case strings.HasPrefix(item, ".") && isValidIdentifier(strings.TrimSpace(item[1:])):
			name := strings.TrimSpace(item[1:])
			result[name] = properties[name]
		default:
			separator := findTopLevelMapKeyValueSeparator(item)
			if separator > 0 {
				name := normalizePropertyKey(strings.TrimSpace(item[:separator]))
				value, ok := e.evaluateRowExpression(strings.TrimSpace(item[separator+1:]), values)
				if !ok {
					return nil, true, false
				}
				result[name] = value
				continue
			}
			if !isValidIdentifier(item) {
				return nil, true, false
			}
			value, ok := e.evaluateRowExpression(item, values)
			if !ok {
				return nil, true, false
			}
			result[item] = value
		}
	}
	return result, true, true
}

func rowProjectionProperties(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case *storage.Node:
		if typed == nil {
			return nil, false
		}
		return typed.Properties, true
	case *storage.Edge:
		if typed == nil {
			return nil, false
		}
		return typed.Properties, true
	default:
		return toStringAnyMap(value)
	}
}
