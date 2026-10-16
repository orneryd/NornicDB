package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Map projection (#907): variable{.key, .*, key: expression, variable}, the
// brace with or without space after the variable, as Neo4j 5.26 evaluates it:
//
//   - a node, relationship or map projects its properties;
//   - a temporal value or duration projects its fields (d{.year}); an
//     unknown field, or .*, is a TypeError;
//   - null projects to null;
//   - any other value is a TypeError ("Type mismatch: expected a map but was
//     Long(1)"); statically known ones are rejected before the statement runs.
//
// Items apply in order, a later one replacing an earlier key: {a: 1}{.a,
// a: 2} is {a: 2}.

// rowMapProjectionSplit splits variable{items} into the variable and the item
// list. ok is false when expression is not a map projection: its brace
// doesn't close it, or what precedes the brace isn't a variable (a map
// literal, a subquery such as COUNT { … }).
func rowMapProjectionSplit(expression string) (variable, items string, ok bool) {
	trimmed := strings.TrimSpace(expression)
	if !strings.HasSuffix(trimmed, "}") {
		return "", "", false
	}
	open := strings.IndexByte(trimmed, '{')
	if open <= 0 || findMatchingDelimiter(trimmed, open, '{', '}') != len(trimmed)-1 {
		return "", "", false
	}
	variable = strings.TrimSpace(trimmed[:open])
	if !isValidIdentifier(variable) && !isBacktickQuotedName(variable) {
		return "", "", false
	}
	if precedingSubqueryExpressionKeyword(trimmed, open) {
		return "", "", false
	}

	return variable, strings.TrimSpace(trimmed[open+1 : len(trimmed)-1]), true
}

func (e *StorageExecutor) evaluateRowMapProjection(expression string, values map[string]interface{}) (interface{}, bool, bool, error) {
	return e.evaluateRowMapProjectionWithCancellation(expression, values, rowPipelineCancellation(values))
}

func (e *StorageExecutor) evaluateRowMapProjectionWithCancellation(expression string, values map[string]interface{}, probe *pipelineCancellation) (interface{}, bool, bool, error) {
	variable, inner, projection := rowMapProjectionSplit(expression)
	if !projection {
		return nil, false, false, nil
	}
	base, resolved, err := e.evaluateRowValueWithCancellation(variable, values, probe)
	if err != nil {
		return nil, true, false, err
	}
	if !resolved {
		return nil, true, false, nil
	}
	if base == nil || isNilRemoteEntity(base) {
		return nil, true, true, nil
	}
	temporal := isRuntimeTemporal(base) || isRuntimeDuration(base)
	properties, hasProperties := rowProjectionPropertiesWithCancellation(e.queryVisibleValue(base), probe)
	if probe != nil && probe.err != nil {
		return nil, true, false, probe.err
	}
	if !hasProperties && !temporal {
		if _, isPath := cypherPathElements(base); isPath {
			return nil, true, false, runtimeTypeError("Type mismatch: expected a map but was Path")
		}
		return nil, true, false, propertyAccessTypeError(base)
	}
	result := make(map[string]interface{})
	if inner == "" {
		return result, true, true, nil
	}
	for _, rawItem := range splitTopLevelComma(inner) {
		if probe != nil && !probe.check() {
			return nil, true, false, probe.err
		}
		item := strings.TrimSpace(rawItem)
		switch {
		case item == ".*":
			if temporal {
				return nil, true, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
					localization.CypherCoreMapProjectionCoercion(formatCypherValueString(base)))
			}
			for name, value := range properties {
				if probe != nil && !probe.check() {
					return nil, true, false, probe.err
				}
				result[name] = value
			}
		case strings.HasPrefix(item, "."):
			name, selector := mapProjectionPropertySelector(item)
			if !selector {
				return nil, true, false, nil
			}
			if !temporal {
				result[name] = properties[name]
				continue
			}
			value, _, supported := evaluateTemporalProperty(base, name)
			if !supported {
				return nil, true, false, temporalNoSuchFieldError(name)
			}
			result[name] = value
		default:
			separator := findTopLevelMapKeyValueSeparator(item)
			if separator > 0 {
				name := normalizePropertyKey(strings.TrimSpace(item[:separator]))
				value, ok, err := e.evaluateRowValueWithCancellation(strings.TrimSpace(item[separator+1:]), values, probe)
				if err != nil {
					return nil, true, false, err
				}
				if !ok {
					return nil, true, false, nil
				}
				result[name] = value
				continue
			}
			if !isValidIdentifier(item) && !isBacktickQuotedName(item) {
				return nil, true, false, nil
			}
			value, ok, err := e.evaluateRowValueWithCancellation(item, values, probe)
			if err != nil {
				return nil, true, false, err
			}
			if !ok {
				return nil, true, false, nil
			}
			result[normalizePropertyKey(item)] = value
		}
	}
	return result, true, true, nil
}

// mapProjectionPropertySelector reads a map projection's property selector,
// .name or .`quoted name`, and returns the property key (unquoted).
func mapProjectionPropertySelector(item string) (string, bool) {
	return mapProjectionPropertySelectorWithCancellation(item, nil)
}
func mapProjectionPropertySelectorWithCancellation(item string, probe *pipelineCancellation) (string, bool) {
	if probe != nil && !probe.check() {
		return "", false
	}

	name := trimSpaceWithCancellation(strings.TrimPrefix(trimSpaceWithCancellation(item, probe), "."), probe)
	switch {
	case isValidIdentifierWithCancellation(name, probe):
		return name, true
	case isBacktickQuotedNameWithCancellation(name, probe):
		return normalizePropertyKeyWithCancellation(name, probe), true
	}
	return "", false
}

// rowProjectionProperties returns the properties of a node, relationship or
// map; ok is false for any other value.
func rowProjectionProperties(value interface{}) (map[string]interface{}, bool) {
	return rowProjectionPropertiesWithCancellation(value, nil)
}

func rowProjectionPropertiesWithCancellation(value interface{}, probe *pipelineCancellation) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case storage.RemoteEntityValue:
		properties, _ := typed["properties"].(map[string]interface{})
		return properties, true
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
	}
	object, isMap := toStringAnyMapWithCancellation(value, probe)
	if _, isPath := cypherPathElementsOfWithCancellation(value, object, isMap, probe); isPath {
		return nil, false
	}
	return object, isMap
}

// A typed nil marker has the same null projection semantics as a nil entity.
func isNilRemoteEntity(value interface{}) bool {
	remote, ok := value.(storage.RemoteEntityValue)
	return ok && remote == nil
}
