package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
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

type mapProjectionPolicy uint8

const (
	mapProjectionRuntime mapProjectionPolicy = iota
	mapProjectionStatic
)

// mapProjectionSplit owns the common variable{items} boundary. Runtime keeps
// the submitted variable spelling and trims items; static analysis resolves the
// symbolic name and excludes unbound keywords before its scope-specific check.
func mapProjectionSplit(expression string, policy mapProjectionPolicy) (variable, items string, ok bool) {
	if policy == mapProjectionRuntime {
		expression = strings.TrimSpace(expression)
	}
	open := strings.IndexByte(expression, '{')
	if open <= 0 || cyphertext.FindMatchingDelimiter(expression, open, '{', '}', cyphertext.OpeningDelimiter) != len(expression)-1 {
		return "", "", false
	}
	variable = strings.TrimSpace(expression[:open])
	if policy == mapProjectionRuntime {
		if !cyphertext.IsIdentifier(variable) && !isBacktickQuotedName(variable) || precedingSubqueryExpressionKeyword(expression, open) {
			return "", "", false
		}
	} else {
		variable = simpleSemanticIdentifier(variable)
		_, subqueryName := staticSubqueryExpressionTypes[upperASCII(variable)]
		if variable == "" || isCypherKeyword(variable) && !subqueryName || isSubqueryBrace(expression, open) {
			return "", "", false
		}
	}
	items = expression[open+1 : len(expression)-1]
	if policy == mapProjectionRuntime {
		items = strings.TrimSpace(items)
	}
	return variable, items, true
}

func (e *StorageExecutor) evaluateRowMapProjection(expression string, values map[string]interface{}) (interface{}, bool, bool, error) {
	variable, inner, projection := mapProjectionSplit(expression, mapProjectionRuntime)
	if !projection {
		return nil, false, false, nil
	}
	base, resolved, err := e.evaluateRowValue(variable, values)
	if err != nil {
		return nil, true, false, err
	}
	if !resolved {
		return nil, true, false, nil
	}
	if cypherfn.RuntimeValue(base) == nil {
		return nil, true, true, nil
	}
	temporal := isRuntimeTemporal(base) || isRuntimeDuration(base)
	properties, hasProperties := rowProjectionProperties(e.queryVisibleValue(base))
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
		item := strings.TrimSpace(rawItem)
		switch {
		case item == ".*":
			if temporal {
				return nil, true, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
					localization.CypherCoreMapProjectionCoercion(formatCypherValueString(base)))
			}
			for name, value := range properties {
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
				value, ok, err := e.evaluateRowValue(strings.TrimSpace(item[separator+1:]), values)
				if err != nil {
					return nil, true, false, err
				}
				if !ok {
					return nil, true, false, nil
				}
				result[name] = value
				continue
			}
			if !cyphertext.IsIdentifier(item) && !isBacktickQuotedName(item) {
				return nil, true, false, nil
			}
			value, ok, err := e.evaluateRowValue(item, values)
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
	name := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(item), "."))
	switch {
	case cyphertext.IsIdentifier(name):
		return name, true
	case isBacktickQuotedName(name):
		return normalizePropertyKey(name), true
	}
	return "", false
}

// rowProjectionProperties returns the properties of a node, relationship or
// map; ok is false for any other value.
func rowProjectionProperties(value interface{}) (map[string]interface{}, bool) {
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
	if _, isPath := cypherPathElements(value); isPath {
		return nil, false
	}
	return toStringAnyMap(value)
}

// A typed nil marker has the same null projection semantics as a nil entity.
func isNilRemoteEntity(value interface{}) bool {
	remote, ok := value.(storage.RemoteEntityValue)
	return ok && remote == nil
}
