package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
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

// rowMapProjectionSplit splits variable{items} into the variable and the item
// list. ok is false when expression is not a map projection: its brace
// doesn't close it, or what precedes the brace isn't a variable (a map
// literal, a subquery such as COUNT { … }).
func rowMapProjectionSplit(expression string) (variable, items string, ok bool) {
	return completeProjection(expression)
}

func (e *StorageExecutor) evaluateRowMapProjection(expression string, values map[string]interface{}) (interface{}, bool, bool, error) {
	variable, inner, projection := rowMapProjectionSplit(expression)
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
	parts := cyphertext.CommaSegments(inner)
	for item, ok := parts.Next(); ok; item, ok = parts.Next() {
		entry, valid := parseBraceEntry(item, projectionBrace)
		if !valid {
			return nil, true, false, nil
		}
		switch entry.kind {
		case wildcardBraceEntry:
			if temporal {
				return nil, true, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", localization.CypherCoreMapProjectionCoercion(formatCypherValueString(base)))
			}
			for name, value := range properties {
				result[name] = value
			}
		case propertyBraceEntry:
			if !temporal {
				result[entry.key] = properties[entry.key]
				continue
			}
			value, _, supported := evaluateTemporalProperty(base, entry.key)
			if !supported {
				return nil, true, false, temporalNoSuchFieldError(entry.key)
			}
			result[entry.key] = value
		case keyedBraceEntry, variableBraceEntry:
			value, ok, err := e.evaluateRowValue(cyphertext.TrimTrivia(entry.value), values)
			if err != nil {
				return nil, true, false, err
			}
			if !ok {
				return nil, true, false, nil
			}
			result[normalizePropertyKey(entry.key)] = value
		}
	}
	return result, true, true, nil
}

// mapProjectionPropertySelector reads a map projection's property selector,
// .name or .`quoted name`, and returns the property key (unquoted).
func mapProjectionPropertySelector(item string) (string, bool) {
	name, wildcard, ok := mapProjectionSelector(item)
	return name, ok && !wildcard
}

// mapProjectionSelector shares selector grammar between preflight and execution;
// trivia around the dot, name or wildcard never changes its token identity.
func mapProjectionSelector(item string) (name string, wildcard, ok bool) {
	reader := cyphertext.NewReader(item, 0)
	dot, exists := reader.Next()
	if !exists || !dot.Is(item, ".") {
		return "", false, false
	}
	token, exists := reader.Next()
	if !exists {
		return "", false, false
	}
	if _, more := reader.Next(); more {
		return "", false, false
	}
	if token.Is(item, "*") {
		return "", true, true
	}
	if _, valid := isOneSymbolicName(token.Text(item)); valid {
		return normalizePropertyKey(token.Text(item)), false, true
	}
	return "", false, false
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
