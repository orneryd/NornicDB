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

// mapProjectionSplit retains TJ's runtime/static name policy. The final brace
// product supplies the syntax and entry boundaries in the carried occurrence.
func mapProjectionSplit(view braceView, policy mapProjectionPolicy, scope *staticTypeScope) (variable braceView, items []recordedMapEntry, ok bool) {
	if policy == mapProjectionRuntime {
		view = view.trim()
	}
	variable, items, ok = view.projectionView()
	if !ok {
		return braceView{}, nil, false
	}
	name := strings.TrimSpace(variable.text())
	if policy == mapProjectionRuntime {
		if !cyphertext.IsIdentifier(name) && !isBacktickQuotedName(name) {
			return braceView{}, nil, false
		}
	} else {
		name = simpleSemanticIdentifier(name)
		_, subqueryName := staticSubqueryExpressionTypes[upperASCII(name)]
		if name == "" || isCypherKeyword(name) && !subqueryName && !(scope != nil && scope.bound(name)) {
			return braceView{}, nil, false
		}
	}
	return variable, items, true
}

func rowMapProjectionSplit(view braceView) (braceView, []recordedMapEntry, bool) {
	return mapProjectionSplit(view, mapProjectionRuntime, nil)
}

func (e *StorageExecutor) evaluateRowMapProjection(view braceView, values map[string]interface{}) (interface{}, bool, bool, error) {
	variable, entries, projection := mapProjectionSplit(view, mapProjectionRuntime, nil)
	if !projection {
		return nil, false, false, nil
	}
	if err := view.admission(); err != nil {
		return nil, true, false, err
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
	if len(entries) == 0 {
		return result, true, true, nil
	}
	// Entries use the brace syntax owner's entry grammar, as the statement's
	// syntax check does: trivia between their tokens changes nothing.
	for _, item := range entries {
		kind, name, value := item.record.Kind, item.name, item.value
		switch kind {
		case wildcardBraceEntry:
			if temporal {
				return nil, true, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
					localization.CypherCoreMapProjectionCoercion(formatCypherValueString(base)))
			}
			for name, value := range properties {
				result[name] = value
			}
		case propertyBraceEntry:
			name = normalizePropertyKey(name)
			if !temporal {
				result[name] = properties[name]
				continue
			}
			value, _, supported := evaluateTemporalProperty(base, name)
			if !supported {
				return nil, true, false, temporalNoSuchFieldError(name)
			}
			result[name] = value
		case keyedBraceEntry:
			evaluated, ok, err := e.evaluateRowValue(value, values)
			if err != nil {
				return nil, true, false, err
			}
			if !ok {
				return nil, true, false, nil
			}
			result[normalizePropertyKey(name)] = evaluated
		case variableBraceEntry:
			key := normalizePropertyKey(name)
			bound, ok := values[key]
			if !ok {
				return nil, true, false, nil
			}
			result[key] = cypherfn.RuntimeValue(bound)
		default:
			return nil, true, false, nil
		}
	}
	return result, true, true, nil
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
