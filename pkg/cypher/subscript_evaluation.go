package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateCypherSubscript owns receiver/index evaluation order and subscript
// value checks for row, compiled fallback and context evaluators. Even a null
// receiver evaluates its index; an index error must not disappear into null.
func (e *StorageExecutor) evaluateCypherSubscript(baseExpression, subscript string, evaluate func(string) (interface{}, bool, error)) (interface{}, bool, error) {
	base, ok, err := evaluate(baseExpression)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if from, to, slice := staticSliceBounds(subscript); slice {
		return evaluateCypherListSlice(base, strings.TrimSpace(from), strings.TrimSpace(to), evaluate)
	}
	indexValue, ok, err := evaluate(subscript)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	if base == nil || indexValue == nil {
		return nil, true, nil
	}
	_, isNode := base.(*storage.Node)
	_, isRelationship := base.(*storage.Edge)
	_, isRemote := base.(storage.RemoteEntityValue)
	if _, isObject := toStringAnyMap(base); isObject || isNode || isRelationship || isRemote {
		// A dynamic property access: n[key] is n.key.
		key, isString := indexValue.(string)
		if !isString {
			return nil, false, mapElementAccessByNonStringError(indexValue)
		}
		value, ok := rowPropertyValue(e.queryVisibleValue(base), key)
		return value, ok, nil
	}
	if key, isKey := indexValue.(string); isKey && (isRuntimeTemporal(base) || isRuntimeDuration(base)) {
		// d['year'] reads the field d.year does.
		value, _, supported := evaluateTemporalProperty(base, key)
		if !supported {
			return nil, false, temporalNoSuchFieldError(key)
		}
		return value, true, nil
	}
	// Only a list is indexed by an integer: n.big[0] on a number is a
	// TypeError wherever it is read, a WHERE included (#907).
	items, isList := cypherListValue(base)
	if !isList {
		return nil, false, subscriptReceiverError(base, indexValue)
	}
	index, ok := rowSubscriptIndex(indexValue)
	if !ok {
		return nil, false, listSubscriptIndexError(base, indexValue)
	}
	if index < 0 {
		index += len(items)
	}
	if index < 0 || index >= len(items) {
		return nil, true, nil
	}
	return items[index], true, nil
}

func listSubscriptIndexError(base, index interface{}) error {
	if isRuntimeNumber(index) {
		return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", localization.CypherCoreListIndexNonInteger(neo4jValueRepr(nil, index)))
	}
	// Indexing reads a list value, including the context literal reader's
	// homogeneous Go slices; render its items as Neo4j's virtual List.
	items, _ := cypherListValue(base)
	parts := make([]string, len(items))
	for i, item := range items {
		parts[i] = neo4jValueRepr(nil, item)
	}
	list, value := "List{"+strings.Join(parts, ", ")+"}", neo4jValueRepr(nil, index)
	message := localization.CypherCoreListIndexNonNumber(list, value)
	if boolean, ok := index.(bool); ok {
		message = localization.CypherCoreListIndexBoolean(list, value, boolean)
	}
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", message)
}

func evaluateCypherListSlice(base interface{}, lowerExpression, upperExpression string, evaluate func(string) (interface{}, bool, error)) (interface{}, bool, error) {
	bound := func(expression string) (interface{}, bool, error) {
		if expression == "" {
			return nil, true, nil
		}
		return evaluate(expression)
	}
	lower, ok, err := bound(lowerExpression)
	if err != nil || !ok {
		return nil, false, err
	}
	upper, ok, err := bound(upperExpression)
	if err != nil || !ok {
		return nil, false, err
	}
	// Evaluate both bounds before null propagation so arithmetic failures
	// remain visible, but a null slice needs no runtime bound-type check.
	if base == nil || lowerExpression != "" && lower == nil || upperExpression != "" && upper == nil {
		return nil, true, nil
	}
	for _, value := range []interface{}{lower, upper} {
		if value != nil && !isCypherInteger(value) {
			return nil, false, invalidSubscriptTypeError("list slice bound requires an INTEGER", value)
		}
	}
	value, ok := cypherListSlice(base, lower, upper, lowerExpression != "", upperExpression != "")
	return value, ok, nil
}
