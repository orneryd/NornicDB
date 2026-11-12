package cypher

import (
	"context"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// evaluateRowFunction owns the row evaluator's scalar function dispatch for a
// standalone call. The call arrives as the facts the caller already parsed, so
// no form below re-reads the expression, parses it again, or looks one up. It
// reports the call's value, handled, and resolved: handled says this exit
// belongs to the call (every outcome below except the end of the dispatch),
// resolved says the call produced a value, and err is the statement error the
// call raised. Handled false with a nil err is a call no form recognizes, and
// the caller keeps reading the expression with its existing fallback.
func (e *StorageExecutor) evaluateRowFunction(operationCtx context.Context, facts functionCallFacts, values map[string]interface{}, origin rowExpressionOriginBinding) (interface{}, bool, bool, error) {
	function, argument := facts.name, facts.inner
	// Every form below reads the argument: bind where the argument starts
	// once for the whole call.
	if saved, bound := advanceRowExpressionOrigin(values, facts.argumentStart, origin); bound {
		defer restoreRowExpressionOrigin(values, saved)
	}

	temporalContext := operationCtx
	if inherited := rowStatementContext(values); inherited != nil {
		temporalContext = inherited
	}
	if value, handled, callErr := e.evaluateTemporalConstructor(temporalContext, func(inner string) (interface{}, bool, error) {
		return e.evaluateRowValue(operationCtx, inner, values)
	}, facts); handled {
		return value, true, callErr == nil, callErr
	}
	if value, matched, resolved, err := e.evaluateRowMathFunction(operationCtx, function, argument, values); matched {
		return value, true, resolved, err
	}
	if value, matched, resolved, err := e.evaluateRowExtensionFunction(operationCtx, function, argument, values); matched {
		return value, true, resolved, err
	}
	if isGDSFunctionName(lowerASCII(function)) {
		value, _, resolved, err := e.evaluateRowGraphFunction(operationCtx, function, argument, values)
		return value, true, resolved, err
	}
	switch lowerASCII(function) {
	case "exists":
		if isRelationshipPatternOperand(strings.TrimSpace(argument)) {
			ctx := operationCtx
			matched, recognized := e.evaluateRowRelationshipPattern(ctx, argument, values)
			return matched, true, recognized, getExpressionFailure(ctx)
		}
	case "graph.names", "graph.propertiesbyname", "graph.byname", "graph.byelementid":
		value, _, resolved, err := e.evaluateRowGraphFunction(operationCtx, function, argument, values)
		return value, true, resolved, err
	case "shortestpath", "allshortestpaths":
		nodes := make(map[string]*storage.Node, len(values))
		for name, value := range values {
			if node, ok := value.(*storage.Node); ok {
				nodes[name] = node
			}
		}
		search := operationCtx
		value, handled, err := e.evaluateShortestPathValue(withValueBindings(search, values), function, argument, nodes)
		return value, true, handled, err
	case "startnode", "endnode":
		if value, handled, err := e.rowRelationshipEndpoint(operationCtx, function, argument, values); handled || err != nil {
			return value, true, handled, err
		}
	case "reduce", "allreduce":
		reduced, reducedOK, reduceErr := e.evaluateRowReduce(operationCtx, function, argument, values, e.evaluateRowValue)
		return reduced, true, reducedOK, reduceErr
	case "coalesce":
		// EXISTS subqueries are non-null booleans and end coalesce
		// immediately.
		operands := splitTopLevelComma(argument)
		coalesced, resolved, coalesceErr := cypherfn.Coalesce(len(operands), func(index int) (interface{}, bool, error) {
			trimmed := strings.TrimSpace(operands[index])
			return e.evaluateRowValue(operationCtx, trimmed, values)
		})
		return coalesced, true, resolved, coalesceErr
	case "length":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		if value == nil || nilRemotePath(value) {
			return nil, true, true, nil
		}
		if remote, ok := value.(storage.RemotePathValue); ok {
			result, err := remotePathFunctionValue("length", remote)
			return result, true, err == nil, err
		}
		object, isMap := toStringAnyMap(value)
		if path, ok := cypherPathResultOf(value, object, isMap); ok {
			return int64(path.Length), true, true, nil
		}
		return nil, true, false, localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
			localization.CypherCoreFunctionArgumentInvalid("length", "a Path", neo4jValueRepr(value)))
	case "range":
		argumentOrigin := rowExpressionOrigin(values)
		var parts rowValuePartList
		if argumentOrigin.had {
			var buffer [8]expressionFragment
			parts.spans = appendExpressionParts(buffer[:0], argument, false)
		} else {
			parts.texts = e.splitFunctionArgs(argument)
		}
		arguments := make([]interface{}, parts.count())
		for index := range arguments {
			part, start := parts.at(index)
			saved, bound := advanceRowExpressionOrigin(values, start, argumentOrigin)
			value, resolved, err := e.evaluateRowValue(operationCtx, strings.TrimSpace(part), values)
			if bound {
				restoreRowExpressionOrigin(values, saved)
			}
			if err != nil {
				return nil, true, false, err
			}
			if !resolved {
				return nil, true, false, nil
			}
			arguments[index] = value
		}
		result, err := evaluateCypherRange(arguments)
		if err != nil {
			return nil, true, false, err
		}
		return result, true, true, nil
	case "head", "last", "reverse", "size":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		if strings.EqualFold(function, "size") {
			size, sizeOK, sizeErr := evaluateCypherSize(value)
			return size, true, sizeOK, sizeErr
		}
		result, err := evaluateListAccessFunction(lowerASCII(function), value)
		if err != nil {
			return nil, true, false, err
		}
		return result, true, true, nil
	case "nodes", "relationships":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		if value == nil || nilRemotePath(value) {
			return nil, true, true, nil
		}
		if remote, ok := value.(storage.RemotePathValue); ok {
			result, err := remotePathFunctionValue(function, remote)
			return result, true, err == nil, err
		}
		path, isPath := toStringAnyMap(value)
		if !isPath {
			return nil, true, false, nil
		}
		nodes, relationships, hasNodes, hasRelationships := pathValueParts(path)
		if strings.EqualFold(function, "relationships") {
			return relationships, true, hasRelationships, nil
		}
		return nodes, true, hasNodes, nil
	case "properties":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		if value == nil {
			return nil, true, true, nil
		}
		switch entity := value.(type) {
		case *storage.Node:
			if entity == nil {
				return nil, true, true, nil
			}
			return cypherfn.EntityProperties(e.queryEntityProperties(entity)), true, true, nil
		case *storage.Edge:
			if entity == nil {
				return nil, true, true, nil
			}
			return cypherfn.EntityProperties(e.queryEntityProperties(entity)), true, true, nil
		case storage.RemoteEntityValue:
			properties, _ := entity["properties"].(map[string]interface{})
			return cypherfn.EntityProperties(properties), true, true, nil
		default:
			object, isMap := toStringAnyMap(value)
			return object, true, isMap, nil
		}
	case "keys":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		switch entity := value.(type) {
		case nil:
			return nil, true, true, nil
		case *storage.Node:
			if entity == nil {
				return nil, true, true, nil
			}
		case *storage.Edge:
			if entity == nil {
				return nil, true, true, nil
			}
		}
		if object, isMap := toStringAnyMap(value); isMap {
			value = object
		}
		object, isMap := toStringAnyMap(value)
		if _, isPath := cypherPathResultOf(value, object, isMap); isPath {
			return nil, true, false, invalidFunctionArgument("keys", value)
		}
		keys, ok := cypherfn.PropertyKeys(e.queryVisibleValue(value))
		if !ok {
			return nil, true, false, invalidFunctionArgument("keys", value)
		}
		return keys, true, true, nil
	case "labels":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		result, err := cypherfn.Labels(value)
		if err != nil {
			return nil, true, false, functionEvaluationError(err)
		}
		return result, true, true, nil
	case "type":
		value, resolved, err := e.evaluateRowValue(operationCtx, argument, values)
		if err != nil {
			return nil, true, false, err
		}
		if !resolved {
			return nil, true, false, nil
		}
		typed, typedOK, typedErr := rowTypeOf(value)
		return typed, true, typedOK, typedErr
	}
	// Registered functions evaluate arguments in this row and statement's
	// context, including nested graph expressions, just like specialized calls.
	if value, handled, resolved, err := e.evaluateRowGraphFunction(operationCtx, function, argument, values); handled {
		return value, true, resolved, err
	}
	return nil, false, false, nil
}
