package cypher

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) orderNodes(nodes []*storage.Node, variable, orderExpr string) []*storage.Node {
	if len(nodes) <= 1 {
		return nodes
	}

	// Parse multiple ORDER BY columns: "n.value ASC, n.name DESC"
	specs := e.parseNodeOrderSpecs(orderExpr, variable)
	if len(specs) == 0 {
		return nodes
	}

	sorted := make([]*storage.Node, len(nodes))
	copy(sorted, nodes)

	sort.Slice(sorted, func(i, j int) bool {
		for _, spec := range specs {
			val1, _ := sorted[i].Properties[spec.propName]
			val2, _ := sorted[j].Properties[spec.propName]

			cmp := e.compareOrderValues(val1, val2)
			if cmp != 0 {
				if spec.descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false // All equal
	})

	return sorted
}

// parseNodeOrderSpecs parses "n.value ASC, n.name DESC" for node sorting
func (e *StorageExecutor) parseNodeOrderSpecs(orderExpr, variable string) []nodeOrderSpec {
	var specs []nodeOrderSpec

	// Split by comma
	parts := splitOutsideParens(orderExpr, ',')

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		tokens := strings.Fields(part)
		if len(tokens) == 0 {
			continue
		}

		expr := tokens[0]
		descending := len(tokens) > 1 && upperASCII(tokens[1]) == "DESC"

		// Node sorting is safe only for direct properties of the matched
		// variable. Aliases and computed expressions must be sorted after
		// projection, when their values actually exist.
		prefix := variable + "."
		if !strings.HasPrefix(expr, prefix) {
			return nil
		}
		propName := expr[len(prefix):]
		if propName == "" || strings.ContainsAny(propName, ".()[]") {
			return nil
		}

		specs = append(specs, nodeOrderSpec{propName: propName, descending: descending})
	}

	return specs
}

// evaluateWhereOnComputedRow is the computed-row entry into the shared row
// predicate evaluator: the post-WITH WHERE position evaluates identically to
// every other WHERE position (null drops the row, AND/OR precedence and IS
// NULL forms come from the one owner) instead of a local text splitter.
func (e *StorageExecutor) evaluateWhereOnComputedRow(ctx context.Context, whereClause string, values map[string]interface{}) bool {
	return e.evaluateRowPredicate(ctx, whereClause, values)
}

// evaluateExpressionFromValues evaluates an expression over a computed values
// map (CALL projections, UNWIND rows, YIELD values, SET expressions, collect
// transforms). The values scope is carried as context bindings and evaluation
// runs through the shared evaluator, so property access, keys(), properties(),
// labels(), type checks, temporal constructors and function dispatch see the
// real values. Node/relationship values are additionally mirrored into the
// entity scopes so legacy handlers that resolve variables there keep working.
//
// The terminal contract is unchanged from the legacy evaluator: expression
// text the shared evaluator does not recognize round-trips to the caller, so
// callers can tell "unrecognized" apart from "evaluated to null" and raise the
// proper statement error (or route to the EXISTS-subquery machinery).
func (e *StorageExecutor) evaluateExpressionFromValues(expr string, values map[string]interface{}) interface{} {
	return e.evaluateExpressionFromValuesContext(context.Background(), expr, values)
}

// evaluateRowFallback is the row evaluator's fallback to the shared
// evaluator. err is the error a function raised there (a registry function's
// argument error), which the shared evaluator records as the statement error
// rather than returning it; the row evaluator reports the expression as
// unresolved and evaluateRowExpressionWithContext records err.
func (e *StorageExecutor) evaluateRowFallback(expr string, values map[string]interface{}) (interface{}, error) {
	ctx := context.WithValue(context.Background(), expressionFailureKey{}, &expressionFailure{})
	// Keep the statement's clock for the functions that read it (decay, #866).
	if statement, ok := values[temporalRowContextKey].(context.Context); ok {
		if instant, ok := statement.Value(temporalStatementTimeKey{}).(time.Time); ok {
			ctx = context.WithValue(ctx, temporalStatementTimeKey{}, instant)
		}
	}
	// And the statement's Cypher version, which the row carries.
	if rowIsCypher25(values) {
		ctx = context.WithValue(ctx, cypherVersionKey{}, "25")
	}
	value := e.evaluateExpressionFromValuesContext(ctx, expr, values)
	return value, getExpressionFailure(ctx)
}

// evaluateExpressionFromValuesContext is evaluateExpressionFromValues with
// the evaluation context: expression failures are recorded on ctx.
func (e *StorageExecutor) evaluateExpressionFromValuesContext(ctx context.Context, expr string, values map[string]interface{}) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	if isCaseExpression(expr) {
		return e.evaluateCaseExpressionFromValues(expr, values)
	}

	// Direct lookup: callers store pre-computed values under the expression
	// text itself (call-tail plans set values["max(length(p))"] per row), and
	// plain variable references are the hot path.
	if val, ok := values[expr]; ok {
		return val
	}

	// Legacy FromValues precedence for a node projected as a map: a
	// "properties" sub-map wins over a same-named top-level key.
	if dot := strings.IndexByte(expr, '.'); dot > 0 {
		if base, ok := values[expr[:dot]].(map[string]interface{}); ok {
			if props, propsOK := base["properties"].(map[string]interface{}); propsOK {
				if property, propertyOK := props[expr[dot+1:]]; propertyOK {
					return property
				}
			}
		}
	}

	// Relationship-pattern fragments are not scalar expressions: leave them
	// for the pattern machinery (WHERE exists-patterns and friends). The
	// shared recognition heuristics would otherwise misread "--" as
	// arithmetic operators and a lone ">()" arrow fragment as a comparison,
	// evaluating the fragment to null instead of round-tripping it.
	if looksLikeRowRelationshipPattern(expr) && !strings.ContainsAny(expr, "'\"") {
		return expr
	}
	if len(expr) > 0 && (expr[0] == '>' || expr[0] == '<') {
		return expr
	}

	ctx = withValueBindings(ctx, values)
	nodes, rels := entityScopesFromValues(values)
	if value, recognized := e.evaluateExpressionWithContextDefined(ctx, expr, nodes, rels); recognized {
		return value
	}
	// The shared recognition heuristics do not know about value bindings; a
	// bound-map property access is evaluable even when the plain node/rel
	// scopes are empty.
	if bindings := valueBindingsFromContext(ctx); bindings != nil {
		if dot := strings.IndexByte(expr, '.'); dot > 0 {
			if _, ok := bindings[expr[:dot]]; ok {
				return e.evaluateExpressionWithContextFull(ctx, expr, nodes, rels, nil, nil, nil, 0)
			}
		}
	}
	return expr
}

// entityScopesFromValues extracts the *storage.Node and *storage.Edge entries of
// a computed values map into node/relationship scopes for the shared evaluator.
// Rows without entities keep nil scopes, so the common scalar row path stays
// allocation-free.
func entityScopesFromValues(values map[string]interface{}) (map[string]*storage.Node, map[string]*storage.Edge) {
	var nodes map[string]*storage.Node
	var rels map[string]*storage.Edge
	for name, val := range values {
		switch entity := val.(type) {
		case *storage.Node:
			if entity == nil {
				continue
			}
			if nodes == nil {
				nodes = make(map[string]*storage.Node, 1)
			}
			nodes[name] = entity
		case *storage.Edge:
			if entity == nil {
				continue
			}
			if rels == nil {
				rels = make(map[string]*storage.Edge, 1)
			}
			rels[name] = entity
		}
	}
	return nodes, rels
}

func (e *StorageExecutor) evaluateCaseExpressionFromValues(expr string, values map[string]interface{}) interface{} {
	// The shared CASE evaluator resolves non-entity variables through the
	// context value bindings, so the values scope is passed the same way as
	// every other scope instead of using a parallel evaluator.
	return e.evaluateCaseExpression(withValueBindings(context.Background(), values), expr, nil, nil, nil, nil, nil, 0)
}

// evaluateConditionFromValues evaluates a boolean condition over a computed
// values scope. It delegates to the shared condition evaluator with the values
// carried as context bindings, mirroring how the main expression evaluator
// resolves non-entity variables.
func (e *StorageExecutor) evaluateConditionFromValues(condition string, values map[string]interface{}) bool {
	return e.evaluateCondition(withValueBindings(context.Background(), values), condition, nil, nil)
}

func parseLiteralValueFromComputedRow(expr string) (interface{}, bool) {
	return parseLiteralScalarForPipeline(expr)
}

// evaluateMapLiteralFromValues evaluates a map literal using computed values
func (e *StorageExecutor) evaluateMapLiteralFromValues(expr string, values map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "{") || !strings.HasSuffix(expr, "}") {
		return result
	}

	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	if inner == "" {
		return result
	}

	// Split by commas, respecting nesting
	pairs := splitTopLevelComma(inner)

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Find the first colon (key: value)
		colonIdx := strings.Index(pair, ":")
		if colonIdx == -1 {
			continue
		}

		key := strings.TrimSpace(pair[:colonIdx])
		valueExpr := strings.TrimSpace(pair[colonIdx+1:])

		// Evaluate the value expression using the values map
		value := e.evaluateExpressionFromValues(valueExpr, values)
		result[key] = value
	}

	return result
}

// executeMatchWithClause handles MATCH ... WHERE ... WITH ... RETURN queries
// This processes computed values (like CASE WHEN) in the WITH clause
// and handles aggregation with implicit GROUP BY
