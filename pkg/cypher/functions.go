// Cypher function implementations for NornicDB.
//
// This file holds the public expression-evaluation entrypoints. The heavy
// implementation is split across `functions_eval_part*.go`.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// PluginFunctionInfo describes a loaded plugin's function for SHOW FUNCTIONS.
type PluginFunctionInfo struct {
	Name        string
	Category    string
	Description string
	Signature   string
}

// PluginFunctionList lists the loaded plugins' functions for SHOW FUNCTIONS
// (#1168); nornicdb.Open sets it, with PluginFunctionLookup. Each entry's
// Name is the name a statement calls (case-sensitive), and Signature is in
// SHOW FUNCTIONS' form, from which the argument and return descriptions are
// read.
//
// Example:
//
//	cypher.PluginFunctionList = func() []cypher.PluginFunctionInfo {
//		return []cypher.PluginFunctionInfo{{
//			Name:        "acme.text.slug",
//			Category:    "text",
//			Description: "Returns the text as a URL slug.",
//			Signature:   "acme.text.slug(text :: STRING) :: STRING",
//		}}
//	}
var PluginFunctionList func() []PluginFunctionInfo

// PluginFunctionLookup is a callback to look up functions from loaded plugins.
// Set by pkg/nornicdb during database initialization.
// Returns the function handler and true if found, nil and false otherwise.
// PluginFunctionHandler is an admitted callable. Raw Go functions must pass
// registry.Compile before lookup publication. Author: Codex NOR-184 lane.
type PluginFunctionHandler = registry.Callable

var PluginFunctionLookup func(name string) (handler *PluginFunctionHandler, found bool)

func isFunctionCall(expr, funcName string) bool {
	return inspectFunctionCall(expr, namedFunctionCall, funcName, nil)
}

// evaluateExpression evaluates an expression for a single node context.
func (e *StorageExecutor) evaluateExpression(ctx context.Context, expr string, varName string, node *storage.Node) interface{} {
	return e.evaluateExpressionWithContext(ctx, expr, map[string]*storage.Node{varName: node}, nil)
}

// evaluateExpressionWithPathContext evaluates an expression with full path context.
func (e *StorageExecutor) evaluateExpressionWithPathContext(ctx context.Context, expr string, pathCtx PathContext) interface{} {
	return e.evaluateExpressionWithContextFull(ctx, expr, pathCtx.nodes, pathCtx.rels, pathCtx.paths, pathCtx.allPathEdges, pathCtx.allPathNodes, pathCtx.pathLength)
}

func (e *StorageExecutor) evaluateExpressionWithContext(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContextFull(ctx, expr, nodes, rels, nil, nil, nil, 0)
}

// evaluateExpressionWithContextDefined distinguishes a valid Cypher null from
// an expression that this evaluator did not recognize. Callers that project
// values must preserve that distinction instead of replacing null with the
// original expression text or another fallback value.
func (e *StorageExecutor) evaluateExpressionWithContextDefined(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, bool) {
	value, resolved, err := e.evaluateContextExpressionResult(ctx, expr, nodes, rels)
	recordExpressionFailure(ctx, err)
	return value, resolved
}

// Context dispatch's handled flag selects an implementation. Resolution belongs
// to this result boundary; errors cannot be presented as successful NULL values.
func (e *StorageExecutor) evaluateContextExpressionResult(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (interface{}, bool, error) {
	ctx = withExpressionFailureSlot(ctx)
	if strings.TrimSpace(expr) == "" {
		return nil, false, nil
	}
	value := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
	err := getExpressionFailure(ctx)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (e *StorageExecutor) evaluateExpressionWithContextFull(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) (result interface{}) {
	defer func() { result = cypherfn.RuntimeValue(result) }()
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	// shortestPath(...) / allShortestPaths(...) as a value: cheap prefix guard
	// keeps the hot path allocation-free, then the shared BFS machinery runs
	// once for this row.
	if len(expr) >= len("shortestPath") {
		head := expr
		if len(head) > len("allShortestPaths") {
			head = head[:len("allShortestPaths")]
		}
		if equalFoldASCII(head[:len("shortestPath")], "shortestpath") || equalFoldASCII(head, "allshortestpaths") {
			var callFacts1 functionCallFacts
			if ok := inspectFunctionCall(expr, qualifiedFunctionCall, "", &callFacts1); ok &&
				(strings.EqualFold(callFacts1.name, "shortestPath") || strings.EqualFold(callFacts1.name, "allShortestPaths")) {
				name, inner := callFacts1.name, callFacts1.inner

				if value, handled, err := e.evaluateShortestPathValue(ctx, name, inner, nodes); handled {
					if err != nil {
						recordExpressionFailure(ctx, err)
					}
					return value
				}
			}
		}
	}
	if expr[len(expr)-1] == '}' && !handedBackByRowEvaluator(ctx, expr) {
		// A map projection (o {.s}, n {.*, k: 1}) has one implementation, the
		// row evaluator's (evaluateRowMapProjection), whatever evaluates it:
		// SET n = o {.s} among others (#907).
		if _, _, projection := mapProjectionSplit(expr, mapProjectionRuntime, nil); projection {
			value, _ := e.checkedPropertyExpression(ctx, expr, entityRow(ctx, nodes, rels))
			return value
		}
	}
	if mayContainSubqueryExpression(expr) && !handedBackByRowEvaluator(ctx, expr) && len(findSubqueryExpressions(expr)) > 0 {
		// A subquery expression, alone or in a larger expression, is
		// evaluated by the row evaluator where it stands, with this row's
		// entities, paths and values (#1055).
		value, _ := e.checkedPropertyExpression(ctx, expr, e.entityRowWithPaths(ctx, nodes, rels, paths))
		return value
	}
	if pattern, projection, ok := splitPatternComprehension(expr); ok {
		return e.evaluatePatternComprehension(ctx, pattern, projection, nodes, rels)
	}
	if open := rowSubscriptStart(expr); open > 0 {
		value, _, err := e.evaluateCypherSubscript(expr[:open], strings.TrimSpace(expr[open+1:len(expr)-1]), func(part string) (interface{}, bool, error) {
			value := e.evaluateExpressionWithContextFull(ctx, part, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
			return value, true, getExpressionFailure(ctx)
		})
		if err != nil {
			recordExpressionFailure(ctx, err)
		}
		return value
	}

	// Direct $param resolution preserves declared types end-to-end.
	// substituteParams's type-preserving short-circuit leaves "$name" as
	// a literal here for composite values; without this branch the
	// generic expression evaluator returns the unresolved string. With
	// it, []string / []float64 / map[string]any params keep their
	// declared shape through reduce(), list comprehensions, SET, and
	// every other expression context.
	if v, ok := resolveValueReference(ctx, expr, parameterName); ok {
		return v
	}
	// Resolve dotted/bracketed parameter map paths like $d.uuid and
	// $d['uuid'] as typed values.
	if v, ok := resolveValueReference(ctx, expr, parameterPath); ok {
		return normalizeExpressionValue(v)
	}
	if baseExpr, property, ok := splitPostfixPropertyAccess(expr); ok {
		base := e.evaluateExpressionWithContextFull(ctx, baseExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		switch value := base.(type) {
		case map[string]interface{}:
			return value[property]
		case *storage.Node:
			if value != nil {
				propertyValue, _ := getNodePropertyValue(e.queryVisibleValue(value).(*storage.Node), property)
				return propertyValue
			}
			return nil
		case *storage.Edge:
			if value != nil {
				return e.queryEntityProperties(value)[property]
			}
			return nil
		}
	}
	if v, ok := e.evaluateExpressionFastLeaf(ctx, expr, nodes, rels, paths); ok {
		return v
	}
	if hasTopLevelExpressionOperator(expr) {
		// The operator scanners are CASE-unaware (keeping the hot scan cheap):
		// a CASE nested in a compound expression would have the `>` of its WHEN
		// conditions misread as a top-level comparison. Evaluate the CASE
		// blocks once and substitute them as literals, then run the normal
		// operator pipeline.
		if spans := caseBlockSpans(expr); len(spans) > 0 {
			return e.evaluateExpressionWithCASESubstituted(ctx, expr, spans, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		}
		return e.evaluateExpressionWithContextFullOperators(ctx, expr, "", nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
	}
	return e.evaluateExpressionWithContextFullFunctions(ctx, expr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

// evaluateExpressionWithCASESubstituted evaluates the CASE … END blocks of expr
// (their spans come from caseBlockSpans) and substitutes each with its literal
// value before running the shared evaluator over the remainder. It is the
// CASE-aware complement of the allocation-conscious operator scanners.
func (e *StorageExecutor) evaluateExpressionWithCASESubstituted(ctx context.Context, expr string, spans []caseBlockSpan, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult, allPathEdges []*storage.Edge, allPathNodes []*storage.Node, pathLength int) interface{} {
	var builder strings.Builder
	builder.Grow(len(expr))
	last := 0
	boundCtx := ctx
	for _, span := range spans {
		ctx = boundCtx
		builder.WriteString(expr[last:span.start])
		value := e.evaluateCaseExpression(ctx, expr[span.start:span.end], nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
		ctx, reference := withExpressionBinding(ctx, expr, value)
		builder.WriteString(reference)
		// Carry this binding to subsequent blocks and the final evaluation.
		boundCtx = ctx
		last = span.end
	}
	builder.WriteString(expr[last:])
	return e.evaluateExpressionWithContextFull(boundCtx, builder.String(), nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func splitPostfixPropertyAccess(expr string) (string, string, bool) {
	lastDot := -1
	for offset := 0; offset < len(expr); {
		dot := findTopLevelOperator(expr[offset:], ".", false, true)
		if dot < 0 {
			break
		}
		lastDot = offset + dot
		offset = lastDot + 1
	}
	if lastDot <= 0 || lastDot == len(expr)-1 {
		return "", "", false
	}
	property := strings.TrimSpace(expr[lastDot+1:])
	if isBacktickQuotedName(property) {
		property = normalizePropertyKey(property)
	} else if !cyphertext.IsIdentifier(property) {
		return "", "", false
	}
	base := strings.TrimSpace(expr[:lastDot])
	// Property access binds tighter than any operator: in 'x' + o.missing the
	// access is o.missing, not ('x' + o).missing. A base with a top-level
	// operator is not a postfix access.
	if !isSimpleIdentifierOrProperty(base) && hasTopLevelExpressionOperator(base) {
		return "", "", false
	}
	return base, property, true
}

func isSimpleIdentifierOrProperty(expr string) bool {
	if expr == "" {
		return false
	}
	expectIdentStart := true
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch {
		case c == '.':
			if expectIdentStart {
				return false
			}
			expectIdentStart = true
		case cyphertext.NameStartAt(expr, i):
			expectIdentStart = false
		case cyphertext.IsDigit(c):
			if expectIdentStart {
				return false
			}
		default:
			return false
		}
	}
	return !expectIdentStart
}

func (e *StorageExecutor) evaluateExpressionFastLeaf(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge, paths map[string]*PathResult) (interface{}, bool) {
	if isWholeCypherQuotedString(expr) {
		if decoded, ok := cyphertext.DecodeString(expr, cyphertext.AcceptedSource); ok {
			return decoded, true
		}
	}

	if value, literal := literalKeywordValue(expr); literal {
		return value, true
	}

	if numericExpressionAccepted(expr, numericValuePrefix) {
		if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
			return num, true
		}
		if num, err := strconv.ParseFloat(expr, 64); err == nil {
			return num, true
		}
	}

	if !isSimpleIdentifierOrProperty(expr) {
		return nil, false
	}

	if dotIdx := strings.IndexByte(expr, '.'); dotIdx > 0 {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]

		if node, ok := nodes[varName]; ok {
			if node == nil {
				return nil, true
			}
			if val, ok := getNodePropertyValue(e.queryVisibleValue(node).(*storage.Node), propName); ok {
				return val, true
			}
			return nil, true
		}
		if rel, ok := rels[varName]; ok {
			if rel == nil {
				return nil, true
			}
			if val, ok := e.queryEntityProperties(rel)[propName]; ok {
				return val, true
			}
			return nil, true
		}
		if val, ok := e.boundValue(ctx, varName); ok {
			switch v := val.(type) {
			case nil:
				return nil, true
			case map[string]interface{}:
				if propVal, exists := v[propName]; exists {
					return propVal, true
				}
				return nil, true
			case *storage.Node:
				if propVal, exists := e.queryEntityProperties(v)[propName]; exists {
					return propVal, true
				}
				return nil, true
			}
		}
		return nil, false
	}

	if node, ok := nodes[expr]; ok {
		if node == nil {
			return nil, true
		}
		return node, true
	}
	if rel, ok := rels[expr]; ok {
		if rel == nil {
			return nil, true
		}
		return rel, true
	}
	if val, ok := e.boundValue(ctx, expr); ok {
		return val, true
	}
	if paths != nil {
		if pathResult, ok := paths[expr]; ok && pathResult != nil {
			return pathContextEntryValue(pathResult), true
		}
	}

	return nil, false
}
