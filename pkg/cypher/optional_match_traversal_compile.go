package cypher

// Per-item projection pre-compilation for the traversal-seeded OPTIONAL MATCH
// pipeline (see optional_match_traversal.go), following the compiled-WHERE
// precedent in binding_where_compile.go: parse each RETURN item once, then
// evaluate the compiled form per row instead of re-walking the string
// evaluator's dispatch chain for every row.
//
// Compilation is a pure optimization and MUST NOT change behavior. Every
// compiled shape replicates the exact branch the full evaluator
// (evaluateExpressionWithContextFull) would take for that expression:
//
//   - literals reproduce evaluateExpressionFastLeaf (quoted strings, booleans,
//     null, int-then-float numbers),
//   - bare variables and var.prop reproduce the fast-leaf / props-literals
//     branch via fastTraversalExprValue (including has_embedding and the
//     single-"value" scalar-wrapper pseudo-node),
//   - whole-expression function calls reproduce the registered-function
//     dispatch (functions_eval_functions.go): same parseFunctionCallWS /
//     splitFunctionArgs inputs, same cypherfn.Context construction, same
//     found/err handling, and a full-evaluator fallback when the registry
//     does not know the function.
//
// A function call is only compiled when the full evaluator would provably
// reach the registered-function dispatch for it: parseFunctionCallWS must
// match the whole expression (which rules out param refs, quoted strings,
// numbers, parenthesized expressions, map literals, and trailing operators or
// indexing), hasTopLevelExpressionOperator must be false, and it must not be
// a CASE expression. Anything else — and any argument that does not compile —
// falls back to the full evaluator, preserving semantics exactly.

import (
	"context"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// compiledTraversalProjection evaluates one pre-parsed projection expression
// against a joined row.
type compiledTraversalProjection func(row traversalOptRow) interface{}

// compileTraversalProjection compiles expr into a per-row evaluator. It always
// succeeds: expressions outside the compilable subset return a closure over
// the full expression evaluator, which is the exact pre-compilation behavior.
func (e *StorageExecutor) compileTraversalProjection(ctx context.Context, expr string) compiledTraversalProjection {
	if fn, ok := e.tryCompileTraversalExpr(ctx, expr); ok {
		return fn
	}
	captured := expr
	return func(row traversalOptRow) interface{} {
		return e.evaluateExpressionWithContext(ctx, captured, row.nodes, row.rels)
	}
}

// tryCompileTraversalExpr compiles the supported expression shapes. ok=false
// means the caller must evaluate the expression with the full evaluator.
func (e *StorageExecutor) tryCompileTraversalExpr(ctx context.Context, expr string) (compiledTraversalProjection, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	// Literals — replicate evaluateExpressionFastLeaf's order and results.
	if isWholeCypherQuotedString(expr) {
		if decoded, ok := decodeCypherQuotedString(expr); ok {
			return func(traversalOptRow) interface{} { return decoded }, true
		}
	}
	if equalFoldASCII(expr, "true") {
		return func(traversalOptRow) interface{} { return true }, true
	}
	if equalFoldASCII(expr, "false") {
		return func(traversalOptRow) interface{} { return false }, true
	}
	if equalFoldASCII(expr, "null") {
		// fastLeaf declines null and the props/literals branch resolves it to
		// nil; the compiled result is identical.
		return func(traversalOptRow) interface{} { return nil }, true
	}
	if exprCanBeNumber(expr) {
		if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
			return func(traversalOptRow) interface{} { return num }, true
		}
		if num, err := strconv.ParseFloat(expr, 64); err == nil {
			return func(traversalOptRow) interface{} { return num }, true
		}
	}

	// Bare variable / var.prop — fastTraversalExprValue already replicates the
	// evaluator's semantics for these shapes; compilation pins the shape so the
	// per-row work is only map lookups. A row where the variable is unbound
	// falls back to the full evaluator, preserving pre-compilation behavior.
	if isCompilableTraversalVarExpr(expr) {
		captured := expr
		return func(row traversalOptRow) interface{} {
			if v, ok := fastTraversalExprValue(captured, row); ok {
				return v
			}
			return e.evaluateExpressionWithContext(ctx, captured, row.nodes, row.rels)
		}, true
	}

	return e.tryCompileTraversalFunctionCall(ctx, expr)
}

// isCompilableTraversalVarExpr reports whether expr is a bare identifier or a
// simple ident.ident property access — the shapes fastTraversalExprValue
// resolves.
func isCompilableTraversalVarExpr(expr string) bool {
	if dotIdx := strings.IndexByte(expr, '.'); dotIdx > 0 {
		return isSimpleTraversalIdentifier(expr[:dotIdx]) && isSimpleTraversalIdentifier(expr[dotIdx+1:])
	}
	return isSimpleTraversalIdentifier(expr)
}

// tryCompileTraversalFunctionCall compiles a whole-expression function call
// into a closure over the registered-function dispatch. Argument strings are
// pre-split once; arguments that themselves compile are evaluated through
// their compiled form when the function requests lazy evaluation via
// Context.Eval, and everything else routes through the full evaluator.
func (e *StorageExecutor) tryCompileTraversalFunctionCall(ctx context.Context, expr string) (compiledTraversalProjection, bool) {
	name, inner, ok := parseFunctionCallWS(expr)
	if !ok {
		return nil, false
	}
	// Guarantee the full evaluator would reach the registered-function
	// dispatch for this expression (see file comment for the branch analysis).
	if hasTopLevelExpressionOperator(expr) || isCaseExpression(expr) {
		return nil, false
	}

	args := e.splitFunctionArgs(inner)
	compiledArgs := make(map[string]compiledTraversalProjection, len(args))
	for _, arg := range args {
		if argFn, ok := e.tryCompileTraversalExpr(ctx, arg); ok {
			compiledArgs[strings.TrimSpace(arg)] = argFn
		}
	}

	captured := expr
	return func(row traversalOptRow) interface{} {
		fnCtx := cypherfn.Context{
			Nodes: row.nodes,
			Rels:  row.rels,
			Eval: func(argExpr string) (interface{}, error) {
				if argFn, ok := compiledArgs[strings.TrimSpace(argExpr)]; ok {
					return argFn(row), nil
				}
				return e.evaluateExpressionWithContext(ctx, argExpr, row.nodes, row.rels), nil
			},
			Now: time.Now,
		}
		if v, found, err := cypherfn.EvaluateFunction(name, args, fnCtx); found {
			if err != nil {
				return nil
			}
			return v
		}
		// Function not in the registry (e.g. length(), exists()): defer to the
		// full evaluator's legacy branches.
		return e.evaluateExpressionWithContext(ctx, captured, row.nodes, row.rels)
	}, true
}
