package cypher

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// Files in the function/expression reader boundary, including mixed files
// whose clause/operator grammars retain the explicitly audited exemptions.
// The shared owner is inspected too; only its token/trivia implementation is
// exempt from private trivia, never from backward query-text reading.
var forwardReaderBoundary = map[string]bool{
	"function_syntax.go": true, "expression_lexical_facts.go": true,
	"expression_lexical_view.go": true, "quantifier_semantic_validation.go": true,
	"runtime_type_errors.go": true, "query_patterns.go": true,
	"expression_validation.go": true, "traversal_endpoint_analysis.go": true, "row_subscript_validation.go": true,
	"keyword_scan.go": true, "row_expression.go": true, "row_value_program.go": true,
	"list_literal.go": true, "match_index_seek.go": true, "executor_subqueries.go": true,
	"row_map_projection.go": true, "static_operator_types.go": true,
	"function_match.go": true, "functions_helpers.go": true, "functions_eval_functions.go": true,
	"function_aliases.go": true, "procedure_call_parsing.go": true,
	"function_semantic_validation.go": true, "static_function_argument_types.go": true,
	"static_function_literal_arguments.go": true,
}

// The shape depends on the query input's type and the loop's operations, not
// names like open, a particular helper, or provenance from an opening offset.
func forwardReaderShapes(node ast.Node) []string {
	inputs := map[string]bool{}
	if function, ok := node.(*ast.FuncDecl); ok {
		for _, field := range function.Type.Params.List {
			query := false
			switch kind := field.Type.(type) {
			case *ast.Ident:
				query = kind.Name == "string"
			case *ast.ArrayType:
				if element, ok := kind.Elt.(*ast.Ident); ok {
					query = element.Name == "byte" || element.Name == "rune"
				}
			}
			if query {
				for _, name := range field.Names {
					inputs[name.Name] = true
				}
			}
		}
	}
	contains := func(node ast.Node, names map[string]bool) bool {
		found := false
		if node == nil {
			return false
		}
		ast.Inspect(node, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && names[id.Name] {
				found = true
			}
			return true
		})
		return found
	}
	// Follow local aliases, slices and byte conversions of the query input.
	for changed := true; changed; {
		changed = false
		ast.Inspect(node, func(n ast.Node) bool {
			if a, ok := n.(*ast.AssignStmt); ok && len(a.Lhs) == len(a.Rhs) {
				for i, left := range a.Lhs {
					if id, ok := left.(*ast.Ident); ok && !inputs[id.Name] && contains(a.Rhs[i], inputs) {
						// Offsets obtained from a reader are integers, not text aliases.
						switch a.Rhs[i].(type) {
						case *ast.Ident, *ast.SliceExpr, *ast.IndexExpr:
							inputs[id.Name] = true
							changed = true
						}
						if call, ok := a.Rhs[i].(*ast.CallExpr); ok {
							textCall := false
							switch name := call.Fun.(type) {
							case *ast.ArrayType:
								textCall = true
							case *ast.SelectorExpr:
								textCall = name.Sel.Name == "TrimSpace" || name.Sel.Name == "TrimPrefix" || name.Sel.Name == "ToUpper" || name.Sel.Name == "ToLower"
							case *ast.Ident:
								textCall = name.Name == "lexicalExpressionText" || name.Name == "trimQueryTrivia" || name.Name == "upperASCII" || name.Name == "lowerASCII" || name.Name == "StripComments" || name.Name == "string"
							}
							if textCall {
								inputs[id.Name] = true
								changed = true
							}
						}
					}
				}
			}
			return true
		})
	}
	reasons := map[string]bool{}
	ast.Inspect(node, func(n ast.Node) bool {
		var loop ast.Node
		var condition ast.Node
		rangeInput := false
		switch candidate := n.(type) {
		case *ast.ForStmt:
			loop, condition = candidate, candidate.Cond
		case *ast.RangeStmt:
			loop = candidate
			rangeInput = contains(candidate.X, inputs)
			if rangeInput {
				if value, ok := candidate.Value.(*ast.Ident); ok {
					inputs[value.Name] = true
				}
			}
		default:
			return true
		}
		decrements := map[string]bool{}
		ast.Inspect(loop, func(n ast.Node) bool {
			switch change := n.(type) {
			case *ast.IncDecStmt:
				if id, ok := change.X.(*ast.Ident); ok && change.Tok == token.DEC {
					decrements[id.Name] = true
				}
			case *ast.AssignStmt:
				for i, left := range change.Lhs {
					if id, ok := left.(*ast.Ident); ok {
						if change.Tok == token.SUB_ASSIGN {
							decrements[id.Name] = true
						}
						if i < len(change.Rhs) {
							if expr, ok := change.Rhs[i].(*ast.BinaryExpr); ok && expr.Op == token.SUB && sameGuardIndex(expr.X, id.Name) {
								decrements[id.Name] = true
							}
						}
					}
				}
			}
			return true
		})
		queryIndexed := rangeInput
		queryIndices := map[string]bool{}
		ast.Inspect(loop, func(n ast.Node) bool {
			if sliced, ok := n.(*ast.SliceExpr); ok && contains(sliced.X, inputs) {
				queryIndexed = true
			}
			if indexed, ok := n.(*ast.IndexExpr); ok && contains(indexed.X, inputs) {
				queryIndexed = true
				ast.Inspect(indexed.Index, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok {
						queryIndices[id.Name] = true
					}
					return true
				})
				if contains(indexed.Index, decrements) {
					reasons["backward query-text index loop"] = true
				}
			}
			return true
		})
		if queryIndexed {
			if condition != nil {
				ast.Inspect(condition, func(n ast.Node) bool {
					if comparison, ok := n.(*ast.BinaryExpr); ok && comparison.Op == token.EQL {
						ast.Inspect(comparison, func(value ast.Node) bool {
							if literal, ok := value.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
								text, _ := strconv.Unquote(literal.Value)
								if strings.ContainsAny(text, " \t\n\r\f\v") {
									reasons["private trivia skip"] = true
								}
							}
							return true
						})
					}
					return true
				})
			}
			triviaTest := func(test ast.Node) bool {
				if test == nil {
					return false
				}
				found := false
				slashes, star := 0, false
				ast.Inspect(test, func(n ast.Node) bool {
					if comparison, ok := n.(*ast.BinaryExpr); ok && comparison.Op == token.EQL && contains(comparison, inputs) {
						ast.Inspect(comparison, func(value ast.Node) bool {
							if literal, ok := value.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
								text, _ := strconv.Unquote(literal.Value)
								found = found || strings.ContainsAny(text, " \t\n\r\f\v")
								if text == "/" {
									slashes++
								}
								star = star || text == "*"
							}
							return true
						})
					}
					if call, ok := n.(*ast.CallExpr); ok && len(call.Args) > 0 && contains(call.Args[0], inputs) {
						name := ""
						switch f := call.Fun.(type) {
						case *ast.Ident:
							name = f.Name
						case *ast.SelectorExpr:
							name = f.Sel.Name
						}
						found = found || name == "isASCIISpace" || name == "isASCIIWhitespace" || name == "isSpaceByte" || name == "isCypherWhitespace" || name == "IsSpace"
						if name == "HasPrefix" && len(call.Args) == 2 {
							if literal, ok := call.Args[1].(*ast.BasicLit); ok {
								text, _ := strconv.Unquote(literal.Value)
								found = found || text == "/*" || text == "//"
							}
						}
					}
					return true
				})
				return found || slashes >= 2 || slashes > 0 && star
			}
			if triviaTest(condition) {
				reasons["private trivia skip"] = true
			}
			ast.Inspect(loop, func(n ast.Node) bool {
				if branch, ok := n.(*ast.IfStmt); ok && triviaTest(branch.Cond) {
					shared := false
					ast.Inspect(branch.Body, func(n ast.Node) bool {
						if call, ok := n.(*ast.CallExpr); ok {
							if name, ok := call.Fun.(*ast.Ident); ok {
								switch name.Name {
								case "nextLexicalToken", "queryGapEnd", "queryCommentEnd", "readQueryGap", "skipCypherLexicalToken":
									shared = true
								}
							}
						}
						return true
					})
					if shared {
						return true
					}
					ast.Inspect(branch.Body, func(n ast.Node) bool {
						if jump, ok := n.(*ast.BranchStmt); ok && jump.Tok == token.CONTINUE {
							reasons["private trivia skip"] = true
						}
						if step, ok := n.(*ast.IncDecStmt); ok && contains(step.X, queryIndices) {
							reasons["private trivia skip"] = true
						}
						return true
					})
				}
				return true
			})
			commentSlash, commentStar := false, false
			if condition != nil {
				ast.Inspect(condition, func(n ast.Node) bool {
					if literal, ok := n.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
						value, _ := strconv.Unquote(literal.Value)
						commentSlash = commentSlash || value == "/"
						commentStar = commentStar || value == "*"
					}
					return true
				})
			}
			if commentSlash && commentStar {
				reasons["private trivia skip"] = true
			}
		}
		return true
	})

	var out []string
	for reason := range reasons {
		out = append(out, reason)
	}
	return out
}

func sameGuardIndex(expression ast.Expr, name string) bool {
	if parenthesized, ok := expression.(*ast.ParenExpr); ok {
		return sameGuardIndex(parenthesized.X, name)
	}
	identifier, ok := expression.(*ast.Ident)
	return ok && identifier.Name == name
}
