package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Generic splitters are call readers too. Inspect AST operations rather than
// formatting, and exercise the detector with alternate-reader mutants below.
func TestFunctionSyntaxOwnerGuard(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "function_syntax.go" {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			for _, reason := range alternateFunctionSyntax(function.Body) {
				if reason == "independent quote/group scanner" && expressionGrammarBoundary[name+":"+function.Name.Name] != "" {
					continue
				}
				if reason == "independent quote/comma/group scanner" && functionGrammarBoundary[name+":"+function.Name.Name] != "" {
					continue
				}
				t.Errorf("%s:%s: %s; consume functionSyntax instead", name, function.Name.Name, reason)
			}
		}
	}
}

var rawFunctionPrefix = regexp.MustCompile(`^[A-Za-z_][A-Za-z_0-9.]*[ \t]*\(`)
var callTextInput = regexp.MustCompile(`(?i)(expr|arg|param|signature|call|content|^text$|^s$)`)

func alternateFunctionSyntax(body ast.Node) []string {
	reasons := map[string]bool{}
	backwardOffsets := map[string]bool{"open": true, "opening": true}
	ast.Inspect(body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for index, value := range assignment.Rhs {
			call, called := value.(*ast.CallExpr)
			if !called || index >= len(assignment.Lhs) {
				continue
			}
			name, named := call.Fun.(*ast.Ident)
			if !named || name.Name != "trimRightIndex" {
				continue
			}
			if offset, named := assignment.Lhs[index].(*ast.Ident); named {
				backwardOffsets[offset.Name] = true
			}
		}
		return true
	})
	// A retained call already owns its close. Reject consumers that ignore
	// that fact and match its opening again. Unrelated map/pattern matching
	// in a larger statement compiler is a different grammar operation.
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && len(call.Args) == 1 {
			if name, named := call.Fun.(*ast.Ident); named && (name.Name == "isIdentByte" || name.Name == "isIdentStartByte") {
				if value, indexed := call.Args[0].(*ast.IndexExpr); indexed {
					if offset, subtracted := value.Index.(*ast.BinaryExpr); subtracted && offset.Op == token.SUB {
						if opening, named := offset.X.(*ast.Ident); named && backwardOffsets[opening.Name] {
							reasons["independent backward callee reader"] = true
						}
					}
				}
			}
		}
		if !ok || len(call.Args) < 2 {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok || (name.Name != "findMatchingDelimiter" && name.Name != "findMatchingParen") {
			return true
		}
		if opening, ok := call.Args[1].(*ast.SelectorExpr); ok && opening.Sel.Name == "open" {
			reasons["independent callable group matcher"] = true
		}
		return true
	})
	patterns := map[string]bool{}
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != len(assignment.Rhs) {
				return true
			}
			for index, left := range assignment.Lhs {
				name, ok := left.(*ast.Ident)
				if ok && !patterns[name.Name] && rawCallablePattern(assignment.Rhs[index], patterns) {
					patterns[name.Name] = true
					changed = true
				}
			}
			return true
		})
	}
	ast.Inspect(body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			quote, comma, group, dot, callText := false, false, false, false, false
			ast.Inspect(n, func(child ast.Node) bool {
				if identifier, ok := child.(*ast.Ident); ok && callTextInput.MatchString(identifier.Name) {
					callText = true
				}
				if identifier, ok := child.(*ast.Ident); ok && (identifier.Name == "delim" || identifier.Name == "delimiter") {
					comma = true
				}
				if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.CHAR {
					value, _ := strconv.Unquote(literal.Value)
					quote = quote || value == "'" || value == `"` || value == "`"
					comma = comma || value == ","
					dot = dot || value == "."
					group = group || value == "(" || value == "[" || value == "{"
				}
				return true
			})
			if quote && group && (comma || dot) {
				reasons["independent quote/comma/group scanner"] = true
			} else if quote && group && callText {
				reasons["independent quote/group scanner"] = true
			}
		case *ast.CallExpr:
			selector, ok := n.Fun.(*ast.SelectorExpr)
			if !ok || len(n.Args) < 2 {
				return true
			}
			library, ok := selector.X.(*ast.Ident)
			if !ok || library.Name != "strings" {
				return true
			}
			switch selector.Sel.Name {
			case "HasPrefix", "Contains", "Index", "IndexByte", "IndexRune", "LastIndex", "Split", "SplitN":
			default:
				return true
			}
			if rawCallablePattern(n.Args[1], patterns) {
				reasons["independent function-prefix reader"] = true
			}
			if expression, ok := n.Args[1].(*ast.BinaryExpr); ok && expression.Op == token.ADD {
				if literal, ok := expression.Y.(*ast.BasicLit); ok {
					value, _ := strconv.Unquote(literal.Value)
					if value == "(" {
						reasons["independent function-prefix reader"] = true
					}
				}
			}
			ast.Inspect(n.Args[1], func(child ast.Node) bool {
				if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.STRING {
					value, _ := strconv.Unquote(literal.Value)
					if rawFunctionPrefix.MatchString(value) {
						reasons["independent function-prefix reader"] = true
					}
				}
				return true
			})
			literal, ok := n.Args[1].(*ast.BasicLit)
			if !ok {
				return true
			}
			value, _ := strconv.Unquote(literal.Value)
			if rawFunctionPrefix.MatchString(value) {
				reasons["independent function-prefix reader"] = true
			}
			if (value == "(" || value == ",") && selector.Sel.Name != "HasPrefix" && selector.Sel.Name != "Contains" {
				ast.Inspect(n.Args[0], func(child ast.Node) bool {
					if identifier, ok := child.(*ast.Ident); ok && callTextInput.MatchString(identifier.Name) {
						reasons["raw call-text delimiter reader"] = true
					}
					return true
				})
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

func TestFunctionSyntaxGuardDetectsAlternateReaders(t *testing.T) {
	for _, source := range []string{
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case '(': } } }",
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case ',': case '(': } } }",
		"package p; func f(s string, delim rune) { for _, c := range s { switch c { case '\\'': case '(': }; if c == delim {} } }",
		`package p; func f(s string) bool { return strings.HasPrefix(s, "substring(") }`,
		`package p; func f(s, name string) bool { return strings.Contains(s, name+"(") }`,
		`package p; func f(expr string) int { return strings.IndexByte(expr, '(') }`,
		`package p; func f(args string) []string { return strings.Split(args, ",") }`,
		"package p; func f(s string) { for _, c := range s { switch c { case '\\'': case '.': case '(': } } }",
		`package p; func f(s string) { call,_:=functionCallAt(s,0); findMatchingDelimiter(s,call.open,'(',')') }`,
		`package p; func f(q string) bool { prefix:="count("; alias:=prefix; return strings.Contains(q,alias) }`,
		`package p; func f(q string, open int) bool { return open>0 && isIdentByte(q[open-1]) }`,
		`package p; func f(q string, open int) bool { before:=trimRightIndex(q,0,open); return before>0 && isIdentByte(q[before-1]) }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutant.go", source, 0)
		require.NoError(t, err)
		require.NotEmpty(t, alternateFunctionSyntax(file.Decls[0].(*ast.FuncDecl).Body), source)
	}
}

// Genuine protocol grammars classify graph steps or variable bindings; they do
// not produce callable identities or argument spans. Raw call-text and prefix
// reading remain forbidden even inside these explicitly named boundaries.
var functionGrammarBoundary = map[string]string{
	"parser.go:tokenize":                                                     "Execution grammar tokens.",
	"create.go:scanCreatePatterns":                                           "Graph node/relationship steps and offsets.",
	"executor_subqueries.go:firstVariableOccurrence":                         "Free-variable diagnostic position.",
	"label_expression_rewrite.go:nameSingleNodePaths":                        "Generated graph node names.",
	"set_semantic_validation.go:expressionFreeVariables":                     "Local variable bindings.",
	"create.go:parseParamPathParts":                                          "Parameter member paths, not callable identities.",
	"executor_query_routing.go:validateSyntaxNornic":                         "Statement grammar and delimiter validity.",
	"group_by_rewrite.go:projectionWords":                                    "Projection clause words.",
	"keyword_scan.go:inPatternClause":                                        "Clause and graph-pattern context.",
	"label_expression_rewrite.go:mayReadPatternInPropertyMap":                "Graph expressions in node property maps.",
	"label_expression_rewrite.go:expression":                                 "Graph-expression rewriting grammar.",
	"literal_normalization.go:normalizeLiteralText":                          "Literal and interpolation rewriting.",
	"map_pattern_context.go:newMapPatternContext":                            "Map versus graph-pattern context.",
	"operators.go:enclosingCaseBlockEnd":                                     "CASE block grammar.",
	"path_selector_search.go:readsPatternVariable":                           "Pattern variable references.",
	"pipeline_write_stretch.go:addStretchPropertyReads":                      "Property dependency tracking.",
	"property_access_semantic_validation.go:scanPropertyAccesses":            "Property and variable references.",
	"query_structure_rewrite.go:topLevelWords":                               "Statement clause words.",
	"return_order_semantic_validation.go:forEachSemanticExpressionReference": "Expression variable references.",
	"set_semantic_validation.go:trailingWordAfterOperand":                    "Operand-following syntax.",
	"standalone_order_rewrite.go:scanStandaloneOrderClauses":                 "Standalone ordering clause grammar.",
	"static_operator_types.go:staticSliceBounds":                             "List subscript slice bounds.",
	"traversal_endpoint_analysis.go:traversalVariablePropertiesRead":         "Graph endpoint property dependencies.",
}

// These audited grammars read CASE, operators, binding visibility, schema or
// graph patterns, not call identities or argument boundaries. Their exemption
// applies only to the added quote/group detector. A comma splitter, dotted
// callee probe, raw prefix reader or retained-call rematch still fails here.
// Group-only expression adapters are deliberately absent: all of them forward
// to parenthesizedFunctionExpression. Generic delimiter/pipe adapters likewise
// read the owner's root punctuation instead of qualifying for an exemption.
var expressionGrammarBoundary = map[string]string{
	"case_closure_balance.go:casePrefixEndRequired":                           "CASE closure alternatives.",
	"case_expression.go:caseBlockSpans":                                       "CASE block visibility.",
	"case_expression.go:caseNestedLacksEnd":                                   "Nested CASE closure.",
	"case_expression.go:caseTypeParameterCloses":                              "Type parameter grammar.",
	"case_expression.go:findCaseKeywordAtLevel":                               "CASE branch keywords.",
	"case_expression.go:findTopLevelKeyword":                                  "CASE-aware operator words.",
	"comparison_chain.go:scanComparisonChain":                                 "Comparison operator spans.",
	"comparison_chain.go:scanPlainComparisonChain":                            "Comparison fast-path eligibility.",
	"cypher25_expression_rewrite.go:mapComprehensionEdits":                    "Map-comprehension grammar.",
	"dynamic_tokens.go:resolveRowDynamicTokens":                               "Graph label/type evaluation.",
	"dynamic_tokens.go:staticPatternTokenError":                               "Graph token type validation; calls use owned spans.",
	"executor_mutations_delete_guard.go:hasTopLevelDeleteLabelQualifier":      "DELETE label qualifier grammar.",
	"executor.go:firstTopLevelCallSubquery":                                   "Statement subquery clause position.",
	"expression_failure.go:inLocalExpressionScope":                            "Binding visibility; calls use owned spans.",
	"expression_validation.go:localScopeStart":                                "Binding input/body boundary.",
	"expression_validation.go:validateListComprehensionVariables":             "List iterator declaration grammar.",
	"fulltext_query.go:ftTokenize":                                            "Lucene grammar, not Cypher call text.",
	"keyword_scan.go:firstKeywordIndexFromDefault":                            "Statement clause keyword owner.",
	"keyword_scan.go:keywordIndexFrom":                                        "Statement clause keyword owner.",
	"knowledgepolicy_ddl.go:kpScanBraceBlock":                                 "Knowledge policy DDL body.",
	"label_expression_rewrite.go:mayUsePatternPredicateWithSyntax":            "Pattern WHERE eligibility; calls use owned facts.",
	"label_expression.go:labelExpressionBarAt":                                "Label disjunction versus projection bar.",
	"map_literal_validation.go:validateStaticMapKeys":                         "Map key versus pattern property grammar.",
	"map_pattern_context.go:insidePatternDelimiter":                           "Map versus graph-pattern context.",
	"match_index_seek.go:topLevelSymbolIndex":                                 "Index predicate operator position.",
	"match_semantic_validation.go:maskNestedScopeBodies":                      "Statement subquery visibility.",
	"match_semantic_validation.go:maskPatternComprehensions":                  "Pattern comprehension visibility.",
	"match_semantic_validation.go:withPatternComprehensionBindings":           "Pattern-local bindings.",
	"merge_semantic_validation.go:mergePatternUsesParameterPredicate":         "Graph parameter-map eligibility.",
	"operators.go:bracketDepthAt":                                             "Operator nesting context.",
	"operators.go:findTopLevelOperator":                                       "Operator spans.",
	"path_selector.go:plainParenthesisedPath":                                 "Graph path grammar; call classification uses owned facts.",
	"pattern_expression.go:splitPatternComprehension":                         "Graph pattern/projection grammar.",
	"pipeline_write_stretch.go:addStretchExpressionReads":                     "Graph label/read dependencies.",
	"property_access_semantic_validation.go:parameterMayRejectPropertyAccess": "Parameter member-access types.",
	"query_patterns.go:countRelationshipPatterns":                             "Relationship pattern count.",
	"query_patterns.go:extractNodeVariables":                                  "Node variable declarations; calls use owned facts.",
	"quoted_variables.go:scanQuotedTokens":                                    "Variable/parameter binding occurrences.",
	"relationship_brackets.go:scanRelationshipBracket":                        "Relationship pattern bounds.",
	"relationship_declaration.go:relationshipDeclarationOf":                   "Relationship type alternatives.",
	"return_order_semantic_validation.go:maskSemanticBraceBodies":             "Subquery/map projection visibility.",
	"row_expression.go:containsRelationshipPattern":                           "Graph pattern eligibility.",
	"row_expression.go:findListComprehensionToken":                            "List IN/WHERE/projection grammar, after call argument extraction.",
	"row_expression.go:splitRowArithmeticTier":                                "Arithmetic operator spans.",
	"schema_contracts.go:parseCurlySection":                                   "Schema map body grammar.",
	"semantic_validation.go:containsIllegalProjectedPatternExpression":        "Graph expression placement.",
	"semantic_validation.go:maskPredicatePatterns":                            "Graph predicate visibility.",
	"set_helpers.go:hasConcatOperator":                                        "Concatenation operator position.",
	"set_semantic_validation.go:expressionLocalBindings":                      "Local binding declarations.",
	"skip_limit_semantic_validation.go:validateStaticPaginationExpressions":   "Subquery pagination scope.",
	"statement_framing.go:topLevelUnionCut":                                   "Statement UNION framing.",
	"static_operator_types.go:staticMapProjectionReceiver":                    "Map projection receiver grammar.",
	"subquery_value_expression.go:findSubqueryExpressions":                    "Subquery expression bodies.",
	"subquery_value_expression.go:patternHasInlineWhere":                      "Pattern-local WHERE grammar.",
	"temporal_pattern.go:compileTemporalPattern":                              "Date/time formatting pattern grammar.",
	"validator_strictness.go:lastTopLevelClauseWord":                          "Statement clause grammar.",
	"where_by_variable.go:hasPatternOrSubquery":                               "Graph pattern/subquery eligibility.",
}

func rawCallablePattern(node ast.Node, aliases map[string]bool) bool {
	found := false
	ast.Inspect(node, func(child ast.Node) bool {
		if literal, ok := child.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, _ := strconv.Unquote(literal.Value)
			found = found || rawFunctionPrefix.MatchString(value)
		}
		if identifier, ok := child.(*ast.Ident); ok {
			found = found || aliases[identifier.Name]
		}
		return !found
	})
	return found
}
