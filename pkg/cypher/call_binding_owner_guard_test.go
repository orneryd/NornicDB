package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

// Scope capabilities are checked across every production file, not a list of
// lowering functions. A name predicate cannot close over a raw source slice;
// binding-plan construction belongs to the shared owner; quoted roles can only
// be copied from native grammar facts. Renaming/moving a consumer cannot evade it.
func bindingOwnerViolations(filename string, source []byte) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), filename, source, 0)
	if err != nil {
		return nil, err
	}
	var violations []string
	// Track source aliases too: moving cypher[:position] into a local first
	// must not turn token presence into an admissible scope capability.
	sources := map[*ast.Object]bool{}
	sensitiveCallbacks := map[*ast.FuncLit]bool{}
	bindingConsumers := map[*ast.FuncDecl]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		if function, ok := node.(*ast.FuncDecl); ok {
			sensitive := false
			ast.Inspect(function, func(node ast.Node) bool {
				if name, ok := node.(*ast.Ident); ok {
					switch name.Name {
					case "StorageExecutor", "typeScope", "matchSemanticScope", "semanticBindingScope", "scopeRecording", "callBranchBindingPlan", "callSubqueryScopeClause", "LogicalFrame", "StatementCacheScope":
						sensitive = true
					}
				}
				return true
			})
			if sensitive && function.Body != nil {
				bindingConsumers[function] = true
				ast.Inspect(function.Body, func(node ast.Node) bool {
					if callback, ok := node.(*ast.FuncLit); ok {
						sensitiveCallbacks[callback] = true
					}
					return true
				})
			}
			for _, field := range function.Type.Params.List {
				if kind, ok := field.Type.(*ast.Ident); ok && kind.Name == "string" {
					for _, name := range field.Names {
						if name.Obj != nil {
							sources[name.Obj] = true
						}
					}
				}
			}
		}
		return true
	})
	rawSource := func(expression ast.Expr) bool {
		found := false
		ast.Inspect(expression, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if call, ok := n.Fun.(*ast.Ident); ok {
					switch call.Name {
					case "logicalStatementSyntax", "logicalProjectionSyntax", "statementPhysicalClauses", "statementFirstPhysicalClause", "statementPhysicalReturnProjection":
						return false // Structural facts from the one admission input owner.
					}
				}
				if call, ok := n.Fun.(*ast.SelectorExpr); ok {
					if owner, ok := call.X.(*ast.Ident); ok && owner.Name == "fabric" {
						switch call.Sel.Name {
						case "ReadQueryStructure", "BranchBindings", "NameOccurrences", "QueryMentionsVariable", "ExpressionReferences", "ExpressionScopes", "RootExpressionScope", "ComprehensionSyntax":
							return false // Native descriptors are facts, not raw text aliases.
						}
					}
				}
			case *ast.Ident:
				found = found || n.Obj != nil && sources[n.Obj]
			case *ast.SelectorExpr:
				found = found || n.Sel.Name == "Text" || n.Sel.Name == "text"
			}
			return true
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(file, func(node ast.Node) bool {
			if loop, ok := node.(*ast.RangeStmt); ok && rawSource(loop.X) {
				for _, value := range []ast.Expr{loop.Key, loop.Value} {
					if name, ok := value.(*ast.Ident); ok && name.Obj != nil && !sources[name.Obj] {
						sources[name.Obj], changed = true, true
					}
				}
			}
			if assignment, ok := node.(*ast.AssignStmt); ok && len(assignment.Lhs) == len(assignment.Rhs) {
				for index, left := range assignment.Lhs {
					if name, ok := left.(*ast.Ident); ok && name.Obj != nil && !sources[name.Obj] && rawSource(assignment.Rhs[index]) {
						sources[name.Obj], changed = true, true
					}
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			// Bound-name decisions include direct loops as well as callbacks.
			// Renaming a token reader cannot hide raw text membership behind it.
			if bindingConsumers[n] && n.Body != nil {
				maps := map[*ast.Object]bool{}
				for _, field := range n.Type.Params.List {
					if kind, ok := field.Type.(*ast.MapType); ok {
						if key, ok := kind.Key.(*ast.Ident); ok && key.Name == "string" {
							for _, name := range field.Names {
								maps[name.Obj] = true
							}
						}
					}
				}
				ast.Inspect(n.Body, func(node ast.Node) bool {
					loop, ok := node.(*ast.RangeStmt)
					if !ok {
						return true
					}
					domain, ok := loop.X.(*ast.Ident)
					if !ok || !maps[domain.Obj] {
						return true
					}
					key, ok := loop.Key.(*ast.Ident)
					if !ok || key.Obj == nil {
						return true
					}
					ast.Inspect(loop.Body, func(node ast.Node) bool {
						decision, ok := node.(*ast.IfStmt)
						if !ok || !rawSource(decision.Cond) {
							return true
						}
						// Plain equality compares declared names. Reading a token,
						// slice or membership reader reconstructs scope from text.
						readsText := false
						ast.Inspect(decision.Cond, func(node ast.Node) bool {
							switch operation := node.(type) {
							case *ast.CallExpr:
								readsText = readsText || rawSource(operation)
							case *ast.IndexExpr:
								readsText = readsText || rawSource(operation.X)
							case *ast.SliceExpr:
								readsText = readsText || rawSource(operation.X)
							}
							return true
						})
						if !readsText {
							return true
						}
						readsName := false
						ast.Inspect(decision.Cond, func(node ast.Node) bool {
							if name, ok := node.(*ast.Ident); ok && name.Obj == key.Obj {
								readsName = true
							}
							return true
						})
						if readsName {
							violations = append(violations, "bound-name decision derives scope from raw text")
						}
						return true
					})
					return true
				})
			}
			scopeCapability := false
			if n.Type.Results != nil {
				ast.Inspect(n.Type.Results, func(node ast.Node) bool {
					if name, ok := node.(*ast.Ident); ok && (name.Name == "localBinding" || name.Name == "expressionLocalNameSet" || name.Name == "nativeScopePredicate") {
						scopeCapability = true
					}
					return true
				})
			}
			if scopeCapability && n.Body != nil {
				ast.Inspect(n.Body, func(node ast.Node) bool {
					switch operation := node.(type) {
					case *ast.IndexExpr:
						if rawSource(operation.X) {
							violations = append(violations, "local scope facts inferred from text")
						}
					case *ast.SliceExpr:
						if rawSource(operation.X) {
							violations = append(violations, "local scope facts inferred from text")
						}
					case *ast.CallExpr:
						for _, argument := range operation.Args {
							if !rawSource(argument) {
								continue
							}
							if target, ok := operation.Fun.(*ast.SelectorExpr); ok {
								if owner, ok := target.X.(*ast.Ident); ok && owner.Name == "fabric" && target.Sel.Name == "ExpressionScopes" {
									continue
								}
							}
							if target, ok := operation.Fun.(*ast.Ident); ok && target.Name == "localBindingIn" && len(operation.Args) > 0 && !rawSource(operation.Args[0]) {
								// This adapter consumes native scope descriptors. A
								// raw/tainted first argument still fails the guard;
								// the remaining name is a lookup key, not source.
								continue
							}
							if target, ok := operation.Fun.(*ast.Ident); ok && (target.Name == "localBindingOf" || target.Name == "expressionLocalBindingNames") {
								continue
							}
							violations = append(violations, "local scope facts bypass native owner")
						}
					}
					return true
				})
			}
			if n.Name.Name == "expressionFreeVariables" || n.Name.Name == "forEachSemanticExpressionReference" {
				ast.Inspect(n.Body, func(node ast.Node) bool {
					switch operation := node.(type) {
					case *ast.IndexExpr:
						if rawSource(operation.X) {
							violations = append(violations, "expression references inferred from source text")
						}
					case *ast.SliceExpr:
						if rawSource(operation.X) {
							violations = append(violations, "expression references inferred from source text")
						}
					case *ast.CallExpr:
						for _, argument := range operation.Args {
							if !rawSource(argument) {
								continue
							}
							if selector, ok := operation.Fun.(*ast.SelectorExpr); ok {
								if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == "fabric" && selector.Sel.Name == "ExpressionReferences" {
									continue
								}
							}
							violations = append(violations, "expression references bypass native roles")
						}
					}
					return true
				})
			}
			cachedScope := false
			callFacts := false
			if n.Type.Results != nil {
				ast.Inspect(n.Type.Results, func(node ast.Node) bool {
					if name, ok := node.(*ast.SelectorExpr); ok && name.Sel.Name == "StatementCacheScope" {
						cachedScope = true
					}
					if name, ok := node.(*ast.Ident); ok && (name.Name == "callBranchBindingPlan" || name.Name == "callSubqueryScopeClause") {
						callFacts = true
					}
					return true
				})
			}
			if callFacts && n.Body != nil {
				ast.Inspect(n.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					for _, argument := range call.Args {
						if !rawSource(argument) {
							continue
						}
						if owner, ok := call.Fun.(*ast.Ident); ok && owner.Name == "logicalStatementSyntax" {
							continue
						}
						if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
							if owner, ok := selector.X.(*ast.Ident); ok && owner.Name == "fabric" {
								switch selector.Sel.Name {
								case "ReadQueryStructure", "SemanticQuery", "BranchBindings":
									continue
								}
							}
						}
						violations = append(violations, "CALL facts depend on a reader outside the native structural boundary")
					}
					return true
				})
			}
			if cachedScope && n.Body != nil {
				ast.Inspect(n.Body, func(node ast.Node) bool {
					if slice, ok := node.(*ast.SliceExpr); ok && rawSource(slice.X) {
						violations = append(violations, "cached logical scope reconstructed from source slice")
					}
					return true
				})
			}
		case *ast.CompositeLit:
			if typ, ok := n.Type.(*ast.Ident); ok && (typ.Name == "yieldItem" || typ.Name == "ASTReturnItem") {
				for _, element := range n.Elts {
					if field, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := field.Key.(*ast.Ident); ok && (key.Name == "Alias" || key.Name == "alias" || key.Name == "name") {
							if _, native := field.Value.(*ast.SelectorExpr); !native {
								violations = append(violations, "declaration name bypasses native facts")
							}
						}
					}
				}
			}

			if name, ok := n.Type.(*ast.SelectorExpr); ok && (name.Sel.Name == "ExpressionScope" || name.Sel.Name == "ExpressionDeclaration") {
				violations = append(violations, "local scope facts constructed outside native owner")
			}
			if name, ok := n.Type.(*ast.SelectorExpr); ok && name.Sel.Name == "ExpressionReference" {
				violations = append(violations, "expression role facts constructed outside native owner")
			}
			if name, ok := n.Type.(*ast.SelectorExpr); ok && name.Sel.Name == "LogicalFrame" {
				fields := map[string]bool{}
				for _, element := range n.Elts {
					if item, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := item.Key.(*ast.Ident); ok {
							fields[key.Name] = true
						}
					}
				}
				shape := reflect.TypeOf(fabric.LogicalFrame{})
				for index := 0; index < shape.NumField(); index++ {
					if !fields[shape.Field(index).Name] {
						violations = append(violations, "logical frame descriptor field dropped")
					}
				}
			}
			if name, ok := n.Type.(*ast.Ident); ok {
				if name.Name == "callBranchBindingPlan" && filepath.Base(filename) != "call_subquery_binding_imports.go" {
					violations = append(violations, "binding plan constructed outside shared owner")
				}
				if name.Name == "quotedToken" {
					for _, element := range n.Elts {
						field, ok := element.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						key, ok := field.Key.(*ast.Ident)
						if !ok || !strings.Contains("|variable|binding|procColumn|selector|", "|"+key.Name+"|") {
							continue
						}
						if _, native := field.Value.(*ast.SelectorExpr); !native {
							violations = append(violations, "quoted binding role inferred outside native facts")
						}
					}
				}
			}
		case *ast.FuncLit:
			if n.Type.Params.NumFields() != 1 || n.Type.Results == nil || n.Type.Results.NumFields() != 1 {
				break
			}
			param, stringName := n.Type.Params.List[0].Type.(*ast.Ident)
			result, boolean := n.Type.Results.List[0].Type.(*ast.Ident)
			if !stringName || !boolean || param.Name != "string" || result.Name != "bool" {
				break
			}
			// A live scope predicate looks up the name in a binding domain. It
			// never indexes/slices source text to reconstruct that domain.
			ast.Inspect(n.Body, func(inner ast.Node) bool {
				if slice, raw := inner.(*ast.SliceExpr); raw && rawSource(slice.X) {
					violations = append(violations, "name predicate derives scope from raw source slice")
				}
				if call, ok := inner.(*ast.CallExpr); ok {
					readsName, readsSource := false, false
					for _, argument := range call.Args {
						// Direct source membership is forbidden everywhere. Within a
						// binding-fact consumer, transformed source is forbidden too.
						// Other domains may inspect their own parsed character sets.
						_, transformed := argument.(*ast.CallExpr)
						readsSource = readsSource || (!transformed || sensitiveCallbacks[n]) && rawSource(argument)
						ast.Inspect(argument, func(node ast.Node) bool {
							if name, ok := node.(*ast.Ident); ok {
								for _, parameter := range n.Type.Params.List[0].Names {
									readsName = readsName || name.Name == parameter.Name
								}
							}
							return true
						})
					}
					if readsName && readsSource {
						violations = append(violations, "name predicate reconstructs membership from source text")
					}
				}
				return true
			})
		case *ast.AssignStmt:
			for index, left := range n.Lhs {
				if field, ok := left.(*ast.SelectorExpr); ok && (field.Sel.Name == "Alias" || field.Sel.Name == "alias" || field.Sel.Name == "Yield") && index < len(n.Rhs) && rawSource(n.Rhs[index]) && filepath.Base(filename) != "semantic_statement_input.go" {
					violations = append(violations, "declaration name inferred from raw source")
				}
			}
			for _, left := range n.Lhs {
				if field, ok := left.(*ast.SelectorExpr); ok && (field.Sel.Name == "binding" || field.Sel.Name == "procColumn") {
					violations = append(violations, "native binding role overwritten by a local classifier")
				}
			}
		}
		return true
	})
	return violations, nil
}

func TestCallBindingOwnerCapabilities(t *testing.T) {
	for _, root := range []string{".", "../fabric"} {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "antlr" {
					return filepath.SkipDir // The maintained grammar owns native roles.
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			violations, err := bindingOwnerViolations(path, source)
			require.NoError(t, err, path)
			if len(violations) > 0 {
				t.Errorf("%s: %v", path, violations)
			}
			return nil
		})
		require.NoError(t, err)
	}
}

func TestCallBindingOwnerDeclaredNameIdentity(t *testing.T) {
	violations, err := bindingOwnerViolations("renamed_consumer.go", []byte("package cypher; func (e *StorageExecutor) compare(selected string, bindings map[string]interface{}) { for name := range bindings { if name != selected {} } }"))
	require.NoError(t, err)
	require.Empty(t, violations)
}
func TestCallBindingOwnerGuardRejectsMutation(t *testing.T) {
	// An expression reader's own argument is not captured statement text. This
	// control distinguishes canonical domain parsing from lookbehind scope inference.
	control := "package cypher; func expression(query string) { scan := func(text string) bool { return strings.Contains(text[:len(text)], \"length\") }; _ = scan }"
	allowed, err := bindingOwnerViolations("expression.go", []byte(control))
	require.NoError(t, err)
	require.Empty(t, allowed)
	formatControl := "package cypher; func format(pattern string) { present := func(letters string) bool { return strings.ContainsAny(parsedCharacters(pattern), letters) }; _ = present }"
	allowed, err = bindingOwnerViolations("format.go", []byte(formatControl))
	require.NoError(t, err)
	require.Empty(t, allowed)
	for _, filename := range []string{"semantic_validation.go", "planner_precheck.go", "cache.go", "analysis_hint.go", "error_classification.go"} {
		// Reintroduce the reviewed :280 mechanism, then rename both the
		// callback and reader. Neither function-name nor file-name matching
		// may be the reason the guard catches it.
		for _, reader := range []string{"isIdentifierReferenced", "renamedTokenReader"} {
			mutation := "package cypher; func mutation(cypher string, position int) { outer := func(variable string) bool { return " + reader + "(cypher[:position], variable) }; _ = outer }"
			violations, err := bindingOwnerViolations(filename, []byte(mutation))
			require.NoError(t, err)
			require.Contains(t, violations, "name predicate derives scope from raw source slice", filename)
		}
	}
	for _, mutation := range []string{
		"package cypher; func mutation() { _ = callBranchBindingPlan{} }",
		"package cypher; func mutation() { _ = quotedToken{binding: true} }",
		"package cypher; func mutation(token *quotedToken, prior string) { token.binding = prior == \"AS\" }",
		"package cypher; func mutation(query string, position int) { prefix := query[:position]; probe := func(name string) bool { return renamedReader(prefix, name) }; _ = probe }",
		"package cypher; func mutation(query string) { probe := func(name string) bool { return strings.Contains(query, name) }; _ = probe }",
		"package cypher; func mutation(query string) typeScope { probe := func(name string) bool { return strings.ContainsAny(renamedReader(query), name) }; _ = probe; return nil }",
		"package cypher; func expressionFreeVariables(expression string) []string { return renamedReader(expression) }",
		"package cypher; func forEachSemanticExpressionReference(expression string) { _ = expression[0] }",
		"package cypher; func mutation() { _ = fabric.ExpressionReference{Name: \"A\"} }",
		"package cypher; func renamed(text string, open int) (localBinding,bool) { _ = text[open]; return localBinding{},false }",
		"package cypher; func renamed(text string) expressionLocalNameSet { return lexicalLocalNames(text) }",
		"package cypher; func renamed(text string, at int) nativeScopePredicate { return lexicalLookbehind(text[:at]) }",
		"package cypher; func mutation() { _ = fabric.ExpressionScope{Open:1} }",
		"package cypher; func mutation(text string, name string) (localBinding, int, bool) { scopes := strings.Split(text, name); return localBindingIn(scopes, 0, name) }",
		"package cypher; func (e *StorageExecutor) renamed(query string,bindings map[string]interface{}) { for name := range bindings { if renamedReader(query,name) {} } }",
		"package cypher; func (e *StorageExecutor) renamed(query string,bindings map[string]interface{}) { for name := range bindings { if query[:3] == name {} } }",
		"package cypher; func mutation() { _ = fabric.LogicalFrame{Inputs: nil, Globals: nil, Columns: nil, InputTypes: nil, OutputTypes: nil} }",
		"package cypher; func mutation(query string) []*multidb.StatementCacheScope { for from := range query { parseUSE(query[from:]) }; return nil }",
	} {
		violations, err := bindingOwnerViolations("renamed_consumer.go", []byte(mutation))
		require.NoError(t, err)
		require.NotEmpty(t, violations)
	}
	violations, err := bindingOwnerViolations("call_subquery_binding_imports.go", []byte("package cypher; func newOwner(query string) callBranchBindingPlan { return renamedReader(query) }"))
	require.NoError(t, err)
	require.Contains(t, violations, "CALL facts depend on a reader outside the native structural boundary")
}

func TestDeclarationOwnerGuardRejectsTextNames(t *testing.T) {
	for _, mutation := range []string{
		`package cypher; func renamed(text string) { value := yieldItem{}; parts := strings.Split(text, ","); value.alias = strings.TrimSpace(parts[0]) }`,
		"package cypher; func renamed(text string) { _ = yieldItem{alias: renamedReader(text)} }",
		"package cypher; func renamed(text string) { _ = ASTReturnItem{Alias: text} }",
		`package cypher; func renamed(text string) { result := ASTCall{}; for _, item := range strings.Split(text, ",") { result.Yield = append(result.Yield, item) } }`,
	} {
		violations, err := bindingOwnerViolations("renamed_metadata.go", []byte(mutation))
		require.NoError(t, err)
		require.NotEmpty(t, violations)
	}
}
