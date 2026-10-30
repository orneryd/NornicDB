//go:build boundplan_census_operator

// Author: #1640 lane (Claude, Rizheg). Analysis-only client. Two convergence
// gates for the typed program: no parameter skip seam (a conditional choice of
// whether the checker receives the request's parameter values), and one reader
// that decides which source tokens are function calls.
package boundplancheck

import (
	"go/ast"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Grammar rules that make source tokens a function call.
var boundCallRules = map[string]bool{"CypherParserRULE_functionInvocation": true, "CypherParserRULE_reduceExpression": true, "CypherParserRULE_allReduceExpression": true}

// The function catalog: a text loop that consults it is deciding calls.
var boundCallCatalog = map[string]bool{"builtInCypherFunctions": true, "lookupFunctionArity": true, "lookupStaticFunctionArguments": true, "knownFunctionCallError": true, "functionCacheDeclarations": true}

var boundCallReaderDispositions = map[string]string{
	"pkg/cypher:syntaxFunctionCallAt": "owner: the one call reader",
	// Readers of a grammar node's own kind decide nothing about tokens.
	"pkg/cypher:pipelineEffectVisitor":       "grammar node consumer",
	"pkg/cypher:variableSyntaxRead":          "grammar node consumer",
	"pkg/cypher:expressionLocalSyntax":       "grammar node consumer",
	"pkg/cypher:scalarAdmissionVisitor":      "grammar node consumer",
	"pkg/cypher:boundTypeValues.localOperand": "grammar node consumer",
	"pkg/cypher:scanQuotedTokens":            "grammar node consumer",
}

var boundSkipSeamDispositions = map[string]string{
	// Release behaviour: CALL IN TRANSACTIONS types its batch size, then with the
	// request's values when present. Same rule: typed by values exactly when given.
	"pkg/cypher:StorageExecutor.classifyTransactionalBatchExpression": "release batch-size check (values when present)",
}

type boundConvergenceMember struct {
	Symbol string
	Kind   string
	Sites  []string
}

func boundConvergenceMembers(t *testing.T, receipt boundCompiledReceipt) (callReaders, skipSeams []boundConvergenceMember) {
	t.Helper()
	program := receipt.program
	calls, seams := map[string]*boundConvergenceMember{}, map[string]*boundConvergenceMember{}
	for _, pkg := range program.Packages {
		if pkg.Types == nil || pkg.TypesInfo == nil || !strings.HasPrefix(pkg.Types.Path(), boundModule) {
			continue
		}
		info := pkg.TypesInfo
		for _, file := range pkg.Syntax {
			relative, _ := filepath.Rel(program.Root, program.FileSet.Position(file.Pos()).Filename)
			relative = filepath.ToSlash(relative)
			if strings.HasSuffix(relative, "_test.go") || boundFixture(relative) || strings.Contains(relative, "/antlr/") || strings.HasPrefix(relative, "internal/boundplancheck") {
				continue
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				name := fn.Name.Name
				if fn.Recv != nil && len(fn.Recv.List) == 1 {
					receiver := fn.Recv.List[0].Type
					if star, ok := receiver.(*ast.StarExpr); ok {
						receiver = star.X
					}
					if ident, ok := receiver.(*ast.Ident); ok {
						name = ident.Name + "." + name
					}
				}
				symbol := strings.TrimPrefix(pkg.Types.Path(), boundModule) + ":" + name
				site := func(node ast.Node) string {
					return relative + ":" + strconv.Itoa(program.FileSet.Position(node.Pos()).Line)
				}
				add := func(into map[string]*boundConvergenceMember, kind string, node ast.Node) {
					member := into[symbol]
					if member == nil {
						member = &boundConvergenceMember{Symbol: symbol, Kind: kind}
						into[symbol] = member
					}
					member.Sites = append(member.Sites, kind+" @"+site(node))
				}
				var loops, scans, parens, catalog bool
				var catalogSite ast.Node
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.ForStmt, *ast.RangeStmt:
						loops = true
					case *ast.BasicLit:
						if node.Kind == token.CHAR && node.Value == "'('" {
							parens = true
						}
					case *ast.SelectorExpr:
						if boundCallRules[node.Sel.Name] {
							add(calls, "grammar call rule "+node.Sel.Name, node)
						}
						if node.Sel.Name == "ScanIdentifier" {
							scans = true
						}
					case *ast.Ident:
						if object := info.Uses[node]; object != nil && object.Pkg() != nil && object.Pkg().Path() == boundModule+"pkg/cypher" && boundCallCatalog[object.Name()] {
							catalog, catalogSite = true, node
						}
					case *ast.IfStmt:
						ast.Inspect(node.Body, func(inner ast.Node) bool {
							if assign, ok := inner.(*ast.AssignStmt); ok {
								for _, left := range assign.Lhs {
									if selector, ok := left.(*ast.SelectorExpr); ok && selector.Sel.Name == "params" {
										if boundParameterHolders[boundNamedHolder(info.TypeOf(selector.X))] {
											add(seams, "conditional params assignment", assign)
										}
									}
								}
							}
							return true
						})
					}
					return true
				})
				if loops && scans && parens && catalog {
					add(calls, "text call scanner consulting the function catalog", catalogSite)
				}
			}
		}
	}
	collect := func(from map[string]*boundConvergenceMember) []boundConvergenceMember {
		var out []boundConvergenceMember
		for _, member := range from {
			member.Sites = boundUniqueStrings(member.Sites)
			out = append(out, *member)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
		return out
	}
	return collect(calls), collect(seams)
}

func boundConvergenceProblems(members []boundConvergenceMember, dispositions map[string]string, label string) []string {
	var problems []string
	seen := map[string]bool{}
	for _, member := range members {
		seen[member.Symbol] = true
		if _, listed := dispositions[member.Symbol]; !listed {
			problems = append(problems, "UNLISTED "+label+" "+member.Symbol+" | "+strings.Join(member.Sites, " ; "))
		}
	}
	for symbol := range dispositions {
		if !seen[symbol] {
			problems = append(problems, "STALE "+label+" disposition "+symbol)
		}
	}
	sort.Strings(problems)
	return problems
}

func TestBoundPlanConvergenceGates(t *testing.T) {
	callReaders, skipSeams := boundConvergenceMembers(t, boundCompiledQuery(t, boundPlanCensusConfig()))
	owners := 0
	for _, member := range callReaders {
		t.Logf("CALL_READER %s [%s] %s", member.Symbol, boundCallReaderDispositions[member.Symbol], strings.Join(member.Sites, " ; "))
		if strings.HasPrefix(boundCallReaderDispositions[member.Symbol], "owner") {
			owners++
		}
	}
	for _, member := range skipSeams {
		t.Logf("SKIP_SEAM %s [%s] %s", member.Symbol, boundSkipSeamDispositions[member.Symbol], strings.Join(member.Sites, " ; "))
	}
	problems := append(boundConvergenceProblems(callReaders, boundCallReaderDispositions, "call reader"), boundConvergenceProblems(skipSeams, boundSkipSeamDispositions, "skip seam")...)
	if owners != 1 {
		problems = append(problems, "call reader owners: "+strconv.Itoa(owners))
	}
	if len(problems) > 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
	t.Logf("CONVERGENCE_GATES call_readers=%d owners=1 text_call_scanners=0 skip_seams=%d", len(callReaders), len(skipSeams))
}

// Controls: a second text call scanner and a skip-mode parameter seam planted
// in pkg/cypher are reported; a text loop that never consults the function
// catalog and an unconditional params copy are not; removal restores.
func TestBoundPlanConvergenceGatesControls(t *testing.T) {
	config := boundPlanCensusConfig()
	baseCalls, baseSeams := boundConvergenceMembers(t, boundCompiledQuery(t, config))
	config.Overlay = map[string][]byte{filepath.Join(config.Dir, "pkg/cypher/r73_planted_convergence.go"): []byte(`package cypher
import "github.com/orneryd/nornicdb/internal/cyphertext"
func plantedTextCallScanner(text string) bool {
 for index := 0; index < len(text); index++ {
  name, ok := cyphertext.ScanIdentifier(text, index)
  if ok && name.End < len(text) && text[name.End] == '(' {
   if _, known := builtInCypherFunctions[name.Value]; known { return true }
  }
 }
 return false
}
func plantedPlainScanner(text string) bool {
 for index := 0; index < len(text); index++ {
  name, ok := cyphertext.ScanIdentifier(text, index)
  if ok && name.End < len(text) && text[name.End] == '(' { return true }
 }
 return false
}
func plantedSkipSeam(flag bool, checker staticOperatorChecker, values map[string]interface{}) staticOperatorChecker {
 if flag { checker.params = values }
 return checker
}
func plantedUnconditionalCopy(checker staticOperatorChecker, values map[string]interface{}) staticOperatorChecker {
 checker.params = values
 return checker
}
`)}
	calls, seams := boundConvergenceMembers(t, boundCompiledQuery(t, config))
	problems := strings.Join(append(boundConvergenceProblems(calls, boundCallReaderDispositions, "call reader"), boundConvergenceProblems(seams, boundSkipSeamDispositions, "skip seam")...), "\n")
	for _, want := range []string{"UNLISTED call reader pkg/cypher:plantedTextCallScanner ", "UNLISTED skip seam pkg/cypher:plantedSkipSeam "} {
		if !strings.Contains(problems, want) {
			t.Fatalf("plant escaped: %s\n%s", want, problems)
		}
	}
	for _, decoy := range []string{"plantedPlainScanner", "plantedUnconditionalCopy"} {
		if strings.Contains(problems, decoy) {
			t.Fatalf("decoy entered: %s\n%s", decoy, problems)
		}
	}
	config.Overlay = nil
	restoredCalls, restoredSeams := boundConvergenceMembers(t, boundCompiledQuery(t, config))
	if len(restoredCalls) != len(baseCalls) || len(restoredSeams) != len(baseSeams) || len(calls) != len(baseCalls)+1 || len(seams) != len(baseSeams)+1 {
		t.Fatalf("population: calls %d/%d/%d seams %d/%d/%d", len(baseCalls), len(calls), len(restoredCalls), len(baseSeams), len(seams), len(restoredSeams))
	}
	t.Logf("CONVERGENCE_PLANT text_scanner=detected skip_seam=detected decoys=excluded calls=%d->%d->%d seams=%d->%d->%d", len(baseCalls), len(calls), len(restoredCalls), len(baseSeams), len(seams), len(restoredSeams))
}
