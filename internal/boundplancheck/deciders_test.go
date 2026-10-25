//go:build boundplan_census_operator

// Author: #1640 lane (Claude, Rizheg). Analysis-only client. Inventories every
// function that decides a procedure CALL's declaration facts (resolution,
// YIELD, arity, argument and name extraction, signature construction) and
// requires each one to be a dispositioned owner, catalog entry or port.
package boundplancheck

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Readers and decoders of procedure declaration facts, by object identity.
var boundProcedureDeciderFuncs = map[string]string{
	"yieldFromSyntax": "yield", "parseYieldClause": "yield", "constructProcedureArity": "arity",
	"procedureArgumentTextsFromSyntax": "args", "explicitProcedureArgumentTexts": "args", "procedureNameAt": "name",
}

// Every member and why it may decide. A member missing here, or an entry here
// that is no longer a member, fails the census.
var boundProcedureDeciderDispositions = map[string]string{
	// The statement compiler: the one owner of declaration facts, and its decoders.
	"pkg/cypher:boundCompilation.procedure":             "owner",
	"pkg/cypher:boundCompilation.procedureDeclaration":  "owner",
	"pkg/cypher:boundCompilation.admitProcedureInvocation": "owner",
	"pkg/cypher:boundCompilation.leaveClause":           "owner",
	"pkg/cypher:constructProcedureArity":                "owner",
	"pkg/cypher:projectedProcedureNodeNames":            "owner",
	// The typed program reads the admitted spec and its own lowered clause.
	"pkg/cypher:boundTypeValues.clause":          "typed-program consumer",
	"pkg/cypher:staticProcedureArgumentTypeError": "typed-program consumer",
	"pkg/cypher:explicitProcedureArgumentTexts":   "typed-program consumer",
	// Execution reads the admitted call.
	"pkg/cypher:executionProcedureCall":           "execution accessor",
	"pkg/cypher:StorageExecutor.procedureArguments": "implicit-argument value binding",
	// The catalog: procedure signatures are declared and registered here.
	"pkg/cypher:validateProcedureSpec":                  "catalog",
	"pkg/cypher:ListRegisteredProcedures":               "catalog",
	"pkg/cypher:registerDeprecatedProcedureAlias":       "catalog",
	"pkg/cypher:registerProcedure":                      "catalog",
	"pkg/cypher:registerBuiltInProcedure":               "catalog",
	"pkg/cypher:registerBuiltInProcedureLiteral":        "catalog",
	"pkg/cypher:registerAdminBuiltInProcedure":          "catalog",
	"pkg/cypher:registerAdminBuiltInProcedureLiteral":   "catalog",
	"pkg/cypher:registerBatchProcedureLiteral":          "catalog",
	"pkg/cypher:registerStatementProcedureLiteral":      "catalog",
	"pkg/cypher:ensureBuiltInProceduresRegistered":      "catalog",
	"pkg/cypher:awaitIndexProcedureSpec":                "catalog",
	"pkg/cypher:dbmsListConfigProcedureSpec":            "catalog",
	"pkg/cypher:dbmsListConnectionsProcedureSpec":       "catalog",
	"pkg/cypher:fulltextAnalyzerProcedureSpec":          "catalog",
	"pkg/cypher:fulltextQueryProcedureSpec":             "catalog",
	"pkg/cypher:queryStatisticsProcedureSpec":           "catalog",
	"pkg/cypher:schemaVisualizationProcedureSpec":       "catalog",
	"pkg/cypher:tokenListingProcedureSpec":              "catalog",
	"pkg/cypher:vectorCreateNodeProcedureSpec":          "catalog",
	"pkg/cypher:vectorQueryProcedureSpec":               "catalog",
	"pkg/cypher:vectorSetterProcedureSpec":              "catalog",
	"pkg/cypher:schemaTypePropertiesProcedureSpec":      "catalog",
	"pkg/cypher:StorageExecutor.compilePersistedProcedure": "catalog",
	"pkg/nornicdb:pluginProcedureForCypher":             "catalog",
	// Before admission, grammar names resolve exactly in the one catalog, as
	// the owner resolves them (no case folding anywhere).
	"pkg/cypher:visitStatementProcedureSyntax":   "owner consumer (exact catalog name)",
	"pkg/cypher:AuthorizeReadOnlyQuery":          "owner consumer (exact catalog name)",
	"pkg/cypher:readStatementExecutionContract":  "owner consumer (exact catalog name)",
	"pkg/cypher:pipelineEffectVisitor":           "owner consumer (exact catalog name)",
	"pkg/cypher:compileExecutionPolicy":          "owner consumer (exact catalog name)",
	"pkg/cypher:ProcedureRegistry.callsUserProcedure": "owner consumer (exact catalog name)",
	// Routers that only classify a CALL as a procedure call.
	"pkg/cypher:statementReadsGraphText":     "router classification",
	"pkg/cypher:systemDatabaseStatementError": "router classification",
	// The TCK harness declares fixture procedures for its scenarios.
	"testing/cypher/tck:parseProcedureFixture": "test fixture catalog",
	// SHOW ... YIELD is a command projection, not a procedure CALL.
	"pkg/cypher:parseYieldClause":          "SHOW command",
	"pkg/cypher:StorageExecutor.applyShowTail": "SHOW command",
}

type boundProcedureDecider struct {
	Symbol string   `json:"symbol"`
	Kinds  []string `json:"kinds"`
	Sites  []string `json:"sites"`
}

func boundIsProcedureSpec(typ types.Type) bool {
	for {
		pointer, ok := types.Unalias(typ).(*types.Pointer)
		if !ok {
			break
		}
		typ = pointer.Elem()
	}
	named, ok := types.Unalias(typ).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == boundModule+"pkg/cypher" && named.Obj().Name() == "ProcedureSpec"
}

func boundProcedureDeciders(t *testing.T, receipt boundCompiledReceipt) []boundProcedureDecider {
	t.Helper()
	program := receipt.program
	found := map[string]*boundProcedureDecider{}
	for _, pkg := range program.Packages {
		if pkg.Types == nil || pkg.TypesInfo == nil || !strings.HasPrefix(pkg.Types.Path(), boundModule) {
			continue
		}
		info := pkg.TypesInfo
		for _, file := range pkg.Syntax {
			position := program.FileSet.Position(file.Pos())
			relative, _ := filepath.Rel(program.Root, position.Filename)
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
					if index, ok := receiver.(*ast.IndexExpr); ok {
						receiver = index.X
					}
					if ident, ok := receiver.(*ast.Ident); ok {
						name = ident.Name + "." + name
					}
				}
				symbol := strings.TrimPrefix(pkg.Types.Path(), boundModule) + ":" + name
				add := func(node ast.Node, kind string) {
					entry := found[symbol]
					if entry == nil {
						entry = &boundProcedureDecider{Symbol: symbol}
						found[symbol] = entry
					}
					entry.Kinds = append(entry.Kinds, kind)
					entry.Sites = append(entry.Sites, kind+" @"+relative+":"+strconv.Itoa(program.FileSet.Position(node.Pos()).Line))
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.CallExpr:
						var ident *ast.Ident
						switch callee := node.Fun.(type) {
						case *ast.Ident:
							ident = callee
						case *ast.SelectorExpr:
							ident = callee.Sel
						}
						if ident == nil {
							return true
						}
						object, ok := info.Uses[ident].(*types.Func)
						if !ok || object.Pkg() == nil || object.Pkg().Path() != boundModule+"pkg/cypher" {
							return true
						}
						if signature, ok := object.Type().(*types.Signature); ok && signature.Recv() != nil {
							if receiver := signature.Recv().Type(); (object.Name() == "Get" || object.Name() == "List") && boundNamedHolder(receiver) == "ProcedureRegistry" {
								add(node, "resolve")
							}
							return true
						}
						if kind, listed := boundProcedureDeciderFuncs[object.Name()]; listed {
							add(node, kind)
						}
					case *ast.SelectorExpr:
						if (node.Sel.Name == "MinArgs" || node.Sel.Name == "MaxArgs") && boundIsProcedureSpec(info.TypeOf(node.X)) {
							add(node, "arity")
						}
					case *ast.CompositeLit:
						if boundIsProcedureSpec(info.TypeOf(node)) {
							add(node, "spec")
						}
					}
					return true
				})
			}
		}
	}
	var out []boundProcedureDecider
	for _, entry := range found {
		entry.Kinds = boundUniqueStrings(entry.Kinds)
		entry.Sites = boundUniqueStrings(entry.Sites)
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

func boundProcedureDeciderProblems(deciders []boundProcedureDecider) []string {
	var problems []string
	members := map[string]bool{}
	for _, decider := range deciders {
		members[decider.Symbol] = true
		if _, listed := boundProcedureDeciderDispositions[decider.Symbol]; !listed {
			problems = append(problems, "UNLISTED procedure decider "+decider.Symbol+" | "+strings.Join(decider.Sites, " ; "))
		}
	}
	for symbol := range boundProcedureDeciderDispositions {
		if !members[symbol] {
			problems = append(problems, "STALE disposition "+symbol)
		}
	}
	sort.Strings(problems)
	return problems
}

func TestBoundPlanProcedureDeciders(t *testing.T) {
	deciders := boundProcedureDeciders(t, boundCompiledQuery(t, boundPlanCensusConfig()))
	for _, decider := range deciders {
		t.Logf("PROCEDURE_DECIDER %s [%s] %s", decider.Symbol, boundProcedureDeciderDispositions[decider.Symbol], strings.Join(decider.Sites, " ; "))
	}
	if problems := boundProcedureDeciderProblems(deciders); len(problems) > 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
	t.Logf("PROCEDURE_DECIDERS count=%d outside=0", len(deciders))
}

// Controls: a second resolver planted in pkg/cypher and a signature
// constructed in pkg/server are reported as unlisted; a same-spelling Get on
// another type is not a member; removing the plants restores the population.
func TestBoundPlanProcedureDecidersControls(t *testing.T) {
	config := boundPlanCensusConfig()
	baseline := boundProcedureDeciders(t, boundCompiledQuery(t, config))
	config.Overlay = map[string][]byte{
		filepath.Join(config.Dir, "pkg/cypher/r73_planted_decider.go"): []byte(`package cypher
func plantedSecondResolver(name string) bool { _, found := globalProcedureRegistry.Get(name); return found }
`),
		filepath.Join(config.Dir, "pkg/server/r73_planted_decider.go"): []byte(`package server
import "github.com/orneryd/nornicdb/pkg/cypher"
func plantedServerSignature() cypher.ProcedureSpec { return cypher.ProcedureSpec{Name: "planted.call", MinArgs: 1} }
type plantedCatalog map[string]int
func (catalog plantedCatalog) Get(name string) int { return catalog[name] }
func plantedDecoyLookup(catalog plantedCatalog) int { return catalog.Get("db.labels") }
`),
	}
	planted := boundProcedureDeciders(t, boundCompiledQuery(t, config))
	problems := strings.Join(boundProcedureDeciderProblems(planted), "\n")
	for _, symbol := range []string{"pkg/cypher:plantedSecondResolver", "pkg/server:plantedServerSignature"} {
		if !strings.Contains(problems, "UNLISTED procedure decider "+symbol+" ") {
			t.Fatalf("planted decider %s escaped:\n%s", symbol, problems)
		}
	}
	if strings.Contains(problems, "plantedDecoyLookup") || strings.Contains(problems, "plantedCatalog") {
		t.Fatalf("decoy entered the decider population:\n%s", problems)
	}
	if len(planted) != len(baseline)+2 {
		t.Fatalf("planted population %d, baseline %d", len(planted), len(baseline))
	}
	config.Overlay = nil
	restored := boundProcedureDeciders(t, boundCompiledQuery(t, config))
	if len(restored) != len(baseline) {
		t.Fatal("plant removal changed the decider population")
	}
	for i := range restored {
		if restored[i].Symbol != baseline[i].Symbol || strings.Join(restored[i].Sites, ";") != strings.Join(baseline[i].Sites, ";") {
			t.Fatal("plant removal did not restore the exact decider population")
		}
	}
	t.Logf("PROCEDURE_DECIDER_PLANT resolver=detected signature=detected decoy=excluded baseline=%d planted=%d restored=%d", len(baseline), len(planted), len(restored))
}
