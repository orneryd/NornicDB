//go:build boundplan_census_operator

// Author: #1640 lane (Claude, Rizheg). Analysis-only client. Lists every
// function reachable from the admitted typed program that receives the typed
// environment (a type scope, an operator checker or the typed program) and
// branches on request-parameter syntax: exactly the readers that could read
// a typed atom as a request parameter and drop that environment.
package boundplancheck

import (
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// The admitted typed program: every type rule it runs is reachable from these.
var boundAdmittedTypeRoots = []workcensus.Root{
	{Package: boundModule + "pkg/cypher", Name: "clause", Receiver: "boundTypeValues", Required: true},
	{Package: boundModule + "pkg/cypher", Name: "body", Receiver: "boundTypeValues", Required: true},
	{Package: boundModule + "pkg/cypher", Name: "expression", Receiver: "boundTypeValues", Required: true},
	{Package: boundModule + "pkg/cypher", Name: "localOperand", Receiver: "boundTypeValues", Required: true},
	{Package: boundModule + "pkg/cypher", Name: "checkClauseOperandHandles", Receiver: "boundTypeValues", Required: true},
}

// Readers of request-parameter values by name.
var boundParameterReaders = map[string]bool{
	"getParamsFromContext": true, "staticParameterOperand": true, "parameterPropertyAccessType": true,
	"propertyAccessExpressionType": true, "parameterExpressionType": true,
}

// Types whose params field holds request-parameter values.
var boundParameterHolders = map[string]bool{"staticTypeScope": true, "staticOperatorChecker": true, "boundTypeValues": true}

type boundParameterReader struct {
	Function string   `json:"function"`
	File     string   `json:"file"`
	Sites    []string `json:"sites"`
}

func boundNamedHolder(typ types.Type) string {
	for {
		pointer, ok := types.Unalias(typ).(*types.Pointer)
		if !ok {
			break
		}
		typ = pointer.Elem()
	}
	if named, ok := types.Unalias(typ).(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == boundModule+"pkg/cypher" {
		return named.Obj().Name()
	}
	return ""
}

// boundReceivesTypedEnvironment reports whether a function receives the typed
// environment through its receiver or a parameter.
func boundReceivesTypedEnvironment(signature *types.Signature) bool {
	if signature.Recv() != nil && boundParameterHolders[boundNamedHolder(signature.Recv().Type())] {
		return true
	}
	for i := 0; i < signature.Params().Len(); i++ {
		if boundParameterHolders[boundNamedHolder(signature.Params().At(i).Type())] {
			return true
		}
	}
	return false
}

func boundAdmittedParameterReaders(t *testing.T, receipt boundCompiledReceipt, roots []workcensus.Root) []boundParameterReader {
	t.Helper()
	program := receipt.program
	reach, err := program.ReachableFrom(roots)
	if err != nil {
		t.Fatal(err)
	}
	infos := map[string]*types.Info{}
	for _, pkg := range program.Packages {
		if pkg.Types != nil {
			infos[pkg.Types.Path()] = pkg.TypesInfo
		}
	}
	seen := map[*ssa.Function]bool{}
	var out []boundParameterReader
	for fn := range reach.Functions {
		top := boundEnclosing(fn)
		if top == nil || seen[top] || top.Pkg == nil || top.Pkg.Pkg.Path() != boundModule+"pkg/cypher" {
			continue
		}
		seen[top] = true
		if !boundReceivesTypedEnvironment(top.Signature) {
			continue
		}
		syntax, ok := top.Syntax().(*ast.FuncDecl)
		info := infos[top.Pkg.Pkg.Path()]
		if !ok || syntax.Body == nil || info == nil {
			continue
		}
		position := program.FileSet.Position(syntax.Pos())
		file, _ := filepath.Rel(program.Root, position.Filename)
		file = filepath.ToSlash(file)
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		var sites []string
		site := func(node ast.Node, what string) {
			sites = append(sites, what+" @"+file+":"+strconv.Itoa(program.FileSet.Position(node.Pos()).Line))
		}
		ast.Inspect(syntax.Body, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.BasicLit:
				if (node.Kind == token.STRING && strings.Contains(node.Value, "$")) || (node.Kind == token.CHAR && node.Value == "'$'") {
					site(node, "dollar-literal "+node.Value)
				}
			case *ast.SelectorExpr:
				if node.Sel.Name == "params" {
					if holder := boundNamedHolder(info.TypeOf(node.X)); boundParameterHolders[holder] {
						site(node, "params-field "+holder)
					}
				}
			case *ast.CallExpr:
				var name *ast.Ident
				switch callee := node.Fun.(type) {
				case *ast.Ident:
					name = callee
				case *ast.SelectorExpr:
					name = callee.Sel
				}
				if name != nil && boundParameterReaders[name.Name] {
					if object, ok := info.Uses[name].(*types.Func); ok && object.Pkg() != nil && object.Pkg().Path() == boundModule+"pkg/cypher" {
						site(node, "parameter-reader "+name.Name)
					}
				}
			}
			return true
		})
		if len(sites) > 0 {
			out = append(out, boundParameterReader{Function: strings.TrimPrefix(top.String(), "github.com/orneryd/nornicdb/"), File: file, Sites: boundUniqueStrings(sites)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Function < out[j].Function })
	return out
}

// The readers that receive the typed environment and branch on parameter
// syntax, each dispositioned from source (1640-r8-parameter-reader-dispositions.json):
// every one keeps the environment. The compiled population must equal this set.
var boundTypedEnvironmentParameterReaders = map[string]bool{
	"(*pkg/cypher.boundTypeValues).expression":             true,
	"(*pkg/cypher.boundTypeValues).expressionText":         true,
	"(*pkg/cypher.boundTypeValues).operandView":            true,
	"(*pkg/cypher.boundTypeValues).view":                   true,
	"(pkg/cypher.staticOperatorChecker).argumentType":       true,
	"(pkg/cypher.staticOperatorChecker).checkAtom":          true,
	"(pkg/cypher.staticOperatorChecker).checkScoped":        true,
	"(pkg/cypher.staticOperatorChecker).maskMapProjections": true,
	"(pkg/cypher.staticTypeScope).parameterExpressionType":  true,
	"(pkg/cypher.staticTypeScope).staticExpressionType":     true,
	"pkg/cypher.staticPatternTokenError":                    true,
	"pkg/cypher.validateStaticPropertySubscripts":           true,
}

func TestBoundPlanAdmittedParameterReaders(t *testing.T) {
	readers := boundAdmittedParameterReaders(t, boundCompiledQuery(t, boundPlanCensusConfig()), boundAdmittedTypeRoots)
	var problems []string
	seen := map[string]bool{}
	for _, reader := range readers {
		name := strings.ReplaceAll(reader.Function, "github.com/orneryd/nornicdb/", "")
		seen[name] = true
		t.Logf("ADMITTED_PARAMETER_READER %s | %s", name, strings.Join(reader.Sites, " ; "))
		if !boundTypedEnvironmentParameterReaders[name] {
			problems = append(problems, "UNDISPOSITIONED typed-environment parameter reader "+name)
		}
	}
	for name := range boundTypedEnvironmentParameterReaders {
		if !seen[name] {
			problems = append(problems, "STALE typed-environment parameter reader "+name)
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatal(strings.Join(problems, "\n"))
	}
	t.Logf("ADMITTED_PARAMETER_READERS count=%d matches=12", len(readers))
}

// Controls: a planted dollar reader that receives the type scope and is
// reachable from a planted root is listed; the same reader unreachable is
// not; a reachable scope helper without parameter syntax is not; a reachable
// dollar reader without the typed environment is not; removing the plant
// restores the baseline.
func TestBoundPlanAdmittedParameterReadersControls(t *testing.T) {
	config := boundPlanCensusConfig()
	baseline := boundAdmittedParameterReaders(t, boundCompiledQuery(t, config), boundAdmittedTypeRoots)
	config.Overlay = map[string][]byte{filepath.Join(config.Dir, "pkg/cypher/r73_planted_param_reader.go"): []byte(`package cypher
import "strings"
func plantedAdmittedRoot(scope staticTypeScope, text string) bool { return plantedDollarReader(scope, text) && plantedPlainHelper(scope, text) && plantedEnvironmentFree(text) }
func plantedDollarReader(scope staticTypeScope, text string) bool { return strings.HasPrefix(text, "$") && scope.cypher25 }
func plantedPlainHelper(scope staticTypeScope, text string) bool { return text != "" && scope.cypher25 }
func plantedEnvironmentFree(text string) bool { return strings.HasPrefix(text, "$") }
func plantedUnreachableDollar(scope staticTypeScope, text string) bool { return strings.HasPrefix(text, "$") && scope.cypher25 }
`)}
	roots := append(append([]workcensus.Root(nil), boundAdmittedTypeRoots...), workcensus.Root{Package: boundModule + "pkg/cypher", Name: "plantedAdmittedRoot", Required: true})
	planted := boundAdmittedParameterReaders(t, boundCompiledQuery(t, config), roots)
	found := map[string]bool{}
	for _, reader := range planted {
		for _, name := range []string{"plantedDollarReader", "plantedPlainHelper", "plantedUnreachableDollar", "plantedAdmittedRoot", "plantedEnvironmentFree"} {
			if strings.HasSuffix(reader.Function, "."+name) {
				found[name] = true
			}
		}
	}
	if !found["plantedDollarReader"] {
		t.Fatal("reachable planted dollar reader not listed")
	}
	if found["plantedUnreachableDollar"] || found["plantedPlainHelper"] || found["plantedAdmittedRoot"] || found["plantedEnvironmentFree"] {
		t.Fatalf("decoy listed: %v", found)
	}
	if len(planted) != len(baseline)+1 {
		t.Fatalf("planted population %d, baseline %d", len(planted), len(baseline))
	}
	config.Overlay = nil
	restored := boundAdmittedParameterReaders(t, boundCompiledQuery(t, config), boundAdmittedTypeRoots)
	if len(restored) != len(baseline) {
		t.Fatal("plant removal changed the reader population")
	}
	for i := range restored {
		if restored[i].Function != baseline[i].Function || strings.Join(restored[i].Sites, ";") != strings.Join(baseline[i].Sites, ";") {
			t.Fatal("plant removal did not restore the exact reader population")
		}
	}
	t.Logf("PARAMETER_READER_PLANT reachable=listed unreachable=excluded plain=excluded baseline=%d planted=%d restored=%d", len(baseline), len(planted), len(restored))
}
