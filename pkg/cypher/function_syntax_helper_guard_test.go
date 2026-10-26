package cypher

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// Follow helper results and aliases, so delegating a private lexical decision
// to an existing keyword/group helper cannot bypass the operation-shape guard.
func helperFunctionSyntaxShapes(body ast.Node) []string {
	keywords, openings, retained := map[string]bool{}, map[string]bool{}, map[string]bool{}
	contains := func(node ast.Node, names map[string]bool) bool {
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && names[id.Name] {
				found = true
			}
			return true
		})
		return found
	}
	for changed := true; changed; {
		changed = false
		ast.Inspect(body, func(node ast.Node) bool {
			a, ok := node.(*ast.AssignStmt)
			if !ok || len(a.Rhs) != 1 || len(a.Lhs) == 0 {
				return true
			}
			id, ok := a.Lhs[0].(*ast.Ident)
			if !ok {
				return true
			}
			word, open, call := contains(a.Rhs[0], keywords), contains(a.Rhs[0], openings), contains(a.Rhs[0], retained)
			if invoke, ok := a.Rhs[0].(*ast.CallExpr); ok {
				switch helper := invoke.Fun.(type) {
				case *ast.Ident:
					if helper.Name == "keywordIndexFrom" && len(invoke.Args) > 1 {
						_, literal := invoke.Args[1].(*ast.BasicLit)
						word = word || !literal
					}
					open = open || helper.Name == "queryGapEnd" && word
				case *ast.SelectorExpr:
					call = call || helper.Sel.Name == "callAt" || helper.Sel.Name == "callOpenedAt" || helper.Sel.Name == "callBeforeParen"
				}
			}
			for _, state := range []struct {
				names map[string]bool
				value bool
			}{{keywords, word}, {openings, open}, {retained, call}} {
				if state.value && !state.names[id.Name] {
					state.names[id.Name], changed = true, true
				}
			}
			return true
		})
	}
	reasons := map[string]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		if (name.Name == "findMatchingParen" || name.Name == "findMatchingDelimiter") && len(call.Args) > 1 && contains(call.Args[1], openings) {
			reasons["helper-based callable identification/group match"] = true
		}
		if name.Name == "isNonFunctionSemanticKeyword" && len(call.Args) == 1 && contains(call.Args[0], retained) {
			reasons["helper-based retained call reclassification"] = true
		}
		if name.Name == "findMatchingDelimiter" && len(call.Args) == 4 {
			zero, z := call.Args[1].(*ast.BasicLit)
			open, o := call.Args[2].(*ast.BasicLit)
			close, c := call.Args[3].(*ast.BasicLit)
			if z && o && c && zero.Value == "0" && open.Value == "'{'" && close.Value == "'}'" {
				reasons["helper-based whole map group match"] = true
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

func TestFunctionSyntaxHelperGuardMutants(t *testing.T) {
	for _, body := range []string{
		`index:=keywordIndexFrom(q,name,0,opts); open:=queryGapEnd(q,index+len(name)); close:=findMatchingParen(q,open); _=close`,
		`index:=keywordIndexFrom(q,name,0,opts); at:=index; open:=queryGapEnd(q,at+len(name)); alias:=open; findMatchingDelimiter(q,alias,'(',')')`,
		`return findMatchingDelimiter(q,0,'{','}')==len(q)-1`,
		`call,ok:=calls.callOpenedAt(open); alias:=call; return ok && !isNonFunctionSemanticKeyword(alias.normalized)`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "mutant.go", "package p; func f(q,name string){"+body+"}", 0)
		require.NoError(t, err)
		require.NotEmpty(t, helperFunctionSyntaxShapes(file.Decls[0].(*ast.FuncDecl)), body)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "control.go", "package p; func f(q string){ call,ok:=calls.callOpenedAt(open); return ok && call.expressionCall() }", 0)
	require.NoError(t, err)
	require.Empty(t, helperFunctionSyntaxShapes(file.Decls[0].(*ast.FuncDecl)))
}
