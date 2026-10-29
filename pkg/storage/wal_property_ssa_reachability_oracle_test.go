package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// Compare the prefilter with the original traversal over a small, fully
// enumerable graph. It contains parameter/return frames, closures, field
// addresses, map lookups, aliases, phi nodes and a recursive call cycle.
func TestBindingSSAReachabilityMatchesTraversal(t *testing.T) {
	const source = `package fixture
	type holder struct { writes map[string][]byte; other map[string][]byte }
	func direct(h *holder) []byte { return h.writes["x"] }
	func helper(m map[string][]byte) (int, []byte) { return 1, m["x"] }
	func pass(h *holder) []byte { _, v := helper(h.writes); return v }
	func unrelated(h *holder) []byte { return h.other["x"] }
	func choose(h *holder, b bool) []byte { m := h.other; if b { m = h.writes }; return m["x"] }
	func closure(h *holder) []byte { m := h.writes; return func() []byte { return m["x"] }() }
	func aggregate(h *holder) []byte { m := map[string][]byte{}; m["x"] = h.writes["x"]; return m["x"] }
	func recursive(m map[string][]byte, n int) []byte { if n == 0 { return m["x"] }; return recursive(m, n-1) }
	func recursionEntry(h *holder) []byte { return recursive(h.writes, 2) }
	func slice(h *holder) []byte { v := direct(h); return v[:] }
	`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, parser.SkipObjectResolution)
	require.NoError(t, err)
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
		Implicits: map[ast.Node]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Scopes: map[ast.Node]*types.Scope{},
		Instances: map[*ast.Ident]types.Instance{}, FileVersions: map[*ast.File]string{},
	}
	pkg, err := (&types.Config{}).Check("fixture", fset, []*ast.File{file}, info)
	require.NoError(t, err)
	prog := ssa.NewProgram(fset, ssa.SanityCheckFunctions)
	ssaPkg := prog.CreatePackage(pkg, []*ast.File{file}, info, true)
	ssaPkg.Build()
	var funcs []*ssa.Function
	seen := map[*ssa.Function]bool{}
	var add func(*ssa.Function)
	add = func(fn *ssa.Function) {
		if fn == nil || seen[fn] {
			return
		}
		seen[fn] = true
		funcs = append(funcs, fn)
		for _, child := range fn.AnonFuncs {
			add(child)
		}
	}
	for _, member := range ssaPkg.Members {
		if fn, ok := member.(*ssa.Function); ok {
			add(fn)
		}
	}
	a := &bindingSSAAnalysis{calls: map[*ssa.Function][]ssa.CallInstruction{}, closures: map[*ssa.Function][]*ssa.MakeClosure{}, stores: map[*ssa.Function][]*ssa.Store{}}
	var values []ssa.Value
	for _, fn := range funcs {
		for _, p := range fn.Params {
			values = append(values, p)
		}
		for _, p := range fn.FreeVars {
			values = append(values, p)
		}
		for _, block := range fn.Blocks {
			for _, instruction := range block.Instrs {
				if v, ok := instruction.(ssa.Value); ok {
					values = append(values, v)
				}
				if c, ok := instruction.(ssa.CallInstruction); ok {
					if callee := c.Common().StaticCallee(); callee != nil {
						a.calls[callee] = append(a.calls[callee], c)
					}
				}
				if c, ok := instruction.(*ssa.MakeClosure); ok {
					callee := c.Fn.(*ssa.Function)
					a.closures[callee] = append(a.closures[callee], c)
				}
				if s, ok := instruction.(*ssa.Store); ok {
					a.stores[fn] = append(a.stores[fn], s)
				}
			}
		}
	}
	fields := pkg.Scope().Lookup("holder").Type().Underlying().(*types.Struct)
	for i := 0; i < fields.NumFields(); i++ {
		a.writes = fields.Field(i)
		g := newBindingSSAReachability(a, funcs)
		g.setRoot(a.writes)
		positives, pruned := 0, 0
		for _, value := range values {
			if !bindingSSAReference(value.Type()) {
				continue
			}
			a.reachability = nil
			want := a.alias(value, nil, map[ssa.Value]bool{})
			a.reachability = g
			got := a.alias(value, nil, map[ssa.Value]bool{})
			require.Equal(t, want, got, "root %s value %s in %s", a.writes.Name(), value, value.Parent())
			if want {
				positives++
			}
			if !g.mayReach(value) {
				pruned++
			}
		}
		require.Positive(t, positives, "fixture must actually reach %s", a.writes.Name())
		require.Positive(t, pruned, "fixture must exercise pruning for %s", a.writes.Name())
	}
}
