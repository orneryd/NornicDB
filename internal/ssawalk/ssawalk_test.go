package ssawalk

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

// fixture defines f, g and h: f has three statements and a return, and h's
// body is one call of a closure.
const fixture = `package fixture

func f(x int, y int, choose bool) int {
	z := x + y
	w := z * 2
	if choose {
		w = x
	}
	return h(w)
}

func g(m map[string][]byte) int {
	return len(m)
}

func h(n int) int {
	return func() int { return n * 3 }()
}
`

// bodies loads fixture and returns every function with a body, outermost
// first and each function's closures after it, as the walkers are given them.
func bodies(t *testing.T) []*ssa.Function {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", fixture, parser.SkipObjectResolution)
	require.NoError(t, err)
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
		Implicits: map[ast.Node]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Scopes: map[ast.Node]*types.Scope{},
		Instances: map[*ast.Ident]types.Instance{}, FileVersions: map[*ast.File]string{},
	}
	pkg, err := (&types.Config{}).Check("fixture", fset, []*ast.File{file}, info)
	require.NoError(t, err)
	program := ssa.NewProgram(fset, ssa.SanityCheckFunctions)
	ssaPkg := program.CreatePackage(pkg, []*ast.File{file}, info, true)
	ssaPkg.Build()
	var functions []*ssa.Function
	for _, name := range []string{"f", "g", "h"} {
		fn, ok := ssaPkg.Members[name].(*ssa.Function)
		require.True(t, ok, name)
		functions = append(functions, fn)
		functions = append(functions, fn.AnonFuncs...)
	}
	return functions
}

// steps is the chain in f's own body: the add, the multiply of it, the phi
// that joins the two assignments to w, and the call that consumes the phi.
type steps struct {
	add, multiply, phi, call ssa.Value
}

func fSteps(t *testing.T, f *ssa.Function) steps {
	t.Helper()
	var s steps
	for _, block := range f.Blocks {
		for _, ins := range block.Instrs {
			switch v := ins.(type) {
			case *ssa.BinOp:
				switch v.Op {
				case token.ADD:
					s.add = v
				case token.MUL:
					s.multiply = v
				}
			case *ssa.Phi:
				s.phi = v
			case *ssa.Call:
				s.call = v
			}
		}
	}
	require.NotNil(t, s.add, "fixture add")
	require.NotNil(t, s.multiply, "fixture multiply")
	require.NotNil(t, s.phi, "fixture phi")
	require.NotNil(t, s.call, "fixture call")
	return s
}

func TestInstructionsEnumerateBodiesInListOrder(t *testing.T) {
	functions := bodies(t)
	var want, got []string
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				want = append(want, fn.Name()+" "+ins.String())
			}
		}
	}
	for _, step := range Instructions(functions) {
		require.Same(t, step.Func, step.Ins.Parent(), "%s belongs to its function", step.Ins)
		got = append(got, step.Func.Name()+" "+step.Ins.String())
	}
	require.Equal(t, want, got)
	require.NotEmpty(t, got)
}

func TestValuesEnumerateParametersFreeVariablesAndInstructions(t *testing.T) {
	functions := bodies(t)
	var want, got []string
	for _, fn := range functions {
		for _, parameter := range fn.Params {
			want = append(want, fn.Name()+" "+parameter.String())
		}
		for _, free := range fn.FreeVars {
			want = append(want, fn.Name()+" "+free.String())
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if value, ok := ins.(ssa.Value); ok {
					want = append(want, fn.Name()+" "+value.String())
				}
			}
		}
	}
	for _, value := range Values(functions) {
		got = append(got, value.Parent().Name()+" "+value.String())
	}
	require.Equal(t, want, got)
	free := 0
	for _, fn := range functions {
		free += len(fn.FreeVars)
	}
	require.Positive(t, free, "the fixture must define a free variable")
}

// knownBuilds a graph in which only f's own chain and its first parameter are
// classified, so the edge predicate decides what the graph holds.
func knownBuilds(f *ssa.Function, s steps) *Graph {
	g := NewGraph()
	known := map[ssa.Value]bool{s.add: true, s.multiply: true, s.phi: true, s.call: true}
	for _, value := range Values([]*ssa.Function{f}) {
		g.AddValue(value)
	}
	edge := func(src, dst ssa.Value) bool { return known[src] && known[dst] }
	g.Add(s.add, s.multiply, edge)
	g.Add(f.Params[0], s.multiply, edge)
	g.Add(s.multiply, s.phi, edge)
	g.Add(f.Params[0], s.phi, edge)
	g.Add(s.phi, s.call, edge)
	return g
}

func TestGraphWalkReachesDependentsFromStarts(t *testing.T) {
	f := bodies(t)[0]
	s := fSteps(t, f)
	g := knownBuilds(f, s)

	all := g.From([]ssa.Value{s.add}, -1)
	require.True(t, all.Reached(s.add))
	require.True(t, all.Reached(s.multiply))
	require.True(t, all.Reached(s.phi))
	require.True(t, all.Reached(s.call))
	require.False(t, all.Reached(f.Params[0]), "a walk follows edges forwards from its starts only")
	require.False(t, all.Exhausted())

	// A nil start and a value with no dependents reach only themselves.
	require.False(t, g.From([]ssa.Value{nil}, -1).Reached(s.phi))
	require.True(t, g.From([]ssa.Value{s.call}, -1).Reached(s.call))

	// Add ignores an edge whose source or destination is unclassified.
	rejecting := NewGraph()
	rejecting.Add(s.add, s.multiply, func(src, dst ssa.Value) bool { return false })
	rejecting.Add(nil, s.multiply, func(src, dst ssa.Value) bool { return true })
	rejecting.Add(s.add, nil, func(src, dst ssa.Value) bool { return true })
	require.False(t, rejecting.From([]ssa.Value{s.add}, -1).Reached(s.multiply))
}

func TestGraphWalkBudgetStopsMarkingAndSaysSo(t *testing.T) {
	f := bodies(t)[0]
	s := fSteps(t, f)
	g := knownBuilds(f, s)

	// Four values depend on add: add itself, multiply, phi and the call.
	partial := g.From([]ssa.Value{s.add}, 2)
	marked := 0
	for _, value := range []ssa.Value{s.add, s.multiply, s.phi, s.call} {
		if partial.Reached(value) {
			marked++
		}
	}
	require.Equal(t, 2, marked, "the budget bounds the values a walk marks")
	require.True(t, partial.Exhausted(), "a walk that stopped at its budget says so")

	exact := g.From([]ssa.Value{s.add}, 4)
	require.True(t, exact.Reached(s.call))
	require.False(t, exact.Exhausted(), "a budget of exactly the reachable count is enough")

	unbounded := g.From([]ssa.Value{s.add}, -1)
	require.True(t, unbounded.Reached(s.call))
	require.False(t, unbounded.Exhausted())
}

func TestGraphKnownIsOnlyWhatTheConsumerClassifies(t *testing.T) {
	functions := bodies(t)
	values := Values(functions)
	require.NotEmpty(t, values)
	g := NewGraph()
	require.False(t, g.Known(values[0]))
	g.AddValue(values[0])
	require.True(t, g.Known(values[0]))
	require.False(t, g.Known(values[len(values)-1]), "AddValue is what classifies a value")
}
