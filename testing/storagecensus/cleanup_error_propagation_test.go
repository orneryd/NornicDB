package storagecensus

import (
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// realComposePath is the module's error-composition owner, pinned here as a
// literal so a change to the analyser's identity cannot silently keep this test
// passing.
const realComposePath = "github.com/orneryd/nornicdb/pkg/errors/compose"

// TestComposeOutcomesKeepsPrimary pins the contract the census relies on when it
// treats a deferred composition as propagation: the composition owner keeps its
// primary failure, so a constructor that composes a cleanup failure with its own
// failure still returns an error that carries the original one. The census
// trusts the owner by identity, so that identity is worth the trust only while
// the shipped implementation behaves this way.
func TestComposeOutcomesKeepsPrimary(t *testing.T) {
	read := errors.New("catalog read failed")
	wrap := fmt.Errorf("open catalog: %w", read)
	cleanup := errors.New("close failed")
	t.Run("primary survives composition", func(t *testing.T) {
		outcome := compose.Outcomes(wrap, cleanup)
		require.ErrorIs(t, outcome, read, "the original read error must stay in the returned error's chain")
		require.ErrorIs(t, outcome, wrap, "the constructor's own wrap must stay in the chain")
		require.ErrorIs(t, outcome, cleanup, "the cleanup failure must be reported too")
	})
	t.Run("primary is first", func(t *testing.T) {
		require.True(t, strings.HasPrefix(compose.Outcomes(wrap, cleanup).Error(), wrap.Error()),
			"the primary failure leads the composed message")
	})
	t.Run("a lone failure is returned as it is", func(t *testing.T) {
		require.Equal(t, wrap, compose.Outcomes(wrap, nil), "a nil cleanup failure adds nothing")
		require.Equal(t, cleanup, compose.Outcomes(nil, cleanup), "without a primary the cleanup failure is the outcome")
		require.Nil(t, compose.Outcomes(nil, nil))
	})
	t.Run("a failure the primary wraps is not repeated", func(t *testing.T) {
		require.Equal(t, wrap, compose.Outcomes(wrap, read), "the primary already wraps this failure")
	})
}

// cleanupFixtureSource is the engine's constructor shapes around a read: the two
// real constructors' deferred cleanup, and near misses that must stay
// decisions. The positive cases import this module's real composition owner; the
// package-level Outcomes is a same-shaped local helper that must not count.
const cleanupFixtureSource = `package storage
 import (
  "fmt"
  "github.com/orneryd/nornicdb/pkg/errors/compose"
 )
 var state int
 var armState int
 var errClosed error = fmt.Errorf("closed")
 func source() error { return nil }
 func cleanup() error { return nil }
 func stop() error { return nil }
 func other() error { return nil }
 func conditional() error { return nil }
 func beginShutdown() {}
 func release() {}
 func body() { state++ }
 func Outcomes(primary error, others ...error) error { return nil }
 func open() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = compose.Outcomes(openErr, cleanup())
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 func build() (result *int, newErr error) {
  releaseConstruction := release
  defer func() {
   if newErr == nil && conditional() != nil {
    newErr = errClosed
    result = nil
   }
   if newErr != nil { beginShutdown() }
   releaseConstruction()
   if newErr != nil { newErr = compose.Outcomes(newErr, stop()) }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("server: %w", err) }
  body()
  return nil, nil
 }
 func wrapOnly() error {
  err := source()
  if err != nil { return fmt.Errorf("catalog: %w", err) }
  body()
  return nil
 }
 func dropped() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = compose.Outcomes(nil, cleanup())
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 func swallowed() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = cleanup()
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 func replaced() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = fmt.Errorf("cleanup failed: %v", cleanup())
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 func lookalikeOutcomes() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = Outcomes(openErr, cleanup())
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 var escaped **int
 func armEscapedNil() (result *int, openErr error) {
  var alias *int = new(int)
  escaped = &alias
  defer func() { if openErr != nil { openErr = compose.Outcomes(openErr,cleanup()) } }()
  err := source()
  if err != nil { alias=nil; return nil,fmt.Errorf("catalog: %w",err) }
  body()
  return nil,nil
 }
 func armEscapedResult() (result *int, openErr error) {
  escaped=&result
  defer func() { if openErr != nil { openErr=compose.Outcomes(openErr,cleanup()) } }()
  err:=source()
  if err!=nil { return nil,fmt.Errorf("catalog: %w",err) }
  body()
  return nil,nil
 }
 func armEffect() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = compose.Outcomes(openErr, cleanup())
   }
  }()
  err := source()
  if err != nil {
   armState++
   return nil, fmt.Errorf("catalog: %w", err)
  }
  body()
  return nil, nil
 }
 func armStore(scratch *int) (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = compose.Outcomes(openErr, cleanup())
   }
  }()
  err := source()
  if err != nil {
   *scratch = armState
   return nil, fmt.Errorf("catalog: %w", err)
  }
  body()
  return nil, nil
 }
 func armCall() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = compose.Outcomes(openErr, cleanup())
   }
  }()
  err := source()
  if err != nil {
   release()
   return nil, fmt.Errorf("catalog: %w", err)
  }
  body()
  return nil, nil
 }
 func captureWithoutCompose() (result *int, openErr error) {
  defer func() {
   if openErr == nil { body() }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 func closureWrap() (result *int, openErr error) {
  defer func() {
   if openErr != nil {
    openErr = fmt.Errorf("open failed: %w", openErr)
   }
  }()
  err := source()
  if err != nil { return nil, fmt.Errorf("catalog: %w", err) }
  body()
  return nil, nil
 }
 type BadgerEngine struct{}
 func (b *BadgerEngine) filterEntityByDecay() bool { return false }
 func suppressionGates(b *BadgerEngine) { if b.filterEntityByDecay() { body() } }
`

// cleanupFixture builds the fixture package in one SSA program with this
// module's real composition owner, parsed from pkg/errors/compose/compose.go. A
// local stand-in would prove nothing about the owner the module ships.
func cleanupFixture(t *testing.T, source string) (*program, *ssa.Package) {
	t.Helper()
	set := token.NewFileSet()
	composePath := filepath.Join("..", "..", "pkg", "errors", "compose", "compose.go")
	composeSource, err := os.ReadFile(composePath)
	require.NoError(t, err)
	composeFile, err := parser.ParseFile(set, composePath, composeSource, 0)
	require.NoError(t, err)
	composeTypes := types.NewPackage(realComposePath, "compose")
	composeInfo := fixtureInfo()
	require.NoError(t, types.NewChecker(&types.Config{Importer: importer.Default()}, set, composeTypes, composeInfo).Files([]*ast.File{composeFile}))

	file, err := parser.ParseFile(set, "coverage.go", source, 0)
	require.NoError(t, err)
	pkgTypes := types.NewPackage(Module+"pkg/storage", "storage")
	info := fixtureInfo()
	resolve := fixtureImporter{compose: composeTypes, fallback: importer.Default()}
	require.NoError(t, types.NewChecker(&types.Config{Importer: resolve}, set, pkgTypes, info).Files([]*ast.File{file}))

	prog := ssa.NewProgram(set, ssa.SanityCheckFunctions)
	created := map[*types.Package]bool{}
	var createAll func(pkgs []*types.Package)
	createAll = func(pkgs []*types.Package) {
		for _, imported := range pkgs {
			if created[imported] {
				continue
			}
			created[imported] = true
			if imported == composeTypes {
				createAll(composeTypes.Imports())
				prog.CreatePackage(composeTypes, []*ast.File{composeFile}, composeInfo, true).Build()
				continue
			}
			prog.CreatePackage(imported, nil, nil, true)
			createAll(imported.Imports())
		}
	}
	createAll(pkgTypes.Imports())
	pkg := prog.CreatePackage(pkgTypes, []*ast.File{file}, info, false)
	pkg.Build()
	return &program{fset: set, ssa: prog}, pkg
}

// fixtureImporter resolves the module's composition owner to the package this
// fixture checked, so the analyser sees the real identity and the real body.
type fixtureImporter struct {
	compose  *types.Package
	fallback types.Importer
}

func (i fixtureImporter) Import(path string) (*types.Package, error) {
	if path == realComposePath {
		return i.compose, nil
	}
	return i.fallback.Import(path)
}

func fixtureInfo() *types.Info {
	return &types.Info{
		Types:        make(map[ast.Expr]types.TypeAndValue),
		Defs:         make(map[*ast.Ident]types.Object),
		Uses:         make(map[*ast.Ident]types.Object),
		Implicits:    make(map[ast.Node]types.Object),
		Instances:    make(map[*ast.Ident]types.Instance),
		Scopes:       make(map[ast.Node]*types.Scope),
		Selections:   make(map[*ast.SelectorExpr]*types.Selection),
		FileVersions: make(map[*ast.File]string),
	}
}

// fixtureBodyAnalysis is the census's own body-write analysis over the fixture,
// so a branch this proof rejects is reported as a hidden disposition.
func fixtureBodyAnalysis(t *testing.T, p *program, pkg *ssa.Package) *decider {
	t.Helper()
	a := &analysis{p: p, bodyMarks: map[ssa.Instruction]bool{}, bodyChains: map[ssa.Instruction]map[string][]string{}}
	for fn := range ssautil.AllFunctions(pkg.Prog) {
		if fn.Pkg != pkg {
			continue
		}
		p.funcs = append(p.funcs, fn)
		if fn.Name() != "body" {
			continue
		}
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				if _, ok := ins.(*ssa.Store); ok {
					a.bodyMarks[ins] = true
					a.bodyChains[ins] = map[string][]string{"body": {p.funcName(fn) + " body"}}
				}
			}
		}
	}
	a.markBodyWriters()
	return &decider{a: a}
}

// TestDeferredCleanupErrorPropagation keeps a constructor's own failure the
// census's propagation case even when the constructor composes its cleanup
// failure with it, and keeps every shape that could lose, replace or swallow
// that failure a decision.
func TestDeferredCleanupErrorPropagation(t *testing.T) {
	p, pkg := cleanupFixture(t, cleanupFixtureSource)
	d := fixtureBodyAnalysis(t, p, pkg)
	for _, tc := range []struct {
		name string
		want bool
		// primary pins whether the deferred cleanup composes this cell's own
		// error as the composition owner's primary failure.
		primary bool
		// cell pins whether the failing arm returns through a named result cell.
		cell bool
	}{
		{"open", true, true, true},
		{"build", true, true, true},
		{"wrapOnly", true, false, false},
		{"dropped", false, false, true},
		{"swallowed", false, false, true},
		{"replaced", false, false, true},
		{"lookalikeOutcomes", false, false, true},
		{"armEscapedNil", false, true, true},
		{"armEscapedResult", false, true, true},
		{"armEffect", false, true, true},
		{"armStore", false, true, true},
		{"armCall", false, true, true},
		{"captureWithoutCompose", false, false, true},
		{"closureWrap", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn := pkg.Func(tc.name)
			require.NotNil(t, fn)
			branch := fixtureBranch(t, fn)
			// Every fixture branch gates a body write in its success arm, so a
			// branch this proof rejects becomes a decision the census reports.
			require.NotEmpty(t, d.gatedWrites(fn, branch.Block()), "fixture branch must gate a body write")
			errv, _, nilComparison := workcensus.NilComparison(branch.Cond)
			require.True(t, nilComparison, "the fixture guards on the read error")
			cell, wrap := fixtureFailureCell(t, fn)
			if tc.cell {
				require.NotNil(t, cell, "the failing arm returns through its named result cell")
				require.NotNil(t, wrap, "the failing arm wraps the read error")
				require.True(t, fixtureWrapCarries(t, wrap, errv), "the wrap carries the exact read error")
				require.Equal(t, tc.primary, fixtureComposePrimary(t, fn, cell),
					"only the shapes this proof accepts compose the cell's own error as the primary failure")
			}
			require.Equal(t, tc.want, d.propagatesError(branch),
				"propagatesError must report whether this branch only fails with the read")
		})
	}

	t.Run("suppressionGates", func(t *testing.T) {
		fn := pkg.Func("suppressionGates")
		require.NotNil(t, fn)
		branch := fixtureBranch(t, fn)
		require.NotEmpty(t, d.gatedWrites(fn, branch.Block()), "the hidden-policy branch gates a body write")
		require.False(t, d.propagatesError(branch), "a hidden-policy read decides, it does not propagate")
	})
}

// fixtureWrapCarries reports whether the wrap call stores the exact read error
// into its own variadic argument array.
func fixtureWrapCarries(t *testing.T, call *ssa.Call, errv ssa.Value) bool {
	t.Helper()
	spread, ok := call.Common().Args[1].(*ssa.Slice)
	require.True(t, ok, "the wrap takes one variadic slice")
	array, ok := spread.X.(*ssa.Alloc)
	require.True(t, ok, "the variadic slice holds a local array")
	for _, ref := range *array.Referrers() {
		index, ok := ref.(*ssa.IndexAddr)
		if !ok {
			continue
		}
		for _, use := range *index.Referrers() {
			store, ok := use.(*ssa.Store)
			if !ok {
				continue
			}
			switch value := store.Val.(type) {
			case *ssa.ChangeInterface:
				return value.X == errv
			case *ssa.MakeInterface:
				return value.X == errv
			}
		}
	}
	return false
}

// fixtureFailureCell reads back what the fixture's failing arm stores: the named
// error result cell and the wrap call it holds.
func fixtureFailureCell(t *testing.T, fn *ssa.Function) (*ssa.Alloc, *ssa.Call) {
	t.Helper()
	var cell *ssa.Alloc
	var wrap *ssa.Call
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			store, ok := ins.(*ssa.Store)
			if !ok {
				continue
			}
			alloc, ok := store.Addr.(*ssa.Alloc)
			if !ok || !fixtureIsErrorCell(alloc) {
				continue
			}
			call, ok := store.Val.(*ssa.Call)
			if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "Errorf" {
				continue
			}
			require.Nil(t, cell, "one named error result cell")
			cell, wrap = alloc, call
		}
	}
	return cell, wrap
}

func fixtureIsErrorCell(alloc *ssa.Alloc) bool {
	pointer, ok := alloc.Type().Underlying().(*types.Pointer)
	return ok && types.Identical(pointer.Elem(), types.Universe.Lookup("error").Type())
}

// fixtureComposePrimary reports whether a closure deferred in fn composes a load
// of cell as the primary failure of the composition owner's Outcomes.
func fixtureComposePrimary(t *testing.T, fn *ssa.Function, cell *ssa.Alloc) bool {
	t.Helper()
	found := false
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			deferInstruction, ok := ins.(*ssa.Defer)
			if !ok {
				continue
			}
			closure, ok := deferInstruction.Common().Value.(*ssa.MakeClosure)
			if !ok {
				continue
			}
			captured, ok := closure.Fn.(*ssa.Function)
			if !ok {
				continue
			}
			index := -1
			for i, binding := range closure.Bindings {
				if binding == cell {
					index = i
				}
			}
			if index < 0 {
				continue
			}
			free := captured.FreeVars[index]
			for _, other := range captured.Blocks {
				for _, instruction := range other.Instrs {
					call, ok := instruction.(*ssa.Call)
					if !ok || len(call.Common().Args) == 0 {
						continue
					}
					callee := call.Common().StaticCallee()
					if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg == nil || callee.Pkg.Pkg.Path() != realComposePath {
						continue
					}
					primary, ok := call.Common().Args[0].(*ssa.UnOp)
					if ok && primary.Op == token.MUL && primary.X == free {
						found = true
					}
				}
			}
		}
	}
	return found
}

// TestRealConstructorCleanupPropagatesReadFailure is the product outcome: the two
// constructors that register their own cleanup and compose it with their own
// failure still fail with the read, so the census does not report them as hidden
// dispositions. They are named as the subject of the test; the proof itself reads
// their shape and never their names.
func TestRealConstructorCleanupPropagatesReadFailure(t *testing.T) {
	p, err := load(root, nil)
	require.NoError(t, err)
	for _, name := range []string{"pkg/nornicdb.Open", "pkg/server.New"} {
		t.Run(name, func(t *testing.T) {
			fn := censusFunction(t, p, name)
			d := &decider{}
			var accepted []string
			for _, block := range fn.Blocks {
				iff, ok := block.Instrs[len(block.Instrs)-1].(*ssa.If)
				if !ok {
					continue
				}
				errv, _, nilComparison := workcensus.NilComparison(iff.Cond)
				if !nilComparison || !realSingleWrapArm(iff, errv) {
					continue
				}
				require.True(t, d.propagatesError(iff),
					"%s %s: a deferred cleanup that composes the constructor's own failure keeps that failure in the returned error",
					name, p.pos(errv.Pos()))
				accepted = append(accepted, p.pos(errv.Pos()))
			}
			require.NotEmpty(t, accepted, "the constructor must still return the read error through its deferred cleanup")
			fmt.Printf("%s: deferred-cleanup wrap arms accepted at %v\n", name, accepted)
		})
	}
}

func censusFunction(t *testing.T, p *program, name string) *ssa.Function {
	t.Helper()
	for _, fn := range p.funcs {
		if p.funcName(fn) == name {
			return fn
		}
	}
	t.Fatalf("missing %s", name)
	return nil
}

// realSingleWrapArm reports whether an arm of branch returns a single-argument
// fmt.Errorf wrap of errv. It is derived here, independently of the analyser, so
// the branches this test selects cannot follow the proof under test.
func realSingleWrapArm(iff *ssa.If, errv ssa.Value) bool {
	for _, arm := range iff.Block().Succs {
		for _, ins := range arm.Instrs {
			call, ok := ins.(*ssa.Call)
			if !ok || len(call.Common().Args) != 2 {
				continue
			}
			callee := call.Common().StaticCallee()
			if callee == nil || callee.Pkg == nil || callee.Pkg.Pkg == nil || callee.Pkg.Pkg.Path() != "fmt" || callee.Name() != "Errorf" {
				continue
			}
			spread, ok := call.Common().Args[1].(*ssa.Slice)
			if !ok {
				continue
			}
			array, ok := spread.X.(*ssa.Alloc)
			if !ok || array.Referrers() == nil {
				continue
			}
			pointer, ok := array.Type().Underlying().(*types.Pointer)
			if !ok {
				continue
			}
			element, ok := pointer.Elem().Underlying().(*types.Array)
			if !ok || element.Len() != 1 {
				continue
			}
			for _, ref := range *array.Referrers() {
				index, ok := ref.(*ssa.IndexAddr)
				if !ok || index.Referrers() == nil {
					continue
				}
				for _, use := range *index.Referrers() {
					store, ok := use.(*ssa.Store)
					if !ok {
						continue
					}
					switch value := store.Val.(type) {
					case *ssa.ChangeInterface:
						if value.X == errv {
							return true
						}
					case *ssa.MakeInterface:
						if value.X == errv {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
