package cypher

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"
	"testing"
)

func relationshipMeasurementBenchmarkType(kind types.Type) bool {
	if kind == nil {
		return false
	}
	kind = types.Unalias(kind)
	if pointer, ok := kind.(*types.Pointer); ok {
		kind = types.Unalias(pointer.Elem())
	}
	named, ok := kind.(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "testing" && named.Obj().Name() == "B"
}

// Only a definite pause of the loop's own testing.B removes preparation from
// benchmark measurement. Conditional timers, another receiver, captured timer
// callbacks and unresolved function values cannot establish that proof.
func relationshipMeasurementPausedCalls(body *ast.BlockStmt, benchmark types.Object, info *types.Info, invocations map[*ast.CallExpr]map[*types.Func]bool, mutatesTimer map[*types.Func]bool) map[*ast.CallExpr]bool {
	pausedCalls := map[*ast.CallExpr]bool{}
	if benchmark == nil {
		return pausedCalls
	}
	paused := false
	for _, statement := range body.List {
		if expression, ok := statement.(*ast.ExprStmt); ok {
			if call, ok := expression.X.(*ast.CallExpr); ok && len(call.Args) == 0 {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					if receiver, ok := selector.X.(*ast.Ident); ok && info.Uses[receiver] == benchmark && relationshipMeasurementBenchmarkType(info.TypeOf(receiver)) {
						switch selector.Sel.Name {
						case "StopTimer":
							paused = true
							continue
						case "StartTimer":
							paused = false
							continue
						case "ResetTimer":
							continue // ResetTimer preserves the current running state.
						}
					}
				}
			}
		}
		if !paused {
			continue
		}
		unsafe := false
		ast.Inspect(statement, func(node ast.Node) bool {
			if _, ok := node.(*ast.FuncLit); ok {
				unsafe = true
				return false
			}
			if expression, ok := node.(ast.Expr); ok && relationshipMeasurementBenchmarkType(info.TypeOf(expression)) {
				unsafe = true
			}
			if call, ok := node.(*ast.CallExpr); ok {
				targets := invocations[call]
				for target := range targets {
					if mutatesTimer[target] {
						unsafe = true
					}
				}
				if _, signature := info.TypeOf(call.Fun).(*types.Signature); signature && len(targets) == 0 {
					builtin := false
					if name, ok := call.Fun.(*ast.Ident); ok {
						_, builtin = info.Uses[name].(*types.Builtin)
					}
					if !builtin {
						unsafe = true
					}
				}
			}
			return true
		})
		if unsafe {
			paused = false
			continue
		}
		for call := range invocations {
			if statement.Pos() <= call.Pos() && call.End() <= statement.End() {
				pausedCalls[call] = true
			}
		}
	}
	return pausedCalls
}

// Admission is setup for a private-kernel fixture. Public Execute benchmarks
// still measure real statement admission. Derive all measured fixture callers
// from compiler objects and invocation edges, including aliases and closures.
func relationshipFixtureMeasurementAdmission(fset *token.FileSet, files []*ast.File, info *types.Info) ([]string, []string) {
	functions, _ := relationshipBoundaryGraph(files, fset, info)
	fixture := func(function *types.Func) bool {
		return strings.HasSuffix(fset.Position(function.Pos()).Filename, "_test.go")
	}
	// Derive every timer-mutating alias/helper/callback from the complete typed
	// invocation graph. A helper can invalidate a pause even without a B argument.
	mutatesTimer := map[*types.Func]bool{}
	changed := true
	for changed {
		changed = false
		for object, function := range functions {
			if mutatesTimer[object] {
				continue
			}
			for _, targets := range function.invocations {
				for target := range targets {
					timer := target.Pkg() != nil && target.Pkg().Path() == "testing" && (target.Name() == "StartTimer" || target.Name() == "StopTimer" || target.Name() == "ResetTimer")
					if timer {
						mutatesTimer[target] = true
					}
					if timer || mutatesTimer[target] {
						mutatesTimer[object] = true
						changed = true
					}
				}
			}
			for _, callbacks := range function.callbacks {
				for callback := range callbacks {
					if mutatesTimer[callback] {
						mutatesTimer[object] = true
						changed = true
					}
				}
			}
		}
	}
	admissionHelpers := map[*types.Func]bool{}
	for object := range functions {
		if !fixture(object) {
			continue
		}
		results := object.Type().(*types.Signature).Results()
		for index := 0; index < results.Len(); index++ {
			if relationshipBoundaryNamedType(results.At(index).Type()) == "admittedRelationshipStatement" {
				admissionHelpers[object] = true
			}
		}
	}
	var violations, population []string
	check := func(root string, starts map[*types.Func]bool) {
		visited := map[*types.Func]bool{}
		var visit func(*types.Func)
		visit = func(object *types.Func) {
			if visited[object] || !fixture(object) {
				return
			}
			visited[object] = true
			if admissionHelpers[object] {
				violations = append(violations, root+" -> "+relationshipBoundaryFunctionName(object))
				return
			}
			if body := functions[object]; body != nil {
				for callee := range body.calls {
					visit(callee)
				}
			}
		}
		for object := range starts {
			visit(object)
		}
	}
	for object, function := range functions {
		if !fixture(object) {
			continue
		}
		for call, targets := range function.invocations {
			for target := range targets {
				if target.Pkg() != nil && target.Pkg().Path() == "testing" && (target.Name() == "AllocsPerRun" || target.Name() == "RunParallel") {
					root := fset.Position(call.Pos()).String() + ": allocation measurement"
					population = append(population, root)
					check(root, function.callbacks[call])
				}
			}
		}
		ast.Inspect(function.body, func(node ast.Node) bool {
			if _, closure := node.(*ast.FuncLit); closure {
				return false
			}
			var body *ast.BlockStmt
			var condition ast.Node
			switch loop := node.(type) {
			case *ast.ForStmt:
				body, condition = loop.Body, loop.Cond
			case *ast.RangeStmt:
				body, condition = loop.Body, loop.X
			default:
				return true
			}
			iterations := false
			var benchmark types.Object
			multipleBenchmarks := false
			if condition != nil {
				ast.Inspect(condition, func(node ast.Node) bool {
					if selector, ok := node.(*ast.SelectorExpr); ok && (selector.Sel.Name == "N" || selector.Sel.Name == "Loop") && relationshipMeasurementBenchmarkType(info.TypeOf(selector.X)) {
						iterations = true
						if identifier, ok := selector.X.(*ast.Ident); ok {
							object := info.Uses[identifier]
							if benchmark != nil && benchmark != object {
								multipleBenchmarks = true
							}
							benchmark = object
						} else {
							multipleBenchmarks = true
						}
					}
					return true
				})
			}
			if !iterations {
				return true
			}
			root := fmt.Sprintf("%s: benchmark iterations", fset.Position(node.Pos()))
			population = append(population, root)
			if multipleBenchmarks {
				benchmark = nil
			}
			pausedCalls := relationshipMeasurementPausedCalls(body, benchmark, info, function.invocations, mutatesTimer)
			starts := map[*types.Func]bool{}
			for call, targets := range function.invocations {
				if call.Pos() < body.Pos() || call.End() > body.End() || pausedCalls[call] {
					continue
				}
				for target := range targets {
					starts[target] = true
				}
				for callback := range function.callbacks[call] {
					starts[callback] = true
				}
			}
			check(root, starts)
			return true
		})
	}
	sort.Strings(population)
	sort.Strings(violations)
	return violations, population
}

func TestRelationshipFixtureMeasurementAdmission(t *testing.T) {
	const controls = `package cypher
import ("context"; "testing")
func measurementGuardBad(t *testing.T) {
	run := func() { _, _ = expressionTestContext(context.Background(), "1") }
	alias := run
	testing.AllocsPerRun(1, func() { alias() })
}
func measurementGuardBadBenchmark(b *testing.B) {
	run := func() { _, _ = expressionTestContext(context.Background(), "1") }
	for range b.N { run() }
}
func measurementGuardBadLoop(b *testing.B) {
	for b.Loop() { _, _ = expressionTestContext(context.Background(), "1") }
}
func measurementGuardBadParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() { _, _ = expressionTestContext(context.Background(), "1") }
	})
}
func measurementGuardPaused(b *testing.B) {
 for range b.N {
  b.StopTimer()
  _, statement := expressionTestContext(context.Background(), "1")
  b.StartTimer()
  _ = statement
 }
}
func measurementGuardResumed(b *testing.B) {
 for range b.N { b.StopTimer(); b.StartTimer(); _, _ = expressionTestContext(context.Background(), "1") }
}
func measurementGuardWrongTimer(b, other *testing.B) {
 for range b.N { other.StopTimer(); _, _ = expressionTestContext(context.Background(), "1"); other.StartTimer() }
}
func measurementGuardConditional(b *testing.B, stop bool) {
 for range b.N { if stop { b.StopTimer() }; _, _ = expressionTestContext(context.Background(), "1") }
}
func measurementGuardTimerAlias(b *testing.B) {
 resume := b.StartTimer
 for range b.N { b.StopTimer(); resume(); _, _ = expressionTestContext(context.Background(), "1") }
}
func measurementGuardGood(t *testing.T) {
	_, statement := expressionTestContext(context.Background(), "1")
	testing.AllocsPerRun(1, func() { _ = statement })
}
func measurementGuardPublic(b *testing.B, exec *StorageExecutor) {
	for range b.N { _, _ = exec.Execute(context.Background(), "RETURN 1", nil) }
}
`
	fset, files, info := relationshipFixtureTypedSource(t, controls, false)
	violations, population := relationshipFixtureMeasurementAdmission(fset, files, info)
	controlFailures := 0
	for _, violation := range violations {
		if strings.Contains(violation, "zz_relationship_fixture_guard_control_test.go:") {
			controlFailures++
		} else {
			t.Error(violation)
		}
	}
	if controlFailures != 8 {
		t.Errorf("all measured-admission controls must fail, paused/prepared/public controls must pass: %d: %v", controlFailures, violations)
	}
	if len(population) < 50 {
		t.Fatalf("complete measured fixture population missing: %d", len(population))
	}
	for _, measurement := range population {
		t.Log(measurement)
	}
	t.Logf("typed measured fixture census: %d measurement regions, %d guard violations, %d negative controls", len(population), len(violations)-controlFailures, controlFailures)
}
