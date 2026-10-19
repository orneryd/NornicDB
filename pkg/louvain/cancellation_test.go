package louvain

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

type countedCancellation struct {
	calls, stop int
	err error
}

func (c *countedCancellation) Continue() bool {
	c.calls++
	return c.stop == 0 || c.calls < c.stop
}

func (c *countedCancellation) Err() error { return c.err }

func cancellationGraph(size int) *Graph {
	graph := NewGraph(size)
	for node := 0; node < size; node++ {
		for distance := 1; distance <= 4; distance++ {
			graph.Link(node, (node+distance)%size, float64(1+distance%3))
		}
	}
	return graph
}

func TestCancellationPreservesRunResults(t *testing.T) {
	for _, resolution := range []float64{0, 0.25, 1, 2} {
		for _, seeded := range []bool{false, true} {
			graph := cancellationGraph(64)
			options := DefaultOptions()
			options.Resolution = resolution
			if seeded {
				options.Seed = make([]int, 64)
				for node := range options.Seed {
					options.Seed[node] = node % 4
				}
			}
			want := Run(graph, options)
			owner := &countedCancellation{}
			got, err := RunWithCancellation(graph, options, owner)
			if err != nil || !reflect.DeepEqual(want.Levels, got.Levels) || !reflect.DeepEqual(want.Representatives, got.Representatives) || len(want.Modularities) != len(got.Modularities) {
				t.Fatalf("resolution=%v seeded=%v: changed result: want %+v got %+v error %v", resolution, seeded, want, got, err)
			}
			for level, modularity := range want.Modularities {
				if math.Float64bits(modularity) != math.Float64bits(got.Modularities[level]) {
					t.Fatalf("changed floating-point reduction at level %d", level)
				}
			}
			if owner.calls < 64 {
				t.Fatal("run did not carry the owner through graph work")
			}
		}
	}
}

func TestCancellationStopsThroughoutRunWithoutRetainingOwner(t *testing.T) {
	graph := cancellationGraph(256)
	options := DefaultOptions()
	complete := &countedCancellation{}
	want, err := RunWithCancellation(graph, options, complete)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("statement ended")
	for _, stop := range []int{1, 128, complete.calls / 7, complete.calls / 3, complete.calls / 2, complete.calls - 1} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			owner := &countedCancellation{stop: stop, err: failure}
			graph.total = 123
			result, err := RunWithCancellation(graph, options, owner)
			if !errors.Is(err, failure) || !reflect.DeepEqual(Result{}, result) || owner.calls != stop {
				t.Fatalf("cancelled run: result %+v error %v work %d/%d", result, err, owner.calls, stop)
			}
			if graph.total != 123 {
				t.Fatal("cancelled run mutated the reusable input graph")
			}
			// A later caller must not inherit a cancelled owner's lifetime.
			later, err := RunWithCancellation(graph, options, &countedCancellation{})
			if err != nil || !reflect.DeepEqual(want, later) {
				t.Fatalf("later run changed: %+v %v", later, err)
			}
		})
	}
}

func TestCancellationOwnsReservationAndRenumbering(t *testing.T) {
	counts := make([]int, 4096)
	for index := range counts {
		counts[index] = 1
	}
	owner := &countedCancellation{stop: 200, err: context.DeadlineExceeded}
	graph, err := NewGraphWithCancellation(len(counts), counts, owner)
	if !errors.Is(err, context.DeadlineExceeded) || graph != nil || owner.calls != 200 {
		t.Fatalf("graph reservation did not stop: %v %v %d", graph, err, owner.calls)
	}
	owner = &countedCancellation{stop: 200, err: context.Canceled}
	count, err := RenumberWithCancellation(counts, owner)
	if !errors.Is(err, context.Canceled) || count != 0 || owner.calls != 200 {
		t.Fatalf("renumbering did not stop: %d %v %d", count, err, owner.calls)
	}
}

type panickingCancellation struct{}
func (panickingCancellation) Continue() bool { panic("unrelated failure") }
func (panickingCancellation) Err() error { return nil }

func TestCancellationDoesNotSwallowUnrelatedPanics(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != "unrelated failure" {
			t.Fatalf("unrelated panic changed: %v", recovered)
		}
	}()
	_, _ = RunWithCancellation(NewGraph(0), DefaultOptions(), panickingCancellation{})
}

// Every graph-sized loop in this library must borrow the same explicit owner.
// This enumerates source rather than maintaining a hand-picked consumer list.
func TestCancellationOwnsEveryLibraryLoop(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil { t.Fatal(err) }
	set := token.NewFileSet()
	loops := 0
	for _, entry := range files {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") { continue }
		file, err := parser.ParseFile(set, entry.Name(), nil, 0)
		if err != nil { t.Fatal(err) }
		ast.Inspect(file, func(node ast.Node) bool {
			var body *ast.BlockStmt
			switch loop := node.(type) {
			case *ast.RangeStmt: body = loop.Body
			case *ast.ForStmt: body = loop.Body
			default: return true
			}
			loops++
			if len(body.List) > 0 {
				if expression, ok := body.List[0].(*ast.ExprStmt); ok {
					if call, ok := expression.X.(*ast.CallExpr); ok && len(call.Args) == 1 {
						callee, named := call.Fun.(*ast.Ident)
						owner, passed := call.Args[0].(*ast.Ident)
						if named && passed && callee.Name == "check" && owner.Name == "cancellation" { return true }
					}
				}
			}
			t.Errorf("%s: graph loop does not begin with its owner check", set.Position(node.Pos()))
			return true
		})
	}
	if loops < 30 { t.Fatalf("incomplete source inventory: only %d loops", loops) }
}
