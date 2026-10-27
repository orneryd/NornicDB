package workcensus

import (
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestGenericCensusProgramReachability(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	inv := mustExtract(t, c)
	root := Root{Package: "census.fixture", Name: "Execute", Required: true}
	r, err := inv.Program.ReachableFrom([]Root{root})
	if err != nil || !r.Contains(root) || len(r.Roots) != 1 {
		t.Fatal(r.Roots, err)
	}
	for _, member := range inv.Members {
		if member.Reachable && member.OwnerSymbol != "" {
			found := false
			for fn := range r.Functions {
				found = found || sourceFunction(fn).String() == member.OwnerSymbol
			}
			if !found {
				t.Fatal("Extract and program query disagree", member.ID)
			}
		}
	}
	empty, err := inv.Program.ReachableFrom(nil)
	if err != nil || len(empty.Functions) != 0 {
		t.Fatal("empty roots widened population", err)
	}
	if _, err := inv.Program.ReachableFrom([]Root{{Package: "census.fixture", Name: "missing", Required: true}}); err == nil {
		t.Fatal("missing root accepted")
	}
	t.Run("unreferenced source declarations", func(t *testing.T) {
		c.Overlay = map[string][]byte{filepath.Join(c.Dir, "unreferenced.go"): []byte(`package fixture
type lexicalHidden struct{}
type lexicalBox[T any] struct{}
func lexicalLeaf() {}
func (lexicalHidden) Value() { lexicalLeaf() }
func (*lexicalHidden) Pointer() { lexicalLeaf() }
func (lexicalBox[T]) GenericValue() { lexicalLeaf() }
func (*lexicalBox[T]) GenericPointer() { func() { lexicalLeaf() }() }
func (_ *lexicalBox[T]) Blank() { lexicalLeaf() }
func (*lexicalBox[T]) Unnamed() { lexicalLeaf() }
func (lexicalBox[T]) WithoutObservation() {}
`)}
		source := mustExtract(t, c)
		entry, err := source.Program.ReachableFrom([]Root{root})
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range []struct{ receiver, name string }{
			{"lexicalHidden", "Value"}, {"lexicalHidden", "Pointer"},
			{"lexicalBox", "GenericValue"}, {"lexicalBox", "GenericPointer"},
			{"lexicalBox", "Blank"}, {"lexicalBox", "Unnamed"},
		} {
			declaration := Root{Package: "census.fixture", Receiver: method.receiver, Name: method.name, Required: true}
			if entry.Contains(declaration) {
				t.Fatal("unused method was added to statement entry reachability", method)
			}
			population, err := source.Program.ReachableFrom([]Root{declaration})
			if err != nil || !population.Contains(declaration) {
				t.Fatal("source declaration cannot be selected as root", method, err)
			}
			if path := population.PathTo(Root{Package: "census.fixture", Name: "lexicalLeaf"}); len(path) < 2 {
				t.Fatal("source method lost its body or closure", method, path)
			}
		}
		unobserved, err := source.Program.ReachableFrom([]Root{{Package: "census.fixture", Receiver: "lexicalBox", Name: "WithoutObservation", Required: true}})
		if err != nil || unobserved.Contains(Root{Package: "census.fixture", Name: "lexicalLeaf"}) {
			t.Fatal("another method lent its observation to an empty body", err)
		}
	})
}

func TestGenericCensusReachabilityPaths(t *testing.T) {
	c := containingFixtureConfig(t, containingFixture)
	c.IncludeProgram = true
	inv := mustExtract(t, c)
	entry := Root{Package: "census.fixture", Name: "containingEntry", Required: true}
	observation := Root{Package: "census.fixture", Name: "lowerObserve"}
	r, err := inv.Program.ReachableFrom([]Root{entry})
	if err != nil {
		t.Fatal(err)
	}
	path := r.PathTo(observation)
	if len(path) != 3 || path[0] != "census.fixture.containingEntry" || path[2] != "census.fixture.lowerObserve" {
		t.Fatal("wrong source path", path)
	}
	if len(r.PathTo(Root{Package: "census.fixture", Name: "foreignObservation"})) != 0 {
		t.Fatal("unreached declaration acquired a path")
	}
	for file, raw := range c.Overlay {
		c.Overlay[file] = []byte(strings.ReplaceAll(string(raw), "_ = lowerObserve(scope);", ""))
	}
	removed := mustExtract(t, c)
	r, err = removed.Program.ReachableFrom([]Root{entry})
	if err != nil {
		t.Fatal(err)
	}
	if r.Contains(observation) || len(r.PathTo(observation)) != 0 {
		t.Fatal("removed observation still has a witness")
	}
	if !r.Contains(Root{Package: "census.fixture", Name: "lowerEvent"}) {
		t.Fatal("control accidentally removed work")
	}
	for file, raw := range c.Overlay {
		c.Overlay[file] = append(raw, []byte(`
func witnessObserve[T any](value T) {}
func witnessIndirect() { witnessObserve[int](1) }
func witnessEntry() { witnessIndirect(); witnessObserve[string]("direct") }
type witnessBox[T any] struct{}
func (*witnessBox[T]) Observe() {}
func witnessMethodEntry() { b:=witnessBox[int]{}; b.Observe(); b.Observe() }
func witnessMethodCarrier() func() { b:=witnessBox[string]{}; return b.Observe }
`)...)
	}
	variants := mustExtract(t, c)
	r, err = variants.Program.ReachableFrom([]Root{{Package: "census.fixture", Name: "witnessEntry", Required: true}})
	if err != nil {
		t.Fatal(err)
	}
	if path := r.PathTo(Root{Package: "census.fixture", Name: "witnessObserve"}); len(path) != 2 {
		t.Fatal("did not select the shortest generic-instance witness", path)
	}
	for _, target := range []Root{
		{Package: "census.fixture", Name: "witnessObserve"},
		{Package: "census.fixture", Receiver: "witnessBox", Name: "Observe"},
	} {
		var origin *ssa.Function
		for _, fn := range variants.Program.Functions {
			if fn.Origin() == nil && rootMatches(fn, target) {
				origin = fn
			}
		}
		if origin == nil {
			t.Fatal("generic source declaration missing", target)
		}
		calls := variants.Program.SourceCallSites(origin)
		if len(calls) != 2 || calls[0].Pos() == calls[1].Pos() {
			t.Fatal("generic source origin lost distinct invocation sites", target, calls)
		}
		if target.Receiver != "" {
			callers := map[string]bool{}
			for _, site := range calls {
				callers[site.Parent().Name()] = true
			}
			if !callers["witnessMethodEntry"] || callers["witnessMethodCarrier"] {
				t.Fatal("stored method value fabricated an invocation", callers)
			}
			carriers := map[string]bool{}
			for _, site := range variants.Program.SourceReferences(origin) {
				carriers[site.Parent().Name()] = true
			}
			if !carriers["witnessMethodCarrier"] {
				t.Fatal("generic source origin lost a stored method carrier", carriers)
			}
		}
	}
}

// WalkCallers attributes a call inside a function literal to its outermost
// source function, visits each caller once, prunes where visit declines to
// expand, and ends where visit stops.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func TestWalkCallersAttributionPruneAndStop(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "walk_callers.go"): []byte(`package fixture
func walkLeaf() {}
func walkMiddle() { walkLeaf() }
func walkTop() { func() { walkMiddle() }() }
func WalkExported() { walkTop() }
`)}
	inv := mustExtract(t, c)
	var leaf *ssa.Function
	for _, fn := range inv.Program.Functions {
		if fn.Pkg != nil && fn.Pkg.Pkg.Path() == "census.fixture" && fn.Name() == "walkLeaf" {
			leaf = fn
		}
	}
	if leaf == nil {
		t.Fatal("walkLeaf not loaded")
	}
	walk := func(expand func(string) bool, stop string) string {
		var seen []string
		inv.Program.WalkCallers([]*ssa.Function{leaf}, func(caller, callee *ssa.Function) (bool, bool) {
			seen = append(seen, caller.Name()+"<-"+callee.Name())
			return expand(caller.Name()), caller.Name() == stop
		})
		return strings.Join(seen, " ")
	}
	all := func(string) bool { return true }
	// The call inside walkTop's function literal is attributed to walkTop.
	if got := walk(all, ""); got != "walkMiddle<-walkLeaf walkTop<-walkMiddle WalkExported<-walkTop" {
		t.Fatal("full walk:", got)
	}
	if got := walk(func(name string) bool { return name != "walkTop" }, ""); got != "walkMiddle<-walkLeaf walkTop<-walkMiddle" {
		t.Fatal("pruned walk:", got)
	}
	if got := walk(all, "walkMiddle"); got != "walkMiddle<-walkLeaf" {
		t.Fatal("stopped walk:", got)
	}
}

// DirectReachableFrom keeps the work a root performs itself (static calls and
// function literals) and leaves out a callback it only hands to other code,
// which ReachableFrom conservatively includes.
// Author: storage-format-owners lane (Claude), #1693, 30 October 2026.
func TestDirectReachableFromLeavesCallbacks(t *testing.T) {
	c := genericConfig(t)
	c.IncludeProgram = true
	c.Overlay = map[string][]byte{filepath.Join(c.Dir, "direct_reach.go"): []byte(`package fixture
var listeners []func()
func directWork() {}
func listenerWork() {}
func DirectRoot() { directWork(); func() { directWork() }(); for _, l := range listeners { l() } }
func Register() { listeners = append(listeners, listenerWork) }
`)}
	inv := mustExtract(t, c)
	root := []Root{{Package: "census.fixture", Name: "DirectRoot", Required: true}}
	direct, err := inv.Program.DirectReachableFrom(root)
	if err != nil {
		t.Fatal(err)
	}
	all, err := inv.Program.ReachableFrom(root)
	if err != nil {
		t.Fatal(err)
	}
	work := Root{Package: "census.fixture", Name: "directWork"}
	listener := Root{Package: "census.fixture", Name: "listenerWork"}
	if !direct.Contains(work) || direct.Contains(listener) {
		t.Fatal("direct population must hold directWork and not the listener")
	}
	if !all.Contains(work) || !all.Contains(listener) {
		t.Fatal("conservative population must hold both")
	}
}
