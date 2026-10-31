// NOR-160 lane. Analysis-only tool, not product code.
//
// Selects the tests that reach a change, from the compiled program: it loads
// the packages with their tests, builds a VTA call graph refined from CHA, and
// walks back from every function whose source overlaps a changed line range to
// the Test functions that reach it. A closure counts as reached by the function
// that declares it, so subtests passed to t.Run belong to their Test. Edges
// whose caller is outside this module are cut: the standard library and third
// party code dispatch through interfaces (fmt to every Stringer and error,
// testing's tRunner to every subtest, slog to every handler), which joins
// unrelated code. So reach through such a dispatch alone is not seen. A test
// whose package does not depend on a changed package cannot reach a change; the
// report counts any such selection, and the tool's controls require none.
//
// Usage: nor160-testselect <ranges file> <package pattern>...
// A ranges line is: <module-relative file> <first line> <last line>.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

const modulePath = "github.com/orneryd/nornicdb"

const storagePkg = "github.com/orneryd/nornicdb/pkg/storage"

type span struct{ first, last int }

type report struct {
	ChangedFunctions     int                 `json:"changed_functions"`
	UnmatchedRanges      []string            `json:"unmatched_ranges"`
	Tests                map[string][]string `json:"tests"`
	TestCounts           map[string][2]int   `json:"selected_of_total"`
	Runs                 map[string]string   `json:"run_regex"`
	Controls             map[string]bool     `json:"controls"`
	ImpossibleSelections []string            `json:"impossible_selections"`
}

func main() {
	root, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	ranges := map[string][]span{}
	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		var name string
		var s span
		if _, err := fmt.Sscan(scan.Text(), &name, &s.first, &s.last); err == nil {
			ranges[name] = append(ranges[name], s)
		}
	}
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Tests: true, Dir: root, BuildFlags: []string{"-tags=noui,nolocalllm"}}
	pkgs, err := packages.Load(cfg, os.Args[2:]...)
	if err != nil {
		panic(err)
	}
	// The compiled import graph, for the dependency control.
	imports := map[string]map[string]bool{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		path := strings.TrimSuffix(p.PkgPath, "_test")
		if imports[path] == nil {
			imports[path] = map[string]bool{}
		}
		for _, dep := range p.Imports {
			imports[path][strings.TrimSuffix(dep.PkgPath, "_test")] = true
		}
	})
	var dependsOn func(from, to string, seen map[string]bool) bool
	dependsOn = func(from, to string, seen map[string]bool) bool {
		if from == to {
			return true
		}
		if seen[from] {
			return false
		}
		seen[from] = true
		for dep := range imports[from] {
			if dependsOn(dep, to, seen) {
				return true
			}
		}
		return false
	}
	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	all := ssautil.AllFunctions(prog)
	graph := vta.CallGraph(all, cha.CallGraph(prog))

	relative := func(pos token.Pos) (string, int) {
		p := prog.Fset.Position(pos)
		rel, err := filepath.Rel(root, p.Filename)
		if err != nil {
			return "", 0
		}
		return filepath.ToSlash(rel), p.Line
	}
	matched := map[string]bool{}
	reached := map[*ssa.Function]bool{}
	var stack []*ssa.Function
	mark := func(fn *ssa.Function) {
		if fn != nil && !reached[fn] {
			reached[fn] = true
			stack = append(stack, fn)
		}
	}
	changed := 0
	for fn := range all {
		if fn.Syntax() == nil {
			continue
		}
		name, first := relative(fn.Syntax().Pos())
		_, last := relative(fn.Syntax().End())
		for i, s := range ranges[name] {
			if s.first <= last && first <= s.last {
				matched[fmt.Sprint(name, "#", i)] = true
				if !reached[fn] {
					changed++
				}
				mark(fn)
			}
		}
	}
	for len(stack) > 0 {
		fn := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		mark(fn.Parent())
		if node := graph.Nodes[fn]; node != nil {
			for _, edge := range node.In {
				if !cutCaller(edge.Caller.Func) {
					mark(edge.Caller.Func)
				}
			}
		}
	}

	r := report{ChangedFunctions: changed, Tests: map[string][]string{}, TestCounts: map[string][2]int{}, Runs: map[string]string{}, Controls: map[string]bool{}}
	for name, spans := range ranges {
		for i, s := range spans {
			if !matched[fmt.Sprint(name, "#", i)] {
				r.UnmatchedRanges = append(r.UnmatchedRanges, fmt.Sprintf("%s:%d-%d", name, s.first, s.last))
			}
		}
	}
	sort.Strings(r.UnmatchedRanges)
	total := map[string]int{}
	for fn := range all {
		if !strings.HasPrefix(fn.Name(), "Test") || fn.Parent() != nil || fn.Signature.Recv() != nil || fn.Pkg == nil {
			continue
		}
		sig := fn.Signature.Params()
		if sig.Len() != 1 || sig.At(0).Type().String() != "*testing.T" {
			continue
		}
		pkg := strings.TrimSuffix(fn.Pkg.Pkg.Path(), "_test")
		total[pkg]++
		if reached[fn] {
			r.Tests[pkg] = append(r.Tests[pkg], fn.Name())
		}
	}
	for pkg, names := range r.Tests {
		sort.Strings(names)
		names = dedupe(names)
		r.Tests[pkg] = names
		r.TestCounts[pkg] = [2]int{len(names), total[pkg]}
		quoted := make([]string, len(names))
		for i, n := range names {
			quoted[i] = regexp.QuoteMeta(n)
		}
		r.Runs[pkg] = "^(" + strings.Join(quoted, "|") + ")$"
	}
	// Controls, in pkg/storage: tests planted by the tool's branch must exist,
	// and be selected through a direct call, an interface call and a subtest
	// closure; the unrelated one must not be.
	has := func(name string) bool {
		for _, n := range r.Tests[storagePkg] {
			if n == name {
				return true
			}
		}
		return false
	}
	exists := map[string]bool{}
	for fn := range all {
		if fn.Pkg != nil && strings.TrimSuffix(fn.Pkg.Pkg.Path(), "_test") == storagePkg {
			exists[fn.Name()] = true
		}
	}
	for _, name := range []string{"TestNOR160SelectControlDirect", "TestNOR160SelectControlInterface", "TestNOR160SelectControlSubtest"} {
		r.Controls["selected: "+name] = exists[name] && has(name)
	}
	// A selected test's package must depend on a package holding a changed function.
	changedPkgs := map[string]bool{}
	for name := range ranges {
		changedPkgs[modulePath+"/"+strings.TrimSuffix(filepath.ToSlash(filepath.Dir(name)), "/")] = true
	}
	for pkg := range r.Tests {
		reaches := false
		for target := range changedPkgs {
			if dependsOn(pkg, target, map[string]bool{}) {
				reaches = true
			}
		}
		if !reaches {
			r.ImpossibleSelections = append(r.ImpossibleSelections, pkg)
		}
	}
	sort.Strings(r.ImpossibleSelections)
	r.Controls["no selected test in a package without a dependency on a changed package"] = len(r.ImpossibleSelections) == 0
	r.Controls["not selected: TestNOR160SelectControlUnrelated"] = exists["TestNOR160SelectControlUnrelated"] && !has("TestNOR160SelectControlUnrelated")
	out, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}

func cutCaller(f *ssa.Function) bool {
	if f == nil || f.Pkg == nil {
		return true
	}
	path := f.Pkg.Pkg.Path()
	return path != modulePath && !strings.HasPrefix(path, modulePath+"/")
}

func dedupe(names []string) []string {
	out := names[:0]
	for i, n := range names {
		if i == 0 || names[i-1] != n {
			out = append(out, n)
		}
	}
	return out
}
