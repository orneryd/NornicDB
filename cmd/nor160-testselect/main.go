// NOR-160 lane. Analysis-only tool, not product code.
//
// Selects the tests that reach a change, from the compiled program: it loads
// the packages with their tests, builds a VTA call graph refined from CHA, and
// walks back from every function whose source overlaps a changed line range to
// the Test functions that reach it. A closure counts as reached by the function
// that declares it, so subtests passed to t.Run belong to their Test. Edges
// whose caller is a standard-library dispatch hub are cut, as in the JSON escape
// census: package testing (tRunner calls every subtest body through one function
// value), log and log/slog (they dispatch to every handler type), and go-logr
// (its slog bridge does the same).
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

const storagePkg = "github.com/orneryd/nornicdb/pkg/storage"

type span struct{ first, last int }

type report struct {
	ChangedFunctions int                 `json:"changed_functions"`
	UnmatchedRanges  []string            `json:"unmatched_ranges"`
	Tests            map[string][]string `json:"tests"`
	TestCounts       map[string][2]int   `json:"selected_of_total"`
	Runs             map[string]string   `json:"run_regex"`
	Controls         map[string]bool     `json:"controls"`
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
	r.Controls["not selected: TestNOR160SelectControlUnrelated"] = exists["TestNOR160SelectControlUnrelated"] && !has("TestNOR160SelectControlUnrelated")
	out, err := json.MarshalIndent(r, "", " ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}

func cutCaller(f *ssa.Function) bool {
	if f == nil || f.Pkg == nil {
		return false
	}
	switch path := f.Pkg.Pkg.Path(); {
	case path == "testing", path == "log", path == "log/slog":
		return true
	case path == "github.com/go-logr/logr" || strings.HasPrefix(path, "github.com/go-logr/logr/"):
		return true
	}
	return false
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
