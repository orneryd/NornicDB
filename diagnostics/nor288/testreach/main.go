// Author: NOR-288 lane (Claude), 2026-10-30. Derives a package's test
// selection from the call graph: every Test function of the package that
// reaches a production function changed by git diff base...HEAD (a function
// whose source span holds an added or removed line, which includes a changed
// signature). Call graph: the union of CHA and VTA (as the owner census);
// closures of a reached function are reached (t.Run calls them through a
// function value). No product execution.
// Usage: testreach <module-root> <base-sha> <package-pattern>
package main

import (
	"encoding/json"
	"fmt"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/callgraph/vta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

var hunk = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

// changedLines maps each production .go file (relative path) to the HEAD
// lines this diff adds; a pure deletion marks the HEAD line it falls before.
func changedLines(root, base string) map[string]map[int]bool {
	out, err := exec.Command("git", "-C", root, "diff", "-U0", base+"...HEAD", "--", "*.go", ":(exclude)*_test.go").Output()
	if err != nil {
		panic(err)
	}
	lines := map[string]map[int]bool{}
	var file string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(l, "+++ b/") {
			file = strings.TrimPrefix(l, "+++ b/")
			continue
		}
		if m := hunk.FindStringSubmatch(l); m != nil && file != "" {
			start, _ := strconv.Atoi(m[1])
			n := 1
			if m[2] != "" {
				n, _ = strconv.Atoi(m[2])
			}
			if lines[file] == nil {
				lines[file] = map[int]bool{}
			}
			if n == 0 {
				lines[file][start] = true
				lines[file][start+1] = true
			}
			for i := start; i < start+n; i++ {
				lines[file][i] = true
			}
		}
	}
	return lines
}

func main() {
	root, base, pattern := os.Args[1], os.Args[2], os.Args[3]
	changed := changedLines(root, base)
	fset := token.NewFileSet()
	cfg := &packages.Config{Mode: packages.LoadAllSyntax, Dir: root, Fset: fset, Tests: true,
		Env: append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=1", "GOWORK=off", "GOFLAGS=-p=2"), BuildFlags: []string{"-mod=readonly", "-race", "-tags=noui,nolocalllm"}}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		panic(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		panic("package load errors")
	}
	prog, ssaPkgs := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	all := ssautil.AllFunctions(prog)
	graphs := []*callgraph.Graph{cha.CallGraph(prog)}
	graphs = append(graphs, vta.CallGraph(all, graphs[0]))
	out := map[*ssa.Function][]*ssa.Function{}
	for _, g := range graphs {
		for _, n := range g.Nodes {
			if n.Func == nil {
				continue
			}
			for _, e := range n.Out {
				if e.Callee.Func != nil {
					out[n.Func] = append(out[n.Func], e.Callee.Func)
				}
			}
		}
	}
	rel := func(f string) string {
		r, err := filepath.Rel(root, f)
		if err != nil {
			return f
		}
		return filepath.ToSlash(r)
	}
	isChanged := map[*ssa.Function]bool{}
	var changedNames []string
	for f := range all {
		if f.Syntax() == nil || !f.Pos().IsValid() {
			continue
		}
		start, end := fset.Position(f.Syntax().Pos()), fset.Position(f.Syntax().End())
		file := rel(start.Filename)
		if strings.HasSuffix(file, "_test.go") || changed[file] == nil {
			continue
		}
		for l := start.Line; l <= end.Line; l++ {
			if changed[file][l] {
				isChanged[f] = true
				changedNames = append(changedNames, f.String()+" @ "+file+":"+strconv.Itoa(start.Line))
				break
			}
		}
	}
	sort.Strings(changedNames)
	type result struct {
		Test    string   `json:"test"`
		Reaches string   `json:"reaches,omitempty"`
		Path    []string `json:"path,omitempty"`
	}
	var selected, unselected []result
	for _, p := range ssaPkgs {
		if p == nil || !strings.HasSuffix(p.Pkg.Path(), strings.TrimPrefix(pattern, "./")) {
			continue
		}
		for _, m := range p.Members {
			fn, ok := m.(*ssa.Function)
			if !ok || !strings.HasPrefix(fn.Name(), "Test") || fn.Syntax() == nil || !strings.HasSuffix(fset.Position(fn.Pos()).Filename, "_test.go") {
				continue
			}
			if sig := fn.Signature; sig.Params().Len() != 1 || sig.Params().At(0).Type().String() != "*testing.T" {
				continue
			}
			from := map[*ssa.Function]*ssa.Function{fn: nil}
			queue := []*ssa.Function{fn}
			var hit *ssa.Function
			for len(queue) > 0 && hit == nil {
				f := queue[0]
				queue = queue[1:]
				if isChanged[f] {
					hit = f
					break
				}
				next := append(append([]*ssa.Function{}, out[f]...), f.AnonFuncs...)
				for _, c := range next {
					if _, seen := from[c]; !seen {
						from[c] = f
						queue = append(queue, c)
					}
				}
			}
			if hit == nil {
				unselected = append(unselected, result{Test: fn.Name()})
				continue
			}
			var path []string
			for f := hit; f != nil; f = from[f] {
				path = append([]string{f.String()}, path...)
			}
			selected = append(selected, result{Test: fn.Name(), Reaches: hit.String(), Path: path})
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Test < selected[j].Test })
	sort.Slice(unselected, func(i, j int) bool { return unselected[i].Test < unselected[j].Test })
	names := make([]string, len(selected))
	for i, r := range selected {
		names[i] = r.Test
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"author":            "NOR-288 lane (Claude),30October2026",
		"method":            "packages.Load(Tests=true) " + pattern + "; SSA; union of CHA and VTA call graphs; closures of reached functions reached; changed = production function spans holding a line of git diff -U0 " + base + "...HEAD",
		"changed_functions": changedNames,
		"selected_count":    len(selected),
		"unselected_count":  len(unselected),
		"selected":          selected,
		"unselected":        unselected,
		"run_regex":         "^(" + strings.Join(names, "|") + ")$",
	}); err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, "NOR288_TESTREACH_DONE", len(selected), len(unselected))
}
