package cypher

// Test-only refactoring step (fix145-1391, never committed): the functions
// that hold an R1 data loop but neither a context nor a probe get a trailing
// probe parameter, and every call passes its caller's probe; callers that
// have none join the set until every caller can pass one. Exported
// functions and functions used as values are reported, not changed.
// ZZ_THREAD=1. Prints `git diff`.

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
)

func TestZZThread1391(t *testing.T) {
	if os.Getenv("ZZ_THREAD") == "" {
		t.Skip("opt-in")
	}
	a, findings := collectCancellationContractFindings(t)
	holds := func(fn *contractFunc) bool {
		ctxName, probeName := cancellationScope(fn.decl)
		return ctxName != "" || probeName != "" || localProbe(fn.decl, fn.decl.Body.End()) != ""
	}
	set := map[*contractFunc]bool{}
	for _, f := range findings {
		if f.rule == "R1" && f.body != nil && !holds(f.fn) && !guardSkipName.MatchString(f.fn.decl.Name.Name) {
			set[f.fn] = true
		}
	}
	// Non-call references (values, tables, interface implementations by name).
	valueRefs := map[*types.Func][]string{}
	callIdents := map[*ast.Ident]bool{}
	for _, fn := range a.fns {
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				switch f := ast.Unparen(call.Fun).(type) {
				case *ast.Ident:
					callIdents[f] = true
				case *ast.SelectorExpr:
					callIdents[f.Sel] = true
				}
			}
			return true
		})
	}
	for id, obj := range a.info.Uses {
		if fn, ok := obj.(*types.Func); ok && !callIdents[id] {
			valueRefs[fn.Origin()] = append(valueRefs[fn.Origin()], a.where(id.Pos()))
		}
	}
	// Callers: call sites of each function.
	type site struct {
		caller *contractFunc
		call   *ast.CallExpr
	}
	sites := map[*contractFunc][]site{}
	for _, fn := range a.fns {
		caller := fn
		ast.Inspect(fn.decl.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if callee := a.callee(call); callee != nil {
					if g := a.fns[callee]; g != nil {
						sites[g] = append(sites[g], site{caller, call})
					}
				}
			}
			return true
		})
	}
	var report []string
	blocked := map[*contractFunc]string{}
	for changed := true; changed; {
		changed = false
		for fn := range set {
			if blocked[fn] != "" {
				continue
			}
			if fn.obj.Exported() {
				blocked[fn] = "exported"
			} else if refs := valueRefs[fn.obj]; len(refs) > 0 {
				blocked[fn] = "used as a value at " + strings.Join(refs, ",")
			}
			if blocked[fn] != "" {
				continue
			}
			for _, s := range sites[fn] {
				if !holds(s.caller) && !set[s.caller] && s.caller.reach {
					set[s.caller] = true
					changed = true
				}
			}
		}
	}
	type edit struct {
		offset int
		text   string
	}
	edits := map[string][]edit{}
	for fn := range set {
		if reason := blocked[fn]; reason != "" {
			report = append(report, fmt.Sprintf("BLOCKED\t%s\t%s\t%s", contractFuncName(fn.decl), a.where(fn.decl.Pos()), reason))
			continue
		}
		// A caller that is blocked would need the probe too.
		params := fn.decl.Type.Params
		file := a.fset.Position(fn.decl.Pos()).Filename
		variadic := false
		if n := len(params.List); n > 0 {
			_, variadic = params.List[n-1].Type.(*ast.Ellipsis)
		}
		if variadic {
			report = append(report, fmt.Sprintf("VARIADIC\t%s\t%s", contractFuncName(fn.decl), a.where(fn.decl.Pos())))
			continue
		}
		insert := a.fset.Position(params.Closing).Offset
		text := "probe *pipelineCancellation"
		if len(params.List) > 0 {
			text = ", " + text
		}
		edits[file] = append(edits[file], edit{insert, text})
		for _, s := range sites[fn] {
			arg := ""
			ctxName, probeName := cancellationScope(s.caller.decl)
			if probeName == "" {
				probeName = localProbe(s.caller.decl, s.call.Pos())
			}
			switch {
			case probeName != "":
				arg = probeName
			case set[s.caller] && blocked[s.caller] == "":
				arg = "probe"
			case ctxName != "":
				arg = "contextPipelineProbe(" + ctxName + ")"
			default:
				arg = "nil"
				report = append(report, fmt.Sprintf("NILCALL\t%s -> %s\t%s", contractFuncName(s.caller.decl), contractFuncName(fn.decl), a.where(s.call.Pos())))
			}
			cfile := a.fset.Position(s.call.Pos()).Filename
			at := a.fset.Position(s.call.Rparen).Offset
			if len(s.call.Args) > 0 {
				arg = ", " + arg
			}
			edits[cfile] = append(edits[cfile], edit{at, arg})
		}
	}
	for file, list := range edits {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].offset > list[j].offset })
		out := string(src)
		for _, e := range list {
			out = out[:e.offset] + e.text + out[e.offset:]
		}
		if err := os.WriteFile(file, []byte(out), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(report)
	for _, line := range report {
		fmt.Println("REPORT\t" + line)
	}
	diff, err := exec.Command("git", "diff", "--", ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("=====PATCH=====")
	os.Stdout.Write(diff)
}
