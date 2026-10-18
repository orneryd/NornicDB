package cypher

// Test-only refactoring step (fix145-1391, never committed): every R1 data
// loop of TestCancellationOwnerStaticInventory gets the owner's check as its
// first statement. Prints `git diff` of the edits. ZZ_GUARD=1.

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var guardSkipName = regexp.MustCompile(`(?i)(release|close|^done|give|unlock|rollback|cleanup|handover|free|reset|clear|stop|finish|^end|restore|recover|cancel)`)

func TestZZGuard1391(t *testing.T) {
	if os.Getenv("ZZ_GUARD") == "" {
		t.Skip("opt-in")
	}
	a, findings := collectCancellationContractFindings(t)
	type edit struct {
		offset int
		text   string
	}
	edits := map[string][]edit{}
	srcs := map[string][]byte{}
	read := func(file string) []byte {
		if src, ok := srcs[file]; ok {
			return src
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		srcs[file] = src
		return src
	}
	declared := map[*ast.FuncDecl]string{}
	var report []string
	qualifier := func(p *types.Package) string {
		if p.Path() == "github.com/orneryd/nornicdb/pkg/cypher" {
			return ""
		}
		return p.Name()
	}
	zero := func(t types.Type) string {
		if _, ok := t.(*types.TypeParam); ok {
			return "*new(" + types.TypeString(t, qualifier) + ")"
		}
		switch u := t.Underlying().(type) {
		case *types.Basic:
			switch {
			case u.Info()&types.IsBoolean != 0:
				return "false"
			case u.Info()&types.IsString != 0:
				return `""`
			case u.Info()&types.IsNumeric != 0:
				return "0"
			}
			return "nil"
		case *types.Struct, *types.Array:
			return types.TypeString(t, qualifier) + "{}"
		}
		return "nil"
	}
	for _, f := range findings {
		if f.rule != "R1" || f.body == nil {
			if f.rule == "R1" {
				report = append(report, "BULK\t"+f.key+"\t"+f.where)
			}
			continue
		}
		decl := f.fn.decl
		name := contractFuncName(decl)
		if guardSkipName.MatchString(decl.Name.Name) {
			report = append(report, "CLEANUP\t"+f.key+"\t"+f.where)
			continue
		}
		file := a.fset.Position(decl.Pos()).Filename
		src := read(file)
		// The innermost function the loop is in: its results.
		var sig *types.Signature
		var lit *ast.FuncLit
		ast.Inspect(decl.Body, func(n ast.Node) bool {
			if fl, ok := n.(*ast.FuncLit); ok && fl.Pos() <= f.node.Pos() && f.node.End() <= fl.End() {
				lit = fl
			}
			return true
		})
		if lit != nil {
			sig, _ = a.info.TypeOf(lit).(*types.Signature)
		} else {
			sig = f.fn.obj.Type().(*types.Signature)
		}
		ctxName, probeName := cancellationScope(decl)
		if probeName == "" {
			probeName = localProbe(decl, f.node.Pos())
		}
		if probeName == "" {
			probeName = declared[decl]
		}
		if probeName == "" {
			if ctxName == "" {
				report = append(report, "NEEDSPROBE\t"+f.key+"\t"+f.where)
				continue
			}
			probeName = "probe"
			uses := false
			ast.Inspect(decl.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "probe" {
					uses = true
				}
				return true
			})
			if uses {
				probeName = "statementProbe"
			}
			declared[decl] = probeName
			lbrace := a.fset.Position(decl.Body.Lbrace).Offset
			edits[file] = append(edits[file], edit{lbrace + 1, fmt.Sprintf("\n\t%s := contextPipelineProbe(%s)", probeName, ctxName)})
		}
		var results []string
		for i := 0; i < sig.Results().Len(); i++ {
			rt := sig.Results().At(i).Type()
			if i == sig.Results().Len()-1 && rt.String() == "error" {
				results = append(results, probeName+".failed()")
				continue
			}
			results = append(results, zero(rt))
		}
		ret := "return"
		if len(results) > 0 {
			ret += " " + strings.Join(results, ", ")
		}
		// Indentation: the loop's own line's, one deeper.
		loopOffset := a.fset.Position(f.node.Pos()).Offset
		lineStart := strings.LastIndexByte(string(src[:loopOffset]), '\n') + 1
		indent := ""
		for _, c := range src[lineStart:loopOffset] {
			if c == '\t' {
				indent += "\t"
			} else {
				break
			}
		}
		lbrace := a.fset.Position(f.body.Lbrace).Offset
		edits[file] = append(edits[file], edit{lbrace + 1, fmt.Sprintf("\n%s\tif %s.stopped() {\n%s\t\t%s\n%s\t}", indent, probeName, indent, ret, indent)})
		_ = name
		_ = token.NoPos
	}
	for file, list := range edits {
		src := read(file)
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
