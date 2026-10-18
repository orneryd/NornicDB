// zzrewrite: one-off refactoring tool (fix145-1391, never committed).
// For each statement-text twin T (list file: "T" or "T<TAB>replacement"):
//   - every call T(a..., probe) becomes X(a...), X = T minus its suffix or
//     the given replacement (a package function such as strings.TrimSpace);
//   - T's declaration is deleted;
//   - X's declaration is replaced by develop's declaration of X (from the
//     zzdev overlay) when develop has it; otherwise X is reported.
//
// Then `git diff` of the result is printed.
//
//	go run ./zzrewrite list.txt pkg/cypher pkg/cypher/fn pkg/cypher/internal/text
package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type edit struct {
	start, end int
	text       string
	removal    bool
}

func declKey(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		t := fd.Recv.List[0].Type
		if s, ok := t.(*ast.StarExpr); ok {
			t = s.X
		}
		if ix, ok := t.(*ast.IndexExpr); ok {
			t = ix.X
		}
		if id, ok := t.(*ast.Ident); ok {
			return id.Name + "." + fd.Name.Name
		}
	}
	return fd.Name.Name
}

func recvPrefix(key string) string {
	if i := strings.LastIndex(key, "."); i >= 0 {
		return key[:i+1]
	}
	return ""
}

func main() {
	listFile, dirs := os.Args[1], os.Args[2:]
	twins := map[string]string{} // T -> replacement callee
	f, err := os.Open(listFile)
	must(err)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		t := parts[0]
		x := strings.TrimSuffix(strings.TrimSuffix(t, "WithCancellation"), "Probed")
		if len(parts) == 2 {
			x = parts[1]
		}
		twins[t] = x
	}
	fset := token.NewFileSet()
	type file struct {
		path string
		src  []byte
		ast  *ast.File
	}
	var files []*file
	for _, dir := range dirs {
		matches, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, path := range matches {
			src, err := os.ReadFile(path)
			must(err)
			af, err := parser.ParseFile(fset, path, src, parser.ParseComments)
			must(err)
			files = append(files, &file{path, src, af})
		}
	}
	// develop's declarations, by key, with their doc comments.
	devDecl := map[string]string{}
	for _, dir := range dirs {
		matches, _ := filepath.Glob(filepath.Join("zzdev", dir, "*.go"))
		for _, path := range matches {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			src, err := os.ReadFile(path)
			must(err)
			dfs := token.NewFileSet()
			af, err := parser.ParseFile(dfs, path, src, parser.ParseComments)
			must(err)
			for _, d := range af.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok {
					start := fd.Pos()
					if fd.Doc != nil {
						start = fd.Doc.Pos()
					}
					devDecl[declKey(fd)] = string(src[dfs.Position(start).Offset:dfs.Position(fd.End()).Offset])
				}
			}
		}
	}
	// Which twin keys exist (T with its receiver).
	twinDecls := map[string]bool{}
	for _, fl := range files {
		for _, d := range fl.ast.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok {
				if _, ok := twins[fd.Name.Name]; ok {
					twinDecls[declKey(fd)] = true
				}
			}
		}
	}
	var report []string
	for _, fl := range files {
		var edits []edit
		off := func(p token.Pos) int { return fset.Position(p).Offset }
		test := strings.HasSuffix(fl.path, "_test.go")
		for _, d := range fl.ast.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			start := fd.Pos()
			if fd.Doc != nil {
				start = fd.Doc.Pos()
			}
			key := declKey(fd)
			if _, isTwin := twins[fd.Name.Name]; isTwin && !test {
				end := off(fd.End())
				for end < len(fl.src) && fl.src[end] == '\n' {
					end++
					break
				}
				edits = append(edits, edit{off(start), end, "", true})
				continue
			}
			// X: a plain form whose twin is in the list.
			for t, x := range twins {
				if strings.Contains(x, ".") || test {
					continue
				}
				if fd.Name.Name == x && twinDecls[recvPrefix(key)+t] {
					if dev, ok := devDecl[key]; ok {
						edits = append(edits, edit{off(start), off(fd.End()), dev, true})
					} else {
						report = append(report, fmt.Sprintf("NODEV\t%s\t%s", fl.path, key))
					}
				}
			}
		}
		ast.Inspect(fl.ast, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var nameID *ast.Ident
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				nameID = fn
			case *ast.SelectorExpr:
				nameID = fn.Sel
			case *ast.IndexExpr:
				if id, ok := fn.X.(*ast.Ident); ok {
					nameID = id
				}
			}
			if nameID == nil {
				return true
			}
			x, ok := twins[nameID.Name]
			if !ok {
				return true
			}
			if strings.Contains(x, ".") {
				if _, isSel := call.Fun.(*ast.SelectorExpr); isSel {
					report = append(report, fmt.Sprintf("SELCALL\t%s\t%s", fset.Position(call.Pos()), nameID.Name))
					return true
				}
			}
			edits = append(edits, edit{off(nameID.Pos()), off(nameID.End()), x, false})
			if len(call.Args) == 0 {
				return true
			}
			last := call.Args[len(call.Args)-1]
			if len(call.Args) == 1 {
				edits = append(edits, edit{off(last.Pos()), off(last.End()), "", true})
			} else {
				edits = append(edits, edit{off(call.Args[len(call.Args)-2].End()), off(last.End()), "", true})
			}
			return true
		})
		if len(edits) == 0 {
			continue
		}
		sort.Slice(edits, func(i, j int) bool {
			if edits[i].start != edits[j].start {
				return edits[i].start < edits[j].start
			}
			return edits[i].end > edits[j].end
		})
		// Drop edits inside an earlier removal or replacement.
		var kept []edit
		limit := -1
		for _, e := range edits {
			if e.start < limit {
				continue
			}
			kept = append(kept, e)
			if e.removal {
				limit = e.end
			}
		}
		var out strings.Builder
		pos := 0
		for _, e := range kept {
			if e.start < pos {
				report = append(report, fmt.Sprintf("OVERLAP\t%s\t%d", fl.path, e.start))
				continue
			}
			out.Write(fl.src[pos:e.start])
			out.WriteString(e.text)
			pos = e.end
		}
		out.Write(fl.src[pos:])
		result := out.String()
		if strings.Contains(result, "strings.") && !strings.Contains(result, "\"strings\"") {
			report = append(report, fmt.Sprintf("NEEDS-STRINGS-IMPORT\t%s", fl.path))
		}
		must(os.WriteFile(fl.path, []byte(result), 0o644))
	}
	sort.Strings(report)
	for _, r := range report {
		fmt.Fprintln(os.Stderr, r)
	}
	diff, err := exec.Command("git", "diff", "--", "pkg").Output()
	must(err)
	os.Stdout.Write(diff)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
